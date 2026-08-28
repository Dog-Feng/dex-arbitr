//! 人工介入状态机。对齐参考 `SymbolStateManager` + `_mark_manual_intervention`。
//!
//! 与 [`super::reduce_only`] 的区别：那个是**交易所明确说不能开仓**，
//! 仓位状态是已知的；这个是**我们不知道自己的真实仓位**——第一腿成交、
//! 第二腿失败、紧急平仓也失败，或者撤单撤不掉。继续按内存里的仓位交易
//! 等于在错误的基础上叠加错误。
//!
//! 四个对齐参考的设计选择：
//!
//! - **按 pair 隔离**（key = pair_id，不是 slot）。裸腿留在某个所上，
//!   同一个币换个所对照样会碰到那条腿。和 `reduce_only` 同样的理由。
//! - **开仓和平仓都挡**（参考 `should_block` 在 `unified_orchestrator.py`
//!   开仓 1442 和平仓 1837 两处都查）。这跟 reduce-only 熔断**相反**，
//!   是刻意的：reduce-only 时仓位已知，挡平仓等于锁死仓位；这里仓位本身
//!   不可信，按错的量平会把敞口放大。代价是仓位会一直挂着，所以必须有
//!   下面的自动解除兜底。
//! - **30 分钟自动解除**（参考 `MANUAL_INTERVENTION_AUTO_RESUME_SECONDS`）。
//!   人没来也不能永久锁死。
//! - **纯内存**：重启即清空，和参考一致。重启会走一遍完整对账，
//!   真有问题会再次触发。
//!
//! 还有一条参考里容易漏掉的规则：**连续 3 次单腿成交，即使每次都补单成功，
//! 也要挂起**（参考 `lighter_batch_executor.py:424-431`）。单次补上说明
//! 这次运气好，连续三次说明这条链路有系统性问题。

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// 参考 `MANUAL_INTERVENTION_AUTO_RESUME_SECONDS = 1800`。
pub const AUTO_RESUME: Duration = Duration::from_secs(1800);
/// 参考：连续单腿达到 3 次就挂起，哪怕每次都补单成功。
pub const SINGLE_LEG_STREAK_LIMIT: u32 = 3;

/// 挂起原因。参考靠字符串前缀 `需人工介入` 区分，这里用枚举避免拼错。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// 第一腿成交、第二腿失败、紧急平仓也失败 → 裸腿且平不掉。
    NakedLegUnrecoverable,
    /// 撤单失败，挂单状态不明，可能稍后成交。
    OrphanOrder,
    /// 紧急平仓失败（最小下单量等原因）。
    EmergencyCloseFailed,
    /// 连续单腿成交超过阈值，即使补单成功。
    SingleLegStreak,
    /// 裸仓量低于对手所最小下单量，自动补不了。
    NakedBelowMinQty,
    /// 第二腿状态不明（sidecar 报 unknown）：可能成交也可能没成交。
    /// **不能**紧急平第一腿——万一第二腿其实成交了，反手平仓会留下
    /// 一条系统毫不知情的反向裸仓，比单腿敞口更难收拾。
    SecondLegUnknown,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NakedLegUnrecoverable => "naked_leg_unrecoverable",
            Self::OrphanOrder => "orphan_order",
            Self::EmergencyCloseFailed => "emergency_close_failed",
            Self::SingleLegStreak => "single_leg_streak",
            Self::NakedBelowMinQty => "naked_below_min_qty",
            Self::SecondLegUnknown => "second_leg_unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Waiting {
    pub cause: Cause,
    /// 人读的补充说明（哪条腿、多少量），写 journal 和面板用。
    pub detail: String,
    pub since: Instant,
    /// 挂起时的格数。参考 `should_block`：格数变了就自动恢复，
    /// 因为那说明行情已经走到另一个区间，旧的判断不再适用。
    pub grid_level: Option<i32>,
}

/// 一次门禁查询的结果。`should_block` 需要 `&mut self`（可能触发自动解除），
/// 所以返回自有值而不是引用。
#[derive(Debug, Clone)]
pub enum Gate {
    Allow,
    /// 本次调用触发了自动解除，调用方负责打日志。
    Resumed(String),
    Block {
        cause: Cause,
        detail: String,
        waited: Duration,
    },
}

impl Gate {
    pub fn blocked(&self) -> bool {
        matches!(self, Self::Block { .. })
    }
}

#[derive(Debug, Default)]
pub struct InterventionGuard {
    /// key = pair_id（币）。
    pairs: HashMap<String, Waiting>,
    /// 连续单腿计数，key = pair_id。补单成功也累加，只有整轮干净成交才清零。
    streak: HashMap<String, u32>,
}

impl InterventionGuard {
    /// 挂起。已挂起的 pair 只更新原因，**不重置 `since`**——否则反复报错
    /// 会把 30 分钟自动解除无限推后，等于永久锁死。
    pub fn mark(
        &mut self,
        pair_id: &str,
        cause: Cause,
        detail: impl Into<String>,
        grid_level: Option<i32>,
        now: Instant,
    ) -> bool {
        let detail = detail.into();
        match self.pairs.get_mut(pair_id) {
            Some(st) => {
                st.cause = cause;
                st.detail = detail;
                false
            }
            None => {
                self.pairs.insert(
                    pair_id.to_string(),
                    Waiting {
                        cause,
                        detail,
                        since: now,
                        grid_level,
                    },
                );
                true
            }
        }
    }

    /// 门禁。开仓和平仓都要查（对齐参考两处调用点）。
    ///
    /// `current_grid` 传当前格数；`None` 表示算不出来（空仓或 base_qty 为 0），
    /// 此时不走格数解除，只靠超时。
    pub fn should_block(
        &mut self,
        pair_id: &str,
        current_grid: Option<i32>,
        now: Instant,
    ) -> Gate {
        let Some(st) = self.pairs.get(pair_id) else {
            return Gate::Allow;
        };
        let waited = now.saturating_duration_since(st.since);
        if waited >= AUTO_RESUME {
            let mins = AUTO_RESUME.as_secs() / 60;
            self.resume(pair_id);
            return Gate::Resumed(format!("auto_resume after {mins}min"));
        }
        // 格数变化 → 行情已换区间，旧判断作废（参考 `current_grid != state.grid_level`）。
        if let (Some(cur), Some(old)) = (current_grid, st.grid_level) {
            if cur != old {
                self.resume(pair_id);
                return Gate::Resumed(format!("grid level changed {old} -> {cur}"));
            }
        }
        Gate::Block {
            cause: st.cause,
            detail: st.detail.clone(),
            waited,
        }
    }

    /// 手动解除（HTTP / 运维）。返回是否真的清掉了东西。
    pub fn resume(&mut self, pair_id: &str) -> bool {
        self.streak.remove(pair_id);
        self.pairs.remove(pair_id).is_some()
    }

    pub fn state(&self, pair_id: &str) -> Option<&Waiting> {
        self.pairs.get(pair_id)
    }

    pub fn is_waiting(&self, pair_id: &str) -> bool {
        self.pairs.contains_key(pair_id)
    }

    pub fn waiting_pairs(&self) -> Vec<String> {
        let mut v: Vec<String> = self.pairs.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// 记一次单腿成交。返回累计次数；达到阈值时调用方应挂起。
    /// 参考：补单成功也要计数。
    pub fn note_single_leg(&mut self, pair_id: &str) -> u32 {
        let n = self.streak.entry(pair_id.to_string()).or_insert(0);
        *n += 1;
        *n
    }

    /// 整轮双腿都干净成交 → 清零连续计数。
    pub fn clear_streak(&mut self, pair_id: &str) {
        self.streak.remove(pair_id);
    }

    pub fn streak(&self, pair_id: &str) -> u32 {
        self.streak.get(pair_id).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g() -> InterventionGuard {
        InterventionGuard::default()
    }

    /// 挂起后开仓和平仓都被挡——与 reduce-only 熔断相反，这是刻意的。
    #[test]
    fn blocks_both_directions() {
        let mut gd = g();
        let t = Instant::now();
        assert!(!gd.should_block("BTC", Some(1), t).blocked());
        gd.mark("BTC", Cause::NakedLegUnrecoverable, "sodex leg 0.5", Some(1), t);
        // 同一个 pair、同一格数：两次查询（开仓路径 + 平仓路径）都要挡。
        assert!(gd.should_block("BTC", Some(1), t).blocked());
        assert!(gd.should_block("BTC", Some(1), t).blocked());
    }

    /// 按 pair 隔离，不牵连其他币。
    #[test]
    fn isolates_other_pairs() {
        let mut gd = g();
        let t = Instant::now();
        gd.mark("BTC", Cause::OrphanOrder, "id=1", Some(1), t);
        assert!(gd.should_block("BTC", Some(1), t).blocked());
        assert!(!gd.should_block("ETH", Some(1), t).blocked());
    }

    /// 30 分钟自动解除。
    #[test]
    fn auto_resumes_after_timeout() {
        let mut gd = g();
        let t = Instant::now();
        gd.mark("BTC", Cause::EmergencyCloseFailed, "d", Some(2), t);
        assert!(gd.should_block("BTC", Some(2), t + Duration::from_secs(1799)).blocked());
        let gate = gd.should_block("BTC", Some(2), t + AUTO_RESUME);
        assert!(matches!(gate, Gate::Resumed(_)));
        // 解除后彻底放行
        assert!(!gd.is_waiting("BTC"));
        assert!(!gd.should_block("BTC", Some(2), t + AUTO_RESUME).blocked());
    }

    /// 格数变化 → 自动解除（参考 `current_grid != state.grid_level`）。
    #[test]
    fn auto_resumes_on_grid_change() {
        let mut gd = g();
        let t = Instant::now();
        gd.mark("BTC", Cause::OrphanOrder, "d", Some(1), t);
        assert!(gd.should_block("BTC", Some(1), t).blocked());
        let gate = gd.should_block("BTC", Some(3), t);
        assert!(matches!(gate, Gate::Resumed(_)));
        assert!(!gd.is_waiting("BTC"));
    }

    /// 格数算不出来（None）时不能凭空解除，只能靠超时。
    #[test]
    fn unknown_grid_does_not_resume() {
        let mut gd = g();
        let t = Instant::now();
        gd.mark("BTC", Cause::OrphanOrder, "d", Some(1), t);
        assert!(gd.should_block("BTC", None, t).blocked());
        // 挂起时格数本身未知，也不该被任意当前格数解除
        let mut gd2 = g();
        gd2.mark("ETH", Cause::OrphanOrder, "d", None, t);
        assert!(gd2.should_block("ETH", Some(5), t).blocked());
    }

    /// 反复报错不能重置计时，否则 30 分钟兜底会被无限推后。
    #[test]
    fn repeated_marks_do_not_extend_the_wait() {
        let mut gd = g();
        let t = Instant::now();
        assert!(gd.mark("BTC", Cause::OrphanOrder, "first", Some(1), t));
        // 29 分钟后又报一次错
        let later = t + Duration::from_secs(1740);
        assert!(!gd.mark("BTC", Cause::NakedLegUnrecoverable, "second", Some(1), later));
        let st = gd.state("BTC").unwrap();
        assert_eq!(st.since, t, "since must not be pushed forward");
        // 原因更新了，但计时仍按第一次算，所以 30 分钟整点仍然解除
        assert_eq!(st.cause, Cause::NakedLegUnrecoverable);
        assert_eq!(st.detail, "second");
        assert!(matches!(
            gd.should_block("BTC", Some(1), t + AUTO_RESUME),
            Gate::Resumed(_)
        ));
    }

    /// 连续 3 次单腿即使每次补单成功也要挂起。
    #[test]
    fn single_leg_streak_reaches_limit() {
        let mut gd = g();
        assert_eq!(gd.note_single_leg("BTC"), 1);
        assert_eq!(gd.note_single_leg("BTC"), 2);
        assert_eq!(gd.note_single_leg("BTC"), SINGLE_LEG_STREAK_LIMIT);
        // 干净成交清零
        gd.clear_streak("BTC");
        assert_eq!(gd.streak("BTC"), 0);
    }

    /// 连续计数按 pair 独立。
    #[test]
    fn streak_is_per_pair() {
        let mut gd = g();
        gd.note_single_leg("BTC");
        gd.note_single_leg("BTC");
        assert_eq!(gd.streak("BTC"), 2);
        assert_eq!(gd.streak("ETH"), 0);
    }

    /// 手动解除同时清掉连续计数，否则解除后一次单腿就又挂起。
    #[test]
    fn manual_resume_clears_streak() {
        let mut gd = g();
        let t = Instant::now();
        gd.note_single_leg("BTC");
        gd.note_single_leg("BTC");
        gd.mark("BTC", Cause::SingleLegStreak, "3x", Some(1), t);
        assert!(gd.resume("BTC"));
        assert_eq!(gd.streak("BTC"), 0);
        assert!(!gd.is_waiting("BTC"));
        // 解除不存在的 pair 是 no-op
        assert!(!gd.resume("BTC"));
    }

    #[test]
    fn waiting_pairs_sorted() {
        let mut gd = g();
        let t = Instant::now();
        gd.mark("SOL", Cause::OrphanOrder, "d", None, t);
        gd.mark("BTC", Cause::OrphanOrder, "d", None, t);
        assert_eq!(gd.waiting_pairs(), vec!["BTC", "SOL"]);
        assert!(!gd.is_empty());
    }
}
