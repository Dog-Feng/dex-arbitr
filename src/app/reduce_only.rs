//! reduce-only 熔断。对齐参考 `ReduceOnlyGuard` + `ReduceOnlyHandler`。
//!
//! 触发源不是我们自己的失败计数，而是**交易所明确告知**该市场只能减仓：
//! 回报里出现 `invalid reduce only mode` 或 `code=21740`。此时继续发开仓单
//! 只会一直被拒，所以按 pair 立刻拉闸。
//!
//! 三个刻意的设计选择，都跟参考一致：
//! - **二值，不计数**：第一次报错就拉闸。这不是「可能是偶发」的失败，
//!   是交易所对账户状态的判定，重试不会变好。
//! - **只挡开仓，放行平仓**：拉闸期间仓位还在，平仓和止损必须能走——
//!   挡掉平仓等于把仓位锁死，比开不了仓危险得多。
//! - **纯内存**：重启即清空。参考也是这样，因为重启后第一次开仓尝试
//!   会立刻重新触发拉闸，不需要持久化。
//!
//! 解除靠探针：每小时 `HH:00:05` 发一笔最小量市价开仓 + 立即 reduce-only
//! 平掉，能开就说明恢复了。探针本身由 `probe` 模块驱动，这里只管状态。

use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// 交易所是否在说「这个市场只能减仓」。对齐参考 `is_reduce_only_error`。
pub fn is_reduce_only_error(msg: &str) -> bool {
    let low = msg.to_lowercase();
    low.contains("invalid reduce only mode")
        || low.contains("21740")
        || low.contains("reduce only order would increase")
        || low.contains("reduce only order")
}

/// `HH:00:{probe_second}` 起 60 秒内算探测窗口。
///
/// 参考写的是 Asia/Shanghai，但整点在任何**整小时偏移**的时区都是同一个瞬间，
/// 所以直接用 Unix 秒算，不需要引时区库。
///
/// 给 60 秒宽容窗口而不是卡单秒：决策环被执行任务占住时可能整秒跳过，
/// 卡死在第 5 秒会让探针整小时不触发。配合 `probed_hour` 去重，
/// 窗口内多轮也只探一次。
pub fn probe_due(unix_secs: i64, probe_second: u32, probed_hour: Option<i64>) -> bool {
    let hour = unix_secs.div_euclid(3600);
    if probed_hour == Some(hour) {
        return false;
    }
    let into_hour = unix_secs.rem_euclid(3600);
    let start = i64::from(probe_second);
    into_hour >= start && into_hour < start + 60
}

#[derive(Debug, Clone)]
pub struct PairState {
    /// 挡开仓。
    pub blocked: bool,
    /// 连平仓都被挡——极少见，只有交易所连 reduce-only 单都拒才置位。
    pub closing_blocked: bool,
    pub first_detected: Instant,
    pub last_error: Option<String>,
    /// 报错的那条腿（venue）。探针优先探它。
    pub failed_venues: HashSet<String>,
    pub last_probe: Option<Instant>,
    /// 已探过的「小时序号」（Unix 秒 / 3600）。用挂钟而非 Instant：
    /// 触发条件是 `HH:00:05` 这个墙上时刻，Instant 表达不了。
    /// 同一小时只探一次，避免决策环在那一秒内跑多轮时重复下单。
    pub probed_hour: Option<i64>,
}

#[derive(Debug, Default)]
pub struct ReduceOnlyGuard {
    /// key = pair_id（币），不是 slot：交易所的限制是账户+市场级别的，
    /// 换个所对同一个币照样被拒。
    pairs: HashMap<String, PairState>,
}

impl ReduceOnlyGuard {
    /// 记一次 reduce-only 报错。已拉闸的 pair 只累加信息，不重置计时。
    pub fn mark(
        &mut self,
        pair_id: &str,
        venue: &str,
        reason: &str,
        closing_blocked: bool,
        now: Instant,
    ) {
        let st = self.pairs.entry(pair_id.to_string()).or_insert(PairState {
            blocked: true,
            closing_blocked: false,
            first_detected: now,
            last_error: None,
            failed_venues: HashSet::new(),
            last_probe: None,
            probed_hour: None,
        });
        st.blocked = true;
        // 单向置位：一旦发现平仓也被挡，后续普通报错不能把它降级。
        st.closing_blocked = st.closing_blocked || closing_blocked;
        st.last_error = Some(reason.to_string());
        if !venue.is_empty() {
            st.failed_venues.insert(venue.to_string());
        }
    }

    pub fn is_blocked(&self, pair_id: &str) -> bool {
        self.pairs.get(pair_id).is_some_and(|s| s.blocked)
    }

    pub fn is_closing_blocked(&self, pair_id: &str) -> bool {
        self.pairs.get(pair_id).is_some_and(|s| s.closing_blocked)
    }

    pub fn state(&self, pair_id: &str) -> Option<&PairState> {
        self.pairs.get(pair_id)
    }

    pub fn clear(&mut self, pair_id: &str) {
        self.pairs.remove(pair_id);
    }

    pub fn blocked_pairs(&self) -> Vec<String> {
        let mut v: Vec<String> = self.pairs.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// 该 pair 这个小时是否已经探过。
    pub fn probed_hour(&self, pair_id: &str) -> Option<i64> {
        self.pairs.get(pair_id).and_then(|s| s.probed_hour)
    }

    /// 记下「这个小时已经探过」，无论成败。防止同一秒内决策环跑多轮时重复下单。
    pub fn mark_probe_hour(&mut self, pair_id: &str, hour: i64) {
        if let Some(st) = self.pairs.get_mut(pair_id) {
            st.probed_hour = Some(hour);
        }
    }

    /// 探针结果。成功 → 解除；失败 → 只记时间和失败腿，保持拉闸。
    /// 对齐参考 `record_probe_result`。
    pub fn record_probe(&mut self, pair_id: &str, venue: &str, success: bool, now: Instant) {
        let Some(st) = self.pairs.get_mut(pair_id) else {
            return;
        };
        st.last_probe = Some(now);
        if success {
            self.pairs.remove(pair_id);
        } else if !venue.is_empty() {
            st.failed_venues.insert(venue.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn detects_reference_error_shapes() {
        assert!(is_reduce_only_error("invalid reduce only mode"));
        assert!(is_reduce_only_error(
            "place failed: Invalid Reduce Only Mode"
        ));
        assert!(is_reduce_only_error("rejected code=21740 msg=..."));
        assert!(is_reduce_only_error("{\"code\":21740}"));
        // 不能误伤普通失败
        assert!(is_reduce_only_error(
            "Reduce only order would increase position"
        ));
        // 不能误伤普通失败
        assert!(!is_reduce_only_error("limit_zero_fill: first leg no fill"));
        assert!(!is_reduce_only_error("insufficient margin"));
        assert!(!is_reduce_only_error("reduce_only: true"));
    }

    /// 探测窗口：HH:00:05 起 60 秒内。
    #[test]
    fn probe_window_covers_top_of_hour() {
        // 1700000000 = 2023-11-14 22:13:20 UTC，先对到整点
        let hour_start = 1_700_000_000i64 / 3600 * 3600;
        assert!(!probe_due(hour_start, 5, None)); // HH:00:00 还没到
        assert!(probe_due(hour_start + 5, 5, None)); // HH:00:05 正好
        assert!(probe_due(hour_start + 30, 5, None)); // 窗口内（决策环卡顿也能补上）
        assert!(probe_due(hour_start + 64, 5, None)); // 末尾
        assert!(!probe_due(hour_start + 65, 5, None)); // 出窗
        assert!(!probe_due(hour_start + 1800, 5, None)); // 半点
    }

    /// 同一小时只探一次，窗口内跑多轮不重复下单。
    #[test]
    fn probe_dedupes_within_same_hour() {
        let hour_start = 1_700_000_000i64 / 3600 * 3600;
        let hour = hour_start / 3600;
        assert!(probe_due(hour_start + 5, 5, None));
        assert!(!probe_due(hour_start + 6, 5, Some(hour)));
        assert!(!probe_due(hour_start + 60, 5, Some(hour)));
        // 下一个小时重新放行
        assert!(probe_due(hour_start + 3605, 5, Some(hour)));
    }

    /// 探过之后 mark_probe_hour 生效；成功解除后状态整条消失。
    #[test]
    fn probe_hour_tracked_per_pair() {
        let mut g = ReduceOnlyGuard::default();
        g.mark("BTC", "sodex", "code=21740", false, t0());
        assert_eq!(g.probed_hour("BTC"), None);
        g.mark_probe_hour("BTC", 42);
        assert_eq!(g.probed_hour("BTC"), Some(42));
        // 探针失败：保持拉闸，小时标记留着
        g.record_probe("BTC", "sodex", false, t0());
        assert!(g.is_blocked("BTC"));
        assert_eq!(g.probed_hour("BTC"), Some(42));
        // 探针成功：整条清掉
        g.record_probe("BTC", "sodex", true, t0());
        assert!(!g.is_blocked("BTC"));
        assert_eq!(g.probed_hour("BTC"), None);
    }

    /// 第一次报错就拉闸，没有计数门槛。
    #[test]
    fn blocks_on_first_error() {
        let mut g = ReduceOnlyGuard::default();
        assert!(!g.is_blocked("BTC"));
        g.mark("BTC", "sodex", "invalid reduce only mode", false, t0());
        assert!(g.is_blocked("BTC"));
        // 平仓不受影响
        assert!(!g.is_closing_blocked("BTC"));
    }

    /// 拉闸是 pair 级别，不牵连其他币。
    #[test]
    fn isolates_other_pairs() {
        let mut g = ReduceOnlyGuard::default();
        g.mark("BTC", "sodex", "code=21740", false, t0());
        assert!(g.is_blocked("BTC"));
        assert!(!g.is_blocked("ETH"));
    }

    /// closing_blocked 单向置位，不会被后续普通报错降级。
    #[test]
    fn closing_blocked_never_downgrades() {
        let mut g = ReduceOnlyGuard::default();
        let t = t0();
        g.mark("BTC", "sodex", "e1", true, t);
        assert!(g.is_closing_blocked("BTC"));
        g.mark("BTC", "lighter", "e2", false, t);
        assert!(g.is_closing_blocked("BTC"));
    }

    /// 重复报错不重置 first_detected，并累积失败腿。
    #[test]
    fn accumulates_without_resetting_clock() {
        let mut g = ReduceOnlyGuard::default();
        let t = t0();
        g.mark("BTC", "sodex", "e1", false, t);
        let first = g.state("BTC").unwrap().first_detected;
        g.mark("BTC", "lighter", "e2", false, t + std::time::Duration::from_secs(30));
        let st = g.state("BTC").unwrap();
        assert_eq!(st.first_detected, first);
        assert_eq!(st.failed_venues.len(), 2);
        assert_eq!(st.last_error.as_deref(), Some("e2"));
    }

    /// 探针成功 → 自动解除。
    #[test]
    fn successful_probe_clears() {
        let mut g = ReduceOnlyGuard::default();
        let t = t0();
        g.mark("BTC", "sodex", "e", false, t);
        g.record_probe("BTC", "sodex", true, t);
        assert!(!g.is_blocked("BTC"));
        assert!(g.is_empty());
    }

    /// 探针失败 → 保持拉闸，记下探测时间。
    #[test]
    fn failed_probe_keeps_block() {
        let mut g = ReduceOnlyGuard::default();
        let t = t0();
        g.mark("BTC", "sodex", "e", false, t);
        g.record_probe("BTC", "sodex", false, t);
        assert!(g.is_blocked("BTC"));
        assert!(g.state("BTC").unwrap().last_probe.is_some());
    }

    /// 没拉闸的 pair 收到探针结果不应凭空建状态。
    #[test]
    fn probe_on_unblocked_pair_is_noop() {
        let mut g = ReduceOnlyGuard::default();
        g.record_probe("BTC", "sodex", false, t0());
        assert!(g.is_empty());
    }
}
