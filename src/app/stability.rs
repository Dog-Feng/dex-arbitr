//! 价格稳定性检查。对齐参考 `passes_price_stability`。
//!
//! 判据是**窗口内极值波动**：`(max − min) / min × 100`，买卖两腿各算一次，
//! 任一腿超阈值即失败。不是标准差——参考用的是极值，因为它对单次插针敏感，
//! 而插针正是先挂后吃最怕的东西（挂单成交在插针价上，对冲腿回不来）。
//!
//! 三个状态：
//! - `Collecting`：窗口还没攒满，先不放行。
//! - `Volatile`：超阈值，**清空历史重新计时**（对齐参考 `reset_price_history`）。
//!   这是刻意的惩罚——不清空的话，价格一稳下来就会立刻放行，
//!   而此时窗口里还留着插针的尾巴。
//! - `Ok`：放行。
//!
//! 开仓和平仓各自独立记状态（key = `slot:action`），因为参考项目里
//! 两条路径的日志和节流是分开的，混在一起会互相覆盖。

use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::Instant;

/// 一次采样：某时刻两腿的参考价。
#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    buy: Decimal,
    sell: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stability {
    /// 窗口未攒满。`covered_secs` / `window_secs` 用于日志。
    Collecting,
    /// 超阈值，已重新计时。
    Volatile,
    Ok,
}

impl Stability {
    pub fn passed(self) -> bool {
        matches!(self, Self::Ok)
    }

    /// 跳过原因标签，喂给面板的 skip 统计。
    pub fn skip_label(self) -> &'static str {
        match self {
            Self::Collecting => "stability_collecting",
            Self::Volatile => "stability_volatile",
            Self::Ok => "ok",
        }
    }
}

/// 检查动作。开仓/平仓独立记状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Open,
    Close,
}

impl Action {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Close => "close",
        }
    }
}

#[derive(Debug, Default)]
pub struct StabilityTracker {
    /// key = slot（币 + 所对）。采样按 slot 共享，两个 action 都读它。
    history: HashMap<String, Vec<Sample>>,
    /// key = `slot:action`。只用于「状态变化才打日志」，不参与判定。
    last: HashMap<String, Stability>,
}

impl StabilityTracker {
    /// 记一次采样。**必须每轮都调**，无论后面是否检查——历史断了窗口就攒不满。
    pub fn record(&mut self, slot: &str, buy: Decimal, sell: Decimal, now: Instant, window_secs: Decimal) {
        if buy <= Decimal::ZERO || sell <= Decimal::ZERO {
            return;
        }
        let hist = self.history.entry(slot.to_string()).or_default();
        hist.push(Sample { at: now, buy, sell });
        // 只保留窗口两倍的历史：判定只看窗口内，多留一倍是为了
        // 让 `covered` 能算出真实覆盖时长而不被裁剪影响。
        let keep = secs_to_duration(window_secs).saturating_mul(2);
        let cutoff = now.checked_sub(keep);
        if let Some(cutoff) = cutoff {
            hist.retain(|s| s.at >= cutoff);
        }
    }

    /// 判定。`window_secs` 或 `threshold_pct` 为 0 时直接放行（关闭检查）。
    pub fn check(
        &mut self,
        slot: &str,
        action: Action,
        now: Instant,
        window_secs: Decimal,
        threshold_pct: Decimal,
    ) -> Stability {
        let key = format!("{slot}:{}", action.as_str());
        if window_secs <= Decimal::ZERO || threshold_pct <= Decimal::ZERO {
            self.last.remove(&key);
            return Stability::Ok;
        }
        let Some(hist) = self.history.get(slot).filter(|h| !h.is_empty()) else {
            self.last.insert(key, Stability::Collecting);
            return Stability::Collecting;
        };

        let window = secs_to_duration(window_secs);
        // 覆盖时长按**最早一条**算，对齐参考 `coverage = now - history[0][0]`。
        let covered = now.saturating_duration_since(hist[0].at);
        if covered < window {
            self.last.insert(key, Stability::Collecting);
            return Stability::Collecting;
        }

        // 取窗口内的样本；全被裁掉时退到最后一条（对齐参考 `or history[-1:]`）。
        let cutoff = now.checked_sub(window);
        let relevant: Vec<&Sample> = match cutoff {
            Some(c) => {
                let v: Vec<&Sample> = hist.iter().filter(|s| s.at >= c).collect();
                if v.is_empty() {
                    hist.last().into_iter().collect()
                } else {
                    v
                }
            }
            None => hist.iter().collect(),
        };

        let vol_buy = volatility_pct(relevant.iter().map(|s| s.buy));
        let vol_sell = volatility_pct(relevant.iter().map(|s| s.sell));
        if vol_buy > threshold_pct || vol_sell > threshold_pct {
            // 重新计时：清空该 slot 的全部历史。
            self.history.remove(slot);
            self.last.insert(key, Stability::Volatile);
            return Stability::Volatile;
        }
        self.last.insert(key, Stability::Ok);
        Stability::Ok
    }

    /// 上一次的判定结果，用于「只在状态变化时打日志」。
    pub fn changed(&self, slot: &str, action: Action, now_state: Stability) -> bool {
        let key = format!("{slot}:{}", action.as_str());
        self.last.get(&key) != Some(&now_state)
    }

    /// 窗口内波动（%），买卖两腿的较大者。仅用于日志展示。
    pub fn volatility(&self, slot: &str, now: Instant, window_secs: Decimal) -> Option<Decimal> {
        let hist = self.history.get(slot)?;
        if hist.is_empty() {
            return None;
        }
        let cutoff = now.checked_sub(secs_to_duration(window_secs));
        let relevant: Vec<&Sample> = match cutoff {
            Some(c) => hist.iter().filter(|s| s.at >= c).collect(),
            None => hist.iter().collect(),
        };
        if relevant.is_empty() {
            return None;
        }
        let b = volatility_pct(relevant.iter().map(|s| s.buy));
        let s = volatility_pct(relevant.iter().map(|s| s.sell));
        Some(b.max(s))
    }

    /// 仓位平掉 / 该 slot 不再关注时清理，避免 map 无界增长。
    pub fn forget(&mut self, slot: &str) {
        self.history.remove(slot);
        self.last.retain(|k, _| !k.starts_with(&format!("{slot}:")));
    }
}

/// `(max − min) / min × 100`。对齐参考 `_calculate_volatility_percent`。
fn volatility_pct(vals: impl Iterator<Item = Decimal>) -> Decimal {
    let mut min = None::<Decimal>;
    let mut max = None::<Decimal>;
    for v in vals {
        min = Some(min.map_or(v, |m: Decimal| m.min(v)));
        max = Some(max.map_or(v, |m: Decimal| m.max(v)));
    }
    match (min, max) {
        (Some(min), Some(max)) if min > Decimal::ZERO => {
            (max - min) / min * Decimal::from(100)
        }
        _ => Decimal::ZERO,
    }
}

fn secs_to_duration(secs: Decimal) -> std::time::Duration {
    let ms = (secs * Decimal::from(1000)).round();
    std::time::Duration::from_millis(ms.try_into().unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::time::Duration;

    /// 3 秒窗口 / 0.05% 阈值，与默认配置一致。
    fn win() -> Decimal {
        dec!(3)
    }
    fn thr() -> Decimal {
        dec!(0.05)
    }

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    /// 窗口没攒满 -> Collecting，不放行。
    #[test]
    fn collecting_until_window_covered() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        t.record("s", dec!(100), dec!(100), t0, win());
        assert_eq!(t.check("s", Action::Open, t0, win(), thr()), Stability::Collecting);

        // 2.9s < 3s 窗口
        let mid = at(t0, 2900);
        t.record("s", dec!(100), dec!(100), mid, win());
        assert_eq!(t.check("s", Action::Open, mid, win(), thr()), Stability::Collecting);
    }

    /// 窗口攒满且平稳 -> Ok。
    #[test]
    fn passes_when_stable_over_window() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        for ms in [0, 1000, 2000, 3100] {
            t.record("s", dec!(100), dec!(200), at(t0, ms), win());
        }
        let now = at(t0, 3100);
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Ok);
    }

    /// 超阈值 -> Volatile，且历史被清空（重新计时）。
    #[test]
    fn volatile_resets_history() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        t.record("s", dec!(100), dec!(200), t0, win());
        t.record("s", dec!(100), dec!(200), at(t0, 1500), win());
        // 0.1% 波动 > 0.05% 阈值
        t.record("s", dec!(100.1), dec!(200), at(t0, 3100), win());

        let now = at(t0, 3100);
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Volatile);
        // 清空后立刻再查 -> 回到 Collecting，而不是又一次 Volatile
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Collecting);
    }

    /// 卖腿单独插针也要拦住（两腿分别判）。
    /// 采样点要落在窗口内：判定只看 `now − window` 之后的样本。
    #[test]
    fn sell_leg_spike_also_fails() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        for ms in [0, 500, 1500, 2500] {
            t.record("s", dec!(100), dec!(200), at(t0, ms), win());
        }
        t.record("s", dec!(100), dec!(200.5), at(t0, 3100), win());
        let now = at(t0, 3100);
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Volatile);
    }

    /// 边界：窗口覆盖用**最早**样本判断，但波动只算**窗口内**样本。
    /// 采样过稀时落在窗口外的旧样本不参与判定，所以 `record` 必须每轮都调。
    /// 与参考项目一致（`coverage` 看 `history[0]`，`relevant` 按 cutoff 过滤）。
    #[test]
    fn sparse_sampling_only_judges_in_window() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        t.record("s", dec!(100), dec!(200), t0, win());
        t.record("s", dec!(100), dec!(200.5), at(t0, 3100), win());
        let now = at(t0, 3100);
        // 窗口内只剩一条样本，无从对比 -> 放行。记录为已知行为。
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Ok);
    }

    /// 阈值或窗口为 0 -> 关闭检查，直接放行。
    #[test]
    fn zero_config_disables_check() {
        let mut t = StabilityTracker::default();
        let now = Instant::now();
        assert_eq!(
            t.check("s", Action::Open, now, Decimal::ZERO, thr()),
            Stability::Ok
        );
        assert_eq!(
            t.check("s", Action::Open, now, win(), Decimal::ZERO),
            Stability::Ok
        );
    }

    /// 开仓和平仓状态互不覆盖。
    #[test]
    fn actions_track_state_independently() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        for ms in [0, 3100] {
            t.record("s", dec!(100), dec!(200), at(t0, ms), win());
        }
        let now = at(t0, 3100);
        assert_eq!(t.check("s", Action::Open, now, win(), thr()), Stability::Ok);
        // Close 首次查，状态独立记录
        assert!(t.changed("s", Action::Close, Stability::Ok));
        assert!(!t.changed("s", Action::Open, Stability::Ok));
    }

    #[test]
    fn volatility_uses_extremes_not_stddev() {
        // 极值判据：一次插针就该体现出来
        let vals = [dec!(100), dec!(100), dec!(100), dec!(101)];
        assert_eq!(volatility_pct(vals.into_iter()), Decimal::from(1));
    }

    #[test]
    fn forget_clears_slot() {
        let mut t = StabilityTracker::default();
        let t0 = Instant::now();
        t.record("s", dec!(100), dec!(200), t0, win());
        t.check("s", Action::Open, t0, win(), thr());
        t.forget("s");
        assert!(t.history.is_empty());
        assert!(t.last.is_empty());
    }
}
