//! 阶段 2：相对当前 STEP 的邻档（只挂外侧加一格 + 内侧减一格）。
//!
//! 不读窗口、不下单。上层用 `target_spread` 反推第一腿限价。

use rust_decimal::Decimal;

use super::{VenueId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSide {
    /// k 增加：空 L 多 R。
    Plus,
    /// k 减少：多 L 空 R。
    Minus,
}

impl QuoteSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plus => "plus",
            Self::Minus => "minus",
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::Plus => Self::Minus,
            Self::Minus => Self::Plus,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdjacentQuote {
    pub side: QuoteSide,
    pub target_spread: Decimal,
    pub is_open: bool,
    pub buy: VenueId,
    pub sell: VenueId,
    pub qty: Decimal,
    pub grid_to: i32,
}

/// 当前 k 上至多两档。`left`/`right` = 窗口 L/R（legs[0]/legs[1]）。
pub fn adjacent_quotes(
    k: i32,
    mu: Decimal,
    delta: Decimal,
    max_step: i32,
    hysteresis: Decimal,
    left: &VenueId,
    right: &VenueId,
    base_qty: Decimal,
    held_qty: Decimal,
) -> Vec<AdjacentQuote> {
    if delta <= Decimal::ZERO || base_qty <= Decimal::ZERO {
        return Vec::new();
    }
    let n = max_step.max(1);
    let h = hysteresis.max(Decimal::ZERO);
    let mut out = Vec::with_capacity(2);
    let one = base_qty;

    if k < n {
        let m = k + 1;
        out.push(AdjacentQuote {
            side: QuoteSide::Plus,
            target_spread: mu + Decimal::from(m) * delta,
            is_open: k >= 0,
            buy: right.clone(),
            sell: left.clone(),
            qty: if k >= 0 {
                one
            } else {
                close_qty(k + 1, held_qty, one)
            },
            grid_to: k + 1,
        });
    }
    if k > -n {
        let reduce_line = if k > 0 {
            mu + Decimal::from(k - 1) * delta + h * delta
        } else if k < 0 {
            mu + Decimal::from(k - 1) * delta - h * delta
        } else {
            mu - delta
        };
        let qty = if k <= 0 {
            one
        } else {
            close_qty(k - 1, held_qty, one)
        };
        out.push(AdjacentQuote {
            side: QuoteSide::Minus,
            target_spread: reduce_line,
            is_open: k <= 0,
            buy: left.clone(),
            sell: right.clone(),
            qty,
            grid_to: k - 1,
        });
    }
    out.retain(|q| q.qty > Decimal::ZERO);
    out
}

fn close_qty(next: i32, held_qty: Decimal, one: Decimal) -> Decimal {
    if next == 0 {
        held_qty
    } else {
        one.min(held_qty)
    }
}

/// 当前可执行价差离格线够不够远（加仓档）。减仓档不走这条。
pub fn add_quote_far_enough(target: Decimal, s_exec: Decimal, plus: bool, gap: Decimal) -> bool {
    if gap <= Decimal::ZERO {
        return true;
    }
    if plus {
        s_exec <= target - gap
    } else {
        s_exec >= target + gap
    }
}

/// 用对手所盘口反推第一腿限价，使成交后可执行价差 ≈ `target`（与 `exec_spread_pct` 同一分母）。
///
/// `min_ticks`：相对本所盘口再退开的 tick 数。0 = 只禁止穿价；≥1 时买必须
/// `≤ bid − n·tick`、卖必须 `≥ ask + n·tick`，贴着盘口的单本拍不挂。
pub fn implied_first_limit(
    target: Decimal,
    first_is_left: bool,
    first_is_buy: bool,
    left_bid: Decimal,
    left_ask: Decimal,
    right_bid: Decimal,
    right_ask: Decimal,
    avg_mid: Decimal,
    tick: Decimal,
    min_ticks: u32,
) -> Option<Decimal> {
    if avg_mid <= Decimal::ZERO {
        return None;
    }
    let offset = target / Decimal::from(100) * avg_mid;
    let raw = match (first_is_left, first_is_buy) {
        (true, false) => right_ask + offset,
        (false, true) => left_bid - offset,
        (true, true) => right_bid + offset,
        (false, false) => left_ask - offset,
    };
    if raw <= Decimal::ZERO {
        return None;
    }
    let px = round_to_tick(raw, tick);
    let (bid, ask) = if first_is_left {
        (left_bid, left_ask)
    } else {
        (right_bid, right_ask)
    };
    if first_is_buy {
        if px >= ask {
            return None;
        }
        if min_ticks > 0 && tick > Decimal::ZERO {
            let cap = bid - tick * Decimal::from(min_ticks);
            if px > cap {
                return None;
            }
        }
    } else {
        if px <= bid {
            return None;
        }
        if min_ticks > 0 && tick > Decimal::ZERO {
            let floor = ask + tick * Decimal::from(min_ticks);
            if px < floor {
                return None;
            }
        }
    }
    Some(px)
}

fn round_to_tick(px: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return px;
    }
    (px / tick).round() * tick
}

/// 反推价相对已挂价是否偏过 `ticks` 个 tick。
pub fn limit_moved_ticks(old: Decimal, new: Decimal, tick: Decimal, ticks: u32) -> bool {
    if tick <= Decimal::ZERO || ticks == 0 {
        return old != new;
    }
    (new - old).abs() >= tick * Decimal::from(ticks)
}

pub fn quote_pending_key(slot: &str, side: QuoteSide) -> String {
    format!("{}|{}", slot, side.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn l() -> VenueId {
        VenueId::from("lighter")
    }
    fn r() -> VenueId {
        VenueId::from("entropy")
    }

    #[test]
    fn k0_has_plus_and_minus_at_one_delta() {
        let q = adjacent_quotes(0, dec!(0), dec!(0.02), 3, Decimal::ZERO, &l(), &r(), dec!(0.001), Decimal::ZERO);
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].side, QuoteSide::Plus);
        assert_eq!(q[0].target_spread, dec!(0.02));
        assert!(q[0].is_open);
        assert_eq!(q[0].sell.as_str(), "lighter");
        assert_eq!(q[0].buy.as_str(), "entropy");
        assert_eq!(q[0].grid_to, 1);
        assert_eq!(q[1].side, QuoteSide::Minus);
        assert_eq!(q[1].target_spread, dec!(-0.02));
        assert!(q[1].is_open);
        assert_eq!(q[1].buy.as_str(), "lighter");
        assert_eq!(q[1].grid_to, -1);
    }

    #[test]
    fn k_plus_one_adds_at_two_delta_and_reduces_at_mu() {
        let q = adjacent_quotes(1, dec!(0), dec!(0.02), 3, Decimal::ZERO, &l(), &r(), dec!(0.001), dec!(0.001));
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].target_spread, dec!(0.04));
        assert!(q[0].is_open);
        assert_eq!(q[0].grid_to, 2);
        assert_eq!(q[1].target_spread, dec!(0));
        assert!(!q[1].is_open);
        assert_eq!(q[1].grid_to, 0);
        assert_eq!(q[1].qty, dec!(0.001));
    }

    #[test]
    fn k_at_max_only_reduces() {
        let q = adjacent_quotes(3, dec!(0), dec!(0.02), 3, Decimal::ZERO, &l(), &r(), dec!(0.001), dec!(0.003));
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].side, QuoteSide::Minus);
        assert!(!q[0].is_open);
        assert_eq!(q[0].grid_to, 2);
    }

    #[test]
    fn k_minus_one_mirrors() {
        let q = adjacent_quotes(-1, dec!(0), dec!(0.02), 3, Decimal::ZERO, &l(), &r(), dec!(0.001), dec!(0.001));
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].side, QuoteSide::Plus);
        assert!(!q[0].is_open);
        assert_eq!(q[0].grid_to, 0);
        assert_eq!(q[1].side, QuoteSide::Minus);
        assert!(q[1].is_open);
        assert_eq!(q[1].grid_to, -2);
        assert_eq!(q[1].target_spread, dec!(-0.04));
    }

    #[test]
    fn k_at_min_only_reduces() {
        let q = adjacent_quotes(-3, dec!(0), dec!(0.02), 3, Decimal::ZERO, &l(), &r(), dec!(0.001), dec!(0.003));
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].side, QuoteSide::Plus);
        assert!(!q[0].is_open);
        assert_eq!(q[0].grid_to, -2);
    }

    #[test]
    fn hysteresis_shifts_reduce_line() {
        let q = adjacent_quotes(1, dec!(0), dec!(0.02), 3, dec!(0.25), &l(), &r(), dec!(0.001), dec!(0.001));
        let red = q.iter().find(|x| !x.is_open).unwrap();
        assert_eq!(red.target_spread, dec!(0.005));
    }

    #[test]
    fn far_enough_rejects_near_line() {
        assert!(add_quote_far_enough(dec!(0.02), dec!(0), true, dec!(0.006)));
        assert!(!add_quote_far_enough(dec!(0.02), dec!(0.018), true, dec!(0.006)));
        assert!(add_quote_far_enough(dec!(-0.02), dec!(0), false, dec!(0.006)));
        assert!(!add_quote_far_enough(dec!(-0.02), dec!(-0.018), false, dec!(0.006)));
    }

    #[test]
    fn implied_plus_sell_left_sits_above_right_ask() {
        let px = implied_first_limit(
            dec!(0.02),
            true,
            false,
            dec!(100),
            dec!(100.02),
            dec!(99.98),
            dec!(100),
            dec!(100),
            dec!(0.01),
            0,
        )
        .unwrap();
        assert_eq!(px, dec!(100.02));
    }

    #[test]
    fn implied_limit_rejects_touch_when_min_ticks() {
        // 卖在 left ask 上：min_ticks=0 能挂，=1 必须再退开 1 tick。
        let at_ask = implied_first_limit(
            dec!(0.02),
            true,
            false,
            dec!(100),
            dec!(100.02),
            dec!(99.98),
            dec!(100),
            dec!(100),
            dec!(0.01),
            1,
        );
        assert!(at_ask.is_none());
        let behind = implied_first_limit(
            dec!(0.05),
            true,
            false,
            dec!(100),
            dec!(100.02),
            dec!(99.98),
            dec!(100),
            dec!(100),
            dec!(0.01),
            1,
        )
        .unwrap();
        assert!(behind >= dec!(100.03));
    }

    #[test]
    fn one_tick_move_is_detected() {
        assert!(limit_moved_ticks(dec!(100), dec!(100.1), dec!(0.1), 1));
        assert!(!limit_moved_ticks(dec!(100), dec!(100.05), dec!(0.1), 1));
        assert!(!limit_moved_ticks(dec!(100), dec!(100), dec!(0.1), 1));
    }
}
