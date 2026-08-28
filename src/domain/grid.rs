use rust_decimal::Decimal;
use std::time::Duration;

use super::VenueId;

/// 目标净利（bp）+ 四腿市价费（%）+ 两所点差中枢平均（%）+ 滞后 → 格距 Δ（%）。
///
/// 1 bp = 0.01%。`target_bp` 是扣完 \(F+C\) 之后要剩的净利。
/// \(h=0.25\) 时一格只锁 0.5Δ，故 Δ = (目标% + F + C) / (1−2h)。
/// `h ≥ 0.5` 时死区吃掉整格，退化为 Δ = 目标% + F + C。
pub fn grid_step_from_target_bp(
    target_bp: Decimal,
    round_trip_taker_pct: Decimal,
    round_trip_spread_pct: Decimal,
    hysteresis: Decimal,
) -> Decimal {
    let target_pct = target_bp / Decimal::from(100);
    let cost = round_trip_taker_pct.max(Decimal::ZERO) + round_trip_spread_pct.max(Decimal::ZERO);
    let need = target_pct.max(Decimal::ZERO) + cost;
    let span = Decimal::ONE - hysteresis.max(Decimal::ZERO) * Decimal::from(2);
    let step = if span <= Decimal::ZERO {
        need
    } else {
        need / span
    };
    step.max(cost)
}

/// 滑动窗口下单参数。格距是相对 μ 的 \(\Delta\)，不是绝对 T1。
#[derive(Debug, Clone)]
pub struct GridParams {
    pub step: Decimal,
    pub max_segments: u32,
    pub persistence: Duration,
    /// `persistence_ms` 内累计达标次数。0 = 连续不掉线。
    pub persistence_min_hits: u32,
    /// 单格数量。目标持仓 = |STEP| × base_qty。
    pub base_qty: Decimal,
    /// 单笔最大开/平仓量（拆单）。0 = 不拆。
    pub split_order_size: Decimal,
    /// 两腿 min_qty 的较大者。拆单不能切出低于它的尾巴。
    pub min_qty: Decimal,
}

/// 平仓视角，只给往返记录和一档厚度校验，不参与 STEP 判据。
#[derive(Debug, Clone, Copy)]
pub struct CloseView {
    pub exit_raw_pct: Decimal,
    pub exit_net_pct: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    GridReduce,
    FundingStopLoss,
    HoldTimeout,
    BalanceFloor,
}

impl CloseReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GridReduce => "grid_reduce",
            Self::FundingStopLoss => "funding_stop_loss",
            Self::HoldTimeout => "hold_timeout",
            Self::BalanceFloor => "balance_floor",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Intent {
    Open {
        qty: Decimal,
        buy: VenueId,
        sell: VenueId,
        grid: i32,
    },
    Close {
        qty: Decimal,
        grid: i32,
        reason: CloseReason,
        /// 决策时算出的往返净利（%），写 journal 用。
        round_trip_pct: Decimal,
    },
    Hold,
}

#[cfg(test)]
mod tests {
    use super::grid_step_from_target_bp;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn target_one_bp_entropy_lighter_zero_fee() {
        // F=1.8bp, C=0, h=0.25 → Δ = (0.01+0.018)/0.5 = 0.056
        let d = grid_step_from_target_bp(dec!(1), dec!(0.018), Decimal::ZERO, dec!(0.25));
        assert_eq!(d, dec!(0.056));
    }

    #[test]
    fn hysteresis_half_falls_back_to_full_grid() {
        let d = grid_step_from_target_bp(dec!(1), dec!(0.018), Decimal::ZERO, dec!(0.5));
        assert_eq!(d, dec!(0.028));
    }

    #[test]
    fn includes_round_trip_spread() {
        // 两所中枢 1.27bp、1.07bp 平均 C=1.17bp；目标 2bp + F 1.8bp，h=0 → Δ=0.0497
        let d = grid_step_from_target_bp(dec!(2), dec!(0.018), dec!(0.0117), Decimal::ZERO);
        assert_eq!(d, dec!(0.0497));
    }

    #[test]
    fn never_thinner_than_fee_plus_spread() {
        let d = grid_step_from_target_bp(Decimal::ZERO, dec!(0.018), dec!(0.0117), Decimal::ZERO);
        assert_eq!(d, dec!(0.0297));
    }
}
