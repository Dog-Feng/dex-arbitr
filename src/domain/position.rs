use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use super::spread::raw_spread_pct;
use super::{slot_key, VenueId};

/// 本次减格/平仓的往返盈亏。`pct` 与全库一致（0.02 = 0.02% = 2 bp）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThisClosePnl {
    pub usdc: Decimal,
    pub pct: Decimal,
}

#[derive(Debug, Clone)]
pub struct Position {
    pub pair_id: String,
    pub buy: VenueId,
    pub sell: VenueId,
    pub qty: Decimal,
    /// 有符号 STEP。0 无仓；正 = 空 L 多 R；负 = 多 L 空 R。
    pub grid: i32,
    /// 开仓时名义 USDC（qty × mid），用于各所占用保证金估算。
    pub entry_notional_usdc: Decimal,
    /// 建仓那一刻的净边（raw − 开仓手续费）。平仓算往返净利用，
    /// 缺了它就没法把平仓手续费纳入止损。
    pub entry_net_pct: Decimal,
    /// 建仓均毛价差（不扣费）。成交价拿不到时回填；journal 记往返用。
    pub entry_raw_pct: Decimal,
    /// 买腿开仓均价（报价货币）。双边已实现盈亏用。
    pub entry_buy_px: Decimal,
    /// 卖腿开仓均价（报价货币）。
    pub entry_sell_px: Decimal,
    /// 开仓时定仓所得单格数量。整个持仓周期用同一把尺，不随后续配置漂移。
    pub base_qty: Decimal,
    /// 首次建仓时刻，持仓时长上限用。
    ///
    /// 补仓**不刷新**它：刷新会让一条被反复加仓的仓位永远不超时，而超时
    /// 限制针对的正是「开进去就再没走掉」这种仓。用 `Instant` 意味着重启后
    /// 重新计时——和参考一样是内存态，重启即丢。
    pub opened_at: Instant,
}

impl Position {
    /// 已持有多久。
    pub fn held_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.opened_at)
    }
}

impl Position {
    /// 与 `Pair::slot_key()` 同一命名空间：币 + 所对。
    pub fn slot_key(&self) -> String {
        slot_key(&self.pair_id, self.buy.as_str(), self.sell.as_str())
    }

    /// 持仓方向是否与给定的买卖所一致。
    pub fn same_direction(&self, buy: &VenueId, sell: &VenueId) -> bool {
        &self.buy == buy && &self.sell == sell
    }

    /// 本地用开平仓价估的双边毛盈亏（不含手续费）。
    pub fn two_leg_pnl_usdc(
        &self,
        qty: Decimal,
        exit_px_buy_venue: Decimal,
        exit_px_sell_venue: Decimal,
    ) -> Option<Decimal> {
        if qty <= Decimal::ZERO
            || self.entry_buy_px <= Decimal::ZERO
            || self.entry_sell_px <= Decimal::ZERO
            || exit_px_buy_venue <= Decimal::ZERO
            || exit_px_sell_venue <= Decimal::ZERO
        {
            return None;
        }
        let long_pnl = (exit_px_buy_venue - self.entry_buy_px) * qty;
        let short_pnl = (self.entry_sell_px - exit_px_sell_venue) * qty;
        Some(long_pnl + short_pnl)
    }

    /// 本次平仓往返：开仓均净边 + 本笔成交价平仓毛边 − 平仓手续费。
    ///
    /// 成交价必须是真实均价。sidecar 若把滑点保护限价当成 avg_price，这里会
    /// 每腿多记约 `max_slippage` 的假亏（开+平约 20 bp），和所方已实现对不上。
    ///
    /// 平仓成交：原买所卖掉（`exit_px_buy_venue`）、原卖所买回（`exit_px_sell_venue`）。
    /// 不读交易所累计已实现——那笔数含资金费、同币其它仓，各所口径也不一样。
    pub fn this_close_pnl(
        &self,
        qty: Decimal,
        exit_px_buy_venue: Decimal,
        exit_px_sell_venue: Decimal,
        close_fee_pct: Decimal,
    ) -> Option<ThisClosePnl> {
        if qty <= Decimal::ZERO
            || self.entry_buy_px <= Decimal::ZERO
            || self.entry_sell_px <= Decimal::ZERO
            || exit_px_buy_venue <= Decimal::ZERO
            || exit_px_sell_venue <= Decimal::ZERO
        {
            return None;
        }
        let notional = qty * (self.entry_buy_px + self.entry_sell_px) / Decimal::from(2);
        if notional <= Decimal::ZERO {
            return None;
        }
        // 平仓方向：买回原卖所、卖回原买所。raw_spread_pct(买价, 卖价)。
        let exit_raw = raw_spread_pct(exit_px_sell_venue, exit_px_buy_venue)?;
        let pct = self.entry_net_pct + exit_raw - close_fee_pct;
        let usdc = pct / Decimal::from(100) * notional;
        Some(ThisClosePnl { usdc, pct })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn pos(buy: Decimal, sell: Decimal) -> Position {
        Position {
            pair_id: "BTC-USD-PERP".into(),
            buy: VenueId::from("lighter"),
            sell: VenueId::from("entropy"),
            qty: dec!(0.01),
            grid: 1,
            entry_notional_usdc: dec!(100),
            entry_net_pct: dec!(0.05),
            entry_raw_pct: dec!(0.05),
            entry_buy_px: buy,
            entry_sell_px: sell,
            base_qty: dec!(0.01),
            opened_at: Instant::now(),
        }
    }

    #[test]
    fn two_leg_pnl_sums_long_and_short() {
        // 开：买 100 / 卖 100.50；平：买所 100.40 卖掉、卖所 100.10 买回
        // 多头 0.40×1 + 空头 0.40×1 = 0.80
        let p = pos(dec!(100), dec!(100.5));
        assert_eq!(
            p.two_leg_pnl_usdc(dec!(1), dec!(100.4), dec!(100.1)),
            Some(dec!(0.8))
        );
    }

    #[test]
    fn two_leg_pnl_none_without_prices() {
        let p = pos(Decimal::ZERO, dec!(100));
        assert!(p.two_leg_pnl_usdc(dec!(1), dec!(100), dec!(100)).is_none());
    }

    #[test]
    fn this_close_pnl_is_entry_net_plus_exit_raw_minus_fee() {
        let mut p = pos(dec!(100), dec!(100.5));
        p.entry_net_pct = dec!(0.46);
        let got = p
            .this_close_pnl(dec!(1), dec!(100.4), dec!(100.1), dec!(0.04))
            .unwrap();
        // exit_raw = (100.4 − 100.1) / 100.1 × 100 ≈ 0.2997
        // pct = 0.46 + 0.2997 − 0.04 ≈ 0.7197
        assert!((got.pct - dec!(0.7197002997)).abs() < dec!(0.0001));
        let notional = (dec!(100) + dec!(100.5)) / dec!(2);
        assert!((got.usdc - got.pct / dec!(100) * notional).abs() < dec!(0.0000001));
    }

    #[test]
    fn this_close_pnl_none_without_exit_prices() {
        let mut p = pos(dec!(100), dec!(100.5));
        p.entry_net_pct = dec!(0.46);
        assert!(p.this_close_pnl(dec!(1), Decimal::ZERO, dec!(100.1), dec!(0.04)).is_none());
        assert!(p.this_close_pnl(dec!(1), dec!(100.4), Decimal::ZERO, dec!(0.04)).is_none());
    }
}
