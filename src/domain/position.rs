use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use super::{slot_key, VenueId};

#[derive(Debug, Clone)]
pub struct Position {
    pub pair_id: String,
    pub buy: VenueId,
    pub sell: VenueId,
    pub qty: Decimal,
    pub grid: u32,
    /// 开仓时名义 USDC（qty × mid），用于各所占用保证金估算。
    pub entry_notional_usdc: Decimal,
    /// 建仓那一刻的净边（raw − 开仓手续费）。平仓算往返净利用，
    /// 缺了它就没法把平仓手续费纳入止损。
    pub entry_net_pct: Decimal,
    /// 建仓均毛价差（不扣费）。剥头皮止盈用：`entry_raw − 当前剩余毛价差`。
    pub entry_raw_pct: Decimal,
    /// 买腿开仓均价（报价货币）。双边已实现盈亏用。
    pub entry_buy_px: Decimal,
    /// 卖腿开仓均价（报价货币）。
    pub entry_sell_px: Decimal,
    /// 开仓时定仓所得单格数量。用于 `GridEngine::segments_held` 的整个持仓
    /// 周期，不随后续保证金/盘口变化重算，避免 base_qty 漂移导致格数误判。
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

    /// 本地用开平仓价估的双边盈亏。执行带不走这里：平仓盈亏只认交易所回报的
    /// 已实现字段（Entropy `closedPnl`，Lighter/SoDEX `realized_pnl`）两腿相加。
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
}
