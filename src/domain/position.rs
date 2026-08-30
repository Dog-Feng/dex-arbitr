use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use super::{slot_key, VenueId};

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
    /// 买腿开仓均价（报价货币）。
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
}
