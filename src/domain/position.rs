use rust_decimal::Decimal;

use super::VenueId;

#[derive(Debug, Clone)]
pub struct Position {
    pub pair_id: String,
    pub buy: VenueId,
    pub sell: VenueId,
    pub qty: Decimal,
    pub grid: u32,
    /// 开仓时名义 USDC（qty × mid），用于各所占用保证金估算。
    pub entry_notional_usdc: Decimal,
}
