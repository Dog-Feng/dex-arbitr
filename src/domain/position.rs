use rust_decimal::Decimal;

use super::VenueId;

#[derive(Debug, Clone)]
pub struct Position {
    pub pair_id: String,
    pub buy: VenueId,
    pub sell: VenueId,
    pub qty: Decimal,
}
