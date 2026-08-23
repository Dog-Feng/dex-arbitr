use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::config::OrderStyle;
use crate::domain::{Bbo, VenueId, VenueMarket};

pub type BboTx = mpsc::UnboundedSender<(VenueId, String, Bbo)>;

#[derive(Debug, Clone)]
pub struct OrderReq {
    pub symbol: String,
    pub market_index: i32,
    pub is_buy: bool,
    pub qty: Decimal,
    pub reduce_only: bool,
    pub style: OrderStyle,
}

#[derive(Debug, Clone)]
pub struct OrderAck {
    pub order_id: String,
    pub filled_qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct VenuePosition {
    pub symbol: String,
    pub qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct Balance {
    pub asset: String,
    pub available: Decimal,
}

#[async_trait]
pub trait ExchangePort: Send + Sync {
    fn id(&self) -> VenueId;
    async fn list_perps(&self) -> Result<Vec<VenueMarket>>;
    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()>;
    async fn place(&self, req: OrderReq) -> Result<OrderAck>;
    async fn cancel(&self, _order_id: &str) -> Result<()> {
        Ok(())
    }
    async fn positions(&self) -> Result<Vec<VenuePosition>>;
    async fn balances(&self) -> Result<Vec<Balance>>;
}
