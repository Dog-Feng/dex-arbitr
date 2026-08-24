use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::config::OrderStyle;
use crate::domain::{Bbo, VenueId, VenueMarket};

pub type BboTx = mpsc::UnboundedSender<(VenueId, String, Bbo)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Accepted,
    Partial,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct OrderReq {
    pub symbol: String,
    pub market_index: i32,
    pub is_buy: bool,
    pub qty: Decimal,
    pub reduce_only: bool,
    pub style: OrderStyle,
    /// Limit maker 挂单价；市价单为 None（由桥接层按 BBO+滑点算保护价）。
    pub limit_price: Option<Decimal>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderAck {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub filled_qty: Decimal,
    pub avg_price: Option<Decimal>,
    pub status: OrderStatus,
}

#[derive(Debug, Clone)]
pub struct CancelReq {
    pub order_id: String,
    pub symbol: String,
    pub market_index: i32,
    /// 查单时用于判断 partial/filled。
    pub qty: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct VenuePosition {
    pub symbol: String,
    pub qty: Decimal,
    pub entry_price: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct Balance {
    pub asset: String,
    pub available: Decimal,
    pub total: Option<Decimal>,
}

#[derive(Debug, Clone, Default)]
pub struct AccountSnapshot {
    pub balances: Vec<Balance>,
    pub positions: Vec<VenuePosition>,
}

#[async_trait]
pub trait ExchangePort: Send + Sync {
    fn id(&self) -> VenueId;
    async fn list_perps(&self) -> Result<Vec<VenueMarket>>;
    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()>;
    async fn place(&self, req: OrderReq) -> Result<OrderAck>;
    async fn cancel(&self, req: &CancelReq) -> Result<()>;
    /// 查询挂单/最近成交；未接线的 venue 返回 Err。
    async fn order_status(&self, req: &CancelReq) -> Result<OrderAck> {
        let _ = req;
        anyhow::bail!("order_status not implemented for {}", self.id().as_str())
    }
    async fn account(&self) -> Result<AccountSnapshot> {
        Ok(AccountSnapshot {
            balances: self.balances().await?,
            positions: self.positions().await?,
        })
    }
    async fn positions(&self) -> Result<Vec<VenuePosition>>;
    async fn balances(&self) -> Result<Vec<Balance>>;
}
