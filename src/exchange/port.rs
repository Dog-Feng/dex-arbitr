use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;
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
    /// 不在活跃列表且无法确认成交（禁止当作 filled）。
    Unknown,
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
    /// 市价单的滑点保护基准价 = **决策那一刻的信号价**。
    /// 对齐参考 `payload["target_price"]`：保护要约束「相对决策价滑了多少」，
    /// 用下单时的盘口做基准等于自我实现，价格跑了照样成交。
    pub target_price: Option<Decimal>,
    /// 允许的滑点上限（%）。超出交易所直接拒单。
    pub slippage_pct: Option<Decimal>,
    /// 市价 / IOC：sidecar 等该单 WS 的毫秒数。0 = sidecar 默认 1 秒。
    pub fill_wait_ms: u64,
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
    /// 该市场累计已实现盈亏（交易所字段）。Entropy 成交用 closedPnl，不走这里。
    pub realized_pnl: Option<Decimal>,
}

/// 平仓后从 DEX 取回的已实现盈亏。`per_fill` 为真时是这一笔成交的 pnl，
/// 为假时是该市场仓位上的累计 realized，要用「平仓后 − 平仓前」才是本笔。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillPnl {
    pub realized_pnl: Decimal,
    pub per_fill: bool,
    pub found: bool,
}

impl FillPnl {
    pub fn missing() -> Self {
        Self {
            realized_pnl: Decimal::ZERO,
            per_fill: false,
            found: false,
        }
    }
}

/// 单市场的当期资金费率。`symbol` 是**该所的原始符号**，跨所匹配靠
/// `Pair` 的 base，不能直接字符串比——Lighter 给 `XAUT-USD`,SoDEX 给裸
/// `CRV`。
#[derive(Debug, Clone)]
pub struct FundingRate {
    pub symbol: String,
    /// 每结算周期的费率，小数（0.0001 = 0.01%）。正 = 多头支付。
    pub rate: Decimal,
    /// 结算周期（秒），由交易所返回。不假定 1h/8h。
    pub interval_secs: u32,
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
    /// 粗筛用的廉价盘口快照。`pair_id → Bbo`。默认空：该所粗筛缺盘口即失败。
    async fn snapshot_bbos(&self, markets: &[VenueMarket]) -> HashMap<String, Bbo> {
        let _ = markets;
        HashMap::new()
    }
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
    /// 全市场当期资金费率。未接线的 venue 返回空表——上层据此**跳过**
    /// 资金费率门，而不是把缺数据当成 0 费率放行。
    async fn funding(&self) -> Result<Vec<FundingRate>> {
        Ok(Vec::new())
    }
    /// 交易所回报的已实现盈亏。盘口订阅没有这个字段。
    async fn fill_realized_pnl(&self, symbol: &str, order_id: Option<&str>) -> Result<FillPnl> {
        let _ = (symbol, order_id);
        Ok(FillPnl::missing())
    }
}
