use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::config::VenueFile;
use crate::domain::{
    symbol::{normalize_perp, whitelist_allows},
    Bbo, VenueId, VenueMarket,
};

use super::port::{
    AccountSnapshot, Balance, BboTx, CancelReq, ExchangePort, FundingRate, OrderAck, OrderReq,
    VenuePosition,
};
use super::{bridge, venue_yaml_path};

pub struct SodexAdapter {
    venue: VenueFile,
    whitelist: Vec<String>,
    venue_path: std::path::PathBuf,
}

impl SodexAdapter {
    pub fn new(venue: VenueFile, whitelist: Vec<String>) -> Self {
        let venue_path = venue_yaml_path(&venue.id);
        Self {
            venue,
            whitelist,
            venue_path,
        }
    }

    fn venue_id(&self) -> VenueId {
        VenueId(self.venue.id.clone())
    }
}

#[derive(Debug, Deserialize)]
struct SymbolsResp {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    data: Vec<RawSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSymbol {
    id: u64,
    name: String,
    #[serde(default)]
    base_coin: String,
    #[serde(default)]
    #[allow(dead_code)]
    quote_coin: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    min_quantity: String,
    #[serde(default)]
    quantity_precision: i32,
}

#[derive(Debug, Deserialize)]
struct WsEnvelope {
    #[serde(default)]
    op: String,
    #[serde(default)]
    channel: String,
    #[serde(default, rename = "type")]
    #[allow(dead_code)]
    kind: String,
    #[serde(default)]
    data: Vec<WsTicker>,
}

#[derive(Debug, Deserialize)]
struct WsTicker {
    #[serde(default)]
    s: String,
    #[serde(default)]
    a: String,
    #[serde(default, rename = "A")]
    ask_qty: String,
    #[serde(default)]
    b: String,
    #[serde(default, rename = "B")]
    bid_qty: String,
}

#[async_trait]
impl ExchangePort for SodexAdapter {
    fn id(&self) -> VenueId {
        self.venue_id()
    }

    async fn list_perps(&self) -> Result<Vec<VenueMarket>> {
        let url = format!(
            "{}/markets/symbols",
            self.venue.rest.trim_end_matches('/')
        );
        let resp: SymbolsResp = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("dex-arbitr/0.1")
            .build()?
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .json()
            .await
            .context("decode sodex symbols")?;
        if resp.code != 0 {
            bail!("sodex symbols code={}", resp.code);
        }

        let mut out = Vec::new();
        for raw in resp.data {
            if !raw.status.is_empty() && raw.status != "TRADING" {
                continue;
            }
            let Some((base, pair_id)) = sodex_pair(&raw) else {
                continue;
            };
            if !whitelist_allows(&base, &self.whitelist) {
                continue;
            }
            let min_qty = Decimal::from_str(&raw.min_quantity).unwrap_or(Decimal::ZERO);
            let market_index = i32::try_from(raw.id).unwrap_or(0);
            out.push(VenueMarket {
                venue: self.venue_id(),
                raw_symbol: raw.name,
                pair_id,
                base,
                market_index,
                qty_precision: raw.quantity_precision.max(0) as u32,
                min_qty,
            });
        }
        info!(venue = self.venue.id, n = out.len(), "loaded perp markets");
        Ok(out)
    }

    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()> {
        let aliases = symbol_alias_map(markets, &self.venue.quote);
        let ws_url = self.venue.ws.clone();
        let venue = self.venue_id();
        let n = markets.len();
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_ws(ws_url.clone(), venue.clone(), aliases.clone(), tx.clone()).await
                {
                    warn!(venue = venue.as_str(), error = %err, "sodex ws stopped");
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
        info!(venue = self.venue.id, n, "subscribed allBookTicker");
        Ok(())
    }

    async fn place(&self, req: OrderReq) -> Result<OrderAck> {
        if !self.venue.keys_ready() {
            bail!("{} keys not configured", self.venue.id);
        }
        if !bridge::bridge_available().await {
            bail!("missing exchange_sidecar; build: cd scripts/exchange_sidecar && go build -o exchange_sidecar .");
        }
        bridge::bridge_place(&self.venue_path, &req).await
    }

    async fn cancel(&self, req: &CancelReq) -> Result<()> {
        if !self.venue.keys_ready() {
            bail!("{} keys not configured", self.venue.id);
        }
        bridge::bridge_cancel(&self.venue_path, req).await
    }

    async fn order_status(&self, req: &CancelReq) -> Result<OrderAck> {
        if !self.venue.keys_ready() {
            bail!("{} keys not configured", self.venue.id);
        }
        bridge::bridge_order_status(
            &self.venue_path,
            req,
            req.qty.unwrap_or(Decimal::ZERO),
        )
        .await
    }

    /// 一次调用同时拿余额和持仓，避免 trait 默认实现的两次进程 + 两次 REST。
    async fn account(&self) -> Result<AccountSnapshot> {
        if !self.venue.keys_ready() {
            warn!(venue = %self.id(), "account skipped: signing keys not loaded");
            return Ok(AccountSnapshot::default());
        }
        if !bridge::bridge_available().await {
            warn!(venue = %self.id(), "account skipped: exchange sidecar not found");
            return Ok(AccountSnapshot::default());
        }
        bridge::bridge_account(&self.venue_path).await
    }

    async fn positions(&self) -> Result<Vec<VenuePosition>> {
        if !self.venue.keys_ready() {
            return Ok(Vec::new());
        }
        if !bridge::bridge_available().await {
            return Ok(Vec::new());
        }
        bridge::bridge_positions(&self.venue_path).await
    }

    async fn balances(&self) -> Result<Vec<Balance>> {
        if !self.venue.keys_ready() {
            return Ok(Vec::new());
        }
        if !bridge::bridge_available().await {
            return Ok(Vec::new());
        }
        bridge::bridge_balances(&self.venue_path).await
    }

    async fn funding(&self) -> Result<Vec<FundingRate>> {
        if !self.venue.keys_ready() || !bridge::bridge_available().await {
            return Ok(Vec::new());
        }
        bridge::bridge_funding(&self.venue_path).await
    }
}

fn sodex_pair(raw: &RawSymbol) -> Option<(String, String)> {
    if let Some((base, pair_id)) = normalize_perp(&raw.name, "perp") {
        return Some((base, pair_id));
    }
    let base = raw.base_coin.trim();
    if base.is_empty() {
        return None;
    }
    normalize_perp(&format!("{base}-USD"), "perp")
}

fn symbol_alias_map(markets: &[VenueMarket], quote: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let q = quote.trim().to_ascii_uppercase();
    for m in markets {
        for key in [
            m.raw_symbol.clone(),
            m.pair_id.clone(),
            format!("{}-USD", m.base),
            format!("{}_{}", m.base, q),
            format!("v{}_{}", m.base, q),
            format!("v{}_v{}", m.base, q.trim_start_matches('V')),
        ] {
            map.insert(key.to_ascii_uppercase(), m.pair_id.clone());
        }
    }
    map
}

async fn run_ws(
    url: String,
    venue: VenueId,
    aliases: HashMap<String, String>,
    tx: mpsc::UnboundedSender<(VenueId, String, Bbo)>,
) -> Result<()> {
    let (ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("ws connect {url}"))?;
    let (mut write, mut read) = ws.split();
    let sub = serde_json::json!({
        "op": "subscribe",
        "params": { "channel": "allBookTicker" }
    });
    write.send(Message::Text(sub.to_string().into())).await?;

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write
                    .send(Message::Text(r#"{"op":"ping"}"#.to_string().into()))
                    .await?;
            }
            frame = read.next() => {
                let Some(frame) = frame else {
                    break;
                };
                let text = match frame? {
                    Message::Text(t) => t.to_string(),
                    Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => break,
                    _ => continue,
                };
                let Ok(msg) = serde_json::from_str::<WsEnvelope>(&text) else {
                    continue;
                };
                if msg.op == "pong" || msg.op == "ping" || msg.op == "subscribe" {
                    continue;
                }
                if msg.channel != "allBookTicker" && msg.channel != "bookTicker" {
                    continue;
                }
                for tick in msg.data {
                    let Some(pair_id) = aliases.get(&tick.s.to_ascii_uppercase()) else {
                        continue;
                    };
                    let Some(bbo) = ticker_bbo(&tick) else {
                        continue;
                    };
                    if tx.send((venue.clone(), pair_id.clone(), bbo)).is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

fn ticker_bbo(tick: &WsTicker) -> Option<Bbo> {
    let bid = Decimal::from_str(tick.b.trim()).ok()?;
    let ask = Decimal::from_str(tick.a.trim()).ok()?;
    let bid_qty = Decimal::from_str(tick.bid_qty.trim()).unwrap_or(Decimal::ZERO);
    let ask_qty = Decimal::from_str(tick.ask_qty.trim()).unwrap_or(Decimal::ZERO);
    // 锁定盘口（ask == bid）是数据异常，和 `Bbo::valid()` 保持一致地丢掉。
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || ask <= bid {
        return None;
    }
    Some(Bbo {
        bid,
        ask,
        bid_qty,
        ask_qty,
        bids: vec![(bid, bid_qty)],
        asks: vec![(ask, ask_qty)],
        ts: Instant::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_symbols_and_maps_btc() {
        let raw = r#"{
            "code":0,
            "data":[
                {"id":24,"name":"BTC-USD","baseCoin":"BTC","quoteCoin":"vUSDC","status":"TRADING","minQuantity":"0.0001","quantityPrecision":4},
                {"id":99,"name":"FOO-USD","baseCoin":"FOO","quoteCoin":"vUSDC","status":"HALT","minQuantity":"1","quantityPrecision":0}
            ]
        }"#;
        let resp: SymbolsResp = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.data.len(), 2);
        let btc = sodex_pair(&resp.data[0]).unwrap();
        assert_eq!(btc, ("BTC".into(), "BTC-USD-PERP".into()));
        assert_eq!(resp.data[1].status, "HALT");
    }

    #[test]
    fn book_ticker_to_bbo() {
        let tick = WsTicker {
            s: "BTC-USD".into(),
            a: "100.2".into(),
            ask_qty: "1.5".into(),
            b: "100.1".into(),
            bid_qty: "2".into(),
        };
        let bbo = ticker_bbo(&tick).unwrap();
        assert_eq!(bbo.bid, dec!(100.1));
        assert_eq!(bbo.ask, dec!(100.2));
        assert_eq!(bbo.ask_qty, dec!(1.5));
    }
}
