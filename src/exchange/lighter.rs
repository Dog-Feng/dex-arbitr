use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
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
    AccountSnapshot, Balance, BboTx, CancelReq, ExchangePort, FillPnl, FundingRate, OrderAck,
    OrderReq, VenuePosition,
};
use super::{bridge, venue_yaml_path};

pub struct LighterAdapter {
    venue: VenueFile,
    whitelist: Vec<String>,
    venue_path: std::path::PathBuf,
}

impl LighterAdapter {
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
struct OrderBooksResp {
    order_books: Vec<RawBook>,
}

#[derive(Debug, Deserialize)]
struct RawBook {
    symbol: String,
    market_id: i32,
    #[serde(default)]
    market_type: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    min_base_amount: String,
    #[serde(default)]
    supported_size_decimals: u32,
}

#[derive(Debug, Deserialize)]
struct WsMsg {
    #[serde(default)]
    channel: String,
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    order_book: Option<WsBook>,
}

#[derive(Debug, Deserialize)]
struct WsBook {
    #[serde(default)]
    bids: Vec<WsLevel>,
    #[serde(default)]
    asks: Vec<WsLevel>,
}

#[derive(Debug, Clone, Deserialize)]
struct WsLevel {
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
}

#[derive(Debug, Default)]
struct LocalBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl LocalBook {
    fn replace(&mut self, book: &WsBook) {
        self.bids.clear();
        self.asks.clear();
        apply_side(&mut self.bids, &book.bids);
        apply_side(&mut self.asks, &book.asks);
    }

    fn apply(&mut self, book: &WsBook) {
        apply_side(&mut self.bids, &book.bids);
        apply_side(&mut self.asks, &book.asks);
    }

    fn bbo(&self) -> Option<Bbo> {
        let (bid, bid_qty) = self.bids.iter().next_back()?;
        let (ask, ask_qty) = self.asks.iter().next()?;
        Some(Bbo {
            bid: *bid,
            ask: *ask,
            bid_qty: *bid_qty,
            ask_qty: *ask_qty,
            bids: self.bids.iter().rev().map(|(p, q)| (*p, *q)).collect(),
            asks: self.asks.iter().map(|(p, q)| (*p, *q)).collect(),
            ts: Instant::now(),
        })
    }
}

fn apply_side(side: &mut BTreeMap<Decimal, Decimal>, levels: &[WsLevel]) {
    for lv in levels {
        let Ok(px) = Decimal::from_str(lv.price.trim()) else {
            continue;
        };
        let sz = Decimal::from_str(lv.size.trim()).unwrap_or(Decimal::ZERO);
        if sz <= Decimal::ZERO {
            side.remove(&px);
        } else {
            side.insert(px, sz);
        }
    }
}

#[async_trait]
impl ExchangePort for LighterAdapter {
    fn id(&self) -> VenueId {
        self.venue_id()
    }

    async fn list_perps(&self) -> Result<Vec<VenueMarket>> {
        let url = format!(
            "{}/api/v1/orderBooks?filter=perp",
            self.venue.rest.trim_end_matches('/')
        );
        let resp: OrderBooksResp = reqwest::Client::builder()
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
            .context("decode orderBooks")?;

        let mut out = Vec::new();
        for raw in resp.order_books {
            if !raw.status.is_empty() && raw.status != "active" {
                continue;
            }
            let Some((base, pair_id)) = normalize_perp(&raw.symbol, &raw.market_type) else {
                continue;
            };
            if !whitelist_allows(&base, &self.whitelist) {
                continue;
            }
            let min_qty = Decimal::from_str(&raw.min_base_amount).unwrap_or(Decimal::ZERO);
            out.push(VenueMarket {
                venue: self.venue_id(),
                raw_symbol: raw.symbol,
                pair_id,
                base,
                market_index: raw.market_id,
                qty_precision: raw.supported_size_decimals,
                min_qty,
            });
        }
        info!(venue = self.venue.id, n = out.len(), "loaded perp markets");
        Ok(out)
    }

    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()> {
        let id_to_pair: Vec<(i32, String)> = markets
            .iter()
            .map(|m| (m.market_index, m.pair_id.clone()))
            .collect();
        let ws_url = self.venue.ws.clone();
        let venue = self.venue_id();
        tokio::spawn(async move {
            loop {
                if let Err(err) = run_ws(ws_url.clone(), venue.clone(), id_to_pair.clone(), tx.clone()).await
                {
                    warn!(venue = venue.as_str(), error = %err, "lighter ws stopped");
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
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

    /// 一次调用同时拿余额和持仓。trait 默认实现是 balances() + positions()，
    /// 那等于两次进程 + 两次全量 REST。
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

    async fn fill_realized_pnl(&self, symbol: &str, order_id: Option<&str>) -> Result<FillPnl> {
        if !self.venue.keys_ready() || !bridge::bridge_available().await {
            return Ok(FillPnl::missing());
        }
        bridge::bridge_fill_pnl(&self.venue_path, symbol, order_id).await
    }
}

async fn run_ws(
    url: String,
    venue: VenueId,
    markets: Vec<(i32, String)>,
    tx: mpsc::UnboundedSender<(VenueId, String, Bbo)>,
) -> Result<()> {
    let (ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("ws connect {url}"))?;
    let (mut write, mut read) = ws.split();
    let mut books: HashMap<i32, LocalBook> = HashMap::new();
    let mut subscribed = false;

    while let Some(frame) = read.next().await {
        let frame = frame?;
        let text = match frame {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(msg) = serde_json::from_str::<WsMsg>(&text) else {
            continue;
        };

        if msg.kind == "ping" {
            write
                .send(Message::Text(r#"{"type":"pong"}"#.to_string().into()))
                .await?;
            continue;
        }

        if msg.kind == "connected" && !subscribed {
            for (i, (market_id, _)) in markets.iter().enumerate() {
                let sub = serde_json::json!({
                    "type": "subscribe",
                    "channel": format!("order_book/{market_id}")
                });
                write.send(Message::Text(sub.to_string().into())).await?;
                if i + 1 < markets.len() && (i + 1) % 10 == 0 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            subscribed = true;
            info!(
                venue = venue.as_str(),
                n = markets.len(),
                "subscribed order_book"
            );
            continue;
        }

        let is_snapshot = msg.kind == "subscribed/order_book";
        let is_delta = msg.kind == "update/order_book";
        if !is_snapshot && !is_delta {
            continue;
        }
        let Some(raw) = msg.order_book else {
            continue;
        };
        let Some(market_id) = parse_market_id(&msg.channel) else {
            continue;
        };
        let Some((_, pair_id)) = markets.iter().find(|(id, _)| *id == market_id) else {
            continue;
        };

        let book = books.entry(market_id).or_default();
        if is_snapshot {
            book.replace(&raw);
        } else {
            book.apply(&raw);
        }
        let Some(bbo) = book.bbo() else {
            continue;
        };
        if tx.send((venue.clone(), pair_id.clone(), bbo)).is_err() {
            break;
        }
    }
    Ok(())
}

fn parse_market_id(channel: &str) -> Option<i32> {
    channel
        .rsplit(['/', ':'])
        .next()
        .and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn lv(price: &str, size: &str) -> WsLevel {
        WsLevel {
            price: price.into(),
            size: size.into(),
        }
    }

    #[test]
    fn snapshot_then_delta_keeps_best() {
        let mut book = LocalBook::default();
        book.replace(&WsBook {
            bids: vec![lv("100", "1"), lv("99", "2")],
            asks: vec![lv("101", "1"), lv("102", "3")],
        });
        let snap = book.bbo().unwrap();
        assert_eq!(snap.bid, dec!(100));
        assert_eq!(snap.ask, dec!(101));

        book.apply(&WsBook {
            bids: vec![lv("100", "0"), lv("100.5", "4")],
            asks: vec![lv("101", "0.5")],
        });
        let next = book.bbo().unwrap();
        assert_eq!(next.bid, dec!(100.5));
        assert_eq!(next.bid_qty, dec!(4));
        assert_eq!(next.ask, dec!(101));
        assert_eq!(next.ask_qty, dec!(0.5));
    }

    #[test]
    fn parse_channel_slash_or_colon() {
        assert_eq!(parse_market_id("order_book/1"), Some(1));
        assert_eq!(parse_market_id("order_book:0"), Some(0));
    }
}
