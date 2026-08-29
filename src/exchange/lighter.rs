use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::VenueFile;
use crate::domain::{
    symbol::{normalize_perp, whitelist_allows},
    Bbo, VenueId, VenueMarket,
};

use super::net::{connect_ws, http_client, json_decimal, spawn_feed_loop, FeedGuard};

use super::port::{
    AccountSnapshot, Balance, BboTx, CancelReq, ExchangePort, FillPnl, FundingRate, OrderAck,
    OrderReq, VenuePosition,
};
use super::{bridge, venue_yaml_path};

pub struct LighterAdapter {
    venue: VenueFile,
    whitelist: Vec<String>,
    venue_path: std::path::PathBuf,
    feeds: FeedGuard,
}

impl LighterAdapter {
    pub fn new(venue: VenueFile, whitelist: Vec<String>) -> Self {
        let venue_path = venue_yaml_path(&venue.id);
        Self {
            venue,
            whitelist,
            venue_path,
            feeds: FeedGuard::new(),
        }
    }

    fn venue_id(&self) -> VenueId {
        VenueId(self.venue.id.clone())
    }
}

#[derive(Debug, Deserialize)]
struct OrderBooksResp {
    #[serde(default, alias = "order_book_details")]
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
    #[serde(default)]
    daily_quote_token_volume: Option<Value>,
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
            "{}/api/v1/orderBookDetails?filter=perp",
            self.venue.rest.trim_end_matches('/')
        );
        let resp: OrderBooksResp = http_client()
            .get(&url)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?
            .error_for_status()?
            .json()
            .await
            .context("decode orderBookDetails")?;

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
            let min_qty = match Decimal::from_str(&raw.min_base_amount) {
                Ok(q) if q > Decimal::ZERO => q,
                other => {
                    warn!(
                        venue = self.venue.id,
                        symbol = %raw.symbol,
                        raw = %raw.min_base_amount,
                        ok = other.is_ok(),
                        "skip market: bad min_qty"
                    );
                    continue;
                }
            };
            out.push(VenueMarket {
                venue: self.venue_id(),
                raw_symbol: raw.symbol,
                pair_id,
                base,
                market_index: raw.market_id,
                qty_precision: raw.supported_size_decimals,
                min_qty,
                volume_24h_usdc: raw.daily_quote_token_volume.as_ref().and_then(json_decimal),
            });
        }
        info!(venue = self.venue.id, n = out.len(), "loaded perp markets");
        Ok(out)
    }

    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()> {
        if markets.is_empty() {
            self.feeds.interrupt();
            return Ok(());
        }
        let id_to_pair: Vec<(i32, String)> = markets
            .iter()
            .map(|m| (m.market_index, m.pair_id.clone()))
            .collect();
        let fp: String = {
            let mut ids: Vec<_> = id_to_pair.iter().map(|(i, p)| format!("{i}:{p}")).collect();
            ids.sort();
            ids.join(",")
        };
        let Some((gen, rx)) = self.feeds.begin(&fp) else {
            return Ok(());
        };
        let ws_url = self.venue.ws.clone();
        let venue = self.venue_id();
        spawn_feed_loop(rx, gen, "lighter ws", move || {
            run_ws(ws_url.clone(), venue.clone(), id_to_pair.clone(), tx.clone())
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

    async fn snapshot_bbos(&self, markets: &[VenueMarket]) -> HashMap<String, Bbo> {
        snapshot_lighter_bbos(&self.venue.rest, markets).await
    }
}

async fn run_ws(
    url: String,
    venue: VenueId,
    markets: Vec<(i32, String)>,
    tx: mpsc::UnboundedSender<(VenueId, String, Bbo)>,
) -> Result<()> {
    let ws = connect_ws(&url).await?;
    let (mut write, mut read) = ws.split();
    let mut books: HashMap<i32, LocalBook> = HashMap::new();
    let mut subscribed = false;
    // Lighter 公共流约 120s 无 client ping 就踢连接。日志里是稳定的 124s
    // 一轮（120s 超时 + 3s 重连）。主动 ping，不要靠空闲超时去拆。
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(30),
        Duration::from_secs(30),
    );
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write
                    .send(Message::Text(r#"{"type":"ping"}"#.to_string().into()))
                    .await?;
            }
            frame = read.next() => {
                let Some(frame) = frame else {
                    break;
                };
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

async fn snapshot_lighter_bbos(rest: &str, markets: &[VenueMarket]) -> HashMap<String, Bbo> {
    let client = http_client();
    let base = rest.trim_end_matches('/');
    let mut out = HashMap::new();
    for chunk in markets.chunks(8) {
        let futs = chunk.iter().map(|m| {
            let url = format!("{base}/api/v1/orderBookOrders?market_id={}&limit=1", m.market_index);
            let pair_id = m.pair_id.clone();
            let client = client.clone();
            async move {
                let resp = client
                    .get(&url)
                    .timeout(Duration::from_secs(8))
                    .send()
                    .await
                    .ok()?;
                let data: Value = resp.json().await.ok()?;
                let bbo = bbo_from_lighter_orders(&data)?;
                Some((pair_id, bbo))
            }
        });
        for (pair_id, bbo) in futures_util::future::join_all(futs).await.into_iter().flatten() {
            out.insert(pair_id, bbo);
        }
    }
    out
}

fn bbo_from_lighter_orders(data: &Value) -> Option<Bbo> {
    let bid_lv = data.get("bids")?.as_array()?.first()?;
    let ask_lv = data.get("asks")?.as_array()?.first()?;
    let bid = json_decimal(bid_lv.get("price")?)?;
    let ask = json_decimal(ask_lv.get("price")?)?;
    let bid_qty = json_decimal(bid_lv.get("remaining_base_amount")?).unwrap_or(Decimal::ZERO);
    let ask_qty = json_decimal(ask_lv.get("remaining_base_amount")?).unwrap_or(Decimal::ZERO);
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
