use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
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

/// Entropy 第一期只暴露 Hyperliquid HIP-3 的 `io` 命名空间，不是通用 HL 适配器。
const HIP3_DEX: &str = "io";

pub struct EntropyAdapter {
    venue: VenueFile,
    whitelist: Vec<String>,
    venue_path: std::path::PathBuf,
}

impl EntropyAdapter {
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
struct MetaResp {
    #[serde(default)]
    universe: Vec<RawAsset>,
}

#[derive(Debug, Deserialize)]
struct RawAsset {
    name: String,
    #[serde(default, rename = "szDecimals")]
    sz_decimals: u32,
    #[serde(default, rename = "isDelisted")]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
struct WsMsg {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    data: Option<WsBbo>,
}

#[derive(Debug, Deserialize)]
struct WsBbo {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    bbo: Vec<Option<WsLevel>>,
}

#[derive(Debug, Deserialize)]
struct WsLevel {
    #[serde(default)]
    px: String,
    #[serde(default)]
    sz: String,
}

#[async_trait]
impl ExchangePort for EntropyAdapter {
    fn id(&self) -> VenueId {
        self.venue_id()
    }

    async fn list_perps(&self) -> Result<Vec<VenueMarket>> {
        let info = info_url(&self.venue.rest);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("dex-arbitr/0.1")
            .build()?;

        let dexes: Value = client
            .post(&info)
            .json(&serde_json::json!({"type": "perpDexs"}))
            .send()
            .await
            .with_context(|| format!("POST {info} perpDexs"))?
            .error_for_status()?
            .json()
            .await
            .context("decode perpDexs")?;
        let Some(dex_index) = perp_dex_index(&dexes, HIP3_DEX) else {
            bail!("HIP-3 dex {HIP3_DEX} not found in perpDexs");
        };

        let body: Value = client
            .post(&info)
            .json(&serde_json::json!({"type": "metaAndAssetCtxs", "dex": HIP3_DEX}))
            .send()
            .await
            .with_context(|| format!("POST {info} metaAndAssetCtxs"))?
            .error_for_status()?
            .json()
            .await
            .context("decode metaAndAssetCtxs")?;

        let markets = parse_io_markets(
            &self.venue_id(),
            dex_index,
            &body,
            &self.whitelist,
        )?;
        info!(
            venue = self.venue.id,
            n = markets.len(),
            dex_index,
            symbols = ?markets.iter().map(|m| m.raw_symbol.as_str()).collect::<Vec<_>>(),
            "loaded perp markets"
        );
        Ok(markets)
    }

    async fn subscribe_bbo(&self, markets: &[VenueMarket], tx: BboTx) -> Result<()> {
        let coins: Vec<(String, String)> = markets
            .iter()
            .map(|m| (m.raw_symbol.clone(), m.pair_id.clone()))
            .collect();
        let ws_url = self.venue.ws.clone();
        let rest = self.venue.rest.clone();
        let venue = self.venue_id();
        let n = markets.len();
        tokio::spawn({
            let coins = coins.clone();
            let venue = venue.clone();
            let tx = tx.clone();
            async move {
                loop {
                    if let Err(err) =
                        run_ws(ws_url.clone(), venue.clone(), coins.clone(), tx.clone()).await
                    {
                        warn!(venue = venue.as_str(), error = %err, "entropy ws stopped");
                    }
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        });
        // HIP-3 的 bbo 频道有时很久才推；REST l2Book 兜底，监控页不会一直空着。
        tokio::spawn(poll_l2_books(rest, venue, coins, tx));
        info!(venue = self.venue.id, n, "subscribed bbo");
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

fn info_url(rest: &str) -> String {
    let r = rest.trim_end_matches('/');
    if r.ends_with("/info") {
        r.to_string()
    } else {
        format!("{r}/info")
    }
}

/// `perp_dex_index` 是 `perpDexs` 数组下标（含 leading null），官方公式
/// `100000 + perp_dex_index * 10000 + index_in_meta`。
fn perp_dex_index(dexes: &Value, name: &str) -> Option<i32> {
    let arr = dexes.as_array()?;
    for (i, item) in arr.iter().enumerate() {
        if item.is_null() {
            continue;
        }
        let Some(n) = item.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        if n.eq_ignore_ascii_case(name) {
            return i32::try_from(i).ok();
        }
    }
    None
}

fn hip3_asset_id(dex_index: i32, index_in_meta: i32) -> i32 {
    100_000 + dex_index * 10_000 + index_in_meta
}

fn parse_io_markets(
    venue: &VenueId,
    dex_index: i32,
    body: &Value,
    whitelist: &[String],
) -> Result<Vec<VenueMarket>> {
    let meta_val = body
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("metaAndAssetCtxs: missing meta"))?;
    let meta: MetaResp = serde_json::from_value(meta_val).context("decode io universe")?;
    let mut out = Vec::new();
    for (i, raw) in meta.universe.iter().enumerate() {
        if raw.is_delisted {
            continue;
        }
        let Some((base, pair_id)) = normalize_perp(&raw.name, "perp") else {
            continue;
        };
        if !whitelist_allows(&base, whitelist) {
            continue;
        }
        let index_in_meta = i32::try_from(i).unwrap_or(0);
        let min_qty = Decimal::new(1, raw.sz_decimals.min(18));
        out.push(VenueMarket {
            venue: venue.clone(),
            raw_symbol: raw.name.clone(),
            pair_id,
            base,
            market_index: hip3_asset_id(dex_index, index_in_meta),
            qty_precision: raw.sz_decimals,
            min_qty,
        });
    }
    Ok(out)
}

async fn run_ws(
    url: String,
    venue: VenueId,
    coins: Vec<(String, String)>,
    tx: mpsc::UnboundedSender<(VenueId, String, Bbo)>,
) -> Result<()> {
    let (ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("ws connect {url}"))?;
    info!(venue = venue.as_str(), n = coins.len(), "entropy ws connected");
    let (mut write, mut read) = ws.split();
    for (i, (coin, _)) in coins.iter().enumerate() {
        let sub = serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": coin}
        });
        write.send(Message::Text(sub.to_string().into())).await?;
        if i + 1 < coins.len() && (i + 1) % 20 == 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                write
                    .send(Message::Text(r#"{"method":"ping"}"#.to_string().into()))
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
                let Ok(msg) = serde_json::from_str::<WsMsg>(&text) else {
                    continue;
                };
                if msg.channel != "bbo" {
                    continue;
                }
                let Some(data) = msg.data else {
                    continue;
                };
                let Some((_, pair_id)) = coins.iter().find(|(c, _)| c.eq_ignore_ascii_case(&data.coin)) else {
                    continue;
                };
                let Some(bbo) = bbo_from_levels(&data.bbo) else {
                    continue;
                };
                if tx.send((venue.clone(), pair_id.clone(), bbo)).is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

fn bbo_from_levels(levels: &[Option<WsLevel>]) -> Option<Bbo> {
    let bid_lv = levels.first().and_then(|v| v.as_ref())?;
    let ask_lv = levels.get(1).and_then(|v| v.as_ref())?;
    level_pair_to_bbo(bid_lv, ask_lv)
}

fn bbo_from_l2(levels: &[Vec<WsLevel>]) -> Option<Bbo> {
    let bid_lv = levels.first()?.first()?;
    let ask_lv = levels.get(1)?.first()?;
    level_pair_to_bbo(bid_lv, ask_lv)
}

fn level_pair_to_bbo(bid_lv: &WsLevel, ask_lv: &WsLevel) -> Option<Bbo> {
    let bid = Decimal::from_str(bid_lv.px.trim()).ok()?;
    let ask = Decimal::from_str(ask_lv.px.trim()).ok()?;
    let bid_qty = Decimal::from_str(bid_lv.sz.trim()).unwrap_or(Decimal::ZERO);
    let ask_qty = Decimal::from_str(ask_lv.sz.trim()).unwrap_or(Decimal::ZERO);
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

async fn poll_l2_books(
    rest: String,
    venue: VenueId,
    coins: Vec<(String, String)>,
    tx: BboTx,
) {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .user_agent("dex-arbitr/0.1")
        .build()
    else {
        return;
    };
    let info = info_url(&rest);
    let mut tick = tokio::time::interval(Duration::from_millis(800));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        for (coin, pair_id) in &coins {
            let body = serde_json::json!({"type": "l2Book", "coin": coin});
            let Ok(resp) = client.post(&info).json(&body).send().await else {
                continue;
            };
            let Ok(val) = resp.json::<Value>().await else {
                continue;
            };
            let Some(levels) = val.get("levels").and_then(|v| v.as_array()) else {
                continue;
            };
            let parsed: Vec<Vec<WsLevel>> = levels
                .iter()
                .filter_map(|side| serde_json::from_value(side.clone()).ok())
                .collect();
            let Some(bbo) = bbo_from_l2(&parsed) else {
                continue;
            };
            if tx.send((venue.clone(), pair_id.clone(), bbo)).is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn io_index_and_sndk_asset_id() {
        let dexes = serde_json::json!([
            null,
            {"name": "xyz"},
            {"name": "flx"},
            {"name": "vntl"},
            {"name": "hyna"},
            {"name": "km"},
            {"name": "abcd"},
            {"name": "cash"},
            {"name": "para"},
            {"name": "mkts"},
            {"name": "io"}
        ]);
        let idx = perp_dex_index(&dexes, "io").unwrap();
        assert_eq!(idx, 10);
        assert_eq!(hip3_asset_id(idx, 2), 200_002);
    }

    #[test]
    fn parses_io_universe_skips_delisted() {
        let body = serde_json::json!([
            {
                "universe": [
                    {"name": "io:OAI", "szDecimals": 3, "isDelisted": true},
                    {"name": "io:ANTH", "szDecimals": 3},
                    {"name": "io:SNDK", "szDecimals": 4}
                ]
            },
            []
        ]);
        let venue = VenueId::from("entropy");
        let markets = parse_io_markets(&venue, 10, &body, &[]).unwrap();
        assert_eq!(markets.len(), 2);
        let sndk = markets.iter().find(|m| m.base == "SNDK").unwrap();
        assert_eq!(sndk.raw_symbol, "io:SNDK");
        assert_eq!(sndk.pair_id, "SNDK-USD-PERP");
        assert_eq!(sndk.market_index, 200_002);
        assert_eq!(sndk.qty_precision, 4);
        assert_eq!(sndk.min_qty, dec!(0.0001));
    }

    #[test]
    fn bbo_levels_to_book() {
        let levels = vec![
            Some(WsLevel {
                px: "1472.7".into(),
                sz: "1.2".into(),
            }),
            Some(WsLevel {
                px: "1472.8".into(),
                sz: "0.5".into(),
            }),
        ];
        let bbo = bbo_from_levels(&levels).unwrap();
        assert_eq!(bbo.bid, dec!(1472.7));
        assert_eq!(bbo.ask, dec!(1472.8));
        assert_eq!(bbo.ask_qty, dec!(0.5));
    }

    #[test]
    fn l2_book_top_of_book() {
        let levels = vec![
            vec![WsLevel {
                px: "1479.7".into(),
                sz: "0.88".into(),
            }],
            vec![WsLevel {
                px: "1479.8".into(),
                sz: "0.06".into(),
            }],
        ];
        let bbo = bbo_from_l2(&levels).unwrap();
        assert_eq!(bbo.bid, dec!(1479.7));
        assert_eq!(bbo.ask, dec!(1479.8));
    }

    #[test]
    fn info_url_appends_or_keeps() {
        assert_eq!(
            info_url("https://api.hyperliquid.xyz"),
            "https://api.hyperliquid.xyz/info"
        );
        assert_eq!(
            info_url("https://api.hyperliquid.xyz/info"),
            "https://api.hyperliquid.xyz/info"
        );
    }
}
