use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, warn};

use super::port::{AccountSnapshot, Balance, CancelReq, OrderAck, OrderReq, OrderStatus, VenuePosition};

const SIDECAR_DIR: &str = "scripts/exchange_sidecar";
const DEFAULT_SIDECAR_TIMEOUT: Duration = Duration::from_secs(45);

fn sidecar_timeout() -> Duration {
    std::env::var("DEX_SIDECAR_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_SIDECAR_TIMEOUT)
}

#[derive(Debug, Deserialize)]
struct BridgeResp {
    ok: bool,
    #[serde(default)]
    error: String,
    #[serde(default)]
    data: Value,
}

/// 统一 Go sidecar（Lighter + SoDEX），对齐 internal/exchange/。
fn sidecar_binary() -> PathBuf {
    if let Ok(p) = std::env::var("DEX_EXCHANGE_SIDECAR") {
        return PathBuf::from(p);
    }
    for name in ["exchange_sidecar.exe", "exchange_sidecar"] {
        let p = Path::new(SIDECAR_DIR).join(name);
        if p.exists() {
            return p;
        }
    }
    Path::new(SIDECAR_DIR).join(if cfg!(windows) {
        "exchange_sidecar.exe"
    } else {
        "exchange_sidecar"
    })
}

pub async fn bridge_available() -> bool {
    sidecar_binary().exists()
}

pub async fn bridge_call(venue_yaml: &Path, cmd: &str, params: Value) -> Result<Value> {
    let bin = sidecar_binary();
    if !bin.exists() {
        anyhow::bail!(
            "missing {}; build: cd scripts/exchange_sidecar && go build -o exchange_sidecar .",
            bin.display()
        );
    }
    let payload = json!({
        "cmd": cmd,
        "venue_yaml": venue_yaml.to_string_lossy(),
        "params": params,
    });
    let mut child = Command::new(&bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn exchange sidecar {}", bin.display()))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(payload.to_string().as_bytes())
            .await
            .context("write sidecar stdin")?;
    }
    let wait = async move {
        child
            .wait_with_output()
            .await
            .context("sidecar wait")
    };
    let out = match tokio::time::timeout(sidecar_timeout(), wait).await {
        Ok(r) => r?,
        Err(_) => {
            warn!(
                cmd,
                timeout_secs = sidecar_timeout().as_secs(),
                "exchange sidecar timed out"
            );
            anyhow::bail!(
                "sidecar {cmd} timed out after {}s",
                sidecar_timeout().as_secs()
            );
        }
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        warn!(cmd, stderr = %stderr, stdout = %stdout, "exchange sidecar failed");
        anyhow::bail!("sidecar {cmd} exit {}: {stderr}", out.status);
    }
    let resp: BridgeResp = serde_json::from_str(stdout.trim()).context("parse sidecar json")?;
    if !resp.ok {
        anyhow::bail!("sidecar {cmd}: {}", resp.error);
    }
    debug!(cmd, "sidecar ok");
    Ok(resp.data)
}

pub async fn bridge_account(venue_yaml: &Path) -> Result<AccountSnapshot> {
    let data = bridge_call(venue_yaml, "account", json!({})).await?;
    Ok(parse_account(&data))
}

pub async fn bridge_balances(venue_yaml: &Path) -> Result<Vec<Balance>> {
    Ok(bridge_account(venue_yaml).await?.balances)
}

pub async fn bridge_positions(venue_yaml: &Path) -> Result<Vec<VenuePosition>> {
    Ok(bridge_account(venue_yaml).await?.positions)
}

pub async fn bridge_place(venue_yaml: &Path, req: &OrderReq) -> Result<OrderAck> {
    let style = match req.style {
        crate::config::OrderStyle::LimitMaker | crate::config::OrderStyle::LimitThenMarket => {
            "limit"
        }
        crate::config::OrderStyle::MarketTaker => "market",
    };
    let params = json!({
        "symbol": req.symbol,
        "market_index": req.market_index,
        "is_buy": req.is_buy,
        "qty": req.qty.to_string(),
        "reduce_only": req.reduce_only,
        "style": style,
        "limit_price": req.limit_price.map(|p| p.to_string()),
        "client_order_id": req.client_order_id,
    });
    let data = bridge_call(venue_yaml, "place", params).await?;
    parse_order_ack(&data)
}

pub async fn bridge_cancel(venue_yaml: &Path, req: &CancelReq) -> Result<()> {
    let params = json!({
        "order_id": req.order_id,
        "symbol": req.symbol,
        "market_index": req.market_index,
    });
    bridge_call(venue_yaml, "cancel", params).await?;
    Ok(())
}

pub async fn bridge_order_status(
    venue_yaml: &Path,
    req: &CancelReq,
    qty: Decimal,
) -> Result<OrderAck> {
    let params = json!({
        "order_id": req.order_id,
        "symbol": req.symbol,
        "market_index": req.market_index,
        "qty": qty.to_string(),
    });
    let data = bridge_call(venue_yaml, "order_status", params).await?;
    parse_order_ack(&data)
}

fn parse_account(data: &Value) -> AccountSnapshot {
    let balances = data
        .get("balances")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|b| {
                    Some(Balance {
                        asset: b.get("asset")?.as_str()?.to_string(),
                        available: dec(b.get("available")?)?,
                        total: b.get("total").and_then(dec),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let positions = data
        .get("positions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| {
                    Some(VenuePosition {
                        symbol: p.get("symbol")?.as_str()?.to_string(),
                        qty: dec(p.get("qty")?)?,
                        entry_price: p.get("entry_price").and_then(dec),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    AccountSnapshot {
        balances,
        positions,
    }
}

fn parse_order_ack(data: &Value) -> Result<OrderAck> {
    let status = match data.get("status").and_then(|v| v.as_str()).unwrap_or("filled") {
        "accepted" => OrderStatus::Accepted,
        "partial" => OrderStatus::Partial,
        "canceled" => OrderStatus::Canceled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Filled,
    };
    Ok(OrderAck {
        order_id: data
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        client_order_id: data
            .get("client_order_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        filled_qty: data
            .get("filled_qty")
            .and_then(dec)
            .unwrap_or(Decimal::ZERO),
        avg_price: data.get("avg_price").and_then(dec),
        status,
    })
}

fn dec(v: &Value) -> Option<Decimal> {
    if let Some(s) = v.as_str() {
        return Decimal::from_str(s).ok();
    }
    v.as_f64()
        .and_then(|f| Decimal::from_str(&f.to_string()).ok())
}
