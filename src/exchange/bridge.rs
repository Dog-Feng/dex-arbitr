use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{broadcast, oneshot};
use tracing::{debug, info, warn};

use super::port::{
    AccountSnapshot, Balance, CancelReq, FillPnl, FundingRate, OrderAck, OrderReq, OrderStatus,
    VenuePosition,
};

const SIDECAR_DIR: &str = "scripts/exchange_sidecar";
/// 写操作（place / cancel）。
///
/// 必须盖住 sidecar 侧最长的成交确认窗口，否则这边先超时、那边还在轮询，
/// 得到的就是「下单其实成交了但本地当没成交」——幻影成交的反向版本。
/// sidecar 的 Lighter 市价腿窗口是 60s（对齐参考的
/// `lighter_market_order_timeout`），其 requestTimeout 为 75s，这里再留
/// 一点余量。想收紧就用 `DEX_SIDECAR_TIMEOUT_SECS` 覆盖。
const WRITE_SIDECAR_TIMEOUT: Duration = Duration::from_secs(80);
/// 只读查询：幂等，不能拖住决策环。
const QUERY_SIDECAR_TIMEOUT: Duration = Duration::from_secs(12);

fn sidecar_timeout(cmd: &str) -> Duration {
    if let Some(secs) = std::env::var("DEX_SIDECAR_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return Duration::from_secs(secs);
    }
    match cmd {
        "place" | "cancel" => WRITE_SIDECAR_TIMEOUT,
        _ => QUERY_SIDECAR_TIMEOUT,
    }
}

/// sidecar 主动推送的订单更新（私有 WS 订单流）。
#[derive(Debug, Clone)]
pub struct OrderPush {
    pub venue: String,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
struct BridgeResp {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: String,
    #[serde(default)]
    data: Value,
    /// 非空表示这是主动推送而不是响应。
    #[serde(default)]
    push: String,
    #[serde(default)]
    venue: String,
}

type Pending = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>>;

/// 常驻 sidecar 连接。进程、认证、市场元数据全程复用。
///
/// 旧实现每次调用起一个进程：光进程启动 33ms，加上全新 TLS 握手和重新
/// 认证，单次 200~800ms。成交检测靠轮询，每轮都要付这个开销，
/// 「第一腿成交 → 第二腿下单」窗口因此远超 1 秒。
struct Sidecar {
    stdin: tokio::sync::Mutex<ChildStdin>,
    pending: Pending,
    next_id: AtomicI64,
    pushes: broadcast::Sender<OrderPush>,
    /// 读循环发现进程退出后置位。下一次调用据此重建。
    dead: Arc<AtomicBool>,
    _child: Child,
}

impl Sidecar {
    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }
}

/// 当前活跃连接。子进程挂掉后置 None，下次调用重建。
///
/// 不能用 `OnceLock`：它只初始化一次，进程死了就永久失效——持仓期间
/// 遇上就意味着平仓指令再也发不出去，仓位一直裸着没人管。
static SIDECAR: Mutex<Option<Arc<Sidecar>>> = Mutex::new(None);
/// 已启动过 WS 订单流的 venue。重启后要重新订阅，否则成交检测退化成纯轮询。
static WATCHED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
/// 各所最近一次下单：Rust 发起到 sidecar 拿到 DEX 回包的墙钟。
static LAST_PLACE_RTT: Mutex<Option<HashMap<String, PlaceChainRtt>>> = Mutex::new(None);

/// 下单链路耗时。Lighter 另有 Go 侧签名→sendTx 分解。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceChainRtt {
    pub wall_ms: u64,
    pub sign_ms: Option<u64>,
    pub send_ms: Option<u64>,
    pub sign_to_ack_ms: Option<u64>,
}

/// Lighter 下单：Go 签名开始到 `/api/v1/sendTx` 确认回包。不含等锁、拉 nonce、成交回查。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LighterPlaceRtt {
    pub sign_ms: u64,
    pub send_ms: u64,
    pub sign_to_ack_ms: u64,
}

pub fn last_place_rtt(venue_id: &str) -> Option<PlaceChainRtt> {
    LAST_PLACE_RTT
        .lock()
        .ok()?
        .as_ref()?
        .get(venue_id)
        .copied()
}

pub fn last_lighter_place_rtt(venue_id: &str) -> Option<LighterPlaceRtt> {
    let r = last_place_rtt(venue_id)?;
    Some(LighterPlaceRtt {
        sign_ms: r.sign_ms?,
        send_ms: r.send_ms?,
        sign_to_ack_ms: r.sign_to_ack_ms?,
    })
}

pub fn note_place_rtt(venue_id: &str, rtt: PlaceChainRtt) {
    if let Ok(mut g) = LAST_PLACE_RTT.lock() {
        g.get_or_insert_with(HashMap::new)
            .insert(venue_id.to_string(), rtt);
    }
}

pub fn note_lighter_place_rtt(venue_id: &str, rtt: LighterPlaceRtt) {
    let mut row = last_place_rtt(venue_id).unwrap_or(PlaceChainRtt {
        wall_ms: rtt.sign_to_ack_ms,
        sign_ms: None,
        send_ms: None,
        sign_to_ack_ms: None,
    });
    row.sign_ms = Some(rtt.sign_ms);
    row.send_ms = Some(rtt.send_ms);
    row.sign_to_ack_ms = Some(rtt.sign_to_ack_ms);
    note_place_rtt(venue_id, row);
}

pub fn parse_lighter_place_rtt_line(line: &str) -> Option<(String, LighterPlaceRtt)> {
    let line = line.trim();
    if !line.starts_with("lighter place rtt ") {
        return None;
    }
    let mut venue = None;
    let mut sign_ms = None;
    let mut send_ms = None;
    let mut total_ms = None;
    for part in line.split_whitespace() {
        if let Some(v) = part.strip_prefix("venue=") {
            venue = Some(v.to_string());
        } else if let Some(v) = part.strip_prefix("sign_ms=") {
            sign_ms = v.parse().ok();
        } else if let Some(v) = part.strip_prefix("send_ms=") {
            send_ms = v.parse().ok();
        } else if let Some(v) = part.strip_prefix("sign_to_ack_ms=") {
            total_ms = v.parse().ok();
        }
    }
    Some((
        venue?,
        LighterPlaceRtt {
            sign_ms: sign_ms?,
            send_ms: send_ms?,
            sign_to_ack_ms: total_ms?,
        },
    ))
}

fn json_ms(data: &Value, key: &str) -> Option<u64> {
    let v = data.get(key)?;
    if let Some(s) = v.as_str() {
        return s.parse().ok();
    }
    v.as_u64()
}

fn venue_id_from_yaml(path: &Path) -> String {
    match path.file_stem().and_then(|s| s.to_str()).unwrap_or("") {
        "lighter_robinhood" => "lighter_rh".into(),
        other => other.into(),
    }
}

fn note_rtt_from_place_json(venue_yaml: &Path, data: &Value, wall_ms: u64) {
    let venue = venue_id_from_yaml(venue_yaml);
    let rtt = PlaceChainRtt {
        wall_ms,
        sign_ms: json_ms(data, "sign_ms"),
        send_ms: json_ms(data, "send_ms"),
        sign_to_ack_ms: json_ms(data, "sign_to_ack_ms"),
    };
    note_place_rtt(&venue, rtt);
}

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

/// 订阅 sidecar 的订单推送。成交检测用它取代 REST 轮询。
///
/// 注意：sidecar 重启后 `pushes` 是新的 channel，旧 receiver 会收到
/// `RecvError::Closed`。调用方应在收到错误后重新订阅。
pub fn subscribe_order_pushes() -> Option<broadcast::Receiver<OrderPush>> {
    sidecar().ok().map(|s| s.pushes.subscribe())
}

/// 取当前连接；没有或已死就重建。
fn sidecar() -> Result<Arc<Sidecar>, String> {
    let mut guard = SIDECAR
        .lock()
        .map_err(|_| "sidecar registry poisoned".to_string())?;
    if let Some(sc) = guard.as_ref() {
        if !sc.is_dead() {
            return Ok(sc.clone());
        }
        warn!("exchange sidecar died; restarting");
    }
    let sc = spawn_sidecar()?;
    *guard = Some(sc.clone());
    drop(guard);

    // 重启后补订阅：WS 流在新进程里是空的。
    let watched: Vec<PathBuf> = match WATCHED.lock() {
        Ok(w) => w.clone(),
        Err(e) => {
            warn!("WATCHED lock poisoned; recovering");
            e.into_inner().clone()
        }
    };
    if !watched.is_empty() {
        let sc2 = sc.clone();
        tokio::spawn(async move {
            for path in watched {
                if let Err(err) = call_on(&sc2, &path, "watch", json!({})).await {
                    warn!(venue = %path.display(), error = %err, "resubscribe order stream failed");
                } else {
                    tracing::info!(venue = %path.display(), "order stream resubscribed");
                }
            }
        });
    }
    Ok(sc)
}

fn spawn_sidecar() -> Result<Arc<Sidecar>, String> {
    let bin = sidecar_binary();
    if !bin.exists() {
        return Err(format!(
            "missing {}; build: cd scripts/exchange_sidecar && go build -o exchange_sidecar .",
            bin.display()
        ));
    }
    let mut child = Command::new(&bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn exchange sidecar {}: {e}", bin.display()))?;

    let stdin = child.stdin.take().ok_or("sidecar stdin unavailable")?;
    let stdout = child.stdout.take().ok_or("sidecar stdout unavailable")?;
    let stderr = child.stderr.take();

    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let (pushes, _) = broadcast::channel(1024);
    let dead = Arc::new(AtomicBool::new(false));

    // 读循环：按 id 把响应投递回等待方，push 广播给订阅者。
    let reader_pending = pending.clone();
    let reader_pushes = pushes.clone();
    let reader_dead = dead.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }
                    let resp: BridgeResp = match serde_json::from_str(&line) {
                        Ok(r) => r,
                        Err(err) => {
                            warn!(error = %err, line = %line, "sidecar: bad json line");
                            continue;
                        }
                    };
                    if !resp.push.is_empty() {
                        let _ = reader_pushes.send(OrderPush {
                            venue: resp.venue,
                            data: resp.data,
                        });
                        continue;
                    }
                    let tx = reader_pending.lock().ok().and_then(|mut m| m.remove(&resp.id));
                    if let Some(tx) = tx {
                        let out = if resp.ok {
                            Ok(resp.data)
                        } else {
                            Err(resp.error)
                        };
                        let _ = tx.send(out);
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(error = %err, "sidecar stdout closed");
                    break;
                }
            }
        }
        // 进程没了：标记死亡（下次调用会重建），并唤醒所有等待方，
        // 避免它们卡到超时。
        reader_dead.store(true, Ordering::Relaxed);
        if let Ok(mut m) = reader_pending.lock() {
            for (_, tx) in m.drain() {
                let _ = tx.send(Err("sidecar process exited".into()));
            }
        }
        tracing::error!("exchange sidecar reader stopped; will restart on next call");
    });

    if let Some(stderr) = stderr {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some((venue, rtt)) = parse_lighter_place_rtt_line(&line) {
                    note_lighter_place_rtt(&venue, rtt);
                    info!(
                        target: "sidecar",
                        venue = %venue,
                        sign_ms = rtt.sign_ms,
                        send_ms = rtt.send_ms,
                        sign_to_ack_ms = rtt.sign_to_ack_ms,
                        "{line}"
                    );
                } else {
                    warn!(target: "sidecar", "{line}");
                }
            }
        });
    }

    Ok(Arc::new(Sidecar {
        stdin: tokio::sync::Mutex::new(stdin),
        pending,
        next_id: AtomicI64::new(1),
        pushes,
        dead,
        _child: child,
    }))
}

pub async fn bridge_call(venue_yaml: &Path, cmd: &str, params: Value) -> Result<Value> {
    let sc = sidecar().map_err(|e| anyhow::anyhow!("{e}"))?;
    call_on(&sc, venue_yaml, cmd, params).await
}

async fn call_on(sc: &Arc<Sidecar>, venue_yaml: &Path, cmd: &str, params: Value) -> Result<Value> {
    let id = sc.next_id.fetch_add(1, Ordering::Relaxed);
    let payload = json!({
        "id": id,
        "cmd": cmd,
        "venue_yaml": venue_yaml.to_string_lossy(),
        "params": params,
    });

    let (tx, rx) = oneshot::channel();
    sc.pending
        .lock()
        .map_err(|_| anyhow::anyhow!("sidecar pending lock poisoned"))?
        .insert(id, tx);

    let mut line = payload.to_string();
    line.push('\n');
    {
        let mut stdin = sc.stdin.lock().await;
        if let Err(err) = stdin.write_all(line.as_bytes()).await {
            sc.pending.lock().ok().and_then(|mut m| m.remove(&id));
            sc.dead.store(true, Ordering::Relaxed);
            return Err(err).context("write sidecar stdin");
        }
        if let Err(err) = stdin.flush().await {
            sc.pending.lock().ok().and_then(|mut m| m.remove(&id));
            sc.dead.store(true, Ordering::Relaxed);
            return Err(err).context("flush sidecar stdin");
        }
    }

    let timeout = sidecar_timeout(cmd);
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(Ok(data))) => {
            debug!(cmd, "sidecar ok");
            Ok(data)
        }
        Ok(Ok(Err(err))) => anyhow::bail!("sidecar {cmd}: {err}"),
        Ok(Err(_)) => anyhow::bail!("sidecar {cmd}: response channel dropped"),
        Err(_) => {
            sc.pending.lock().ok().and_then(|mut m| m.remove(&id));
            warn!(cmd, timeout_secs = timeout.as_secs(), "exchange sidecar timed out");
            anyhow::bail!("sidecar {cmd} timed out after {}s", timeout.as_secs());
        }
    }
}

/// 启动该 venue 的私有 WS 订单流。幂等，并登记以便 sidecar 重启后自动补订阅。
pub async fn bridge_watch(venue_yaml: &Path) -> Result<()> {
    bridge_call(venue_yaml, "watch", json!({})).await?;
    let mut w = match WATCHED.lock() {
        Ok(g) => g,
        Err(e) => {
            warn!("WATCHED lock poisoned; recovering");
            e.into_inner()
        }
    };
    if !w.iter().any(|p| p == venue_yaml) {
        w.push(venue_yaml.to_path_buf());
    }
    Ok(())
}

pub async fn bridge_account(venue_yaml: &Path) -> Result<AccountSnapshot> {
    let data = bridge_call(venue_yaml, "account", json!({})).await?;
    Ok(parse_account(&data))
}

pub async fn bridge_balances(venue_yaml: &Path) -> Result<Vec<Balance>> {
    Ok(bridge_account(venue_yaml).await?.balances)
}

pub async fn bridge_funding(venue_yaml: &Path) -> Result<Vec<FundingRate>> {
    let data = bridge_call(venue_yaml, "funding", json!({})).await?;
    Ok(parse_funding(&data))
}

fn parse_funding(data: &Value) -> Vec<FundingRate> {
    data.get("rates")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|r| {
                    // interval_secs 缺失/为 0 时丢掉这一行：年化系数除以它，
                    // 0 会让整条记录变成无意义的数。宁缺勿错。
                    let interval = r.get("interval_secs").and_then(|v| v.as_u64())?;
                    if interval == 0 {
                        return None;
                    }
                    Some(FundingRate {
                        symbol: r.get("symbol")?.as_str()?.to_string(),
                        rate: dec(r.get("rate")?)?,
                        interval_secs: interval as u32,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub async fn bridge_positions(venue_yaml: &Path) -> Result<Vec<VenuePosition>> {
    Ok(bridge_account(venue_yaml).await?.positions)
}

pub async fn bridge_fill_pnl(
    venue_yaml: &Path,
    symbol: &str,
    order_id: Option<&str>,
) -> Result<FillPnl> {
    let params = json!({
        "symbol": symbol,
        "order_id": order_id.unwrap_or(""),
    });
    let data = bridge_call(venue_yaml, "fill_pnl", params).await?;
    Ok(FillPnl {
        realized_pnl: data.get("realized_pnl").and_then(dec).unwrap_or(Decimal::ZERO),
        per_fill: data.get("per_fill").and_then(|v| v.as_bool()).unwrap_or(false),
        found: data.get("found").and_then(|v| v.as_bool()).unwrap_or(false),
    })
}

pub async fn bridge_place(venue_yaml: &Path, req: &OrderReq) -> Result<OrderAck> {
    let style = match req.style {
        crate::config::OrderStyle::LimitMaker | crate::config::OrderStyle::LimitThenMarket => {
            "limit"
        }
        crate::config::OrderStyle::MarketTaker => "market",
        // 独立的 style，**不能**并进 "limit"：sidecar 对 "limit" 走单次回查
        // （限价单会驻留，查一次就够），而 IOC 不驻留，单次查询多半看到 0，
        // 分不清「整单撤销」还是「索引没跟上」。走自己的分支才能拿到轮询确认。
        crate::config::OrderStyle::AggressiveLimit => "aggressive_limit",
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
        // 市价单滑点保护：基准是决策信号价，超出交易所拒单。
        "target_price": req.target_price.map(|p| p.to_string()),
        "slippage_pct": req.slippage_pct.map(|p| p.to_string()),
        "fill_wait_ms": req.fill_wait_ms,
    });
    let t0 = Instant::now();
    let data = bridge_call(venue_yaml, "place", params).await?;
    let wall_ms = t0.elapsed().as_millis() as u64;
    note_rtt_from_place_json(venue_yaml, &data, wall_ms);
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
                        realized_pnl: p.get("realized_pnl").and_then(dec),
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
    let status = match data.get("status").and_then(|v| v.as_str()).unwrap_or("unknown") {
        "accepted" => OrderStatus::Accepted,
        "partial" => OrderStatus::Partial,
        "filled" => OrderStatus::Filled,
        "canceled" => OrderStatus::Canceled,
        "rejected" => OrderStatus::Rejected,
        "unknown" | "not_found" => OrderStatus::Unknown,
        _ => OrderStatus::Unknown,
    };
    let order_id = data
        .get("order_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if order_id.is_empty()
        && matches!(
            status,
            OrderStatus::Accepted | OrderStatus::Partial | OrderStatus::Filled
        )
    {
        anyhow::bail!("sidecar place returned empty order_id");
    }
    Ok(OrderAck {
        order_id,
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
    // JSON number 路径：先尝试从原始 JSON 表示解析，避免 f64 中间格式丢精度。
    // serde_json 在 arbitrary_precision 特性下保留原始字符串；
    // 没开启时退回 f64，通过 to_string() 重建字符串后解析——精度损失仍限于
    // f64 的 15-17 有效位，对价格和数量已够用但不完美。
    if let Some(n) = v.as_number() {
        // 优先用原始 JSON 数值字符串（arbitrary_precision 模式下可用）
        let s = n.to_string();
        if let Ok(d) = Decimal::from_str(&s) {
            return Some(d);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_lighter_place_rtt_line() {
        let line = "lighter place rtt venue=lighter order=42 sign_ms=3 send_ms=41 sign_to_ack_ms=44 result=ok";
        let (venue, rtt) = parse_lighter_place_rtt_line(line).expect("parse");
        assert_eq!(venue, "lighter");
        assert_eq!(
            rtt,
            LighterPlaceRtt {
                sign_ms: 3,
                send_ms: 41,
                sign_to_ack_ms: 44,
            }
        );
        assert!(parse_lighter_place_rtt_line("sidecar panic").is_none());
    }

    #[test]
    fn venue_id_from_lighter_yaml_names() {
        assert_eq!(
            venue_id_from_yaml(Path::new("config/venues/lighter.yaml")),
            "lighter"
        );
        assert_eq!(
            venue_id_from_yaml(Path::new("config/venues/lighter_robinhood.yaml")),
            "lighter_rh"
        );
    }
}
