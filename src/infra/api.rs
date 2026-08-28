use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use rust_decimal::Decimal;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use crate::app::control::{validate, ArbitrageControl, ArbitrageParams};
use crate::infra::journal::ExecRecord;

/// 单个已加载所的元数据，用于 `/api/venues` 响应。
#[derive(Debug, Clone, Serialize)]
pub struct VenueMeta {
    pub id: String,
    /// 人读名称（lighter → Lighter, lighter_rh → Lighter RH, sodex → SoDEX）
    pub label: String,
    /// 是否已配置私钥（可以真实下单）
    pub keys_ready: bool,
    /// 报价币
    pub quote: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PairRow {
    pub pair_id: String,
    pub buy: String,
    pub sell: String,
    pub raw_pct: String,
    pub net_pct: String,
    pub fee_pct: String,
    pub nat_pct: String,
    pub res_pct: String,
    pub entry_pct: String,
    pub dev_pct: String,
    /// 当前格宽 Δ（%）。未满点差窗时 C=0，只含费。
    #[serde(default)]
    pub delta_pct: String,
    pub grid: String,
    pub target_qty: String,
    pub actual_qty: String,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PositionRow {
    pub pair_id: String,
    pub buy: String,
    pub sell: String,
    pub qty: String,
    pub grid: i32,
    pub entry_notional: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VenueBalanceRow {
    pub venue: String,
    pub available: String,
    pub total: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExchangePositionRow {
    pub venue: String,
    pub symbol: String,
    pub qty: String,
    pub entry_price: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VenueLiveRow {
    pub venue: String,
    /// 满窗为点差中枢（%）；未满为 `n/cap`；还没采样为 `—`。
    pub spread_mu: String,
    /// 本次进程该所成交名义（USDC）。
    pub volume: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NakedExposureRow {
    pub pair_id: String,
    pub venue: String,
    pub qty: String,
    pub counterparty: String,
    pub source: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VenueMatchRow {
    pub left: String,
    pub right: String,
    pub n: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LiveSnapshot {
    pub pairs: Vec<PairRow>,
    pub positions: Vec<PositionRow>,
    pub balances: Vec<VenueBalanceRow>,
    pub exchange_positions: Vec<ExchangePositionRow>,
    /// 已加载所各一行：点差中枢 + 本次交易量。两所中枢的平均折进 Δ。
    #[serde(default)]
    pub venue_stats: Vec<VenueLiveRow>,
    pub naked_exposures: Vec<NakedExposureRow>,
    #[serde(default)]
    pub venue_matches: Vec<VenueMatchRow>,
    pub stats: ApiStats,
    pub monitor_only: bool,
    pub paper_trading: bool,
    /// 套利开关当前状态（供 UI 轮询用）。
    pub arbitrage_enabled: bool,
    /// 正在按所选 DEX 拉市场并匹配。
    #[serde(default)]
    pub matching: bool,
    /// 启动时加载的交集全集（不订阅、不进决策）。
    #[serde(default)]
    pub available: Vec<AvailableSymbol>,
    /// 本次进程执行带上各笔平仓实际盈亏之和（两所账户权益差）。
    #[serde(default)]
    pub session_pnl_usdc: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AvailableSymbol {
    pub pair_id: String,
    pub symbol: String,
    pub venue_pairs: Vec<AvailableVenuePair>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AvailableVenuePair {
    pub venues: Vec<String>,
    pub min_qty: String,
    pub qty_precision: u32,
    pub round_trip_fee_pct: String,
    pub mid: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ApiStats {
    pub matched_pairs: usize,
    pub open_positions: usize,
    pub best_net_pct: Option<String>,
}

#[derive(Clone)]
pub struct ApiHub {
    pub state: Arc<RwLock<LiveSnapshot>>,
    /// 本次进程的执行带。不落盘，重启即空。
    session_execs: Arc<Mutex<Vec<ExecRecord>>>,
    pub web_root: PathBuf,
    pub auth_token: Option<String>,
    /// 运行时套利开关与热改参数，与 Controller 共享。
    pub control: Arc<Mutex<ArbitrageControl>>,
    /// 进程启动时从 yaml 拷出的默认值，页面「重置」用。
    pub yaml_defaults: ArbitrageParams,
    /// 已加载所的元数据（只读，进程启动时固定）。
    pub venues: Vec<VenueMeta>,
}

impl ApiHub {
    pub fn new(
        web_root: PathBuf,
        auth_token: Option<String>,
        control: Arc<Mutex<ArbitrageControl>>,
        venues: Vec<VenueMeta>,
        yaml_defaults: ArbitrageParams,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(LiveSnapshot::default())),
            session_execs: Arc::new(Mutex::new(Vec::new())),
            web_root,
            auth_token,
            control,
            yaml_defaults,
            venues,
        }
    }

    pub fn publish(&self, snap: LiveSnapshot) {
        if let Ok(mut w) = self.state.write() {
            *w = snap;
        }
    }

    pub fn push_execution(&self, rec: ExecRecord) {
        if let Ok(mut v) = self.session_execs.lock() {
            v.insert(0, rec);
            const MAX: usize = 200;
            if v.len() > MAX {
                v.truncate(MAX);
            }
        }
    }

    fn recent_executions(&self, limit: usize) -> Vec<ExecRecord> {
        self.session_execs
            .lock()
            .map(|v| v.iter().take(limit).cloned().collect())
            .unwrap_or_default()
    }

    /// 本次运行已记账的平仓实际盈亏之和（执行带 close 记录的 pnl_usdc）。
    pub fn session_pnl_usdc(&self) -> Decimal {
        self.session_execs
            .lock()
            .map(|v| {
                v.iter()
                    .filter(|r| r.action == "close")
                    .filter_map(|r| r.pnl_usdc)
                    .sum()
            })
            .unwrap_or(Decimal::ZERO)
    }

    pub fn spawn(self: Arc<Self>, bind: &str) -> tokio::task::JoinHandle<()> {
        let mut addr: SocketAddr = bind.parse().expect("invalid http.bind");
        let token_set = self
            .auth_token
            .as_deref()
            .map(|t| !t.trim().is_empty())
            .unwrap_or(false);
        if !token_set && !addr.ip().is_loopback() {
            tracing::warn!(
                configured = %addr,
                "http.auth_token empty; refusing public bind, falling back to 127.0.0.1"
            );
            addr.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        }
        let hub = self.clone();
        tokio::spawn(async move {
            let api = Router::new()
                .route("/api/health", get(health))
                .route("/api/pairs", get(pairs))
                .route("/api/positions", get(positions))
                .route("/api/executions", get(executions))
                .route("/api/snapshot", get(snapshot))
                .route("/api/config", get(get_config).post(post_config))
                .route("/api/config/defaults", get(get_config_defaults))
                .route("/api/arbitrage/start", post(arbitrage_start))
                .route("/api/arbitrage/stop", post(arbitrage_stop))
                .route("/api/venues", get(get_venues))
                .route("/api/pairs/available", get(available_pairs))
                .route_layer(middleware::from_fn_with_state(
                    hub.clone(),
                    require_api_auth,
                ))
                .with_state(hub.clone());

            let app = Router::new()
                .merge(api)
                .fallback_service(
                    ServeDir::new(hub.web_root.clone())
                        .not_found_service(ServeFile::new(hub.web_root.join("index.html"))),
                )
                .layer(CorsLayer::permissive());
            tracing::info!(%addr, "http api listening");
            let listener = tokio::net::TcpListener::bind(addr).await.expect("bind http");
            if axum::serve(listener, app).await.is_err() {
                tracing::warn!("http server stopped");
            }
        })
    }
}

async fn require_api_auth(
    State(hub): State<Arc<ApiHub>>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = hub.auth_token.as_deref().filter(|t| !t.is_empty()) else {
        return next.run(req).await;
    };
    let got = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if got != expected {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({
            "ok": false,
            "error": "invalid or missing bearer token"
        })))
            .into_response();
    }
    next.run(req).await
}

async fn health(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    let updated = hub.state.read().map(|s| s.updated_at).unwrap_or(0);
    Json(serde_json::json!({ "ok": true, "updated_at": updated }))
}

async fn snapshot(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    match hub.state.read() {
        Ok(s) => Json(s.clone()).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "lock poisoned").into_response(),
    }
}

async fn pairs(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    match hub.state.read() {
        Ok(s) => Json(serde_json::json!({ "pairs": s.pairs, "stats": s.stats })).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "lock poisoned").into_response(),
    }
}

async fn positions(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    match hub.state.read() {
        Ok(s) => Json(serde_json::json!({
            "positions": s.positions,
            "balances": s.balances,
            "exchange_positions": s.exchange_positions,
            "venue_stats": s.venue_stats,
            "naked_exposures": s.naked_exposures,
        }))
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "lock poisoned").into_response(),
    }
}

#[derive(Serialize)]
struct ExecRow {
    ts: i64,
    pair_id: String,
    action: String,
    buy_venue: String,
    sell_venue: String,
    qty: String,
    net_pct: Option<String>,
    result: String,
    detail: String,
    grid_from: Option<i32>,
    grid_to: Option<i32>,
    pnl_usdc: Option<String>,
    pnl_pct: Option<String>,
}

impl From<ExecRecord> for ExecRow {
    fn from(r: ExecRecord) -> Self {
        Self {
            ts: r.ts,
            pair_id: r.pair_id,
            action: r.action,
            buy_venue: r.buy_venue,
            sell_venue: r.sell_venue,
            qty: fmt_qty(r.qty),
            net_pct: r.net_pct.map(|v| format!("{:.6}", v.round_dp(6))),
            result: r.result,
            detail: r.detail,
            grid_from: r.grid_from,
            grid_to: r.grid_to,
            pnl_usdc: r.pnl_usdc.map(|v| format!("{:.4}", v.round_dp(4))),
            pnl_pct: r.pnl_pct.map(|v| format!("{:.6}", v.round_dp(6))),
        }
    }
}

async fn executions(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    let out: Vec<ExecRow> = hub
        .recent_executions(50)
        .into_iter()
        .map(Into::into)
        .collect();
    Json(serde_json::json!({ "executions": out })).into_response()
}

async fn available_pairs(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    match hub.state.read() {
        Ok(s) => Json(serde_json::json!({ "pairs": s.available })).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "lock poisoned").into_response(),
    }
}

async fn get_venues(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    let active = hub
        .control
        .lock()
        .map(|c| c.params.active_venues.clone())
        .unwrap_or_default();
    let venues: Vec<_> = hub
        .venues
        .iter()
        .map(|v| {
            serde_json::json!({
                "id": v.id,
                "label": v.label,
                "keys_ready": v.keys_ready,
                "quote": v.quote,
                "active": active.contains(&v.id),
            })
        })
        .collect();
    Json(serde_json::json!({ "venues": venues }))
}

pub fn fmt_pct(v: Decimal) -> String {
    let sign = if v >= Decimal::ZERO { "+" } else { "" };
    format!("{sign}{:.3}%", v)
}

pub fn fmt_qty(v: Decimal) -> String {
    format!("{:.6}", v.round_dp(6))
}

// ── Control endpoints ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct ConfigResponse {
    enabled: bool,
    params: ArbitrageParams,
}

async fn get_config(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    let ctrl = hub.control.lock().unwrap_or_else(|e| e.into_inner());
    Json(ConfigResponse {
        enabled: ctrl.enabled,
        params: ctrl.params.clone(),
    })
}

async fn get_config_defaults(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    Json(ConfigResponse {
        enabled: false,
        params: hub.yaml_defaults.clone(),
    })
}

async fn post_config(
    State(hub): State<Arc<ApiHub>>,
    Json(params): Json<ArbitrageParams>,
) -> impl IntoResponse {
    let v = validate(&params);
    if !v.ok {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({ "ok": false, "errors": v.errors }))).into_response();
    }
    {
        let mut ctrl = hub.control.lock().unwrap_or_else(|e| e.into_inner());
        ctrl.params = params;
    }
    tracing::info!("arbitrage params updated via API");
    Json(serde_json::json!({ "ok": true })).into_response()
}

async fn arbitrage_start(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    {
        let mut ctrl = hub.control.lock().unwrap_or_else(|e| e.into_inner());
        let n = ctrl.params.active_venues.len();
        if n < 2 {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "ok": false,
                    "errors": ["请至少选择两个交易所再启动"]
                })),
            )
                .into_response();
        }
        if ctrl.params.pairs.is_empty() {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "ok": false,
                    "errors": ["请至少填写一个交易对和每格数量再启动"]
                })),
            )
                .into_response();
        }
        ctrl.enabled = true;
        ctrl.rematch = true;
    }
    tracing::info!("arbitrage enabled via API; pair match queued");
    Json(serde_json::json!({ "ok": true, "enabled": true })).into_response()
}

async fn arbitrage_stop(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    {
        let mut ctrl = hub.control.lock().unwrap_or_else(|e| e.into_inner());
        ctrl.enabled = false;
    }
    tracing::info!("arbitrage disabled via API");
    Json(serde_json::json!({ "ok": true, "enabled": false }))
}
