use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use rust_decimal::Decimal;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use crate::infra::journal::{ExecJournal, ExecRecord};

#[derive(Debug, Clone, Default, Serialize)]
pub struct PairRow {
    pub pair_id: String,
    pub buy: String,
    pub sell: String,
    pub raw_pct: String,
    pub net_pct: String,
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
    pub grid: u32,
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
pub struct NakedExposureRow {
    pub pair_id: String,
    pub venue: String,
    pub qty: String,
    pub counterparty: String,
    pub source: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LiveSnapshot {
    pub pairs: Vec<PairRow>,
    pub positions: Vec<PositionRow>,
    pub balances: Vec<VenueBalanceRow>,
    pub exchange_positions: Vec<ExchangePositionRow>,
    pub naked_exposures: Vec<NakedExposureRow>,
    pub stats: ApiStats,
    pub monitor_only: bool,
    pub paper_trading: bool,
    pub updated_at: i64,
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
    pub journal_path: Option<String>,
    pub web_root: PathBuf,
    pub auth_token: Option<String>,
}

impl ApiHub {
    pub fn new(
        journal_path: Option<String>,
        web_root: PathBuf,
        auth_token: Option<String>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(LiveSnapshot::default())),
            journal_path,
            web_root,
            auth_token,
        }
    }

    pub fn publish(&self, snap: LiveSnapshot) {
        if let Ok(mut w) = self.state.write() {
            *w = snap;
        }
    }

    pub fn spawn(self: Arc<Self>, bind: &str) -> tokio::task::JoinHandle<()> {
        let addr: SocketAddr = bind.parse().expect("invalid http.bind");
        let hub = self.clone();
        tokio::spawn(async move {
            let api = Router::new()
                .route("/api/health", get(health))
                .route("/api/pairs", get(pairs))
                .route("/api/positions", get(positions))
                .route("/api/executions", get(executions))
                .route("/api/snapshot", get(snapshot))
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
            tracing::info!(%addr, "http api listening (public bind)");
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
}

impl From<ExecRecord> for ExecRow {
    fn from(r: ExecRecord) -> Self {
        Self {
            ts: r.ts,
            pair_id: r.pair_id,
            action: r.action,
            buy_venue: r.buy_venue,
            sell_venue: r.sell_venue,
            qty: r.qty.to_string(),
            net_pct: r.net_pct.map(|v| v.to_string()),
            result: r.result,
            detail: r.detail,
        }
    }
}

async fn executions(State(hub): State<Arc<ApiHub>>) -> impl IntoResponse {
    let path = match &hub.journal_path {
        Some(p) => p.clone(),
        None => {
            return Json(serde_json::json!({ "executions": [] })).into_response();
        }
    };
    match ExecJournal::open(&path) {
        Ok(j) => match j.recent(50) {
            Ok(rows) => {
                let out: Vec<ExecRow> = rows.into_iter().map(Into::into).collect();
                Json(serde_json::json!({ "executions": out })).into_response()
            }
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("journal read: {err}"),
            )
                .into_response(),
        },
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("journal open: {err}"),
        )
            .into_response(),
    }
}

pub fn fmt_pct(v: Decimal) -> String {
    let sign = if v >= Decimal::ZERO { "+" } else { "" };
    format!("{sign}{:.3}%", v)
}
