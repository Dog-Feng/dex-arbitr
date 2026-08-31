use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{AppConfig, OrderStyle};
use crate::domain::spread::raw_spread_pct;
use crate::domain::{
    add_quote_far_enough, adjacent_quotes, grid_step_from_target_bp, implied_first_limit,
    is_cross_dex, match_all_pairs, new_books, order_pairs_legs, quote_pending_key,
    read_book, AdjacentQuote, Bbo, Books, CloseReason, CloseView, Intent, Pair, QuoteSide, VenueId,
    VenueMarket, WindowGridEngine, WindowGridParams,
};
use crate::exchange::{make_adapter, ExchangePort};
use crate::exec::{
    best_sequenced_spread, closing_sequenced_spread, plan_adjacent, plan_hedge,
    resting_open_spread_ok, sequenced_spread, symmetric_grid_costs, Adapters, ExecResult, HedgePlan,
    LimitMarketRun,
};
use crate::infra::api::{
    self, ApiHub, AvailableSymbol, AvailableVenuePair, ExchangePositionRow, LiveSnapshot,
    NakedExposureRow, PairRow, PositionRow, ScanSnapshot, ScanVenueCell, VenueBalanceRow,
    VenueLiveRow, VenueMatchRow,
};
use crate::infra::dashboard::{self, LivePanel};
use crate::infra::history::{residual_net, HistoryStore, NaturalSpread};
use crate::infra::journal::{ExecRecord, now_ts};

use super::balance::{refresh_accounts, BalanceCache, VenueAccountCache};
use super::control::{ArbitrageControl, ArbitrageParams};
use super::exec_worker::{
    spawn_account_refresher, spawn_limit_market, spawn_naked_hedge, spawn_run_plan, ExecEvent,
    NakedHedgeMsg, RunPlanMsg,
};
use super::positions::PositionStore;
use super::reconcile::{
    audit_position_qty, counterparty_hedge_is_buy, detect_naked_exposures, exchange_opposite_hedge,
    hedge_grid_step, hedge_qty,
    symbol_matches_symbol, NakedExposure, NakedSource,
};
use super::intervention::{Cause, Gate, InterventionGuard, SINGLE_LEG_STREAK_LIMIT};

/// 邻档双边都成、紧急平完之后，等账户刷新再挂，避免同一秒减仓档贴上去。
const ADJACENT_RACE_QUIET: Duration = Duration::from_secs(3);
use super::risk::{books_quality_ok, books_tradable};
use super::scan::{
    candidate_cap, coarse_spread_sum, expand_scan_subscribe, filter_scan_markets,
    merge_coarse_refresh, pair_has_books, pair_volume_ok, rank_bases, select_candidates, CoarseCfg,
    ScanEngine, ScanPhase, COARSE_PROBE_BATCH, COARSE_PROBE_WAIT_SECS,
};
use super::sizing::{check_capacity, mid_from_bbo, LegMargin};
use super::window_spread::{
    exec_spread_pct, mid_spread_pct, own_spread_mid_pct, pair_spread_hub_avg, VenueSpreadBook,
    WindowBook,
};

pub struct Controller {
    cfg: AppConfig,
    adapters: Vec<Arc<dyn ExchangePort>>,
    adapters_by_id: Adapters,
    pairs: Vec<Pair>,
    /// 点「启动套利」后按所选所 + 用户填写的 symbol 匹配出的所对。启动进程时为空。
    available_pairs: Vec<Pair>,
    /// 各所完整永续列表（SoDEX 订 allBookTicker 建别名用）。
    listed_markets: HashMap<String, Vec<VenueMarket>>,
    books: Books,
    positions: PositionStore,
    windows: WindowBook,
    /// 每所一条买卖点差窗口。阶段 1 折两所平均进 Δ；阶段 2 只折市价所中枢。
    venue_spreads: VenueSpreadBook,
    window_grid: WindowGridEngine,
    event_rx: Option<mpsc::UnboundedReceiver<(VenueId, String, Bbo)>>,
    /// 启动套利后才 subscribe；bootstrap 先建 channel，避免 sender 全掉导致环退出。
    bbo_tx: Option<mpsc::UnboundedSender<(VenueId, String, Bbo)>>,
    /// 已经拉起过私有盘口 WS 的所，避免重复 subscribe 刷出多路重连。
    subscribed: HashSet<String>,
    /// 已订阅的 (venue, pair_id)。白名单扩容时只给新币再拉一路 WS。
    subscribed_markets: HashSet<(String, String)>,
    matching: bool,
    history: Option<HistoryStore>,
    panel: LivePanel,
    /// key = slot（币 + 所对）
    pending: HashMap<String, PendingLimit>,
    hedging: HashSet<String>,
    exec_tx: mpsc::UnboundedSender<ExecEvent>,
    exec_rx: Option<mpsc::UnboundedReceiver<ExecEvent>>,
    scan_engine: ScanEngine,
    scan_universe: Vec<Pair>,
    scan_candidates: Vec<Pair>,
    scan_phase: ScanPhase,
    scan_error: Option<String>,
    scan_venues: Vec<String>,
    scan_was_running: bool,
    last_coarse_at: Instant,
    scan_probe_books: HashMap<(String, String), Bbo>,
    scan_probe_queue: Vec<Pair>,
    scan_probe_until: Option<Instant>,
    balance: BalanceCache,
    venue_accounts: VenueAccountCache,
    api: Option<Arc<ApiHub>>,
    /// 运行时套利开关（与 ApiHub 共享的 Arc）。HTTP API 写，决策环读。
    /// `None` 表示没有 HTTP 服务（`http.enabled: false`），此时始终按
    /// `execution.enabled` 的静态值运行。
    control: Option<Arc<std::sync::Mutex<ArbitrageControl>>>,
    ui_pairs: HashMap<String, PairRow>,
    naked_exposures: Vec<NakedExposure>,
    naked_hedging: HashSet<String>,
    intervention: InterventionGuard,
    /// 上次把内存盘口推到 HTTP 快照的时间。事件环按 WS 更新，但推页面要节流。
    last_snap_at: Instant,
    /// 残仓低于 min_qty 的起始时刻。连续 5 分钟才报灰尘仓介入。
    dust_since: HashMap<String, Instant>,
    /// 对账无法校正时的告警节流（同一槽位不要每秒刷 WARN）。
    mismatch_log_at: HashMap<String, Instant>,
    /// 本槽位上次平仓时刻。账户快照滞后时不要立刻按旧仓把内存再开回来。
    last_flat_at: HashMap<String, Instant>,
    /// 上一拍套利开关，用来检测「停止」边沿并清空所对列表。
    was_enabled: bool,
    /// 本次进程各所成交名义（qty × 成交价），开平都累计。
    session_volume: HashMap<String, Decimal>,
    /// 阶段 2：每 slot 一对邻档共享 winner 与彼此的撤单旗。
    quote_races: HashMap<String, QuoteRace>,
    /// 输掉邻档竞态后，该 slot 冷却到这个时刻才允许再挂。
    quote_quiet_until: HashMap<String, Instant>,
    /// 套利开着才允许邻档路径发市价对冲 / 紧急平。停止后已发出的限价可听到成交。
    orders_live: Arc<AtomicBool>,
}

#[derive(Clone)]
struct QuoteRace {
    winner: Arc<AtomicBool>,
    plus_cancel: Arc<AtomicBool>,
    minus_cancel: Arc<AtomicBool>,
}

impl QuoteRace {
    fn new() -> Self {
        Self {
            winner: Arc::new(AtomicBool::new(false)),
            plus_cancel: Arc::new(AtomicBool::new(false)),
            minus_cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    fn flags(&self, side: QuoteSide) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
        match side {
            QuoteSide::Plus => (self.plus_cancel.clone(), self.minus_cancel.clone()),
            QuoteSide::Minus => (self.minus_cancel.clone(), self.plus_cancel.clone()),
        }
    }
}

#[derive(Clone)]
struct PendingLimit {
    plan: HedgePlan,
    since: Instant,
    cancel: Arc<AtomicBool>,
    rest_quote: bool,
    side: Option<QuoteSide>,
}

impl Controller {
    pub async fn run(cfg: AppConfig) -> Result<()> {
        let mut adapters: Vec<Arc<dyn ExchangePort>> = Vec::new();
        let mut adapters_by_id = HashMap::new();
        for id in &cfg.venues {
            let venue = cfg.load_venue(id)?;
            if venue.keys_ready() {
                if venue.id == "sodex" {
                    info!(
                        venue = id,
                        account_id = venue.account_index,
                        api_key_name = %venue.api_key_name,
                        "sodex keys loaded"
                    );
                } else {
                    info!(
                        venue = id,
                        account_index = venue.account_index,
                        api_key_index = venue.api_key_index,
                        "signing keys loaded"
                    );
                }
            } else {
                info!(venue = id, "no signing keys; market data still works");
            }
            // 白名单跟页面走：适配器列出全部永续，匹配时再用 live/yaml 过滤。
            let adapter = make_adapter(venue, Vec::new());
            adapters_by_id.insert(id.clone(), adapter.clone());
            adapters.push(adapter);
        }
        let history = if cfg.history.enabled {
            let store = HistoryStore::open(cfg.history.clone())?;
            info!(
                path = %cfg.history.db_path,
                snapshots = store.snapshot_count(),
                "spread history sqlite ready; persisted natural spreads loaded"
            );
            Some(store)
        } else {
            None
        };
        let api = if cfg.http.enabled {
            let control = Arc::new(std::sync::Mutex::new(ArbitrageControl::new(&cfg)));
            // 构建所的元数据列表供 /api/venues 返回。keys_ready 告诉前端哪些所已配私钥。
            let venue_metas: Vec<crate::infra::api::VenueMeta> = cfg
                .venues
                .iter()
                .filter_map(|id| {
                    cfg.load_venue(id).ok().map(|v| crate::infra::api::VenueMeta {
                        id: id.clone(),
                        label: crate::exchange::venue_display_label(id),
                        keys_ready: v.keys_ready(),
                        quote: v.quote.clone(),
                    })
                })
                .collect();
            let hub = Arc::new(ApiHub::new(
                PathBuf::from(&cfg.http.web_root),
                cfg.http.auth_token.clone(),
                Arc::clone(&control),
                venue_metas,
                ArbitrageParams::from_config(&cfg),
            ));
            hub.clone().spawn(&cfg.http.bind);
            Some((hub, control))
        } else {
            None
        };
        let (api_hub, control) = match api {
            Some((h, c)) => (Some(h), Some(c)),
            None => (None, None),
        };
        let (exec_tx, exec_rx) = mpsc::unbounded_channel();
        let window_samples = cfg.grid.window_samples;
        let sample_interval_ms = cfg.grid.sample_interval_ms;
        let mut this = Self {
            cfg,
            adapters,
            adapters_by_id,
            pairs: Vec::new(),
            available_pairs: Vec::new(),
            listed_markets: HashMap::new(),
            books: new_books(),
            positions: PositionStore::default(),
            windows: WindowBook::new(window_samples, sample_interval_ms),
            venue_spreads: VenueSpreadBook::new(window_samples, sample_interval_ms),
            window_grid: WindowGridEngine::default(),
            event_rx: None,
            bbo_tx: None,
            subscribed: HashSet::new(),
            subscribed_markets: HashSet::new(),
            matching: false,
            history,
            panel: LivePanel::new(0),
            pending: HashMap::new(),
            hedging: HashSet::new(),
            exec_tx,
            exec_rx: Some(exec_rx),
            scan_engine: ScanEngine::default(),
            scan_universe: Vec::new(),
            scan_candidates: Vec::new(),
            scan_phase: ScanPhase::Idle,
            scan_error: None,
            scan_venues: Vec::new(),
            scan_was_running: false,
            last_coarse_at: Instant::now(),
            scan_probe_books: HashMap::new(),
            scan_probe_queue: Vec::new(),
            scan_probe_until: None,
            balance: BalanceCache::default(),
            venue_accounts: VenueAccountCache::default(),
            api: api_hub,
            control,
            ui_pairs: HashMap::new(),
            naked_exposures: Vec::new(),
            naked_hedging: HashSet::new(),
            intervention: InterventionGuard::default(),
            last_snap_at: Instant::now(),
            dust_since: HashMap::new(),
            mismatch_log_at: HashMap::new(),
            last_flat_at: HashMap::new(),
            was_enabled: false,
            session_volume: HashMap::new(),
            quote_races: HashMap::new(),
            quote_quiet_until: HashMap::new(),
            orders_live: Arc::new(AtomicBool::new(false)),
        };
        this.bootstrap().await?;
        this.publish_api_snapshot();
        // 余额给看板用：三条环都要拉。之前只绑在 execution 环上，
        // loop_events / loop_scan 下 LiveSnapshot.balances 一直空，页面显示「—」。
        let (balance, accounts) =
            refresh_accounts(&this.adapters, &this.cfg.sizing).await;
        this.balance = balance;
        this.venue_accounts = accounts;
        this.reconcile_exchange_positions(true);
        spawn_account_refresher(
            this.exec_tx.clone(),
            this.adapters.clone(),
            this.cfg.clone(),
            this.control.clone(),
        );
        info!(venues = ?this.balance.by_venue, "account balances loaded");
        this.publish_api_snapshot();
        // 无 HTTP 面板时没有「启动套利」按钮，立刻按 yaml 启用的交易对激活。
        if this.control.is_none() {
            let scan_only = this.cfg.scan.enabled && !this.cfg.execution.enabled;
            let result = if scan_only {
                this.scan_venues = this.cfg.venues.clone();
                this.activate_scan().await
            } else {
                this.activate_pairs().await
            };
            if let Err(err) = result {
                warn!(error = %err, "startup pair activate failed");
                if scan_only {
                    this.fail_scan(err.to_string());
                    let _ = this.subscribe_pairs(&[]).await;
                }
            }
            this.publish_api_snapshot();
        }

        // HTTP 面板：必须走统一决策环。yaml `execution.enabled` 默认是 false，
        // 启动按钮只置 `control.enabled`，若因此落到 loop_events，则：
        // - 同 pair_id 只处理第一条 Pair（三所两两组合会漏）
        // - 余额刷新卡在 BBO 回调之后，顶栏不按 refresh_balance_secs 更新
        // - 裸仓补对冲 / 先平后开调度都不跑
        if this.cfg.http.enabled || this.cfg.execution.enabled {
            // 私有 WS 订单流：成交检测靠它从轮询变成事件驱动。
            for id in &this.cfg.venues {
                let path = crate::exchange::venue_yaml_path(id);
                match crate::exchange::bridge_watch(&path).await {
                    Ok(()) => info!(venue = id, "private order stream started"),
                    Err(err) => warn!(
                        venue = id,
                        error = %err,
                        "private order stream unavailable; falling back to REST polling"
                    ),
                }
            }
            this.loop_unified().await
        } else if this.cfg.scan.enabled {
            this.loop_scan().await
        } else {
            this.loop_events().await
        }
    }

    async fn bootstrap(&mut self) -> Result<()> {
        if self.adapters.len() < 2 {
            anyhow::bail!("need at least two venues");
        }
        // 只建盘口 channel。交易对匹配推迟到「启动套利」，按当时勾选的 DEX 拉市场。
        let (tx, rx) = mpsc::unbounded_channel();
        self.bbo_tx = Some(tx);
        self.event_rx = Some(rx);
        self.panel = LivePanel::new(0);
        self.panel.scan_mode = self.cfg.scan.enabled && !self.cfg.execution.enabled;
        Ok(())
    }

    /// 匹配完成后立刻进快照，价差监控页启动就能看到交易对，不必等盘口。
    fn seed_matched_pairs(&mut self) {
        let rows: Vec<(String, PairRow)> = self
            .pairs
            .iter()
            .map(|pair| {
                (
                    pair.slot_key(),
                    PairRow {
                        pair_id: pair.pair_id.clone(),
                        buy: pair.legs[0].venue.to_string(),
                        sell: pair.legs[1].venue.to_string(),
                        raw_pct: "—".into(),
                        net_pct: "—".into(),
                        fee_pct: "—".into(),
                        nat_pct: "—".into(),
                        res_pct: "—".into(),
                        entry_pct: "—".into(),
                        dev_pct: "—".into(),
                        delta_pct: api::fmt_pct(self.live_delta(pair)),
                        grid: "0".into(),
                        target_qty: String::new(),
                        actual_qty: "0".into(),
                        status: "已匹配".into(),
                    },
                )
            })
            .collect();
        self.ui_pairs.clear();
        self.ui_pairs.extend(rows);
    }

    fn venue_match_rows(&self) -> Vec<VenueMatchRow> {
        let mut out = Vec::new();
        for i in 0..self.cfg.venues.len() {
            for j in (i + 1)..self.cfg.venues.len() {
                let left = &self.cfg.venues[i];
                let right = &self.cfg.venues[j];
                let n = self
                    .ui_pairs
                    .values()
                    .filter(|p| {
                        let a = p.buy.as_str();
                        let b = p.sell.as_str();
                        (a == left && b == right) || (a == right && b == left)
                    })
                    .count();
                if n == 0 {
                    continue;
                }
                out.push(VenueMatchRow {
                    left: left.clone(),
                    right: right.clone(),
                    n,
                });
            }
        }
        out
    }

    fn take_scan_rematch(&self) -> Option<bool> {
        let Some(ctrl) = self.control.as_ref() else {
            return None;
        };
        let Ok(mut ctrl) = ctrl.lock() else {
            return None;
        };
        if !ctrl.rematch_scan {
            return None;
        }
        ctrl.rematch_scan = false;
        Some(ctrl.scan_running && ctrl.params.scan_venues.len() >= 2)
    }

    fn scan_is_running(&self) -> bool {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.scan_running)
            .unwrap_or(self.cfg.scan.enabled && !self.cfg.execution.enabled)
    }

    fn live_scan_venues(&self) -> Vec<String> {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.params.scan_venues.clone())
            .filter(|v| v.len() >= 2)
            .unwrap_or_else(|| self.scan_venues.clone())
    }

    async fn sync_scan_edge(&mut self) {
        let on = self.scan_is_running();
        if self.scan_was_running && !on {
            self.stop_scan_runtime().await;
        }
        self.scan_was_running = on;
    }

    async fn stop_scan_runtime(&mut self) {
        self.scan_universe.clear();
        self.scan_candidates.clear();
        self.scan_engine.clear();
        self.clear_scan_probe();
        self.scan_phase = ScanPhase::Idle;
        self.scan_error = None;
        self.scan_venues.clear();
        if self.pairs.is_empty() {
            let _ = self.subscribe_pairs(&[]).await;
        } else {
            let _ = self.subscribe_for_active().await;
        }
        info!("scan stopped; scan books unsubscribed");
    }

    fn take_rematch(&self) -> Option<Vec<String>> {
        let mut ctrl = self.control.as_ref()?.lock().ok()?;
        if !ctrl.rematch {
            return None;
        }
        ctrl.rematch = false;
        if ctrl.params.active_venues.len() < 2 {
            return None;
        }
        Some(ctrl.params.active_venues.clone())
    }

    async fn rematch_if_requested(&mut self) {
        self.sync_enabled_edge();
        self.sync_scan_edge().await;
        self.sync_page_config();
        match self.take_scan_rematch() {
            Some(true) => {
                self.scan_phase = ScanPhase::Starting;
                self.matching = true;
                self.publish_api_snapshot();
                let result = self.activate_scan().await;
                self.matching = false;
                if let Err(err) = result {
                    warn!(error = %err, "scan match failed");
                    self.fail_scan(err.to_string());
                    let _ = self.subscribe_pairs(&[]).await;
                }
                self.publish_api_snapshot();
                return;
            }
            Some(false) => {
                self.fail_scan("请至少勾选两个交易所再启动扫描".into());
                let _ = self.subscribe_pairs(&[]).await;
                self.publish_api_snapshot();
                return;
            }
            None => {}
        }
        // 扫描不改执行 `pairs` 下标，飞单不必挡住扫描启动。
        if self.execution_in_flight() {
            return;
        }
        let Some(_venues) = self.take_rematch() else {
            return;
        };
        self.matching = true;
        self.ui_pairs.clear();
        self.publish_api_snapshot();
        let result = self.activate_pairs().await;
        self.matching = false;
        if let Err(err) = result {
            warn!(error = %err, "match pairs for selected venues failed");
        }
        self.publish_api_snapshot();
    }

    /// 有 HTTP 时把页面参数覆盖进 `self.cfg`；无 HTTP 则保持 yaml（纯后端测试）。
    fn sync_page_config(&mut self) {
        let Some(lp) = self.live_params() else {
            return;
        };
        lp.apply_to(&mut self.cfg);
        self.windows
            .configure(self.cfg.grid.window_samples, self.cfg.grid.sample_interval_ms);
        self.venue_spreads
            .configure(self.cfg.grid.window_samples, self.cfg.grid.sample_interval_ms);
        self.scan_engine.configure(
            self.cfg.scan.window_samples.max(10),
            self.cfg.grid.sample_interval_ms.max(1),
        );
    }

    fn forget_persist(&mut self, slot: &str) {
        self.window_grid.forget(slot);
    }

    fn slot_is_live(&self, slot: &str) -> bool {
        self.positions
            .get(slot)
            .is_some_and(|p| p.qty > Decimal::ZERO)
            || self.slot_has_pending(slot)
            || self.hedging.contains(slot)
    }

    fn live_slots(&self) -> HashSet<String> {
        self.pairs
            .iter()
            .map(|p| p.slot_key())
            .filter(|s| self.slot_is_live(s))
            .collect()
    }

    /// 检测启动/停止边沿。停止时清所对列表和空闲窗口；再启动时点差中枢从空样本重算。
    fn sync_enabled_edge(&mut self) {
        let on = self.arbitrage_enabled();
        if self.was_enabled && !on {
            self.on_arbitrage_stopped();
        } else if !self.was_enabled && on {
            self.on_arbitrage_started();
        }
        self.was_enabled = on;
    }

    fn venues_for_live_slots(&self, slots: &HashSet<String>) -> HashSet<String> {
        self.pairs
            .iter()
            .filter(|p| slots.contains(&p.slot_key()))
            .flat_map(|p| {
                [
                    p.legs[0].venue.to_string(),
                    p.legs[1].venue.to_string(),
                ]
            })
            .collect()
    }

    fn drop_idle_windows(&mut self) -> HashSet<String> {
        let keep = self.live_slots();
        let keep_venues = self.venues_for_live_slots(&keep);
        self.windows.drop_except(&keep);
        self.venue_spreads.drop_except_venues(&keep_venues);
        let idle: Vec<String> = self
            .pairs
            .iter()
            .map(|p| p.slot_key())
            .filter(|s| !keep.contains(s))
            .collect();
        for slot in idle {
            self.window_grid.forget(&slot);
        }
        keep
    }

    fn on_arbitrage_stopped(&mut self) {
        self.orders_live.store(false, Ordering::Release);
        self.cancel_all_adjacent_quotes();
        let keep = self.drop_idle_windows();
        self.ui_pairs.retain(|k, _| keep.contains(k));
        info!("arbitrage stopped; pair list cleared, idle μ and venue-spread windows dropped");
        self.publish_api_snapshot();
    }

    fn on_arbitrage_started(&mut self) {
        self.orders_live.store(true, Ordering::Release);
        self.drop_idle_windows();
        info!("arbitrage started; venue-spread hubs reset except venues with live positions");
    }

    /// 仅在启动套利时调用：对所选所 `list_perps`，再按用户填写的 symbol 过滤。
    async fn load_available_pairs(&mut self, venue_ids: &[String]) -> Result<()> {
        let mut listed: Vec<Vec<VenueMarket>> = Vec::new();
        self.listed_markets.clear();
        for id in venue_ids {
            let Some(adapter) = self.adapters_by_id.get(id).cloned() else {
                continue;
            };
            match adapter.list_perps().await {
                Ok(m) => {
                    listed.push(m.clone());
                    self.listed_markets.insert(id.clone(), m);
                }
                Err(e) => warn!(venue = %id, error = %e, "list_perps failed; venue excluded"),
            }
        }
        if listed.len() < 2 {
            anyhow::bail!("need at least two venues with market data");
        }
        let wanted: HashSet<String> = self
            .cfg
            .pairs
            .enabled
            .iter()
            .map(|p| p.symbol.to_ascii_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        if wanted.is_empty() {
            self.available_pairs.clear();
            info!("no enabled symbols; skip pair matching");
            return Ok(());
        }
        self.available_pairs = match_all_pairs(&listed)
            .into_iter()
            .filter(|p| wanted.contains(&p.legs[0].base.to_ascii_uppercase()))
            .collect();
        info!(
            n = self.available_pairs.len(),
            venues = ?venue_ids,
            symbols = ?wanted,
            "available pairs loaded for selected venues"
        );
        Ok(())
    }

    /// 页面点启动或 rematch 时执行。只订阅选中且配置合法的对。
    async fn activate_pairs(&mut self) -> Result<()> {
        self.sync_page_config();
        let active_venues = self
            .live_params()
            .map(|lp| lp.active_venues.clone())
            .unwrap_or_else(|| self.cfg.venues.clone());
        if active_venues.len() < 2 {
            anyhow::bail!("need at least two selected venues");
        }
        self.load_available_pairs(&active_venues).await?;

        let selected: Vec<Pair> = self
            .available_pairs
            .iter()
            .filter(|p| {
                let v0 = p.legs[0].venue.as_str();
                let v1 = p.legs[1].venue.as_str();
                active_venues.iter().any(|v| v == v0)
                    && active_venues.iter().any(|v| v == v1)
            })
            .filter(|p| self.cfg.pair_setting(&p.legs[0].base).is_some())
            .filter(|p| self.validate_pair_qty(p))
            .cloned()
            .collect();
        for s in &self.cfg.pairs.enabled {
            let hit = selected
                .iter()
                .any(|p| p.legs[0].base.eq_ignore_ascii_case(&s.symbol));
            if !hit {
                warn!(
                    symbol = %s.symbol,
                    "no matching venue pair among selected exchanges; skipped"
                );
            }
        }

        self.pairs = self.merge_kept_pairs(selected);
        self.subscribe_for_active().await?;
        let panel_rows = if self.cfg.execution.enabled || !self.cfg.scan.enabled {
            self.pairs.len() * self.pair_stride()
        } else {
            0
        };
        self.panel.resize(panel_rows);
        self.seed_matched_pairs();
        info!(
            n = self.pairs.len(),
            available = self.available_pairs.len(),
            scan = self.cfg.scan.enabled,
            execution = self.cfg.execution.enabled,
            enabled = ?self.cfg.pairs.enabled.iter().map(|p| p.symbol.as_str()).collect::<Vec<_>>(),
            "activated perp pairs"
        );
        self.log_effective_thresholds();
        for row in self.venue_match_rows() {
            info!(
                left = %row.left,
                right = %row.right,
                n = row.n,
                "venue pair intersection"
            );
        }
        self.start_private_streams(&active_venues).await;
        Ok(())
    }

    fn validate_pair_qty(&self, pair: &Pair) -> bool {
        let symbol = &pair.legs[0].base;
        let Some(s) = self.cfg.pair_setting(symbol) else {
            return false;
        };
        let min_qty = pair.min_qty();
        let precision = pair
            .legs
            .iter()
            .map(|l| l.qty_precision)
            .min()
            .unwrap_or(8);
        if s.base_qty < min_qty {
            tracing::error!(
                pair = %pair.pair_id,
                venue_a = pair.legs[0].venue.as_str(),
                venue_b = pair.legs[1].venue.as_str(),
                base_qty = %s.base_qty,
                min_qty = %min_qty,
                "configured base_qty below venue minimum; pair skipped"
            );
            return false;
        }
        if s.base_qty != s.base_qty.round_dp(precision) {
            tracing::error!(
                pair = %pair.pair_id,
                base_qty = %s.base_qty,
                precision,
                "configured base_qty violates venue precision; pair skipped"
            );
            return false;
        }
        true
    }

    async fn subscribe_for_active(&mut self) -> Result<()> {
        let pairs = self.pairs.clone();
        self.subscribe_pairs_inner(&pairs, true).await
    }

    /// 扫描订阅：只订传入的 Pair 腿，不动 `self.pairs`，SoDEX 也不拿未过滤全集。
    async fn subscribe_pairs(&mut self, pairs: &[Pair]) -> Result<()> {
        self.subscribe_pairs_inner(pairs, false).await
    }

    async fn subscribe_pairs_inner(&mut self, pairs: &[Pair], sodex_use_listed: bool) -> Result<()> {
        let tx = self
            .bbo_tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("bbo channel missing"))?;
        let mut by_venue: HashMap<String, Vec<VenueMarket>> = HashMap::new();
        for pair in pairs {
            for leg in &pair.legs {
                let entry = by_venue
                    .entry(leg.venue.as_str().to_string())
                    .or_default();
                if !entry.iter().any(|m| m.pair_id == leg.pair_id) {
                    entry.push(leg.clone());
                }
            }
        }
        for (id, mkts) in &by_venue {
            let Some(adapter) = self.adapters_by_id.get(id).cloned() else {
                continue;
            };
            let to_sub: &[VenueMarket] = if sodex_use_listed && id == "sodex" {
                self.listed_markets
                    .get(id)
                    .map(|m| m.as_slice())
                    .unwrap_or(mkts)
            } else {
                mkts
            };
            adapter.subscribe_bbo(to_sub, tx.clone()).await?;
            self.subscribed.insert(id.clone());
            for m in mkts {
                self.subscribed_markets
                    .insert((id.clone(), m.pair_id.clone()));
            }
        }
        let active: HashSet<String> = by_venue.keys().cloned().collect();
        let stale: Vec<String> = self
            .subscribed
            .iter()
            .filter(|id| !active.contains(*id))
            .cloned()
            .collect();
        for id in stale {
            if let Some(adapter) = self.adapters_by_id.get(&id).cloned() {
                let _ = adapter.subscribe_bbo(&[], tx.clone()).await;
            }
            self.subscribed.remove(&id);
            self.subscribed_markets.retain(|(v, _)| v != &id);
        }
        Ok(())
    }

    async fn activate_scan(&mut self) -> Result<()> {
        self.sync_page_config();
        self.scan_phase = ScanPhase::Starting;
        self.scan_error = None;
        self.scan_engine.clear();
        self.clear_scan_probe();
        let venues = self.live_scan_venues();
        if venues.len() < 2 {
            anyhow::bail!("请至少勾选两个交易所再启动扫描");
        }
        self.scan_venues = venues.clone();
        let mut listed: Vec<(String, Vec<VenueMarket>)> = Vec::new();
        for id in &venues {
            let Some(adapter) = self.adapters_by_id.get(id).cloned() else {
                continue;
            };
            match adapter.list_perps().await {
                Ok(m) => listed.push((id.clone(), m)),
                Err(e) => warn!(venue = %id, error = %e, "list_perps failed; venue excluded"),
            }
        }
        let min_vol = self.scan_min_volume();
        let (kept, dropped) = filter_scan_markets(listed, min_vol);
        if !dropped.is_empty() {
            warn!(?dropped, "venues excluded from scan match");
        }
        if kept.len() < 2 {
            self.scan_universe.clear();
            self.scan_candidates.clear();
            anyhow::bail!("至少两个所有 24h 成交量数据才能扫描（缺字段的所已剔除）");
        }
        self.scan_universe = order_pairs_legs(match_all_pairs(&kept), &self.cfg.venues);
        info!(
            universe = self.scan_universe.len(),
            venues = ?venues,
            "scan universe matched after volume gate"
        );
        self.scan_phase = ScanPhase::Coarse;
        self.publish_api_snapshot();
        let books = self.collect_rest_bbos(&self.scan_universe).await;
        let missing: Vec<Pair> = self
            .scan_universe
            .iter()
            .filter(|p| !pair_has_books(p, &books))
            .cloned()
            .collect();
        if missing.is_empty() {
            self.finish_scan_coarse(books).await?;
            return Ok(());
        }
        info!(
            missing = missing.len(),
            rest = books.len(),
            "scan coarse REST incomplete; short-subscribe remaining pairs"
        );
        self.scan_probe_books = books;
        self.scan_probe_queue = missing;
        self.start_next_scan_probe_batch().await?;
        Ok(())
    }

    fn scan_min_volume(&self) -> Decimal {
        if self.cfg.scan.min_volume_24h_usdc <= Decimal::ZERO {
            Decimal::from(10_000_000)
        } else {
            self.cfg.scan.min_volume_24h_usdc
        }
    }

    fn coarse_cfg(&self, require_fresh: bool) -> CoarseCfg {
        CoarseCfg {
            min_volume: self.scan_min_volume(),
            max_own_spread_pct: self.cfg.scan.max_own_spread_pct,
            min_level_notional_usdc: self.cfg.scan.min_level_notional_usdc,
            freshness_ms: self.cfg.system.data_freshness_ms,
            require_fresh,
        }
    }

    fn clear_scan_probe(&mut self) {
        self.scan_probe_books.clear();
        self.scan_probe_queue.clear();
        self.scan_probe_until = None;
    }

    fn fail_scan(&mut self, err: String) {
        self.scan_universe.clear();
        self.scan_candidates.clear();
        self.scan_engine.clear();
        self.clear_scan_probe();
        self.scan_phase = ScanPhase::Error;
        self.scan_error = Some(err);
        self.scan_was_running = false;
        if let Some(ctrl) = self.control.as_ref() {
            if let Ok(mut g) = ctrl.lock() {
                g.scan_running = false;
                g.rematch_scan = false;
                g.params.scan_enabled = false;
            }
        }
    }

    async fn start_next_scan_probe_batch(&mut self) -> Result<()> {
        if self.scan_probe_queue.is_empty() {
            let books = std::mem::take(&mut self.scan_probe_books);
            return self.finish_scan_coarse(books).await;
        }
        let n = COARSE_PROBE_BATCH.min(self.scan_probe_queue.len());
        let batch: Vec<Pair> = self.scan_probe_queue.drain(..n).collect();
        info!(
            n = batch.len(),
            left = self.scan_probe_queue.len(),
            "scan coarse WS probe batch"
        );
        self.subscribe_pairs(&batch).await?;
        self.scan_probe_until =
            Some(Instant::now() + Duration::from_secs(COARSE_PROBE_WAIT_SECS));
        Ok(())
    }

    async fn finish_scan_coarse(&mut self, books: HashMap<(String, String), Bbo>) -> Result<()> {
        self.clear_scan_probe();
        let cap = candidate_cap(self.cfg.scan.watch_top, self.cfg.scan.candidate_cap);
        let cfg = self.coarse_cfg(false);
        self.scan_candidates = select_candidates(&self.scan_universe, &books, &cfg, cap);
        info!(
            candidates = self.scan_candidates.len(),
            cap,
            "scan coarse filter done"
        );
        let to_sub = expand_scan_subscribe(&self.scan_candidates, &self.scan_universe);
        self.subscribe_pairs(&to_sub).await?;
        self.last_coarse_at = Instant::now();
        self.scan_phase = if self.scan_candidates.is_empty() {
            ScanPhase::Live
        } else {
            ScanPhase::Sampling
        };
        Ok(())
    }

    async fn tick_scan_probe(&mut self) -> bool {
        let Some(until) = self.scan_probe_until else {
            return false;
        };
        if Instant::now() < until {
            return true;
        }
        if let Ok(live) = self.books.read() {
            for (k, v) in live.iter() {
                self.scan_probe_books.insert(k.clone(), v.clone());
            }
        }
        self.scan_probe_until = None;
        if let Err(err) = self.start_next_scan_probe_batch().await {
            warn!(error = %err, "scan WS probe batch failed");
            self.fail_scan(err.to_string());
            let _ = self.subscribe_pairs(&[]).await;
        }
        true
    }

    async fn refresh_scan_volumes(&mut self) {
        let min = self.scan_min_volume();
        let venues = self.scan_venues.clone();
        for id in venues {
            let Some(adapter) = self.adapters_by_id.get(&id).cloned() else {
                continue;
            };
            match adapter.list_perps().await {
                Ok(markets) => {
                    let vol: HashMap<String, Option<Decimal>> = markets
                        .into_iter()
                        .map(|m| (m.pair_id, m.volume_24h_usdc))
                        .collect();
                    for p in &mut self.scan_universe {
                        for leg in &mut p.legs {
                            if leg.venue.as_str() == id {
                                if let Some(v) = vol.get(&leg.pair_id) {
                                    leg.volume_24h_usdc = *v;
                                }
                            }
                        }
                    }
                }
                Err(e) => warn!(venue = %id, error = %e, "scan volume refresh list_perps failed"),
            }
        }
        let before = self.scan_universe.len();
        self.scan_universe.retain(|p| pair_volume_ok(p, min));
        let dropped = before.saturating_sub(self.scan_universe.len());
        if dropped > 0 {
            info!(dropped, "scan universe pairs dropped after 24h volume refresh");
        }
    }

    fn scan_keep_venue_coins(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        for p in &self.scan_candidates {
            for v in &self.scan_venues {
                out.insert(format!("{v}|{}", p.pair_id));
            }
        }
        out
    }

    fn retain_topn_keys(&self, books: &HashMap<(String, String), Bbo>, cfg: &CoarseCfg) -> HashSet<String> {
        let target_bp = self.cfg.pairs.defaults.target_bp;
        let h = self.cfg.grid.step_hysteresis;
        let mut scored = Vec::new();
        for p in &self.scan_candidates {
            let fee = self
                .cfg
                .market_round_trip_taker(&p.legs[0].venue, &p.legs[1].venue);
            if let Some(s) = self.scan_engine.score(p, target_bp, fee, h) {
                scored.push(s);
            }
        }
        let rows = rank_bases(
            scored,
            &self.scan_engine,
            &self.scan_venues,
            self.cfg.scan.watch_top,
        );
        let mut keys = HashSet::new();
        for r in rows {
            let Some(pair) = self.scan_candidates.iter().find(|p| {
                p.pair_id == r.pair_id
                    && p.legs[0].venue.as_str() == r.left
                    && p.legs[1].venue.as_str() == r.right
            }) else {
                continue;
            };
            if !self.scan_engine.is_filled(pair) {
                continue;
            }
            if coarse_spread_sum(pair, books, cfg).is_none() {
                continue;
            }
            keys.insert(pair.slot_key());
        }
        keys
    }

    async fn collect_rest_bbos(&self, pairs: &[Pair]) -> HashMap<(String, String), Bbo> {
        let mut by_venue: HashMap<String, Vec<VenueMarket>> = HashMap::new();
        for pair in pairs {
            for leg in &pair.legs {
                let entry = by_venue
                    .entry(leg.venue.as_str().to_string())
                    .or_default();
                if !entry.iter().any(|m| m.pair_id == leg.pair_id) {
                    entry.push(leg.clone());
                }
            }
        }
        let mut out = HashMap::new();
        for (id, mkts) in by_venue {
            let Some(adapter) = self.adapters_by_id.get(&id).cloned() else {
                continue;
            };
            let snap = adapter.snapshot_bbos(&mkts).await;
            for (pair_id, bbo) in snap {
                out.insert((id.clone(), pair_id), bbo);
            }
        }
        out
    }

    async fn start_private_streams(&self, venues: &[String]) {
        for id in venues {
            let path = crate::exchange::venue_yaml_path(id);
            match crate::exchange::bridge_watch(&path).await {
                Ok(()) => info!(venue = id, "private order stream started"),
                Err(err) => warn!(
                    venue = id,
                    error = %err,
                    "private order stream unavailable; falling back to REST polling"
                ),
            }
        }
    }

    fn merge_kept_pairs(&self, mut new_pairs: Vec<Pair>) -> Vec<Pair> {
        let slots: HashSet<String> = new_pairs.iter().map(|p| p.slot_key()).collect();
        for p in &self.pairs {
            let slot = p.slot_key();
            if slots.contains(&slot) {
                continue;
            }
            let live = self
                .positions
                .get(&slot)
                .map(|x| x.qty > Decimal::ZERO)
                .unwrap_or(false)
                || self.slot_has_pending(&slot)
                || self.hedging.contains(&slot);
            if live {
                new_pairs.push(p.clone());
            }
        }
        new_pairs
    }

    /// 读运行时套利开关。`None` 时回落到静态 `execution.enabled`。
    fn arbitrage_enabled(&self) -> bool {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.enabled)
            .unwrap_or(self.cfg.execution.enabled)
    }

    /// 读运行时可热改参数快照。每次决策调用一次，避免锁在整个决策过程中持有。
    fn live_params(&self) -> Option<ArbitrageParams> {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.params.clone())
    }

    /// 启动匹配后打一行：目标 bp、反推的 Δ、四腿市价费、两所点差中枢。
    /// 点差窗未满时 C=0，满窗后每拍用 live C 重算 Δ。
    fn log_effective_thresholds(&self) {
        let mut seen = HashSet::new();
        for pair in &self.pairs {
            let a = &pair.legs[0].venue;
            let b = &pair.legs[1].venue;
            if self.grid_params(pair).is_none() {
                continue;
            }
            if !seen.insert((
                pair.legs[0].base.clone(),
                a.as_str().to_string(),
                b.as_str().to_string(),
            )) {
                continue;
            }
            let (fee, c, _) = self.pair_delta_inputs(a, b);
            let delta = grid_step_from_target_bp(
                self.cfg.target_bp_for(&pair.legs[0].base, a.as_str(), b.as_str()),
                fee,
                c,
                self.cfg.grid.step_hysteresis,
            );
            info!(
                symbol = %pair.legs[0].base,
                left = a.as_str(),
                right = b.as_str(),
                target_bp = %self.cfg.target_bp_for(&pair.legs[0].base, a.as_str(), b.as_str()),
                delta = %delta,
                round_trip_fee = %fee,
                round_trip_spread = %c,
                symmetric = self.cfg.grid.symmetric_limit,
                "window-step Δ derived from target_bp"
            );
        }
    }

    /// `(F, 折进 Δ 的 C, 空仓点差门)`. 阶段 2：F = 2×(maker挂+taker市)，C = 市价所中枢。
    fn pair_delta_inputs(&self, v0: &VenueId, v1: &VenueId) -> (Decimal, Decimal, Option<Decimal>) {
        let c0 = self.venue_spreads.live_mu(v0.as_str());
        let c1 = self.venue_spreads.live_mu(v1.as_str());
        let both = c0.zip(c1);
        if self.cfg.grid.symmetric_limit {
            let (fee, hedge_c) = symmetric_grid_costs(&self.cfg, v0, v1, c0, c1);
            let c = hedge_c.unwrap_or(Decimal::ZERO);
            let gate = if both.is_some() { hedge_c } else { None };
            (fee, c, gate)
        } else {
            let fee = self.cfg.market_round_trip_taker(v0, v1);
            let avg = both.map(|(a, b)| pair_spread_hub_avg(a, b));
            (fee, avg.unwrap_or(Decimal::ZERO), avg)
        }
    }

    fn live_delta(&self, pair: &Pair) -> Decimal {
        let v0 = &pair.legs[0].venue;
        let v1 = &pair.legs[1].venue;
        let (fee, c, _) = self.pair_delta_inputs(v0, v1);
        grid_step_from_target_bp(
            self.cfg.target_bp_for(&pair.legs[0].base, v0.as_str(), v1.as_str()),
            fee,
            c,
            self.cfg.grid.step_hysteresis,
        )
    }

    fn grid_params(&self, pair: &Pair) -> Option<crate::domain::GridParams> {
        self.cfg.grid_for(
            &pair.legs[0].base,
            pair.legs[0].venue.as_str(),
            pair.legs[1].venue.as_str(),
            pair.min_qty(),
        )
    }

    fn position_mid(&self, pos: &crate::domain::Position) -> Option<Decimal> {
        let bb = self.book(pos.buy.as_str(), &pos.pair_id)?;
        let sb = self.book(pos.sell.as_str(), &pos.pair_id)?;
        mid_from_bbo(&bb, &sb)
    }

    fn book(&self, venue: &str, pair_id: &str) -> Option<Bbo> {
        read_book(&self.books, venue, pair_id)
    }

    fn put_book(&self, venue: &str, pair_id: String, bbo: Bbo) {
        if let Ok(mut w) = self.books.write() {
            w.insert((venue.to_string(), pair_id), bbo);
        }
    }

    fn reconcile_exchange_positions(&mut self, on_startup: bool) {
        if !self.venue_accounts.all_fresh() {
            return;
        }
        let foreign = detect_naked_exposures(&self.pairs, &self.venue_accounts);
        for n in &foreign {
            let new = !self.naked_exposures.iter().any(|e| {
                e.pair_id == n.pair_id && e.venue == n.venue && e.source == NakedSource::Foreign
            });
            if new {
                warn!(
                    pair = %n.pair_id,
                    venue = %n.venue,
                    qty = %n.qty,
                    counterparty = %n.counterparty,
                    "foreign exchange position detected (not auto-hedging)"
                );
                if on_startup {
                    self.log_naked_journal(n, "foreign_startup");
                }
            }
        }
        self.naked_exposures
            .retain(|n| n.source == NakedSource::BotFailure);
        self.naked_exposures.extend(foreign);
        self.audit_memory_positions();
        self.restore_memory_from_exchange();
    }

    fn slot_audit_inflight(&self, slot: &str) -> bool {
        self.hedging.contains(slot) || self.positions.is_pending(slot)
    }

    fn recently_flattened(&self, slot: &str) -> bool {
        const QUIET: Duration = Duration::from_secs(8);
        self.last_flat_at
            .get(slot)
            .is_some_and(|t| t.elapsed() < QUIET)
    }

    /// 内存持仓 vs 交易所实盘的数量对账。
    /// 两腿反向时按重叠对冲量校正：实盘少则缩内存，实盘多则在上限内抬内存，
    /// 后续平仓才按真实对冲量走。跳变过大不抬仓，节流告警。
    /// 只有一腿进账、或本槽位还在对冲中：不动内存。
    fn audit_memory_positions(&mut self) {
        let mut fixes = Vec::new();
        for pair in &self.pairs {
            let slot = pair.slot_key();
            if self.slot_audit_inflight(&slot) {
                continue;
            }
            let Some(pos) = self.positions.get(&slot) else {
                continue;
            };
            if let Some((mem, exch)) = audit_position_qty(pair, &self.venue_accounts, pos.qty) {
                fixes.push((slot, pair.pair_id.clone(), mem, exch));
            }
        }
        for (slot, pair_id, mem, exch) in fixes {
            match self.positions.reconcile_qty(&slot, exch) {
                Some((before, after)) if after > before => {
                    self.mismatch_log_at.remove(&slot);
                    warn!(
                        pair = %pair_id,
                        memory_qty = %before,
                        exchange_qty = %after,
                        "raised memory position to exchange qty"
                    );
                }
                Some((before, after)) => {
                    self.mismatch_log_at.remove(&slot);
                    if after.is_zero() {
                        self.last_flat_at.insert(slot.clone(), Instant::now());
                    }
                    warn!(
                        pair = %pair_id,
                        memory_qty = %before,
                        exchange_qty = %after,
                        "shrunk memory position to exchange qty"
                    );
                }
                None => {
                    if !self.should_log_mismatch(&slot) {
                        continue;
                    }
                    if exch > mem {
                        warn!(
                            pair = %pair_id,
                            memory_qty = %mem,
                            exchange_qty = %exch,
                            "position mismatch; memory not raised (exchange jump exceeds cap)"
                        );
                    } else {
                        warn!(
                            pair = %pair_id,
                            memory_qty = %mem,
                            exchange_qty = %exch,
                            "position mismatch between memory and exchange"
                        );
                    }
                }
            }
        }
    }

    /// 内存已空但两所仍有反向仓：按重叠量把 STEP 捡回来，避免当空仓继续挂邻档。
    fn restore_memory_from_exchange(&mut self) {
        let mut restores = Vec::new();
        for pair in &self.pairs {
            let slot = pair.slot_key();
            if self.slot_audit_inflight(&slot) || self.recently_flattened(&slot) {
                continue;
            }
            if self.positions.get(&slot).is_some_and(|p| p.qty > Decimal::ZERO) {
                continue;
            }
            let Some(h) = exchange_opposite_hedge(pair, &self.venue_accounts) else {
                continue;
            };
            let min_qty = pair.min_qty();
            if min_qty > Decimal::ZERO && h.qty < min_qty {
                continue;
            }
            restores.push((slot, pair.clone(), h));
        }
        for (slot, pair, h) in restores {
            let Some(params) = self.grid_params(&pair) else {
                continue;
            };
            if params.base_qty > Decimal::ZERO && h.qty < params.min_qty && params.min_qty > Decimal::ZERO {
                continue;
            }
            let plus = h.buy == pair.legs[1].venue.as_str();
            let k = hedge_grid_step(
                h.qty,
                params.base_qty,
                params.max_segments as i32,
                plus,
            );
            let mid = self
                .book(pair.legs[0].venue.as_str(), &pair.pair_id)
                .and_then(|a| {
                    self.book(pair.legs[1].venue.as_str(), &pair.pair_id)
                        .and_then(|b| mid_from_bbo(&a, &b))
                })
                .unwrap_or_else(|| {
                    if h.buy_px > Decimal::ZERO && h.sell_px > Decimal::ZERO {
                        (h.buy_px + h.sell_px) / Decimal::from(2)
                    } else {
                        Decimal::ZERO
                    }
                });
            let notional = h.qty * mid;
            self.cancel_adjacent_quotes(&slot);
            self.positions.record_open(
                &slot,
                &pair.pair_id,
                VenueId::from(h.buy.as_str()),
                VenueId::from(h.sell.as_str()),
                h.qty,
                k,
                notional,
                Decimal::ZERO,
                Decimal::ZERO,
                params.base_qty,
                h.buy_px,
                h.sell_px,
            );
            self.windows.freeze(&slot);
            warn!(
                pair = %pair.pair_id,
                qty = %h.qty,
                step = k,
                buy = %h.buy,
                sell = %h.sell,
                "restored memory position from exchange"
            );
        }
    }

    fn should_log_mismatch(&mut self, slot: &str) -> bool {
        const INTERVAL: Duration = Duration::from_secs(30);
        let now = Instant::now();
        if self
            .mismatch_log_at
            .get(slot)
            .is_some_and(|t| now.duration_since(*t) < INTERVAL)
        {
            return false;
        }
        self.mismatch_log_at.insert(slot.to_string(), now);
        true
    }

    fn log_naked_journal(&self, n: &NakedExposure, reason: &str) {
        self.log_record(
            &n.pair_id,
            &n.venue,
            &n.counterparty,
            hedge_qty(n.qty),
            "naked",
            reason,
            &format!("venue={} qty={}", n.venue, n.qty),
        );
    }

    fn record_naked_from_failed_hedge(&mut self, plan: &HedgePlan, first_qty: Decimal) {
        if first_qty <= Decimal::ZERO {
            return;
        }
        let signed = if plan.first.is_buy {
            first_qty
        } else {
            -first_qty
        };
        let exposure = NakedExposure {
            pair_id: plan.pair_id.clone(),
            venue: plan.first.venue.clone(),
            qty: signed,
            counterparty: plan.second.venue.clone(),
            source: NakedSource::BotFailure,
        };
        if self
            .naked_exposures
            .iter()
            .any(|n| n.pair_id == exposure.pair_id && n.venue == exposure.venue)
        {
            return;
        }
        warn!(
            pair = %exposure.pair_id,
            venue = %exposure.venue,
            qty = %exposure.qty,
            counterparty = %exposure.counterparty,
            "record naked exposure after failed hedge"
        );
        self.log_naked_journal(&exposure, "hedge_fail");
        self.naked_exposures.push(exposure);
    }

    async fn try_hedge_naked_exposures(&mut self) {
        if !self.arbitrage_enabled()
            || !self.cfg.execution.hedge_failed_legs
            || self.naked_exposures.is_empty()
            || !self.venue_accounts.all_fresh()
        {
            return;
        }
        let candidate = self
            .naked_exposures
            .iter()
            .find(|n| {
                n.source == NakedSource::BotFailure
                    && !self.naked_hedging.contains(&naked_key(n))
            })
            .cloned();
        let Some(naked) = candidate else {
            return;
        };
        if self
            .intervention
            .should_block(&naked.pair_id, None, Instant::now())
            .blocked()
        {
            return;
        }
        let Some(pair) = self
            .pairs
            .iter()
            .find(|p| {
                p.pair_id == naked.pair_id
                    && p.leg(&naked.counterparty).is_some()
                    && p.leg(&naked.venue).is_some()
            })
            .cloned()
        else {
            return;
        };
        let slot = pair.slot_key();
        if self.slot_has_pending(&slot) || self.hedging.contains(&slot) {
            return;
        }
        let Some(counter_leg) = pair.leg(&naked.counterparty).cloned() else {
            return;
        };
        let (v0, v1) = (
            pair.legs[0].venue.as_str().to_string(),
            pair.legs[1].venue.as_str().to_string(),
        );
        let Some(b0) = self.book(&v0, &pair.pair_id) else {
            return;
        };
        let Some(b1) = self.book(&v1, &pair.pair_id) else {
            return;
        };
        let qty = hedge_qty(naked.qty);
        if books_tradable(&self.cfg, &pair, &b0, &b1, qty).is_err() {
            return;
        }
        let is_buy = counterparty_hedge_is_buy(naked.qty);
        let hedge_leg = crate::exec::HedgeLeg {
            venue: naked.counterparty.clone(),
            symbol: counter_leg.raw_symbol.clone(),
            market_index: counter_leg.market_index,
            is_buy,
            style: OrderStyle::MarketTaker,
            min_qty: counter_leg.min_qty,
            limit_price: None,
        };
        if qty < counter_leg.min_qty {
            warn!(
                pair = %naked.pair_id,
                qty = %qty,
                min_qty = %counter_leg.min_qty,
                "naked exposure below counterparty min qty; needs manual action"
            );
            return;
        }
        let key = naked_key(&naked);
        self.naked_hedging.insert(key.clone());
        info!(
            pair = %naked.pair_id,
            venue = %naked.counterparty,
            qty = %qty,
            is_buy,
            "attempting naked exposure hedge"
        );
        spawn_naked_hedge(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            key,
            naked.pair_id.clone(),
            naked.venue.clone(),
            naked.counterparty.clone(),
            hedge_leg,
            qty,
            is_buy,
        );
    }

    async fn loop_unified(&mut self) -> Result<()> {
        let mut rx = self.event_rx.take().expect("bootstrap must run first");
        let mut exec_rx = self.exec_rx.take().expect("exec channel");
        let mut interval_ms = if self.scan_is_running() {
            self.cfg.scan.analysis_interval_ms.max(10)
        } else {
            self.cfg.execution.loop_interval_ms.max(10)
        };
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(ev) = exec_rx.recv() => {
                    self.handle_exec_event(ev).await;
                    self.publish_api_snapshot();
                }
                msg = rx.recv() => {
                    let Some((venue, pair_id, bbo)) = msg else {
                        break;
                    };
                    self.put_book(venue.as_str(), pair_id, bbo);
                }
                _ = tick.tick() => {
                    while let Ok((venue, pair_id, bbo)) = rx.try_recv() {
                        self.put_book(venue.as_str(), pair_id, bbo);
                    }
                    self.sync_page_config();
                    self.rematch_if_requested().await;
                    let want = if self.scan_is_running() {
                        self.cfg.scan.analysis_interval_ms.max(10)
                    } else {
                        self.cfg.execution.loop_interval_ms.max(10)
                    };
                    if want != interval_ms {
                        interval_ms = want;
                        tick = tokio::time::interval(Duration::from_millis(interval_ms));
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    if self.scan_is_running() {
                        self.tick_scan().await;
                    } else {
                        self.tick_execution().await;
                    }
                    self.panel.flush();
                }
            }
        }
        Ok(())
    }

    async fn tick_execution(&mut self) {
        self.sync_enabled_edge();
        self.sync_page_config();
        // ① 有持仓的先跑（先平后开），② 挂单/对冲中的必跑（否则监视会停），
        // ③ 剩下的才考虑开新仓，受 in-flight 串行限制。未启动则第三段跳过。
        let mut active: HashSet<usize> = HashSet::new();
        let mut must_run: Vec<usize> = Vec::new();
        for (pi, pair) in self.pairs.iter().enumerate() {
            let slot = pair.slot_key();
            let has_pos = self
                .positions
                .get(&slot)
                .map(|p| p.qty > Decimal::ZERO)
                .unwrap_or(false);
            if has_pos || self.slot_has_pending(&slot) || self.hedging.contains(&slot) {
                must_run.push(pi);
            }
        }
        for pi in must_run {
            self.process_pair(pi).await;
            active.insert(pi);
        }
        self.try_hedge_naked_exposures().await;
        if self.arbitrage_enabled() {
            for pi in 0..self.pairs.len() {
                if active.contains(&pi) {
                    continue;
                }
                if self.execution_in_flight() {
                    break;
                }
                self.process_pair(pi).await;
            }
        }
        self.publish_api_snapshot();
    }

    async fn loop_scan(&mut self) -> Result<()> {
        let mut rx = self.event_rx.take().expect("bootstrap must run first");
        let mut exec_rx = self.exec_rx.take().expect("exec channel");
        let mut interval_ms = self.cfg.scan.analysis_interval_ms.max(10);
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(ev) = exec_rx.recv() => {
                    self.handle_exec_event(ev).await;
                    self.publish_api_snapshot();
                }
                msg = rx.recv() => {
                    let Some((venue, pair_id, bbo)) = msg else {
                        break;
                    };
                    self.put_book(venue.as_str(), pair_id, bbo);
                }
                _ = tick.tick() => {
                    self.sync_page_config();
                    let want = self.cfg.scan.analysis_interval_ms.max(10);
                    if want != interval_ms {
                        interval_ms = want;
                        tick = tokio::time::interval(Duration::from_millis(interval_ms));
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    self.rematch_if_requested().await;
                    self.tick_scan().await;
                    self.publish_api_snapshot();
                }
            }
        }
        Ok(())
    }

    async fn tick_scan(&mut self) {
        if !self.scan_is_running() {
            return;
        }
        if self.scan_phase == ScanPhase::Idle || self.scan_phase == ScanPhase::Error {
            return;
        }
        if self.tick_scan_probe().await {
            self.publish_api_snapshot();
            return;
        }
        let sampling = self.scan_phase == ScanPhase::Sampling || self.scan_phase == ScanPhase::Live;
        if sampling
            && self.last_coarse_at.elapsed()
                >= Duration::from_secs(self.cfg.scan.coarse_refresh_secs.max(30))
            && !self.scan_universe.is_empty()
        {
            // 先拍内存盘口，再 list_perps。刷新量会卡住几秒，若先拉市场再读盘口，
            // 全会超过 data_freshness_ms，粗筛把未满窗的候选全部踢掉并退订。
            let snapshot = self.books.read().map(|b| b.clone()).unwrap_or_default();
            self.refresh_scan_volumes().await;
            let cap = candidate_cap(self.cfg.scan.watch_top, self.cfg.scan.candidate_cap);
            let cfg = self.coarse_cfg(true);
            let next = select_candidates(&self.scan_universe, &snapshot, &cfg, cap);
            let retain = self.retain_topn_keys(&snapshot, &cfg);
            let merged = merge_coarse_refresh(&self.scan_candidates, next, &retain, cap);
            if merged.is_empty() && !self.scan_candidates.is_empty() {
                warn!(
                    had = self.scan_candidates.len(),
                    "scan coarse refresh produced empty set; keep current candidates"
                );
            } else if merged.iter().map(|p| p.slot_key()).collect::<HashSet<_>>()
                != self
                    .scan_candidates
                    .iter()
                    .map(|p| p.slot_key())
                    .collect::<HashSet<_>>()
            {
                self.scan_candidates = merged;
                let to_sub = expand_scan_subscribe(&self.scan_candidates, &self.scan_universe);
                let _ = self.subscribe_pairs(&to_sub).await;
                let keep_slots: HashSet<String> =
                    self.scan_candidates.iter().map(|p| p.slot_key()).collect();
                let keep_vc = self.scan_keep_venue_coins();
                self.scan_engine.drop_except(&keep_slots, &keep_vc);
            }
            self.last_coarse_at = Instant::now();
        }
        let snapshot = self.books.read().map(|b| b.clone()).unwrap_or_default();
        let now_ms = unix_now_ms();
        let freshness = self.cfg.system.data_freshness_ms;
        for pair in &self.scan_candidates {
            let v0 = pair.legs[0].venue.as_str();
            let v1 = pair.legs[1].venue.as_str();
            let Some(b0) = snapshot.get(&(v0.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            let Some(b1) = snapshot.get(&(v1.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            if !b0.is_fresh(freshness) || !b1.is_fresh(freshness) || !b0.valid() || !b1.valid() {
                continue;
            }
            self.scan_engine.observe(pair, b0, b1, now_ms);
        }
        // 候选所对之外的 DEX 列：只要同币有盘口就单独入窗，避免 Lighter×RH 占满候选后
        // SoDEX / Entropy 整列都是 —。
        let pair_ids: Vec<String> = self
            .scan_candidates
            .iter()
            .map(|p| p.pair_id.clone())
            .collect();
        let venues = self.scan_venues.clone();
        for pid in &pair_ids {
            for v in &venues {
                let Some(b) = snapshot.get(&(v.clone(), pid.clone())) else {
                    continue;
                };
                if !b.is_fresh(freshness) || !b.valid() {
                    continue;
                }
                self.scan_engine.observe_venue(v, pid, b, now_ms);
            }
        }
        let filled = self.scan_engine.filled_n(&self.scan_candidates);
        self.scan_phase = if filled > 0 {
            ScanPhase::Live
        } else if self.scan_candidates.is_empty() {
            ScanPhase::Live
        } else {
            ScanPhase::Sampling
        };
        self.publish_api_snapshot();
    }

    async fn loop_events(&mut self) -> Result<()> {
        let mut rx = self.event_rx.take().expect("bootstrap must run first");
        let mut exec_rx = self.exec_rx.take().expect("exec channel");
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut rematch_tick = tokio::time::interval(Duration::from_millis(200));
        rematch_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(ev) = exec_rx.recv() => {
                    self.handle_exec_event(ev).await;
                    self.publish_api_snapshot();
                }
                msg = rx.recv() => {
                    let Some((venue, pair_id, bbo)) = msg else {
                        break;
                    };
                    self.put_book(venue.as_str(), pair_id.clone(), bbo.clone());
                    if let Some(pi) = self.pairs.iter().position(|p| p.pair_id == pair_id) {
                        if let Some(vi) = self.cfg.venues.iter().position(|v| v == venue.as_str()) {
                            self.panel.set(
                                self.book_slot(pi, vi),
                                dashboard::book_line(
                                    venue.as_str(),
                                    &pair_id,
                                    bbo.bid,
                                    bbo.ask,
                                    bbo.bid_qty,
                                    bbo.ask_qty,
                                ),
                            );
                        }
                        self.process_pair(pi).await;
                    }
                    self.panel.flush();
                    // WS 盘口已经写进内存并跑完本轮决策；100ms 推一次给页面，
                    // 避免每个 BBO 都序列化快照。
                    self.publish_api_snapshot_throttled(Duration::from_millis(100));
                }
                _ = rematch_tick.tick() => {
                    self.rematch_if_requested().await;
                }
                _ = tick.tick() => {
                    self.publish_api_snapshot();
                }
            }
        }
        Ok(())
    }

    fn pair_stride(&self) -> usize {
        self.cfg.venues.len() + 2
    }

    fn book_slot(&self, pair_i: usize, venue_i: usize) -> usize {
        pair_i * self.pair_stride() + venue_i
    }

    fn spread_slot(&self, pair_i: usize) -> usize {
        pair_i * self.pair_stride() + self.cfg.venues.len()
    }

    fn set_spread(&mut self, pair_i: usize, lines: [String; 2]) {
        let slot = self.spread_slot(pair_i);
        self.panel.set(slot, lines[0].clone());
        self.panel.set(slot + 1, lines[1].clone());
    }

    async fn process_pair(&mut self, pair_i: usize) {
        self.sync_page_config();
        self.sync_enabled_edge();
        let pair = self.pairs[pair_i].clone();
        let slot = pair.slot_key();

        // 停止后空闲槽不再入窗、不算 STEP、不刷监控行。有仓/挂单/对冲仍走，方便平仓。
        if !self.arbitrage_enabled() && !self.slot_is_live(&slot) {
            self.ui_pairs.remove(&slot);
            self.windows.drop_slot(&slot);
            self.window_grid.forget(&slot);
            return;
        }

        // 挂单监视排在所有盘口门槛**之前**：单子一旦挂出去就必须盯到撤单或成交。
        if self.slot_has_pending(&slot) {
            self.watch_pending_slot(pair_i, &pair, &slot);
            if !(self.cfg.grid.symmetric_limit && self.arbitrage_enabled()) {
                if !self.cfg.grid.symmetric_limit {
                    self.cancel_adjacent_quotes(&slot);
                }
                return;
            }
        }
        // 两腿市价没有 pending，只有 hedging。不能空 return：否则监控行停在
        // 「开仓」且价差/持仓整行冻住，直到成交回调。
        if self.hedging.contains(&slot) {
            self.paint_inflight_slot(pair_i, &pair, &slot);
            return;
        }

        // 活跃所过滤：只有两腿都在 active_venues 里的 pair 才能开仓。
        // 平仓不受此限——已有持仓的 pair 不管所是否还在列表里都继续平。
        // 空列表 = 未选所，不开新仓（页面默认不勾 DEX）。
        let has_pos = self
            .positions
            .get(&slot)
            .map(|p| p.qty > Decimal::ZERO)
            .unwrap_or(false);
        if !has_pos {
            if let Some(lp) = self.live_params() {
                let v0 = pair.legs[0].venue.as_str();
                let v1 = pair.legs[1].venue.as_str();
                if lp.active_venues.len() < 2
                    || !lp.active_venues.iter().any(|v| v == v0)
                    || !lp.active_venues.iter().any(|v| v == v1)
                {
                    self.forget_persist(&slot);
                    return;
                }
            }
        }

        let v0 = pair.legs[0].venue.clone();
        let v1 = pair.legs[1].venue.clone();
        let b0 = self.book(v0.as_str(), &pair.pair_id);
        let b1 = self.book(v1.as_str(), &pair.pair_id);
        match (&b0, &b1) {
            (None, None) => {
                self.panel.stats.bump_skip("wait");
                self.mark_ui_status(&slot, "等盘口");
                self.forget_persist(&slot);
                return;
            }
            (None, Some(_)) => {
                self.panel.stats.bump_skip("wait");
                self.mark_ui_status(&slot, &format!("等盘口 {}", v0.as_str()));
                self.forget_persist(&slot);
                return;
            }
            (Some(_), None) => {
                self.panel.stats.bump_skip("wait");
                self.mark_ui_status(&slot, &format!("等盘口 {}", v1.as_str()));
                self.forget_persist(&slot);
                return;
            }
            (Some(_), Some(_)) => {}
        }
        let b0 = b0.unwrap();
        let b1 = b1.unwrap();

        let Some(mid) = mid_from_bbo(&b0, &b1) else {
            self.panel.stats.bump_skip("no_mid");
            self.mark_ui_status(&slot, "无中价");
            self.forget_persist(&slot);
            return;
        };
        let base = pair.legs[0].base.clone();
        let pos = self.positions.get(&slot).cloned();

        let Some(mut params) = self.cfg.grid_for(
            &base,
            v0.as_str(),
            v1.as_str(),
            pair.min_qty(),
        ) else {
            self.mark_ui_status(&slot, "未配置");
            return;
        };
        // `base_qty` 是**单格**数量。有仓时用 Position 里冻结的尺，
        // 绝不能拿总持仓量覆盖，否则 3 格会被算成 1 格。
        if let Some(p) = pos.as_ref().filter(|p| p.base_qty > Decimal::ZERO) {
            params.base_qty = p.base_qty;
        }

        // 入窗只要求盘口新鲜合法。厚度不够仍要采 μ，否则薄盘口永远凑不满窗口。
        if books_quality_ok(&self.cfg, &b0, &b1).is_ok() {
            let now = unix_now_ms();
            if let Some(s) = mid_spread_pct(&b0, &b1) {
                self.windows.observe(&slot, now, s);
            }
            if let Some(c) = own_spread_mid_pct(&b0) {
                self.venue_spreads.observe(v0.as_str(), now, c);
            }
            if let Some(c) = own_spread_mid_pct(&b1) {
                self.venue_spreads.observe(v1.as_str(), now, c);
            }
        }
        // 重启后内存窗口是空的，冻 μ 也丢了。有仓且窗已满则冻当前 live μ，
        // 避免持仓期 STEP 跟着滑动均值漂移。不是建仓时的 μ，但是能拿到的最好近似。
        if pos
            .as_ref()
            .is_some_and(|p| p.qty > Decimal::ZERO)
            && !self.windows.is_frozen(&slot)
            && self.windows.live_mu(&slot).is_some()
        {
            self.windows.freeze(&slot);
        }

        // 有仓：新鲜度 + 合法 BBO。格子减格的一档厚度在作出 Close 之后按本笔 qty 校验。
        // 空仓：数据质量 + 一档深度都要过。
        let gate = if pos.is_some() {
            books_quality_ok(&self.cfg, &b0, &b1)
        } else {
            books_tradable(&self.cfg, &pair, &b0, &b1, params.base_qty)
        };
        if let Err(reason) = gate {
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), reason);
            self.forget_persist(&slot);
            return;
        }

        // L = legs[0]，R = legs[1]。正 STEP = 空 L 多 R。决策用可执行价差。

        let fee = self.cfg.exec_fee(&v0) + self.cfg.exec_fee(&v1);
        let net = match pos.as_ref().filter(|p| p.qty > Decimal::ZERO) {
            Some(p) => {
                let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
                sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO)
            }
            None => mid_spread_pct(&b0, &b1).map(|raw| crate::domain::NetSpread {
                buy: v1.clone(),
                sell: v0.clone(),
                raw_pct: raw,
                fee_pct: fee,
                slip_pct: Decimal::ZERO,
                net_pct: raw - fee,
            }),
        };
        let Some(net) = net else {
            self.panel.stats.bump_skip("no_spread");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_spread"));
            self.mark_ui_status(&slot, "无价差");
            self.forget_persist(&slot);
            return;
        };

        let cross = is_cross_dex(net.buy.as_str(), net.sell.as_str());
        let natural = self.sample_and_natural(&pair, &net, cross);

        // 平仓视角：买回原 sell 所的 Ask、卖回原 buy 所的 Bid，用当前盘口重算。
        //
        // qty 传 0：先算出价差，让格子能判断「该不该减」。真正下单前再
        // 用本笔平仓量做一档校验，不够就丢掉平仓意图（见下方 thin_book）。
        let close_view = pos.as_ref().and_then(|p| {
            let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
            closing_sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO).map(|c| {
                CloseView {
                    exit_raw_pct: c.raw_pct,
                    exit_net_pct: c.net_pct,
                }
            })
        });

        if pos
            .as_ref()
            .is_some_and(|p| p.qty > Decimal::ZERO && params.base_qty <= Decimal::ZERO)
        {
            warn!(
                pair = %pair.pair_id,
                "held position has no segment size; cannot add or reduce grids"
            );
        }

        let (fee_rt, c, spread_rt) = self.pair_delta_inputs(&v0, &v1);
        params.step = grid_step_from_target_bp(
            self.cfg.target_bp_for(&base, v0.as_str(), v1.as_str()),
            fee_rt,
            c,
            self.cfg.grid.step_hysteresis,
        );

        let wparams = WindowGridParams::from_grid(&params, self.cfg.grid.step_hysteresis);
        let k = pos.as_ref().map(|p| p.grid).unwrap_or(0);
        let held_qty = pos.as_ref().map(|p| p.qty).unwrap_or(Decimal::ZERO);
        let s_plus = exec_spread_pct(&b0, &b1, true);
        let s_minus = exec_spread_pct(&b0, &b1, false);
        let mu = self.windows.quote_mu(&slot);

        if self.cfg.grid.symmetric_limit && self.arbitrage_enabled() {
            self.maintain_adjacent_quotes(
                pair_i, &pair, &slot, &v0, &v1, &b0, &b1, &params, k, held_qty, has_pos, mid,
                &net, pos.as_ref(), mu, s_plus, s_minus, spread_rt,
            )
            .await;
            return;
        }
        if self.cfg.grid.symmetric_limit {
            self.cancel_adjacent_quotes(&slot);
        }

        let mut intent = match (mu, s_plus, s_minus, spread_rt) {
            (Some(mu), Some(sp), Some(sm), Some(_)) => self.window_grid.decide(
                &slot,
                k,
                sp,
                sm,
                mu,
                &v0,
                &v1,
                held_qty,
                &wparams,
                Instant::now(),
            ),
            (Some(mu), Some(sp), Some(sm), None) if has_pos => self.window_grid.decide(
                &slot,
                k,
                sp,
                sm,
                mu,
                &v0,
                &v1,
                held_qty,
                &wparams,
                Instant::now(),
            ),
            (None, _, _, _) => {
                self.window_grid.forget(&slot);
                let n = self.windows.sample_count(&slot);
                let cap = self.windows.cap();
                self.fill_monitor_row(
                    &slot,
                    &pair,
                    &net,
                    pos.as_ref(),
                    &format!("采样 {n}/{cap}"),
                    &b0,
                    &b1,
                    Some(mid),
                );
                if !has_pos {
                    return;
                }
                Intent::Hold
            }
            (_, _, _, None) if !has_pos => {
                self.window_grid.forget(&slot);
                let cap = self.venue_spreads.cap();
                let n0 = self.venue_spreads.sample_count(v0.as_str());
                let n1 = self.venue_spreads.sample_count(v1.as_str());
                self.fill_monitor_row(
                    &slot,
                    &pair,
                    &net,
                    pos.as_ref(),
                    &format!("点差 {n0}/{cap} {n1}/{cap}"),
                    &b0,
                    &b1,
                    Some(mid),
                );
                return;
            }
            _ => {
                self.window_grid.forget(&slot);
                Intent::Hold
            }
        };
        if let Intent::Close {
            round_trip_pct, ..
        } = &mut intent
        {
            if let (Some(p), Some(cv)) = (pos.as_ref(), close_view) {
                *round_trip_pct = p.entry_net_pct + cv.exit_net_pct;
            }
        }

        // 容量校验只拦 Open：Close 绝不能被保证金/深度拦住，否则仓位平不掉。
        // 空仓开仓与加仓走同一条路径。本地点差在入口 `books_quality_ok` 已查过。
        if let Intent::Open { qty, buy, sell, .. } = &intent {
            let reserved = self.positions.reserved_margin_by_venue(
                |v| self.cfg.leverage_for(v),
                |p| self.position_mid(p),
            );
            let (bb, sb) = books_for_direction(buy, &v0, &b0, &b1);
            if let Err(reason) = check_capacity(
                &self.cfg.sizing,
                *qty,
                self.leg_margin(&reserved, buy.as_str()),
                self.leg_margin(&reserved, sell.as_str()),
                bb,
                sb,
                mid,
            ) {
                self.panel.stats.bump_skip(reason);
                self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
                self.fill_monitor_row(
                    &slot,
                    &pair,
                    &net,
                    pos.as_ref(),
                    &skip_reason_label(reason),
                    &b0,
                    &b1,
                    Some(mid),
                );
                return;
            }
            if sequenced_spread(&self.cfg, buy, sell, bb, sb, *qty).is_none() {
                self.panel.stats.bump_skip("thin_book");
                self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "thin_book"));
                self.fill_monitor_row(
                    &slot,
                    &pair,
                    &net,
                    pos.as_ref(),
                    "深度不足",
                    &b0,
                    &b1,
                    Some(mid),
                );
                return;
            }
        }

        let want_open = matches!(intent, Intent::Open { .. });
        if matches!(intent, Intent::Open { .. }) && !self.arbitrage_enabled() {
            intent = Intent::Hold;
        }
        let open_skip = if want_open && matches!(intent, Intent::Hold) && !self.arbitrage_enabled() {
            Some("未启动")
        } else {
            None
        };

        // 一档撑不住本笔平仓量 → 丢掉格子平仓意图。
        if let Intent::Close { qty, reason, .. } = &intent {
            if matches!(
                reason,
                CloseReason::GridReduce
            ) {
                if let Some(p) = pos.as_ref() {
                    let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
                    if closing_sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, *qty).is_none()
                    {
                        self.panel.stats.bump_skip("thin_book");
                        self.set_spread(
                            pair_i,
                            dashboard::skip_lines(&pair.pair_id, "thin_book"),
                        );
                        self.paint_skip_with_books(
                            &slot,
                            &pair,
                            &v0,
                            &v1,
                            &b0,
                            &b1,
                            pos.as_ref(),
                            "thin_book",
                        );
                        return;
                    }
                }
            }
        }

        let residual = if cross {
            match &natural {
                Some(nat) => residual_net(net.net_pct, nat.value),
                None => net.net_pct,
            }
        } else {
            net.net_pct
        }
        .round_dp(6);
        let label = intent_label(&intent);
        let min_pts = self.cfg.history.min_points;
        let pts = natural.as_ref().map(|n| n.points).unwrap_or_else(|| {
            self.history
                .as_ref()
                .map(|s| s.window_points(&pair.pair_id, net.buy.as_str(), net.sell.as_str()))
                .unwrap_or(0)
        });
        let nat_value = self.ui_nat(&pair, &net, natural.as_ref());

        self.panel.stats.bump_intent(label);
        let ui = open_skip.unwrap_or_else(|| ui_intent_label(&intent, label));
        self.record_ui_pair(
            &slot,
            &pair,
            &net,
            &params,
            pos.as_ref(),
            ui,
            residual,
            nat_value,
        );
        self.set_spread(
            pair_i,
            dashboard::spread_lines(
                &pair.pair_id,
                net.buy.as_str(),
                net.sell.as_str(),
                net.raw_pct,
                net.net_pct,
                net.slip_pct,
                nat_value,
                residual,
                pts,
                min_pts,
                ui,
            ),
        );

        if matches!(intent, Intent::Hold) {
            self.maybe_mark_dust(&slot, &pair, pos.as_ref(), &params);
            return;
        }

        if let Intent::Open { grid, qty, .. } = &intent {
            if pos.is_some() {
                info!(
                    pair = %pair.pair_id,
                    target_grid = grid,
                    add_qty = %qty,
                    "grid: topping up to target segments"
                );
            }
        }
        if let Intent::Close {
            reason,
            round_trip_pct,
            ..
        } = &intent
        {
            info!(
                pair = %pair.pair_id,
                reason = reason.as_str(),
                round_trip_pct = %round_trip_pct.round_dp(4),
                entry_net_pct = %pos.as_ref().map(|p| p.entry_net_pct).unwrap_or_default().round_dp(4),
                "grid: closing"
            );
        }
        if self.positions.is_pending(&slot) {
            self.mark_ui_status(&slot, "开仓中");
            return;
        }
        // 本槽已在入口因 pending/hedging 返回。这里的 in_flight 只可能是**别的槽**。
        // 平仓不能被别的币卡住；新开/加仓仍等，避免多对同时占保证金和下单通道。
        if matches!(intent, Intent::Open { .. }) && self.execution_in_flight() {
            self.panel.stats.bump_skip("in_flight");
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "in_flight");
            return;
        }
        // 人工介入等待：开仓和平仓**都挡**（对齐参考 `should_block` 在开仓
        // 与平仓两条路径上都查）。这跟下面的 reduce-only 熔断相反，是刻意的：
        // reduce-only 时仓位是已知的，挡平仓等于锁死仓位；而介入态意味着
        // 内存里的仓位本身不可信，按错的量去平会把敞口放大。
        // 兜底是 30 分钟自动解除和格数变化解除，不会永久锁死。
        let cur_grid = pos.as_ref().map(|p| p.grid);
        match self
            .intervention
            .should_block(&pair.pair_id, cur_grid, Instant::now())
        {
            Gate::Allow => {}
            Gate::Resumed(why) => {
                warn!(pair = %pair.pair_id, reason = %why, "manual intervention wait cleared; resuming trading");
                self.log_record(
                    &pair.pair_id,
                    &pair.legs[0].venue.to_string(),
                    &pair.legs[1].venue.to_string(),
                    Decimal::ZERO,
                    "intervention",
                    "resumed",
                    &why,
                );
            }
            Gate::Block {
                cause,
                detail,
                waited,
            } => {
                // 挂起那一刻已经打过 ERROR 并写了 journal，这里每轮只记 skip 计数，
                // 用 debug 避免刷屏。面板上能看到 `intervention` 的跳过数。
                self.panel.stats.bump_skip("intervention");
                tracing::debug!(
                    pair = %pair.pair_id,
                    cause = cause.as_str(),
                    detail = %detail,
                    waited_secs = waited.as_secs(),
                    "pair waiting for manual intervention; open and close both skipped"
                );
                self.paint_skip_with_books(
                    &slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "intervention",
                );
                return;
            }
        }
        let Some(mut plan) = plan_hedge(&pair, &intent, pos.as_ref(), &self.cfg) else {
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "no_plan");
            return;
        };
        plan.decision_net_pct = net.net_pct;
        plan.decision_raw_pct = net.raw_pct;
        // 固化开仓时的单格数量，供 Position.base_qty 使用。平仓时 params.base_qty
        // 由持仓自身携带，不需要从 plan 传入，所以只在 is_open 时写有意义的值。
        if plan.is_open {
            plan.base_qty = params.base_qty;
        }
        match &intent {
            Intent::Open { grid, .. } => {
                plan.grid_from = pos.as_ref().map(|p| p.grid).unwrap_or(0);
                plan.grid_to = *grid;
            }
            Intent::Close { grid, .. } => {
                plan.grid_from = pos.as_ref().map(|p| p.grid).unwrap_or(0);
                plan.grid_to = *grid;
            }
            Intent::Hold => {}
        }
        force_market_taker(&mut plan);
        if self.cfg.live_test.dex_test_mode && plan.qty > self.cfg.live_test.max_qty {
            plan.qty = self.cfg.live_test.max_qty;
        }
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            first_style = plan.first.style.as_str(),
            first_buy = plan.first.is_buy,
            second = %plan.second.venue,
            second_style = plan.second.style.as_str(),
            qty = %plan.qty,
            open = plan.is_open,
            "window-step: dual market taker"
        );

        self.publish_api_snapshot();
        if matches!(intent, Intent::Open { .. }) {
            self.positions.reserve_open(&slot);
        }
        self.hedging.insert(slot.clone());
        spawn_run_plan(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair_i,
            plan,
        );
    }

    /// 挂单监视：开仓单价差跌出持有区 → 置 cancel，后台执行 task 撤单。
    ///
    /// 平仓单**不**因价差变化撤——平仓要走完，否则会一直留着单腿风险。
    /// 单轮超时由执行器自己管；这里若按整轮计划起点超时并置 cancel，
    /// `limit_retry_count` 的后续重挂会被直接跳过。
    fn watch_pending_slot(&mut self, pair_i: usize, pair: &Pair, slot: &str) {
        for side in [QuoteSide::Plus, QuoteSide::Minus] {
            let key = quote_pending_key(slot, side);
            if self.pending.contains_key(&key) {
                self.watch_one_pending(pair_i, pair, slot, &key);
            }
        }
        if self.pending.contains_key(slot) {
            self.watch_one_pending(pair_i, pair, slot, slot);
        }
    }

    fn watch_one_pending(&mut self, pair_i: usize, pair: &Pair, slot: &str, key: &str) {
        let Some(pending) = self.pending.get(key).cloned() else {
            return;
        };
        let deadline = if pending.rest_quote {
            Duration::from_secs(24 * 3600)
        } else {
            self.pending_hard_deadline()
        };
        if pending.since.elapsed() > deadline {
            tracing::error!(
                pair = %pair.pair_id,
                slot,
                elapsed_secs = pending.since.elapsed().as_secs(),
                "pending limit exceeded hard deadline; force-clearing state (check for orphan orders)"
            );
            pending.cancel.store(true, Ordering::Relaxed);
            self.pending.remove(key);
            if pending.rest_quote {
                self.finish_adjacent_slot(slot);
            } else {
                self.hedging.remove(slot);
                self.positions.release_pending(slot);
            }
            self.forget_persist(slot);
            self.log_plan_record(&pending.plan, "exec_fail", "watchdog_timeout", "");
            return;
        }
        if pending.rest_quote {
            if self.quote_winner_taken(slot) {
                self.mark_ui_status(slot, "对冲中");
                return;
            }
            self.watch_adjacent_events(pair, slot, key, &pending);
            let ui = if pending.cancel.load(Ordering::Relaxed) {
                "撤单中"
            } else {
                "邻档挂单"
            };
            self.mark_ui_status(slot, ui);
            return;
        }

        let already = pending.cancel.load(Ordering::Relaxed);
        let spread = self.pending_spread(pair, &pending);
        let floor = self
            .grid_params(pair)
            .map(|p| p.step * self.cfg.grid.step_hysteresis)
            .unwrap_or(Decimal::ZERO);
        let spread_ok = match (&spread, pending.plan.is_open) {
            // 平仓单：价差怎么变都要走完
            (_, false) => true,
            (Some((net, _residual)), true) => {
                let same_dir = pending.plan.buy_venue == net.buy.as_str()
                    && pending.plan.sell_venue == net.sell.as_str();
                resting_open_spread_ok(net.raw_pct, same_dir, floor)
            }
            // 开仓单但读不到盘口：不当成「价差没了」，交给执行器本轮超时
            (None, true) => true,
        };

        let ui = if already {
            "撤单中"
        } else if !spread_ok {
            pending.cancel.store(true, Ordering::Release);
            self.panel.stats.cancel_gone += 1;
            let raw = spread.as_ref().map(|(n, _)| n.raw_pct);
            info!(
                pair = %pair.pair_id,
                raw = raw.map(|v| v.round_dp(4)).unwrap_or_default().to_string(),
                floor = %floor.round_dp(4),
                "resting limit: spread gone, requesting cancel"
            );
            "撤单中"
        } else {
            "挂单中"
        };

        let Some(params) = self.grid_params(pair) else {
            return;
        };
        if let Some((net, residual)) = spread {
            self.record_ui_pair(slot, pair, &net, &params, None, ui, residual, None);
            self.set_spread(
                pair_i,
                dashboard::spread_lines(
                    &pair.pair_id,
                    net.buy.as_str(),
                    net.sell.as_str(),
                    net.raw_pct,
                    net.net_pct,
                    net.slip_pct,
                    None,
                    residual,
                    0,
                    self.cfg.history.min_points,
                    ui,
                ),
            );
        } else {
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, ui));
        }
    }

    /// 两腿市价对冲进行中：继续用当前盘口刷监控行，状态标成开仓中/平仓中。
    fn paint_inflight_slot(&mut self, pair_i: usize, pair: &Pair, slot: &str) {
        let pos = self.positions.get(slot).cloned();
        let status = if self.positions.is_pending(slot) {
            "开仓中"
        } else if pos.as_ref().is_some_and(|p| p.qty > Decimal::ZERO) {
            "平仓中"
        } else {
            "下单中"
        };
        let v0 = pair.legs[0].venue.clone();
        let v1 = pair.legs[1].venue.clone();
        let (Some(b0), Some(b1)) = (
            self.book(v0.as_str(), &pair.pair_id),
            self.book(v1.as_str(), &pair.pair_id),
        ) else {
            self.mark_ui_status(slot, status);
            return;
        };
        let net = match pos.as_ref().filter(|p| p.qty > Decimal::ZERO) {
            Some(p) => {
                let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
                sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO)
            }
            None => best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, Decimal::ZERO),
        };
        let Some(net) = net else {
            self.mark_ui_status(slot, status);
            return;
        };
        self.fill_monitor_row(
            slot,
            pair,
            &net,
            pos.as_ref(),
            status,
            &b0,
            &b1,
            mid_from_bbo(&b0, &b1),
        );
        let cross = is_cross_dex(net.buy.as_str(), net.sell.as_str());
        let official = self.sample_and_natural(pair, &net, cross);
        let nat_value = self.ui_nat(pair, &net, official.as_ref());
        let residual = if cross {
            match nat_value {
                Some(n) => residual_net(net.net_pct, n),
                None => net.net_pct,
            }
        } else {
            net.net_pct
        };
        self.set_spread(
            pair_i,
            dashboard::spread_lines(
                &pair.pair_id,
                net.buy.as_str(),
                net.sell.as_str(),
                net.raw_pct,
                net.net_pct,
                net.slip_pct,
                nat_value,
                residual,
                official.as_ref().map(|n| n.points).unwrap_or(0),
                self.cfg.history.min_points,
                status,
            ),
        );
    }

    /// 挂单期间按**计划的方向**算净边（不双向取优）。
    /// residual 只给监控行展示；开仓挂单是否还够看毛价差 vs Δ×滞后。
    fn pending_spread(
        &self,
        pair: &Pair,
        pending: &PendingLimit,
    ) -> Option<(crate::domain::NetSpread, Decimal)> {
        let buy = VenueId::from(pending.plan.buy_venue.as_str());
        let sell = VenueId::from(pending.plan.sell_venue.as_str());
        let bb = self.book(buy.as_str(), &pair.pair_id)?;
        let sb = self.book(sell.as_str(), &pair.pair_id)?;
        let net = sequenced_spread(&self.cfg, &buy, &sell, &bb, &sb, Decimal::ZERO)?;
        let mut residual = net.net_pct;
        if is_cross_dex(buy.as_str(), sell.as_str()) {
            if let Some(store) = &self.history {
                if let Some(nat) = store.natural(&pair.pair_id, buy.as_str(), sell.as_str()) {
                    residual = residual_net(net.net_pct, nat.value);
                }
            }
        }
        Some((net, residual.round_dp(6)))
    }

    fn execution_in_flight(&self) -> bool {
        !self.hedging.is_empty() || self.pending.values().any(|p| !p.rest_quote)
    }

    /// 一轮 limit-then-market 的正常耗时上限：
    /// 每轮挂单等待 × 轮数 + 撤单竞态 + 一次写操作的 sidecar 超时，再留一倍余量。
    fn pending_hard_deadline(&self) -> Duration {
        let rounds = u64::from(self.cfg.order.limit_retry_count.max(1));
        let per_round = self.cfg.order.limit_timeout_ms.max(200) + 1_000;
        Duration::from_millis(per_round * rounds) + Duration::from_secs(240)
    }

    async fn handle_exec_event(&mut self, ev: ExecEvent) {
        match ev {
            ExecEvent::RunPlan(msg) => self.on_run_plan(msg).await,
            ExecEvent::Accounts(msg) => {
                self.venue_accounts.absorb(msg.accounts);
                self.balance.by_venue = self.venue_accounts.to_balance_map();
                self.balance.last_refresh = msg.balance.last_refresh;
                self.reconcile_exchange_positions(false);
            }
            ExecEvent::NakedHedge(msg) => self.on_naked_hedge(msg),
        }
    }

    fn on_naked_hedge(&mut self, msg: NakedHedgeMsg) {
        self.naked_hedging.remove(&msg.key);
        match msg.result {
            Ok(fill) => {
                info!(
                    pair = %msg.pair_id,
                    venue = %fill.venue,
                    qty = %fill.qty,
                    "bot failure naked hedge filled"
                );
                self.naked_exposures.retain(|n| {
                    n.source != NakedSource::BotFailure
                        || n.pair_id != msg.pair_id
                        || n.venue != msg.venue
                });
                self.log_record(
                    &msg.pair_id,
                    &msg.venue,
                    &msg.counterparty,
                    fill.qty,
                    "naked_hedge",
                    "filled",
                    "",
                );
            }
            Err(err) => {
                warn!(
                    pair = %msg.pair_id,
                    error = %err,
                    "naked exposure hedge failed"
                );
                if err.contains("SECOND_LEG_UNKNOWN") {
                    for n in &mut self.naked_exposures {
                        if n.pair_id == msg.pair_id
                            && n.venue == msg.venue
                            && n.source == NakedSource::BotFailure
                        {
                            n.source = NakedSource::SecondLegUnknown;
                        }
                    }
                    if let Some(slot) = self
                        .pairs
                        .iter()
                        .find(|p| p.pair_id == msg.pair_id)
                        .map(|p| p.slot_key())
                    {
                        self.mark_intervention_for(
                            &msg.pair_id,
                            &slot,
                            Cause::SecondLegUnknown,
                            format!(
                                "naked hedge on {} unverifiable; not retrying",
                                msg.counterparty
                            ),
                        );
                    }
                }
            }
        }
    }

    async fn on_run_plan(&mut self, msg: RunPlanMsg) {
        self.hedging.remove(&msg.slot);
        if let Some(side) = msg.plan.quote_side {
            self.pending.remove(&quote_pending_key(&msg.slot, side));
        } else {
            self.pending.remove(&msg.slot);
        }
        let pair_i = self
            .pairs
            .iter()
            .position(|p| p.slot_key() == msg.slot)
            .unwrap_or(msg.pair_i);
        let Some(pair) = self.pairs.get(pair_i).cloned() else {
            self.positions.release_pending(&msg.slot);
            return;
        };
        match msg.result {
            Ok(result) => {
                let hedged = result.hedged_qty();
                info!(
                    pair = %msg.plan.pair_id,
                    first = %result.first.venue,
                    first_qty = %result.first.qty,
                    second = %result.second.venue,
                    second_qty = %result.second.qty,
                    hedged = %hedged,
                    planned = %msg.plan.qty,
                    "dual-market executed"
                );
                if let Some(orphan) = &result.orphan_order {
                    warn!(
                        pair = %msg.plan.pair_id,
                        venue = %msg.plan.first.venue,
                        order_id = %orphan,
                        "orphan resting order left on venue; manual check required"
                    );
                    self.log_plan_record(
                        &msg.plan,
                        "orphan_order",
                        "cancel_failed",
                        &format!("order_id={orphan}"),
                    );
                }
                if hedged <= Decimal::ZERO {
                    warn!(pair = %msg.plan.pair_id, "exec reported success with zero hedged qty");
                    self.positions.release_pending(&msg.slot);
                    return;
                }
                if result.orphan_order.is_none() {
                    // 两腿干净成交：连击归零（对齐参考在成功路径上重置计数）。
                    self.intervention.clear_streak(&msg.plan.pair_id);
                }
                let mut rec_plan = msg.plan.clone();
                rec_plan.qty = hedged;
                let detail = format!("hedged={hedged} planned={}", msg.plan.qty);
                self.log_plan_record(
                    &rec_plan,
                    if rec_plan.is_open { "open" } else { "close" },
                    "both_filled",
                    &detail,
                );
                self.naked_exposures
                    .retain(|n| n.pair_id != msg.plan.pair_id);
                self.apply_fill(&pair, &msg.plan, &result, pair_i);
                if msg.plan.rest_quote {
                    if let Some(side) = msg.plan.quote_side {
                        self.cancel_quote_side(&msg.slot, side.opposite());
                    }
                }
                // 第二腿少成交的部分是真实单边敞口。必须排在 retain 之后，
                // 否则刚登记就被这一行清掉。
                if result.unhedged_qty > Decimal::ZERO {
                    self.record_naked_from_failed_hedge(&msg.plan, result.unhedged_qty);
                }
                // 两腿都对冲上了，但第一腿还留着一张撤不掉的单。仓位记账是
                // 对的，可那张单随时可能成交出第三条腿——先停手。
                // 必须排在 `apply_fill` 之后：挂起要记的是本笔成交后的格数。
                if let Some(oid) = result.orphan_order.clone() {
                    self.mark_intervention(
                        &msg.slot,
                        &msg.plan,
                        Cause::OrphanOrder,
                        format!(
                            "hedge ok but order {oid} on {} could not be canceled",
                            msg.plan.first.venue
                        ),
                    );
                }
                if msg.plan.rest_quote {
                    self.finish_adjacent_slot(&msg.slot);
                }
            }
            Err(err) => {
                if err.contains("EMERGENCY_CLOSED") {
                    let recovered = if msg.plan.rest_quote {
                        "second leg failed; first emergency closed"
                    } else {
                        "dual-market unhedged leg closed"
                    };
                    warn!(pair = %msg.plan.pair_id, error = %err, "{recovered}");
                    self.log_plan_record(&msg.plan, "exec_fail", "emergency_closed", &err);
                    // 紧急平仓**成功**，敞口已经收掉，仓位状态是干净的，
                    // 所以不挂起。但这算一次单腿成交：参考的规则是连续 3 次
                    // 即使每次都补上也要挂起，因为那说明链路有系统性问题。
                    let n = self.intervention.note_single_leg(&msg.plan.pair_id);
                    if n >= SINGLE_LEG_STREAK_LIMIT {
                        self.mark_intervention(
                            &msg.slot,
                            &msg.plan,
                            Cause::SingleLegStreak,
                            format!("{n} consecutive single-leg fills (all recovered, but link looks broken)"),
                        );
                    } else {
                        warn!(
                            pair = %msg.plan.pair_id,
                            streak = n,
                            limit = SINGLE_LEG_STREAK_LIMIT,
                            "single-leg fill recovered; will pause this pair if the streak reaches the limit"
                        );
                    }
                } else if err.contains("SECOND_LEG_UNKNOWN") {
                    if msg.plan.rest_quote {
                        warn!(
                            pair = %msg.plan.pair_id,
                            error = %err,
                            "second leg outcome unknown; first leg left in place on purpose"
                        );
                        self.log_plan_record(&msg.plan, "exec_fail", "second_leg_unknown", &err);
                        let signed = if msg.plan.first.is_buy {
                            msg.plan.qty
                        } else {
                            -msg.plan.qty
                        };
                        let exposure = crate::app::reconcile::NakedExposure {
                            pair_id: msg.plan.pair_id.clone(),
                            venue: msg.plan.first.venue.clone(),
                            qty: signed,
                            counterparty: msg.plan.second.venue.clone(),
                            source: NakedSource::SecondLegUnknown,
                        };
                        if !self.naked_exposures.iter().any(|n| {
                            n.pair_id == exposure.pair_id && n.venue == exposure.venue
                        }) {
                            warn!(
                                pair = %exposure.pair_id,
                                venue = %exposure.venue,
                                qty = %exposure.qty,
                                "second leg unknown — manual check required before resuming"
                            );
                            self.naked_exposures.push(exposure);
                        }
                        self.mark_intervention(
                            &msg.slot,
                            &msg.plan,
                            Cause::SecondLegUnknown,
                            format!(
                                "second leg on {} unverifiable; check venue before resuming",
                                msg.plan.second.venue
                            ),
                        );
                    } else {
                        warn!(
                            pair = %msg.plan.pair_id,
                            error = %err,
                            "dual-market fill unverifiable; not sending more"
                        );
                        self.log_plan_record(&msg.plan, "exec_fail", "second_leg_unknown", &err);
                        self.mark_intervention(
                            &msg.slot,
                            &msg.plan,
                            Cause::SecondLegUnknown,
                            "dual-market fill unverifiable; check both venues before resuming"
                                .into(),
                        );
                    }
                } else if err.contains("NAKED_FIRST_LEG") {
                    warn!(pair = %msg.plan.pair_id, error = %err, "naked first leg");
                    self.log_plan_record(&msg.plan, "exec_fail", "naked", &err);
                    if msg.plan.rest_quote {
                        self.record_naked_from_failed_hedge(&msg.plan, msg.plan.qty);
                        self.mark_intervention(
                            &msg.slot,
                            &msg.plan,
                            Cause::NakedLegUnrecoverable,
                            format!("naked leg on {} and emergency close failed", msg.plan.first.venue),
                        );
                    } else {
                        self.mark_intervention(
                            &msg.slot,
                            &msg.plan,
                            Cause::NakedLegUnrecoverable,
                            format!("dual-market emergency close failed; {err}"),
                        );
                    }
                } else if err.contains("QUOTE_LOST_RACE") {
                    info!(pair = %msg.plan.pair_id, "adjacent quote lost race; extra fill closed");
                    self.log_plan_record(&msg.plan, "cancel", "quote_lost_race", &err);
                    self.quote_quiet_until
                        .insert(msg.slot.clone(), Instant::now() + ADJACENT_RACE_QUIET);
                } else if err.contains("ARB_STOPPED") {
                    info!(
                        pair = %msg.plan.pair_id,
                        "adjacent fill after stop; not hedging"
                    );
                    self.log_plan_record(&msg.plan, "cancel", "arb_stopped", &err);
                } else if err.contains("limit_zero_fill") {
                    info!(pair = %msg.plan.pair_id, "limit-then-market: zero fill after wait/cancel");
                    self.log_plan_record(&msg.plan, "cancel", "zero_fill", &err);
                } else {
                    warn!(pair = %msg.plan.pair_id, error = %err, "limit-then-market failed");
                    self.log_plan_record(&msg.plan, "exec_fail", "error", &err);
                }
                if err.contains("ORPHAN_ORDER") {
                    warn!(
                        pair = %msg.plan.pair_id,
                        venue = %msg.plan.first.venue,
                        "orphan resting order left on venue; manual check required"
                    );
                    // 撤不掉的挂单可能稍后成交，届时会凭空多出一条腿。
                    // 在搞清楚它到底成没成之前不能继续交易这个币。
                    self.mark_intervention(
                        &msg.slot,
                        &msg.plan,
                        Cause::OrphanOrder,
                        format!("uncancelable resting order on {}", msg.plan.first.venue),
                    );
                }
                if msg.plan.rest_quote {
                    self.finish_adjacent_slot(&msg.slot);
                    if !err.contains("limit_zero_fill")
                        && !err.contains("QUOTE_LOST_RACE")
                        && !err.contains("ARB_STOPPED")
                    {
                        self.forget_persist(&msg.slot);
                    }
                } else {
                    self.positions.release_pending(&msg.slot);
                    self.forget_persist(&msg.slot);
                }
            }
        }
    }

    fn log_plan_record(
        &self,
        plan: &HedgePlan,
        action: &str,
        result: &str,
        detail: &str,
    ) {
        let Some(hub) = &self.api else {
            return;
        };
        hub.push_execution(ExecRecord {
            ts: now_ts(),
            pair_id: plan.pair_id.clone(),
            action: action.to_string(),
            buy_venue: plan.buy_venue.clone(),
            sell_venue: plan.sell_venue.clone(),
            qty: plan.qty,
            net_pct: if plan.is_open && action == "open" {
                Some(plan.decision_net_pct)
            } else {
                None
            },
            result: result.to_string(),
            detail: detail.to_string(),
            grid_from: Some(plan.grid_from),
            grid_to: Some(plan.grid_to),
        });
    }

    #[allow(clippy::too_many_arguments)]
    /// 挂起一个 pair 等人工处理。对齐参考 `_mark_manual_intervention`。
    ///
    /// 只在**首次**挂起时打 ERROR + 写 journal；重复触发不重置计时，否则
    /// 反复报错会把 30 分钟自动解除无限推后，等于永久锁死这个币。
    /// 挂起时点的持仓格数，供 `should_block` 的「格数变化 → 解除」用。
    ///
    /// 空仓或单格量未知时返回 `None`：解除判定要求挂起时和当前都是 `Some`，
    /// 拿不准就只走 30 分钟超时，不猜。
    fn current_grid_level(&self, slot: &str) -> Option<i32> {
        let pos = self.positions.get(slot)?;
        if pos.base_qty <= Decimal::ZERO {
            return None;
        }
        Some(pos.grid)
    }

    /// 挂起一个币对等待人工介入。
    ///
    /// `slot` 用来读**记账完成后**的格数——调用点必须在 `apply_fill` 之后，
    /// 否则记下的是成交前的旧格数，下一轮 `should_block` 会把这次成交本身
    /// 造成的格数变化当成「行情换区间」，刚挂起就自动解除。
    fn mark_intervention(&mut self, slot: &str, plan: &HedgePlan, cause: Cause, detail: String) {
        self.mark_intervention_for(&plan.pair_id, slot, cause, detail);
    }

    fn mark_intervention_for(&mut self, pair_id: &str, slot: &str, cause: Cause, detail: String) {
        let grid_level = self.current_grid_level(slot);
        let first = self.intervention.mark(
            pair_id,
            cause,
            detail.clone(),
            grid_level,
            Instant::now(),
        );
        if !first {
            return;
        }
        let mins = super::intervention::AUTO_RESUME.as_secs() / 60;
        tracing::error!(
            pair = %pair_id,
            cause = cause.as_str(),
            detail = %detail,
            grid_level = ?grid_level,
            auto_resume_mins = mins,
            "MANUAL INTERVENTION REQUIRED: pausing this pair (opens and closes both blocked)"
        );
        self.log_record(
            pair_id,
            "",
            "",
            Decimal::ZERO,
            "intervention",
            cause.as_str(),
            &detail,
        );
    }

    fn maybe_mark_dust(
        &mut self,
        slot: &str,
        pair: &Pair,
        pos: Option<&crate::domain::Position>,
        params: &crate::domain::GridParams,
    ) {
        let Some(p) = pos.filter(|p| p.qty > Decimal::ZERO) else {
            self.dust_since.remove(slot);
            return;
        };
        let dust = params.min_qty > Decimal::ZERO && p.qty < params.min_qty;
        if !dust {
            self.dust_since.remove(slot);
            return;
        }
        let since = *self
            .dust_since
            .entry(slot.to_string())
            .or_insert_with(Instant::now);
        if since.elapsed() > Duration::from_secs(300) {
            self.mark_intervention_for(
                &pair.pair_id,
                slot,
                Cause::NakedBelowMinQty,
                format!("residual {} below min_qty {}", p.qty, params.min_qty),
            );
        }
    }

    fn log_record(
        &self,
        pair_id: &str,
        buy_venue: &str,
        sell_venue: &str,
        qty: Decimal,
        action: &str,
        result: &str,
        detail: &str,
    ) {
        let Some(hub) = &self.api else {
            return;
        };
        hub.push_execution(ExecRecord {
            ts: now_ts(),
            pair_id: pair_id.to_string(),
            action: action.to_string(),
            buy_venue: buy_venue.to_string(),
            sell_venue: sell_venue.to_string(),
            qty,
            net_pct: None,
            result: result.to_string(),
            detail: detail.to_string(),
            grid_from: None,
            grid_to: None,
        });
    }

    fn bump_session_volume(&mut self, fill: &crate::exec::ExecFill) {
        if fill.qty <= Decimal::ZERO || fill.price <= Decimal::ZERO {
            return;
        }
        *self
            .session_volume
            .entry(fill.venue.clone())
            .or_insert(Decimal::ZERO) += fill.qty * fill.price;
    }

    /// 用**实际成交量**回写持仓，不是计划量：部分成交时用 plan.qty
    /// 会让内存持仓虚高，之后按虚高量平仓就留下尾巴。
    fn apply_fill(&mut self, pair: &Pair, plan: &HedgePlan, result: &ExecResult, pair_i: usize) {
        let qty = result.hedged_qty();
        let entry_net = self.realized_entry_net(plan, result);
        let entry_raw = self.realized_entry_raw(plan, result);
        self.bump_session_volume(&result.first);
        self.bump_session_volume(&result.second);
        if plan.is_open {
            let notional = qty
                * self
                    .book(&plan.buy_venue, &pair.pair_id)
                    .and_then(|bb| {
                        self.book(&plan.sell_venue, &pair.pair_id)
                            .and_then(|sb| mid_from_bbo(&bb, &sb))
                    })
                    .unwrap_or(Decimal::ZERO);
            let prev_k = self.positions.get(&plan.slot).map(|p| p.grid).unwrap_or(0);
            self.positions.record_open(
                &plan.slot,
                &pair.pair_id,
                VenueId::from(plan.buy_venue.as_str()),
                VenueId::from(plan.sell_venue.as_str()),
                qty,
                plan.grid_to,
                notional,
                entry_net,
                entry_raw,
                plan.base_qty,
                result.price_on(&plan.buy_venue).unwrap_or(Decimal::ZERO),
                result.price_on(&plan.sell_venue).unwrap_or(Decimal::ZERO),
            );
            if prev_k == 0 && plan.grid_to != 0 {
                self.windows.freeze(&plan.slot);
            }
            info!(
                pair = %pair.pair_id,
                qty = %qty,
                step = plan.grid_to,
                notional_usdc = %notional.round_dp(2),
                entry_net_pct = %entry_net.round_dp(4),
                "position opened"
            );
        } else {
            self.positions.record_close(&plan.slot, qty);
            if self.positions.get(&plan.slot).is_none() {
                self.windows.unfreeze(&plan.slot);
                self.last_flat_at.insert(plan.slot.clone(), Instant::now());
            }
            info!(pair = %pair.pair_id, qty = %qty, step = plan.grid_to, "position closed");
        }
        self.forget_persist(&plan.slot);
        let label = if plan.is_open {
            "filled_open"
        } else {
            "filled_close"
        };
        self.set_spread(
            pair_i,
            dashboard::spread_lines(
                &pair.pair_id,
                plan.buy_venue.as_str(),
                plan.sell_venue.as_str(),
                Decimal::ZERO,
                entry_net,
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                0,
                self.cfg.history.min_points,
                label,
            ),
        );
    }

    /// 建仓净边优先按两腿真实成交价算；成交价拿不到（市价腿 sidecar 不回
    /// avg_price）时退回决策时的净边。**不扣 nat**：nat 是结构性基差，
    /// 平仓时会对称地还回来，扣了会低估往返净利。
    fn realized_entry_net(&self, plan: &HedgePlan, result: &ExecResult) -> Decimal {
        let (buy_px, sell_px) = if result.first.is_buy {
            (result.first.price, result.second.price)
        } else {
            (result.second.price, result.first.price)
        };
        let fee = match plan.style {
            OrderStyle::MarketTaker | OrderStyle::AggressiveLimit => {
                self.cfg.taker_fee(&VenueId::from(plan.buy_venue.as_str()))
                    + self.cfg.taker_fee(&VenueId::from(plan.sell_venue.as_str()))
            }
            _ => self.cfg.round_leg_fee(
                &VenueId::from(plan.buy_venue.as_str()),
                &VenueId::from(plan.sell_venue.as_str()),
            ),
        };
        match raw_spread_pct(buy_px, sell_px) {
            Some(raw) if buy_px > Decimal::ZERO && sell_px > Decimal::ZERO => raw - fee,
            _ => plan.decision_net_pct,
        }
    }

    fn realized_entry_raw(&self, plan: &HedgePlan, result: &ExecResult) -> Decimal {
        let (buy_px, sell_px) = if result.first.is_buy {
            (result.first.price, result.second.price)
        } else {
            (result.second.price, result.first.price)
        };
        match raw_spread_pct(buy_px, sell_px) {
            Some(raw) if buy_px > Decimal::ZERO && sell_px > Decimal::ZERO => raw,
            _ => plan.decision_raw_pct,
        }
    }

    /// 两个方向都采样再取该方向的 nat。
    /// 只采「当轮最优方向」会让样本变成条件分布，中位数系统性偏高，
    /// residual 被长期压低到永远开不了仓。nat 只对跨 DEX 有意义。
    fn sample_and_natural(
        &self,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        cross: bool,
    ) -> Option<NaturalSpread> {
        let store = self.history.as_ref()?;
        if !cross {
            return None;
        }
        let v0 = pair.legs[0].venue.as_str();
        let v1 = pair.legs[1].venue.as_str();
        let b0 = self.book(v0, &pair.pair_id)?;
        let b1 = self.book(v1, &pair.pair_id)?;
        for (buy, sell, bb, sb) in [(v0, v1, &b0, &b1), (v1, v0, &b1, &b0)] {
            let Some(raw) = raw_spread_pct(bb.ask, sb.bid) else {
                continue;
            };
            if let Err(err) = store.maybe_sample(&pair.pair_id, buy, sell, raw, raw) {
                warn!(error = %err, pair = %pair.pair_id, "history sample failed");
            }
        }
        store.natural(&pair.pair_id, net.buy.as_str(), net.sell.as_str())
    }

    fn leg_margin(&self, reserved: &HashMap<String, Decimal>, venue: &str) -> LegMargin {
        LegMargin {
            available_usdc: self.balance.venue_available(venue),
            leverage: self.cfg.leverage_for(venue),
            reserved_usdc: reserved.get(venue).copied().unwrap_or(Decimal::ZERO),
        }
    }

    fn mark_ui_status(&mut self, slot: &str, status: &str) {
        if let Some(row) = self.ui_pairs.get_mut(slot) {
            row.status = status.to_string();
        }
    }

    fn paint_skip_with_books(
        &mut self,
        slot: &str,
        pair: &Pair,
        v0: &VenueId,
        v1: &VenueId,
        b0: &Bbo,
        b1: &Bbo,
        pos: Option<&crate::domain::Position>,
        reason: &str,
    ) {
        let net = match pos {
            Some(p) => {
                let (bb, sb) = books_for_direction(&p.buy, v0, b0, b1);
                sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO)
            }
            None => best_sequenced_spread(&self.cfg, v0, v1, b0, b1, Decimal::ZERO),
        };
        let label = skip_reason_label(reason);
        if let Some(net) = net {
            self.fill_monitor_row(
                slot,
                pair,
                &net,
                pos,
                &label,
                b0,
                b1,
                mid_from_bbo(b0, b1),
            );
        } else {
            self.mark_ui_status(slot, &label);
        }
    }

    fn ui_nat(
        &self,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        official: Option<&NaturalSpread>,
    ) -> Option<Decimal> {
        if let Some(n) = official {
            return Some(n.value);
        }
        self.history.as_ref()?.preview_natural(
            &pair.pair_id,
            net.buy.as_str(),
            net.sell.as_str(),
        )
        .map(|n| n.value)
    }

    fn fill_monitor_row(
        &mut self,
        slot: &str,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        pos: Option<&crate::domain::Position>,
        status: &str,
        _b0: &Bbo,
        _b1: &Bbo,
        _mid: Option<Decimal>,
    ) {
        let Some(mut params) = self.grid_params(pair) else {
            self.mark_ui_status(slot, "未配置");
            return;
        };
        if let Some(p) = pos.filter(|p| p.base_qty > Decimal::ZERO) {
            params.base_qty = p.base_qty;
        }
        let cross = is_cross_dex(net.buy.as_str(), net.sell.as_str());
        let official = self.sample_and_natural(pair, net, cross);
        let nat_value = self.ui_nat(pair, net, official.as_ref());
        let residual = if cross {
            match nat_value {
                Some(n) => residual_net(net.net_pct, n),
                None => net.net_pct,
            }
        } else {
            net.net_pct
        };
        self.record_ui_pair(slot, pair, net, &params, pos, status, residual, nat_value);
    }

    fn record_ui_pair(
        &mut self,
        slot: &str,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        params: &crate::domain::GridParams,
        pos: Option<&crate::domain::Position>,
        status: &str,
        residual: Decimal,
        nat: Option<Decimal>,
    ) {
        let actual = pos.map(|p| p.qty).unwrap_or(Decimal::ZERO);
        let step = pos.map(|p| p.grid).unwrap_or(0);
        let n = self.windows.sample_count(slot);
        let cap = self.windows.cap();
        let mu = self.windows.quote_mu(slot);
        let last_s = self.windows.last_s(slot);
        let entry = mu
            .map(api::fmt_pct)
            .unwrap_or_else(|| format!("{n}/{cap}"));
        let dev = match (last_s, mu) {
            (Some(s), Some(m)) => api::fmt_pct(s - m),
            _ => "—".into(),
        };
        self.ui_pairs.insert(
            slot.to_string(),
            PairRow {
                pair_id: pair.pair_id.clone(),
                buy: net.buy.to_string(),
                sell: net.sell.to_string(),
                raw_pct: api::fmt_pct(net.raw_pct),
                net_pct: api::fmt_pct(net.net_pct),
                fee_pct: api::fmt_pct(net.fee_pct),
                nat_pct: nat.map(api::fmt_pct).unwrap_or_else(|| "—".into()),
                res_pct: api::fmt_pct(residual),
                entry_pct: entry,
                dev_pct: dev,
                delta_pct: api::fmt_pct(self.live_delta(pair)),
                grid: fmt_step(step),
                target_qty: api::fmt_qty(params.base_qty),
                actual_qty: api::fmt_qty(actual),
                status: status.to_string(),
            },
        );
    }

    fn available_pairs_payload(&self) -> Vec<AvailableSymbol> {
        use std::collections::BTreeMap;
        let mut by_symbol: BTreeMap<String, AvailableSymbol> = BTreeMap::new();
        for pair in &self.available_pairs {
            let symbol = pair.legs[0].base.clone();
            let v0 = pair.legs[0].venue.as_str();
            let v1 = pair.legs[1].venue.as_str();
            let mid = match (
                self.book(v0, &pair.pair_id),
                self.book(v1, &pair.pair_id),
            ) {
                (Some(a), Some(b)) => mid_from_bbo(&a, &b).map(|m| m.to_string()),
                _ => None,
            };
            let entry = by_symbol.entry(symbol.clone()).or_insert_with(|| AvailableSymbol {
                pair_id: pair.pair_id.clone(),
                symbol: symbol.clone(),
                venue_pairs: Vec::new(),
            });
            entry.venue_pairs.push(AvailableVenuePair {
                venues: vec![v0.to_string(), v1.to_string()],
                min_qty: pair.min_qty().to_string(),
                qty_precision: pair.legs.iter().map(|l| l.qty_precision).min().unwrap_or(8),
                round_trip_fee_pct: {
                    let (fee, _, _) = self.pair_delta_inputs(
                        &pair.legs[0].venue,
                        &pair.legs[1].venue,
                    );
                    fee.to_string()
                },
                mid,
            });
        }
        by_symbol.into_values().collect()
    }

    fn publish_api_snapshot_throttled(&mut self, min_interval: Duration) {
        if self.last_snap_at.elapsed() < min_interval {
            return;
        }
        self.last_snap_at = Instant::now();
        self.publish_api_snapshot();
    }

    fn publish_api_snapshot(&self) {
        let Some(hub) = &self.api else {
            return;
        };
        let positions: Vec<PositionRow> = self
            .positions
            .all_open()
            .into_iter()
            .map(|p| PositionRow {
                pair_id: p.pair_id.clone(),
                buy: p.buy.to_string(),
                sell: p.sell.to_string(),
                qty: p.qty.to_string(),
                grid: p.grid,
                entry_notional: p.entry_notional_usdc.to_string(),
            })
            .collect();
        let balances: Vec<VenueBalanceRow> = self
            .cfg
            .venues
            .iter()
            .map(|v| {
                let acct = self.venue_accounts.get(v);
                VenueBalanceRow {
                    venue: v.clone(),
                    available: acct
                        .map(|a| a.available.to_string())
                        .unwrap_or_else(|| self.balance.venue_available(v).to_string()),
                    total: acct
                        .map(|a| a.total.to_string())
                        .unwrap_or_else(|| self.balance.venue_available(v).to_string()),
                }
            })
            .collect();
        let exchange_positions: Vec<ExchangePositionRow> = self
            .venue_accounts
            .venues
            .iter()
            .flat_map(|v| {
                v.positions.iter().map(|p| ExchangePositionRow {
                    venue: v.venue.clone(),
                    symbol: p.symbol.clone(),
                    qty: p.qty.to_string(),
                    entry_price: p.entry_price.map(|x| x.to_string()),
                })
            })
            .collect();
        let selected: Vec<String> = self
            .live_params()
            .map(|p| p.active_venues)
            .unwrap_or_default();
        let venue_stats: Vec<VenueLiveRow> = selected
            .iter()
            .map(|v| {
                let cap = self.venue_spreads.cap();
                let n = self.venue_spreads.sample_count(v);
                let spread_mu = self
                    .venue_spreads
                    .live_mu(v)
                    .map(|m| format!("{:.4}%", m))
                    .unwrap_or_else(|| {
                        if n == 0 {
                            "—".into()
                        } else {
                            format!("{n}/{cap}")
                        }
                    });
                let volume = self
                    .session_volume
                    .get(v)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                VenueLiveRow {
                    venue: v.clone(),
                    spread_mu,
                    volume: format!("{:.2}", volume.round_dp(2)),
                    place_rtt: place_rtt_text(v),
                }
            })
            .collect();
        let best = self
            .ui_pairs
            .values()
            .filter_map(|r| {
                let s = r.net_pct.trim().trim_end_matches('%');
                Decimal::from_str(s.trim_start_matches('+')).ok()
            })
            .max();
        let mut pairs: Vec<PairRow> = self.ui_pairs.values().cloned().collect();
        pairs.sort_by(|a, b| {
            a.pair_id
                .cmp(&b.pair_id)
                .then(a.buy.cmp(&b.buy))
                .then(a.sell.cmp(&b.sell))
        });
        hub.publish(LiveSnapshot {
            pairs,
            positions,
            balances,
            exchange_positions,
            venue_stats,
            naked_exposures: self
                .naked_exposures
                .iter()
                .map(|n| NakedExposureRow {
                    pair_id: n.pair_id.clone(),
                    venue: n.venue.clone(),
                    qty: n.qty.to_string(),
                    counterparty: n.counterparty.clone(),
                    source: match n.source {
                        NakedSource::Foreign => "foreign".into(),
                        NakedSource::BotFailure => "bot_failure".into(),
                        NakedSource::SecondLegUnknown => "second_leg_unknown".into(),
                    },
                })
                .collect(),
            venue_matches: self.venue_match_rows(),
            stats: api::ApiStats {
                matched_pairs: self.ui_pairs.len(),
                open_positions: self.positions.open_count(),
                best_net_pct: best.map(api::fmt_pct),
            },
            arbitrage_enabled: self
                .control
                .as_ref()
                .and_then(|c| c.lock().ok())
                .map(|c| c.enabled)
                .unwrap_or(self.cfg.execution.enabled),
            matching: self.matching,
            available: self.available_pairs_payload(),
            scan: self.build_scan_snapshot(),
            scan_running: self.scan_is_running(),
            updated_at: now_ts(),
        });
    }

    fn build_scan_snapshot(&self) -> ScanSnapshot {
        if !self.scan_is_running() && self.scan_phase == ScanPhase::Idle {
            return ScanSnapshot {
                status: ScanPhase::Idle.as_str().into(),
                ..ScanSnapshot::default()
            };
        }
        let target_bp = self.cfg.pairs.defaults.target_bp;
        let h = self.cfg.grid.step_hysteresis;
        let mut scored = Vec::new();
        for p in &self.scan_candidates {
            let fee = self
                .cfg
                .market_round_trip_taker(&p.legs[0].venue, &p.legs[1].venue);
            if let Some(s) = self.scan_engine.score(p, target_bp, fee, h) {
                scored.push(s);
            }
        }
        let domain_rows = rank_bases(
            scored,
            &self.scan_engine,
            &self.scan_venues,
            self.cfg.scan.watch_top,
        );
        let rows = domain_rows
            .into_iter()
            .map(|r| api::ScanRow {
                rank: r.rank,
                base: r.base,
                pair_id: r.pair_id,
                left: r.left,
                right: r.right,
                same_family: r.same_family,
                eligible: r.eligible,
                edge: api::fmt_pct(r.edge),
                sigma: api::fmt_pct(r.sigma),
                delta: api::fmt_pct(r.delta),
                mu: api::fmt_pct(r.mu),
                hub_c: api::fmt_pct(r.hub_c),
                crosses: r.crosses,
                n: r.n,
                cap: r.cap,
                venues: r
                    .venues
                    .into_iter()
                    .map(|(k, c)| {
                        (
                            k,
                            ScanVenueCell {
                                mid_mean: c
                                    .mid_mean
                                    .map(|m| format!("{m}"))
                                    .unwrap_or_else(|| "—".into()),
                                own_spread_mean: c
                                    .own_spread_mean
                                    .map(api::fmt_pct)
                                    .unwrap_or_else(|| "—".into()),
                            },
                        )
                    })
                    .collect(),
            })
            .collect();
        ScanSnapshot {
            updated_at: now_ts(),
            status: self.scan_phase.as_str().into(),
            error: self.scan_error.clone(),
            universe: self.scan_universe.len(),
            candidates: self.scan_candidates.len(),
            sampling_n: self.scan_engine.sampling_n(&self.scan_candidates),
            filled_n: self.scan_engine.filled_n(&self.scan_candidates),
            window_n: self.scan_engine.max_n(&self.scan_candidates),
            watch_top: self.cfg.scan.watch_top,
            window_samples: self.scan_engine.cap(),
            sample_interval_ms: self.cfg.grid.sample_interval_ms,
            venues: self.scan_venues.clone(),
            rows,
        }
    }

    fn quote_winner_taken(&self, slot: &str) -> bool {
        self.quote_races
            .get(slot)
            .is_some_and(|r| r.winner.load(Ordering::SeqCst))
    }

    fn finish_adjacent_slot(&mut self, slot: &str) {
        if self.slot_has_pending(slot) {
            return;
        }
        if !self.quote_winner_taken(slot) {
            self.positions.release_pending(slot);
        }
        self.quote_races.remove(slot);
    }

    fn adjacent_flags(
        &mut self,
        slot: &str,
        side: QuoteSide,
    ) -> (Arc<AtomicBool>, Arc<AtomicBool>, Arc<AtomicBool>) {
        let plus_key = quote_pending_key(slot, QuoteSide::Plus);
        let minus_key = quote_pending_key(slot, QuoteSide::Minus);
        let plus_pending = self.pending.contains_key(&plus_key);
        let minus_pending = self.pending.contains_key(&minus_key);
        if !plus_pending && !minus_pending {
            self.quote_races.remove(slot);
        }
        let race = self
            .quote_races
            .entry(slot.to_string())
            .or_insert_with(QuoteRace::new);
        match side {
            QuoteSide::Plus if !plus_pending && race.plus_cancel.load(Ordering::SeqCst) => {
                race.plus_cancel = Arc::new(AtomicBool::new(false));
            }
            QuoteSide::Minus if !minus_pending && race.minus_cancel.load(Ordering::SeqCst) => {
                race.minus_cancel = Arc::new(AtomicBool::new(false));
            }
            _ => {}
        }
        let (cancel, peer) = race.flags(side);
        (cancel, peer, race.winner.clone())
    }

    fn reserved_with_pending_quotes(
        &self,
        slot: &str,
        skip: QuoteSide,
        mid: Decimal,
    ) -> HashMap<String, Decimal> {
        let mut reserved = self.positions.reserved_margin_by_venue(
            |v| self.cfg.leverage_for(v),
            |p| self.position_mid(p),
        );
        if mid <= Decimal::ZERO {
            return reserved;
        }
        for side in [QuoteSide::Plus, QuoteSide::Minus] {
            if side == skip {
                continue;
            }
            let Some(p) = self.pending.get(&quote_pending_key(slot, side)) else {
                continue;
            };
            if !p.plan.is_open {
                continue;
            }
            let need = p.plan.qty * mid;
            if need <= Decimal::ZERO {
                continue;
            }
            for v in [&p.plan.buy_venue, &p.plan.sell_venue] {
                let m = need / self.cfg.leverage_for(v).max(Decimal::ONE);
                *reserved.entry(v.clone()).or_default() += m;
            }
        }
        reserved
    }

    fn slot_has_pending(&self, slot: &str) -> bool {
        self.pending.contains_key(&quote_pending_key(slot, QuoteSide::Plus))
            || self.pending.contains_key(&quote_pending_key(slot, QuoteSide::Minus))
            || self.pending.contains_key(slot)
    }

    /// 已有单边敞口时不再挂开仓邻档，避免在未对冲的 RH/lighter 仓上继续加码。
    fn pair_has_naked(&self, pair_id: &str) -> bool {
        self.naked_exposures
            .iter()
            .any(|n| n.pair_id == pair_id && n.qty.abs() > Decimal::ZERO)
    }

    fn cancel_quote_side(&mut self, slot: &str, side: QuoteSide) {
        if let Some(p) = self.pending.get(&quote_pending_key(slot, side)) {
            p.cancel.store(true, Ordering::Release);
        }
    }

    fn cancel_adjacent_quotes(&mut self, slot: &str) {
        self.cancel_quote_side(slot, QuoteSide::Plus);
        self.cancel_quote_side(slot, QuoteSide::Minus);
    }

    fn cancel_all_adjacent_quotes(&mut self) {
        for p in self.pending.values() {
            if p.rest_quote {
                p.cancel.store(true, Ordering::Release);
            }
        }
    }

    fn watch_adjacent_events(&mut self, pair: &Pair, slot: &str, _key: &str, pending: &PendingLimit) {
        let v0 = pair.legs[0].venue.clone();
        let v1 = pair.legs[1].venue.clone();
        if self.book(v0.as_str(), &pair.pair_id).is_none() {
            pending.cancel.store(true, Ordering::Release);
            return;
        }
        if self.book(v1.as_str(), &pair.pair_id).is_none() {
            pending.cancel.store(true, Ordering::Release);
            return;
        }
        // 盘口短暂过期（3s）或 BBO 闪一下不合法，不撤已挂单。
        // 撤了又会重挂，和「离格线够远」是同一类误撤。真正没盘口上面已经 return。
        let Some(params) = self.grid_params(pair) else {
            return;
        };
        let delta = params.step;
        let ratio = self.cfg.grid.quote_reprice_ratio;
        if !self.windows.is_frozen(slot) {
            let min_move = ratio * delta;
            if self.windows.maybe_advance_quote(slot, min_move) {
                info!(pair = %pair.pair_id, "μ_quote moved ≥ reprice ratio; cancel adjacent");
                self.cancel_adjacent_quotes(slot);
                return;
            }
        }
        let Some(mu) = self.windows.quote_mu(slot) else {
            pending.cancel.store(true, Ordering::Release);
            return;
        };
        let Some(side) = pending.side else {
            return;
        };
        let k = self.positions.get(slot).map(|p| p.grid).unwrap_or(0);
        let held = self.positions.get(slot).map(|p| p.qty).unwrap_or(Decimal::ZERO);
        let quotes = adjacent_quotes(
            k,
            mu,
            delta,
            params.max_segments as i32,
            self.cfg.grid.step_hysteresis,
            &v0,
            &v1,
            params.base_qty,
            held,
        );
        if !quotes.iter().any(|q| q.side == side) {
            info!(pair = %pair.pair_id, side = side.as_str(), "adjacent: cancel; side no longer quoted");
            pending.cancel.store(true, Ordering::Release);
        }
        // 加仓「离格线够远」只决定要不要新挂，不撤已经挂着的单。
        // 价差朝格线靠时正是要成交的时候；这里撤会变成无限挂撤，
        // 而且经常撤在成交竞态上，第一腿成了却认成零成交、不对冲。
        // 反推限价交叉同理：那是市价已经撞上挂单价，应让它成交。
    }

    fn quote_limit_price(
        &self,
        q: &AdjacentQuote,
        left: &VenueId,
        b0: &Bbo,
        b1: &Bbo,
    ) -> Option<Decimal> {
        let (first, _) = crate::exec::first_limit_venue_all_in_or_left(
            &self.cfg,
            &q.buy,
            &q.sell,
            left,
            self.venue_spreads.live_mu(q.buy.as_str()),
            self.venue_spreads.live_mu(q.sell.as_str()),
        );
        let first_is_left = first.as_str() == left.as_str();
        let first_is_buy = first.as_str() == q.buy.as_str();
        let mid0 = (b0.bid + b0.ask) / Decimal::from(2);
        let mid1 = (b1.bid + b1.ask) / Decimal::from(2);
        let avg = (mid0 + mid1) / Decimal::from(2);
        let tick = if first_is_left {
            b0.price_tick()
        } else {
            b1.price_tick()
        };
        implied_first_limit(
            q.target_spread,
            first_is_left,
            first_is_buy,
            b0.bid,
            b0.ask,
            b1.bid,
            b1.ask,
            avg,
            tick,
            self.cfg.order.maker_inside_ticks.max(1),
        )
    }

    fn cached_first_qty(&self, venue: &str, symbol: &str) -> Option<Decimal> {
        let acct = self.venue_accounts.get(venue)?;
        if !acct.fresh {
            return None;
        }
        Some(
            acct.positions
                .iter()
                .filter(|p| symbol_matches_symbol(&p.symbol, symbol, symbol))
                .map(|p| p.qty)
                .sum(),
        )
    }

    async fn maintain_adjacent_quotes(
        &mut self,
        pair_i: usize,
        pair: &Pair,
        slot: &str,
        v0: &VenueId,
        v1: &VenueId,
        b0: &Bbo,
        b1: &Bbo,
        params: &crate::domain::GridParams,
        k: i32,
        held_qty: Decimal,
        has_pos: bool,
        mid: Decimal,
        net: &crate::domain::NetSpread,
        pos: Option<&crate::domain::Position>,
        mu: Option<Decimal>,
        s_plus: Option<Decimal>,
        s_minus: Option<Decimal>,
        spread_rt: Option<Decimal>,
    ) {
        if mu.is_none() {
            self.cancel_adjacent_quotes(slot);
            let n = self.windows.sample_count(slot);
            let cap = self.windows.cap();
            self.fill_monitor_row(
                slot,
                pair,
                net,
                pos,
                &format!("采样 {n}/{cap}"),
                b0,
                b1,
                Some(mid),
            );
            return;
        }
        if spread_rt.is_none() && !has_pos {
            let cap = self.venue_spreads.cap();
            let n0 = self.venue_spreads.sample_count(v0.as_str());
            let n1 = self.venue_spreads.sample_count(v1.as_str());
            if self.slot_has_pending(slot) {
                let n_rest = [QuoteSide::Plus, QuoteSide::Minus]
                    .iter()
                    .filter(|s| self.pending.contains_key(&quote_pending_key(slot, **s)))
                    .count();
                self.fill_monitor_row(
                    slot,
                    pair,
                    net,
                    pos,
                    &format!("邻档 {n_rest}/2"),
                    b0,
                    b1,
                    Some(mid),
                );
            } else {
                self.fill_monitor_row(
                    slot,
                    pair,
                    net,
                    pos,
                    &format!("点差 {n0}/{cap} {n1}/{cap}"),
                    b0,
                    b1,
                    Some(mid),
                );
            }
            return;
        }
        if self.quote_winner_taken(slot) || self.hedging.contains(slot) {
            self.fill_monitor_row(slot, pair, net, pos, "对冲中", b0, b1, Some(mid));
            return;
        }
        if self
            .quote_quiet_until
            .get(slot)
            .is_some_and(|until| Instant::now() < *until)
        {
            self.fill_monitor_row(slot, pair, net, pos, "竞态冷却", b0, b1, Some(mid));
            return;
        }
        self.quote_quiet_until.remove(slot);
        let (fee_rt, c_rt, _) = self.pair_delta_inputs(v0, v1);
        if fee_rt <= Decimal::ZERO {
            self.fill_monitor_row(slot, pair, net, pos, "费率未加载", b0, b1, Some(mid));
            return;
        }
        if !has_pos {
            let min_move = self.cfg.grid.quote_reprice_ratio * params.step;
            if self.windows.maybe_advance_quote(slot, min_move) {
                self.cancel_adjacent_quotes(slot);
            }
        }
        let Some(mu) = self.windows.quote_mu(slot).or(mu) else {
            return;
        };
        let quotes = adjacent_quotes(
            k,
            mu,
            params.step,
            params.max_segments as i32,
            self.cfg.grid.step_hysteresis,
            v0,
            v1,
            params.base_qty,
            held_qty,
        );
        let gap = self.cfg.grid.min_quote_gap_ratio * params.step;
        let wanted: Vec<QuoteSide> = quotes.iter().map(|q| q.side).collect();
        for side in [QuoteSide::Plus, QuoteSide::Minus] {
            if !wanted.contains(&side) {
                self.cancel_quote_side(slot, side);
            }
        }
        let n_rest = wanted
            .iter()
            .filter(|s| self.pending.contains_key(&quote_pending_key(slot, **s)))
            .count();
        let status = if n_rest == 0 && self.pair_has_naked(&pair.pair_id) {
            "单边敞口".to_string()
        } else {
            format!("邻档 {n_rest}/{}", quotes.len())
        };
        self.fill_monitor_row(slot, pair, net, pos, &status, b0, b1, Some(mid));
        if self.hedging.contains(slot) {
            return;
        }
        for q in quotes {
            let key = quote_pending_key(slot, q.side);
            if self.pending.contains_key(&key) {
                continue;
            }
            if self.pair_has_naked(&pair.pair_id) {
                continue;
            }
            if q.is_open {
                let s = if q.side == QuoteSide::Plus {
                    s_plus
                } else {
                    s_minus
                };
                if let Some(s) = s {
                    if !add_quote_far_enough(q.target_spread, s, q.side == QuoteSide::Plus, gap) {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            if q.is_open {
                let reserved = self.reserved_with_pending_quotes(slot, q.side, mid);
                let (bb, sb) = books_for_direction(&q.buy, v0, b0, b1);
                if check_capacity(
                    &self.cfg.sizing,
                    q.qty,
                    self.leg_margin(&reserved, q.buy.as_str()),
                    self.leg_margin(&reserved, q.sell.as_str()),
                    bb,
                    sb,
                    mid,
                )
                .is_err()
                {
                    continue;
                }
            }
            let Some(px) = self.quote_limit_price(&q, v0, b0, b1) else {
                continue;
            };
            let Some(mut plan) = plan_adjacent(
                pair,
                &q,
                &self.cfg,
                v0,
                px,
                k,
                params.base_qty,
                self.venue_spreads.live_mu(q.buy.as_str()),
                self.venue_spreads.live_mu(q.sell.as_str()),
            )
            else {
                continue;
            };
            plan.decision_net_pct = net.net_pct;
            plan.decision_raw_pct = net.raw_pct;
            let baseline = self
                .cached_first_qty(&plan.first.venue, &plan.first.symbol)
                .unwrap_or(Decimal::ZERO);
            let (cancel, peer_cancel, winner) = self.adjacent_flags(slot, q.side);
            if q.is_open && !self.positions.is_pending(slot) {
                self.positions.reserve_open(slot);
            }
            self.pending.insert(
                key,
                PendingLimit {
                    plan: plan.clone(),
                    since: Instant::now(),
                    cancel: cancel.clone(),
                    rest_quote: true,
                    side: Some(q.side),
                },
            );
            info!(
                pair = %plan.pair_id,
                side = q.side.as_str(),
                first = %plan.first.venue,
                px = %px,
                target = %q.target_spread.round_dp(4),
                delta = %params.step.round_dp(4),
                fee = %fee_rt.round_dp(4),
                spread_c = %c_rt.round_dp(4),
                open = plan.is_open,
                "adjacent: post first-leg limit"
            );
            spawn_limit_market(
                self.exec_tx.clone(),
                self.cfg.clone(),
                self.adapters_by_id.clone(),
                self.books.clone(),
                pair_i,
                plan,
                LimitMarketRun {
                    baseline,
                    min_qty: params.min_qty.max(Decimal::new(1, 8)),
                    cancel,
                    rest_until_event: true,
                    peer_cancel: Some(peer_cancel),
                    winner: Some(winner),
                    orders_live: Arc::clone(&self.orders_live),
                },
            );
        }
    }
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn fmt_step(k: i32) -> String {
    if k == 0 {
        "0".into()
    } else {
        format!("{k:+}")
    }
}

fn force_market_taker(plan: &mut HedgePlan) {
    plan.style = OrderStyle::MarketTaker;
    plan.first.style = OrderStyle::MarketTaker;
    plan.second.style = OrderStyle::MarketTaker;
}

fn place_rtt_text(venue: &str) -> String {
    match crate::exchange::last_place_rtt(venue) {
        Some(r) => {
            if let (Some(sign), Some(send), Some(ack)) = (r.sign_ms, r.send_ms, r.sign_to_ack_ms) {
                format!("{}+{}={}ms · 全链路{}ms", sign, send, ack, r.wall_ms)
            } else {
                format!("{}ms", r.wall_ms)
            }
        }
        None => "—".into(),
    }
}

fn skip_reason_label(reason: &str) -> String {
    match reason {
        "thin_book" => "深度不足".into(),
        "stale" => "盘口过期".into(),
        "invalid_bbo" => "盘口非法".into(),
        "no_min_qty" => "无最小量".into(),
        "no_margin" | "no_capacity" => "保证金不足".into(),
        "no_mid" => "无中价".into(),
        "no_size" => "数量无效".into(),
        "in_flight" => "排队".into(),
        "intervention" => "人工介入".into(),
        "no_plan" => "无法规划".into(),
        "no_baseline" => "无底仓".into(),
        other => other.to_string(),
    }
}

fn naked_key(n: &NakedExposure) -> String {
    format!("{}|{}", n.pair_id, n.venue)
}

/// 按买所是不是 legs[0] 决定 (buy_book, sell_book)。
fn books_for_direction<'a>(
    buy: &VenueId,
    v0: &VenueId,
    b0: &'a Bbo,
    b1: &'a Bbo,
) -> (&'a Bbo, &'a Bbo) {
    if buy == v0 {
        (b0, b1)
    } else {
        (b1, b0)
    }
}

fn intent_label(intent: &Intent) -> &'static str {
    match intent {
        Intent::Open { .. } => "open",
        Intent::Close { .. } => "close",
        Intent::Hold => "hold",
    }
}

fn ui_intent_label(intent: &Intent, _stats_label: &str) -> &'static str {
    match intent {
        Intent::Open { .. } => "开仓中",
        Intent::Close { .. } => "平仓中",
        Intent::Hold => "持有",
    }
}
