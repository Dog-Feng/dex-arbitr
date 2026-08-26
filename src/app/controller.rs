use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{AppConfig, OrderStyle};
use crate::domain::spread::raw_spread_pct;
use crate::domain::{
    is_cross_dex, match_all_pairs, new_books, read_book, whitelist_allows, Bbo, Books, CloseReason,
    CloseView, GridEngine, Intent, Pair, VenueId, VenueMarket,
};
use crate::exchange::{make_adapter, ExchangePort};
use crate::exec::{
    best_sequenced_spread, closing_sequenced_spread, plan_hedge, sequence::sequenced_spread,
    watch_resting_limit, Adapters, ExecResult, HedgeExecutor, HedgePlan, LimitMarketRun, LimitWatch,
};
use crate::infra::api::{
    self, ApiHub, ExchangePositionRow, LiveSnapshot, NakedExposureRow, PairRow, PositionRow,
    VenueBalanceRow, VenueMatchRow,
};
use crate::infra::dashboard::{self, LivePanel};
use crate::infra::history::{residual_net, HistoryStore, NaturalSpread};
use crate::infra::journal::{ExecRecord, now_ts};

use super::backoff::{self, BackoffController, BackoffParams};
use super::balance::{refresh_accounts, BalanceCache, VenueAccountCache};
use super::control::{ArbitrageControl, ArbitrageParams};
use super::limits::{balance_health, position_expired, BalanceHealth, DailyTrades, NotionalLimits};
use super::exec_worker::{
    spawn_account_refresher, spawn_funding_refresher, spawn_limit_market, spawn_run_plan, ExecEvent,
    RunPlanMsg,
};
use super::funding::{refresh_funding, FundingCache, UnfavorableTracker};
use super::positions::PositionStore;
use super::reconcile::{
    audit_position_qty, counterparty_hedge_is_buy, detect_naked_exposures, hedge_qty,
    symbol_matches_symbol, NakedExposure, NakedSource,
};
use super::intervention::{Cause, Gate, InterventionGuard, SINGLE_LEG_STREAK_LIMIT};
use super::probe;
use super::reduce_only::{is_reduce_only_error, probe_due, ReduceOnlyGuard};
use super::risk::{books_quality_ok, books_tradable, stable_ok};
use super::scan::OpportunityTracker;
use super::stability::{Action as StabilityAction, StabilityTracker};
use super::sizing::{mid_from_bbo, preview_segment_qty, resolve_qty, BindingLeg, LegMargin};

pub struct Controller {
    cfg: AppConfig,
    adapters: Vec<Arc<dyn ExchangePort>>,
    adapters_by_id: Adapters,
    pairs: Vec<Pair>,
    books: Books,
    positions: PositionStore,
    grid: GridEngine,
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
    scanner: OpportunityTracker,
    last_token_log: HashMap<String, LoggedToken>,
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
    stability: StabilityTracker,
    /// 交易所侧「只能减仓」拉闸。挡开仓，放行平仓。
    reduce_only: ReduceOnlyGuard,
    intervention: InterventionGuard,
    funding: FundingCache,
    funding_unfavorable: UnfavorableTracker,
    /// nonce / 限流错误的按所退避。挡开仓，放行平仓。
    backoff: BackoffController,
    /// 每日开仓次数配额（本地日切）。
    daily: DailyTrades,
    /// 余额低于清仓线的所。命中后该所仓位一律强平。
    balance_critical: HashSet<String>,
    /// 余额低于告警线的所。只停开新仓，已有仓位照常按格子平。
    ///
    /// 和 `balance_critical` 分开而不是用一个枚举：两者的作用面不同，
    /// 一个进平仓判定、一个进开仓判定，分开存能让调用点各查各的、
    /// 不必每次都去解一层 `BalanceHealth`。
    balance_low: HashSet<String>,
    /// 上次把内存盘口推到 HTTP 快照的时间。事件环按 WS 更新，但推页面要节流。
    last_snap_at: Instant,
}

struct LoggedToken {
    pair_id: String,
    buy: String,
    sell: String,
    raw: Decimal,
    residual: Decimal,
    at: Instant,
}

#[derive(Clone)]
struct PendingLimit {
    plan: HedgePlan,
    since: Instant,
    cancel: Arc<AtomicBool>,
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
                info!(venue = id, "no signing keys; monitor_only still works");
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
        // cfg 随后被移进结构体，退避参数先取出来。
        let cfg_risk_backoff_min = cfg.risk.backoff_min_secs;
        let cfg_risk_backoff_max = cfg.risk.backoff_max_secs;
        let cfg_risk_backoff_mult = cfg.risk.backoff_multiplier;
        let cfg_risk_backoff_reset = cfg.risk.backoff_reset_secs;
        let mut this = Self {
            cfg,
            adapters,
            adapters_by_id,
            pairs: Vec::new(),
            books: new_books(),
            positions: PositionStore::default(),
            grid: GridEngine::default(),
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
            scanner: OpportunityTracker::default(),
            last_token_log: HashMap::new(),
            balance: BalanceCache::default(),
            venue_accounts: VenueAccountCache::default(),
            api: api_hub,
            control,
            ui_pairs: HashMap::new(),
            naked_exposures: Vec::new(),
            naked_hedging: HashSet::new(),
            stability: StabilityTracker::default(),
            reduce_only: ReduceOnlyGuard::default(),
            funding: FundingCache::default(),
            funding_unfavorable: UnfavorableTracker::default(),
            intervention: InterventionGuard::default(),
            backoff: BackoffController::new(BackoffParams {
                min: Duration::from_secs(cfg_risk_backoff_min),
                max: Duration::from_secs(cfg_risk_backoff_max),
                multiplier: cfg_risk_backoff_mult,
                reset_after: Duration::from_secs(cfg_risk_backoff_reset),
            }),
            daily: DailyTrades::default(),
            balance_critical: HashSet::new(),
            balance_low: HashSet::new(),
            last_snap_at: Instant::now(),
        };
        this.bootstrap().await?;
        this.publish_api_snapshot();
        // 余额给看板用：三条环都要拉。之前只绑在 execution 环上，
        // loop_events / loop_scan 下 LiveSnapshot.balances 一直空，页面显示「—」。
        let (balance, accounts) =
            refresh_accounts(&this.adapters, &this.cfg.sizing).await;
        this.balance = balance;
        this.venue_accounts = accounts;
        this.classify_balance_health();
        this.reconcile_exchange_positions(true);
        spawn_account_refresher(
            this.exec_tx.clone(),
            this.adapters.clone(),
            this.cfg.clone(),
            this.control.clone(),
        );
        info!(venues = ?this.balance.by_venue, "account balances loaded");
        this.publish_api_snapshot();
        // 无 HTTP 面板时没有「启动套利」按钮，按 yaml 里的所立刻匹配。
        if this.control.is_none() {
            if let Err(err) = this.apply_venue_match(this.cfg.venues.clone()).await {
                warn!(error = %err, "startup pair match failed");
            }
            this.publish_api_snapshot();
        }

        if this.cfg.execution.enabled {
            // 私有 WS 订单流：成交检测靠它从轮询变成事件驱动。
            // 只在真正要下单时启动；paper / monitor_only 不需要。
            if !this.cfg.system.monitor_only && !this.cfg.execution.paper_trading {
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
            }
            this.funding = refresh_funding(&this.adapters).await;
            spawn_funding_refresher(
                this.exec_tx.clone(),
                this.adapters.clone(),
                this.cfg.clone(),
                this.control.clone(),
            );
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
        self.ui_pairs.clear();
        for pair in &self.pairs {
            self.ui_pairs.insert(
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
                    grid: "T0".into(),
                    target_qty: String::new(),
                    actual_qty: "0".into(),
                    status: "已匹配".into(),
                },
            );
        }
    }

    fn venue_match_rows(&self) -> Vec<VenueMatchRow> {
        let mut out = Vec::new();
        for i in 0..self.cfg.venues.len() {
            for j in (i + 1)..self.cfg.venues.len() {
                let left = &self.cfg.venues[i];
                let right = &self.cfg.venues[j];
                let n = self
                    .pairs
                    .iter()
                    .filter(|p| {
                        let a = p.legs[0].venue.as_str();
                        let b = p.legs[1].venue.as_str();
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

    fn take_rematch(&self) -> Option<Vec<String>> {
        let mut ctrl = self.control.as_ref()?.lock().ok()?;
        if !ctrl.rematch {
            return None;
        }
        ctrl.rematch = false;
        let venues = if ctrl.params.active_venues.is_empty() {
            self.cfg.venues.clone()
        } else {
            ctrl.params.active_venues.clone()
        };
        Some(venues)
    }

    async fn rematch_if_requested(&mut self) {
        self.sync_page_config();
        let Some(venues) = self.take_rematch() else {
            return;
        };
        self.matching = true;
        self.ui_pairs.clear();
        self.publish_api_snapshot();
        let result = self.apply_venue_match(venues).await;
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
        self.backoff.set_params(BackoffParams {
            min: Duration::from_secs(lp.backoff_min_secs),
            max: Duration::from_secs(lp.backoff_max_secs.max(lp.backoff_min_secs)),
            multiplier: lp.backoff_multiplier.max(1),
            reset_after: Duration::from_secs(lp.backoff_reset_secs),
        });
    }

    /// 按所选 DEX 拉永续列表、两两匹配，并为尚未订阅的所拉起盘口。
    async fn apply_venue_match(&mut self, venues: Vec<String>) -> Result<()> {
        self.sync_page_config();
        let mut listed: Vec<(String, Vec<VenueMarket>)> = Vec::new();
        for id in &venues {
            let Some(adapter) = self.adapters_by_id.get(id).cloned() else {
                warn!(venue = %id, "active venue not loaded; skip");
                continue;
            };
            listed.push((id.clone(), adapter.list_perps().await?));
        }
        let listed_all = listed.clone();
        let wl = self.cfg.pairs.whitelist.clone();
        if !wl.is_empty() {
            for (_, mkts) in &mut listed {
                mkts.retain(|m| whitelist_allows(&m.base, &wl));
            }
        }
        if listed.len() < 2 {
            anyhow::bail!("need at least two selected venues to match pairs");
        }
        let books: Vec<Vec<VenueMarket>> = listed.iter().map(|(_, b)| b.clone()).collect();
        let matched = match_all_pairs(&books);
        self.pairs = self.merge_kept_pairs(matched);
        let tx = self
            .bbo_tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("bbo channel missing"))?;
        for (id, mkts) in &listed {
            let Some(adapter) = self.adapters_by_id.get(id).cloned() else {
                continue;
            };
            // SoDEX 订的是全市场 allBookTicker：用完整列表建别名，只订一次。
            if id == "sodex" {
                if self.subscribed.contains(id) {
                    continue;
                }
                let all = listed_all
                    .iter()
                    .find(|(vid, _)| vid == id)
                    .map(|(_, m)| m.as_slice())
                    .unwrap_or(mkts);
                adapter.subscribe_bbo(all, tx.clone()).await?;
                self.subscribed.insert(id.clone());
                continue;
            }
            let fresh: Vec<VenueMarket> = mkts
                .iter()
                .filter(|m| self.subscribed_markets.insert((id.clone(), m.pair_id.clone())))
                .cloned()
                .collect();
            if fresh.is_empty() {
                continue;
            }
            adapter.subscribe_bbo(&fresh, tx.clone()).await?;
            self.subscribed.insert(id.clone());
        }
        let panel_rows = if self.cfg.execution.enabled || !self.cfg.scan.enabled {
            self.pairs.len() * self.pair_stride()
        } else {
            0
        };
        self.panel.resize(panel_rows);
        self.seed_matched_pairs();
        info!(
            n = self.pairs.len(),
            venues = ?listed.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            scan = self.cfg.scan.enabled,
            execution = self.cfg.execution.enabled,
            whitelist = ?self.cfg.pairs.whitelist,
            "matched perp pairs"
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
        Ok(())
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
                || self.pending.contains_key(&slot)
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

    /// 开仓用配置的 T1 对比毛价差，不按所对费率抬高。
    /// T1 低于某所对平仓费时打 warn：格子仍会开，但往返可能覆盖不了手续费。
    fn log_effective_thresholds(&self) {
        let t1 = self.cfg.grid.initial_spread_threshold;
        info!(t1 = %t1, "entry uses configured T1 on raw spread");
        let mut seen = HashSet::new();
        for pair in &self.pairs {
            let a = &pair.legs[0].venue;
            let b = &pair.legs[1].venue;
            if !seen.insert((a.as_str().to_string(), b.as_str().to_string())) {
                continue;
            }
            let fee = self.cfg.round_leg_fee(a, b);
            if t1 < fee {
                warn!(
                    left = a.as_str(),
                    right = b.as_str(),
                    t1 = %t1,
                    exit_fee = %fee,
                    "T1 is below this venue pair's exit fee; opens may not cover round-trip"
                );
            }
        }
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
    }

    /// 内存持仓 vs 交易所实盘的数量对账。对齐参考 `_audit_position_alignment`：
    /// 报警 + 单向收缩到实盘量，绝不放大。
    fn audit_memory_positions(&mut self) {
        let mut fixes = Vec::new();
        for pair in &self.pairs {
            let slot = pair.slot_key();
            let Some(pos) = self.positions.get(&slot) else {
                continue;
            };
            if let Some((mem, exch)) = audit_position_qty(pair, &self.venue_accounts, pos.qty) {
                fixes.push((slot, pair.pair_id.clone(), mem, exch));
            }
        }
        for (slot, pair_id, mem, exch) in fixes {
            warn!(
                pair = %pair_id,
                memory_qty = %mem,
                exchange_qty = %exch,
                "position mismatch between memory and exchange"
            );
            if self.positions.reconcile_qty(&slot, exch).is_some() {
                warn!(pair = %pair_id, qty = %exch, "shrunk memory position to exchange qty");
            }
        }
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

    /// 整点探针。对齐参考 `_probe_reduce_only_recovery`：每小时 `HH:00:05`
    /// 对拉闸中的 pair 各发一轮最小量试单，能开就解闸。
    ///
    /// 拉闸是纯内存状态，没有探针就只能靠重启清除——那意味着交易所早已恢复，
    /// 机器人还在自我禁言。
    async fn try_reduce_only_probe(&mut self) {
        if self.cfg.system.monitor_only
            || self.cfg.execution.paper_trading
            || !self.cfg.risk.reduce_only_probe_enabled
            || self.reduce_only.is_empty()
            || self.execution_in_flight()
        {
            return;
        }
        let unix = now_ts();
        let hour = unix.div_euclid(3600);
        let probe_second = self.cfg.risk.reduce_only_probe_second;
        // 一次 tick 只探一个 pair：探针要下真单，串行化避免 nonce 竞争。
        let Some(pair_id) = self.reduce_only.blocked_pairs().into_iter().find(|p| {
            probe_due(unix, probe_second, self.reduce_only.probed_hour(p))
        }) else {
            return;
        };
        // 探哪条腿：优先此前报错的那个所，它才是限制的来源。
        let failed = self
            .reduce_only
            .state(&pair_id)
            .and_then(|s| s.failed_venues.iter().next().cloned());
        let Some((pair, leg)) = self.pairs.iter().find_map(|p| {
            if p.pair_id != pair_id {
                return None;
            }
            let leg = match failed.as_deref() {
                Some(v) => p.leg(v)?,
                None => p.legs.first()?,
            };
            Some((p.clone(), leg.clone()))
        }) else {
            return;
        };
        // 无论成败都先记「这小时探过」，否则失败后会在整个窗口内反复下单。
        self.reduce_only.mark_probe_hour(&pair_id, hour);

        let out = probe::run_probe(
            &self.cfg,
            &self.adapters_by_id,
            &pair_id,
            leg.venue.as_str(),
            &leg.raw_symbol,
            leg.market_index,
            leg.min_qty,
            &self.books,
        )
        .await;

        self.reduce_only
            .record_probe(&pair_id, &out.venue, out.success, Instant::now());
        self.log_record(
            &pair_id,
            &out.venue,
            "",
            leg.min_qty,
            "reduce_only_probe",
            if out.success { "cleared" } else { "still_blocked" },
            &out.detail,
        );
        if out.success {
            info!(pair = %pair_id, venue = %out.venue, "reduce_only probe cleared; opens re-enabled");
        }
        // 探针开了仓但平不掉 → 登记裸仓，走既有自动补对冲路径，不能只打日志了事。
        if let Some(qty) = out.naked_qty {
            let counterparty = pair
                .legs
                .iter()
                .map(|l| l.venue.as_str().to_string())
                .find(|v| v != &out.venue)
                .unwrap_or_default();
            let exposure = NakedExposure {
                pair_id: pair_id.clone(),
                venue: out.venue.clone(),
                qty,
                counterparty,
                source: NakedSource::BotFailure,
            };
            if !self
                .naked_exposures
                .iter()
                .any(|n| n.pair_id == exposure.pair_id && n.venue == exposure.venue)
            {
                warn!(
                    pair = %exposure.pair_id,
                    venue = %exposure.venue,
                    qty = %exposure.qty,
                    "reduce_only probe left a naked leg"
                );
                self.log_naked_journal(&exposure, "probe_naked");
                self.naked_exposures.push(exposure);
            }
        }
    }

    async fn try_hedge_naked_exposures(&mut self) {
        if self.cfg.system.monitor_only
            || self.cfg.execution.paper_trading
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
        if self.pending.contains_key(&slot) || self.hedging.contains(&slot) {
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
        match HedgeExecutor::market_leg(
            &self.cfg,
            &self.adapters_by_id,
            &naked.pair_id,
            &hedge_leg,
            qty,
            is_buy,
            false,
            &self.books,
            false,
        )
        .await
        {
            Ok(fill) => {
                info!(
                    pair = %naked.pair_id,
                    venue = %fill.venue,
                    qty = %fill.qty,
                    "bot failure naked hedge filled"
                );
                self.naked_exposures.retain(|n| {
                    n.source != NakedSource::BotFailure
                        || n.pair_id != naked.pair_id
                        || n.venue != naked.venue
                });
                self.log_record(
                    &naked.pair_id,
                    &naked.venue,
                    &naked.counterparty,
                    fill.qty,
                    "naked_hedge",
                    "filled",
                    "",
                );
            }
            Err(err) => {
                warn!(
                    pair = %naked.pair_id,
                    error = %err,
                    "naked exposure hedge failed"
                );
            }
        }
        self.naked_hedging.remove(&key);
    }

    async fn loop_unified(&mut self) -> Result<()> {
        let mut rx = self.event_rx.take().expect("bootstrap must run first");
        let mut exec_rx = self.exec_rx.take().expect("exec channel");
        let mut interval_ms = self.cfg.execution.loop_interval_ms.max(10);
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                Some(ev) = exec_rx.recv() => {
                    self.handle_exec_event(ev).await;
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
                    let want = self.cfg.execution.loop_interval_ms.max(10);
                    if want != interval_ms {
                        interval_ms = want;
                        tick = tokio::time::interval(Duration::from_millis(interval_ms));
                        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    }
                    self.rematch_if_requested().await;
                    self.tick_execution().await;
                    self.panel.flush();
                }
            }
        }
        Ok(())
    }

    async fn tick_execution(&mut self) {
        self.sync_page_config();
        // ① 有持仓的先跑（先平后开），② 挂单/对冲中的必跑（否则监视会停），
        // ③ 剩下的才考虑开新仓，受槽位和 in-flight 限制。
        let mut active: HashSet<usize> = HashSet::new();
        let mut must_run: Vec<usize> = Vec::new();
        for (pi, pair) in self.pairs.iter().enumerate() {
            let slot = pair.slot_key();
            let has_pos = self
                .positions
                .get(&slot)
                .map(|p| p.qty > Decimal::ZERO)
                .unwrap_or(false);
            if has_pos || self.pending.contains_key(&slot) || self.hedging.contains(&slot) {
                must_run.push(pi);
            }
        }
        for pi in must_run {
            self.process_pair(pi).await;
            active.insert(pi);
        }
        self.try_reduce_only_probe().await;
        self.try_hedge_naked_exposures().await;
        for pi in 0..self.pairs.len() {
            if active.contains(&pi) {
                continue;
            }
            if self.execution_in_flight() {
                break;
            }
            if !self.positions.can_open(self.cfg.sizing.max_concurrent_pairs) {
                break;
            }
            self.process_pair(pi).await;
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
                    self.paint_scan();
                    self.publish_api_snapshot();
                }
            }
        }
        Ok(())
    }

    fn paint_scan(&mut self) {
        let snapshot = self.books.read().map(|b| b.clone()).unwrap_or_default();
        self.sample_scan_history(&snapshot);
        let mut round = self.scanner.evaluate(
            &self.pairs,
            &snapshot,
            self.cfg.system.data_freshness_ms,
            self.cfg.scan.min_spread_pct,
            Instant::now(),
        );
        OpportunityTracker::apply_cross_natural(
            &mut round,
            self.history.as_ref(),
            self.cfg.scan.min_spread_pct,
            self.cfg.scan.cross_use_natural,
        );
        self.emit_token_lines(&round);
    }

    fn sample_scan_history(&self, snapshot: &crate::domain::BookMap) {
        let Some(store) = &self.history else {
            return;
        };
        for pair in &self.pairs {
            let v0 = pair.legs[0].venue.as_str();
            let v1 = pair.legs[1].venue.as_str();
            if !is_cross_dex(v0, v1) {
                continue;
            }
            let Some(b0) = snapshot.get(&(v0.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            let Some(b1) = snapshot.get(&(v1.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            if !b0.is_fresh(self.cfg.system.data_freshness_ms)
                || !b1.is_fresh(self.cfg.system.data_freshness_ms)
                || !b0.valid()
                || !b1.valid()
            {
                continue;
            }
            for (buy, sell, bb, sb) in [(v0, v1, b0, b1), (v1, v0, b1, b0)] {
                let Some(raw) = raw_spread_pct(bb.ask, sb.bid) else {
                    continue;
                };
                if let Err(err) = store.maybe_sample(&pair.pair_id, buy, sell, raw, raw) {
                    warn!(error = %err, pair = %pair.pair_id, "scan history sample failed");
                }
            }
        }
    }

    fn emit_token_lines(&mut self, round: &super::scan::ScanRound) {
        let interval = Duration::from_secs(self.cfg.scan.log_interval_secs.max(5));
        let min_change = Decimal::new(5, 2);
        let mut live = HashSet::new();
        let now = Instant::now();
        for o in &round.opportunities {
            let key = o.key();
            live.insert(key.clone());
            let due = match self.last_token_log.get(&key) {
                None => true,
                Some(prev) => {
                    prev.at.elapsed() >= interval
                        || (prev.raw - o.raw_pct).abs() >= min_change
                        || (prev.residual - o.residual_pct).abs() >= min_change
                }
            };
            if !due {
                continue;
            }
            info!(
                "{}",
                dashboard::token_key_line(
                    &o.pair_id,
                    &o.buy,
                    &o.sell,
                    o.raw_pct,
                    o.nat_pct,
                    o.residual_pct,
                    o.cross_dex,
                    o.age_secs(),
                )
            );
            self.last_token_log.insert(
                key,
                LoggedToken {
                    pair_id: o.pair_id.clone(),
                    buy: o.buy.clone(),
                    sell: o.sell.clone(),
                    raw: o.raw_pct,
                    residual: o.residual_pct,
                    at: now,
                },
            );
        }
        let gone: Vec<String> = self
            .last_token_log
            .keys()
            .filter(|k| !live.contains(*k))
            .cloned()
            .collect();
        for key in gone {
            if let Some(prev) = self.last_token_log.remove(&key) {
                info!(
                    "{}",
                    dashboard::token_gone_line(&prev.pair_id, &prev.buy, &prev.sell)
                );
            }
        }
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
        let pair = self.pairs[pair_i].clone();
        let slot = pair.slot_key();

        // 挂单监视排在所有盘口门槛**之前**：单子一旦挂出去就必须盯到撤单或成交。
        if self.pending.contains_key(&slot) {
            self.watch_pending_slot(pair_i, &pair, &slot);
            return;
        }
        if self.hedging.contains(&slot) {
            return;
        }

        // 活跃所过滤：只有两腿都在 active_venues 里的 pair 才能开仓。
        // 平仓不受此限——已有持仓的 pair 不管所是否还在列表里都继续平。
        // 注意：空列表 = 全部所都活跃（默认行为）。
        let has_pos = self
            .positions
            .get(&slot)
            .map(|p| p.qty > Decimal::ZERO)
            .unwrap_or(false);
        if !has_pos {
            if let Some(lp) = self.live_params() {
                if !lp.active_venues.is_empty() {
                    let v0 = pair.legs[0].venue.as_str();
                    let v1 = pair.legs[1].venue.as_str();
                    if !lp.active_venues.iter().any(|v| v == v0)
                        || !lp.active_venues.iter().any(|v| v == v1)
                    {
                        return;
                    }
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
                return;
            }
            (None, Some(_)) => {
                self.panel.stats.bump_skip("wait");
                self.mark_ui_status(&slot, &format!("等盘口 {}", v0.as_str()));
                return;
            }
            (Some(_), None) => {
                self.panel.stats.bump_skip("wait");
                self.mark_ui_status(&slot, &format!("等盘口 {}", v1.as_str()));
                return;
            }
            (Some(_), Some(_)) => {}
        }
        let b0 = b0.unwrap();
        let b1 = b1.unwrap();

        let Some(mid) = mid_from_bbo(&b0, &b1) else {
            self.panel.stats.bump_skip("no_mid");
            self.mark_ui_status(&slot, "无中价");
            return;
        };
        let base = pair.legs[0].base.clone();
        let pos = self.positions.get(&slot).cloned();

        // 有仓：入口只查数据质量。格子/剥头皮平仓的一档厚度在作出 Close
        // 之后按**本笔 qty** 校验，不够就丢掉平仓意图（对齐参考）。
        // 空仓：数据质量 + 一档深度都要过。
        let gate = if pos.is_some() {
            books_quality_ok(&self.cfg, &b0, &b1)
        } else {
            let probe = self
                .cfg
                .min_book_qty(&base)
                .max(self.cfg.grid_for(&base, pair.min_qty()).base_qty);
            books_tradable(&self.cfg, &pair, &b0, &b1, probe)
        };
        if let Err(reason) = gate {
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), reason);
            return;
        }
        if !stable_ok(&self.cfg, Decimal::ONE) {
            self.panel.stats.bump_skip("depeg");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "depeg"));
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "depeg");
            return;
        }

        // 稳定性采样：**每轮都要记**，无论后面是否走到检查。
        // 漏采样会让窗口永远攒不满，检查退化成「一直 Collecting」全程不放行。
        // 两腿各自的中价（参考项目买卖腿分开判波动，不能合并成一个数）。
        let two = Decimal::from(2);
        self.stability.record(
            &slot,
            (b0.bid + b0.ask) / two,
            (b1.bid + b1.ask) / two,
            Instant::now(),
            self.cfg.risk.price_stability_window_secs,
        );

        // `base_qty` 是**单格**数量，多格逻辑靠它把持仓量换算成格数
        // （`segments_held = qty / base_qty`）。有仓时绝不能拿总持仓量覆盖它，
        // 否则 3 格仓位会被算成 1 格，按格减仓直接失效。
        let mut params = self.cfg.grid_for(&base, pair.min_qty());

        // 有仓 → 锁定持仓方向；空仓 → 双向取优。
        // 有仓时绝不能再取双向最优：方向一翻，显示和判定都会跳到反方向。
        // 开仓视角对持仓不带 qty 做一档校验（加仓量要到决策后才知道）。
        let net = match &pos {
            Some(p) => {
                let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
                sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO)
            }
            None => best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, params.base_qty),
        };
        let Some(mut net) = net else {
            self.panel.stats.bump_skip("no_spread");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_spread"));
            self.mark_ui_status(&slot, "无价差");
            return;
        };

        if pos.is_none() {
            let reserved = self
                .positions
                .reserved_margin_by_venue(|v| self.cfg.leverage_for(v));
            let global_min = self.balance.global_min();
            let (buy_leg, sell_leg) = (
                pair.leg(net.buy.as_str()).cloned(),
                pair.leg(net.sell.as_str()).cloned(),
            );
            let (Some(buy_leg), Some(sell_leg)) = (buy_leg, sell_leg) else {
                return;
            };
            let (buy_book, sell_book) = books_for_direction(&net.buy, &v0, &b0, &b1);
            let Some(r) = resolve_qty(
                &self.cfg.sizing,
                global_min,
                self.leg_margin(&reserved, buy_leg.venue.as_str()),
                self.leg_margin(&reserved, sell_leg.venue.as_str()),
                buy_book,
                sell_book,
                mid,
                &buy_leg,
                &sell_leg,
            ) else {
                self.panel.stats.bump_skip("no_size");
                self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_size"));
                self.fill_monitor_row(
                    &slot,
                    &pair,
                    &net,
                    pos.as_ref(),
                    "定仓不足",
                    &b0,
                    &b1,
                    Some(mid),
                );
                return;
            };
            // `resolve_qty` 给的是保证金/深度允许的**总量**上限。多格下
            // 实际下单量 = base_qty × segments，所以单格量要按最大格数摊分，
            // 否则开满 max_segments 格会把保证金上限撑破 max_segments 倍。
            let segs = Decimal::from(params.max_segments.max(1));
            params.base_qty = r.qty / segs;
            let bind = match r.binding {
                BindingLeg::Buy => buy_leg.venue.as_str(),
                BindingLeg::Sell => sell_leg.venue.as_str(),
            };
            info!(
                pair = %pair.pair_id,
                buy = %net.buy,
                sell = %net.sell,
                binding_venue = bind,
                notional_usdc = %r.notional_usdc,
                total_qty = %r.qty,
                per_segment_qty = %params.base_qty,
                max_segments = params.max_segments,
                "sized by min-margin leg; same qty on both venues"
            );
            // 用真实 qty 复核一档厚度。**锁在刚才定仓的那个方向**上重算，
            // 不能再取双向最优：换了方向，qty 就是按另一对盘口算出来的。
            let Some(net2) = sequenced_spread(
                &self.cfg,
                &net.buy,
                &net.sell,
                buy_book,
                sell_book,
                r.qty,
            ) else {
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
            };
            net = net2;
        }

        let cross = is_cross_dex(net.buy.as_str(), net.sell.as_str());
        let natural = self.sample_and_natural(&pair, &net, cross);

        // 平仓视角：买回原 sell 所的 Ask、卖回原 buy 所的 Bid，用当前盘口重算。
        //
        // qty 传 0：先算出价差，让格子能判断「该不该减」。真正下单前再
        // 用本笔平仓量做一档校验，不够就丢掉平仓意图（见下方 thin_book）。
        // 加仓不依赖平仓视角，仍看开仓方向的 raw。
        //
        // 格子判据用 raw（对齐参考的毛价差）。
        let close_view = pos.as_ref().and_then(|p| {
            let (bb, sb) = books_for_direction(&p.buy, &v0, &b0, &b1);
            closing_sequenced_spread(&self.cfg, &p.buy, &p.sell, bb, sb, Decimal::ZERO).map(|c| {
                CloseView {
                    exit_raw_pct: c.raw_pct,
                    exit_net_pct: c.net_pct,
                }
            })
        });

        let mut intent = self.grid.decide(
            &slot,
            &net,
            close_view,
            pos.as_ref(),
            &params,
            Instant::now(),
        );

        // 资金费率维度。价差看的是「进出能不能赚」,费率看的是「持着要不要
        // 付钱」——两个所的费率不对称时，一个价差本来够的仓会被费率吃穿。
        //
        // 放在这里（grid 决策之后）而不是塞进 `GridEngine`：费率不是格子状态，
        // 它对开仓是**否决权**、对持仓是**独立退出理由**,不该和格子判据搅在一起。
        intent = self.apply_funding(&slot, &pair, &net, pos.as_ref(), intent);
        intent = self.apply_risk_limits(&pair, &net, pos.as_ref(), intent);
        if matches!(intent, Intent::Open { .. }) && !self.arbitrage_enabled() {
            intent = Intent::Hold;
        }

        // 对齐参考：一档撑不住本笔平仓量 → 丢掉格子/剥头皮平仓意图。
        // 费率/超时/余额清仓不走这条（风控离场，不是网格平仓）。
        if let Intent::Close { qty, reason, .. } = &intent {
            if matches!(
                reason,
                CloseReason::GridReduce | CloseReason::ScalpTakeProfit
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
        let mut label = intent_label(&intent);
        if matches!(intent, Intent::Hold) && self.grid.is_scalping(&slot) {
            label = "scalp";
        }
        let min_pts = self.cfg.history.min_points;
        let pts = natural.as_ref().map(|n| n.points).unwrap_or_else(|| {
            self.history
                .as_ref()
                .map(|s| s.window_points(&pair.pair_id, net.buy.as_str(), net.sell.as_str()))
                .unwrap_or(0)
        });
        let nat_value = self.ui_nat(&pair, &net, natural.as_ref());

        self.panel.stats.bump_intent(label);
        self.record_ui_pair(
            &slot,
            &pair,
            &net,
            &params,
            pos.as_ref(),
            label,
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
                label,
            ),
        );

        if matches!(intent, Intent::Hold) {
            return;
        }

        // 价格稳定性（对齐参考 `passes_price_stability`）：先挂后吃最怕插针——
        // 挂单在插针价上成交，对冲腿却回不来。开仓和平仓独立判定。
        let stab_action = if matches!(intent, Intent::Close { .. }) {
            StabilityAction::Close
        } else {
            StabilityAction::Open
        };
        let state = self.stability.check(
            &slot,
            stab_action,
            Instant::now(),
            self.cfg.risk.price_stability_window_secs,
            self.cfg.risk.price_stability_threshold_pct,
        );
        if !state.passed() {
            let reason = state.skip_label();
            if self.stability.changed(&slot, stab_action, state) {
                let vol = self
                    .stability
                    .volatility(&slot, Instant::now(), self.cfg.risk.price_stability_window_secs);
                info!(
                    pair = %pair.pair_id,
                    action = ?stab_action,
                    volatility_pct = ?vol.map(|v| v.round_dp(4)),
                    threshold_pct = %self.cfg.risk.price_stability_threshold_pct,
                    "stability gate blocked action"
                );
            }
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
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
            return;
        }
        if self.execution_in_flight() {
            self.panel.stats.bump_skip("in_flight");
            return;
        }
        // 人工介入等待：开仓和平仓**都挡**（对齐参考 `should_block` 在开仓
        // 与平仓两条路径上都查）。这跟下面的 reduce-only 熔断相反，是刻意的：
        // reduce-only 时仓位是已知的，挡平仓等于锁死仓位；而介入态意味着
        // 内存里的仓位本身不可信，按错的量去平会把敞口放大。
        // 兜底是 30 分钟自动解除和格数变化解除，不会永久锁死。
        let cur_grid = pos
            .as_ref()
            .filter(|_| params.base_qty > Decimal::ZERO)
            .map(|p| self.grid.segments_held(p.qty, params.base_qty));
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
                return;
            }
        }
        // reduce-only 拉闸：只挡开仓。平仓和止损必须放行——挡掉平仓会把仓位
        // 锁死在交易所里，比开不了仓危险得多（对齐参考：closes 始终允许）。
        if matches!(intent, Intent::Open { .. }) && self.reduce_only.is_blocked(&pair.pair_id) {
            self.panel.stats.bump_skip("reduce_only");
            return;
        }
        if matches!(intent, Intent::Open { .. })
            && pos.is_none()
            && !self.positions.can_open(self.cfg.sizing.max_concurrent_pairs)
        {
            self.panel.stats.bump_skip("slots");
            return;
        }

        let Some(mut plan) = plan_hedge(&pair, &intent, pos.as_ref(), &self.cfg) else {
            return;
        };
        plan.decision_net_pct = net.net_pct;
        plan.decision_raw_pct = net.raw_pct;
        if self.cfg.live_test.dex_test_mode
            && !self.cfg.execution.paper_trading
            && plan.qty > self.cfg.live_test.max_qty
        {
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
            "limit-then-market: post high-fee venue first"
        );

        // monitor_only：到规划为止。**不能**登记 pending——旧实现登记完就 return，
        // 而 pending 只在执行回调里清理，于是第一个 open 信号就把整个决策环卡死。
        if self.cfg.system.monitor_only {
            self.panel.stats.skip_send += 1;
            return;
        }

        if self.cfg.execution.paper_trading {
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
                true,
            );
            return;
        }

        // 实盘：baseline 必须是查询成功的真实持仓。取不到就放弃本轮——
        // 用 0 兜底会把该所既有仓位误判成第一腿成交，直接发出裸的第二腿。
        let baseline = match self.snapshot_first_leg_position(&plan.first).await {
            Some(v) => v,
            None => {
                warn!(
                    pair = %plan.pair_id,
                    venue = %plan.first.venue,
                    "cannot read first leg baseline position; skipping this round"
                );
                self.panel.stats.bump_skip("no_baseline");
                return;
            }
        };
        let min_fill = pair
            .leg(&plan.first.venue)
            .map(|l| l.min_qty)
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::new(1, 6));
        let cancel = Arc::new(AtomicBool::new(false));
        if matches!(intent, Intent::Open { .. }) {
            self.positions.reserve_open(&slot);
        }
        if matches!(self.cfg.order.style, OrderStyle::LimitThenMarket) {
            self.pending.insert(
                slot.clone(),
                PendingLimit {
                    plan: plan.clone(),
                    since: Instant::now(),
                    cancel: cancel.clone(),
                },
            );
            self.hedging.insert(slot.clone());
            spawn_limit_market(
                self.exec_tx.clone(),
                self.cfg.clone(),
                self.adapters_by_id.clone(),
                self.books.clone(),
                pair_i,
                plan,
                LimitMarketRun {
                    baseline,
                    min_qty: min_fill,
                    cancel,
                },
            );
        } else {
            self.hedging.insert(slot.clone());
            spawn_run_plan(
                self.exec_tx.clone(),
                self.cfg.clone(),
                self.adapters_by_id.clone(),
                self.books.clone(),
                pair_i,
                plan,
                false,
            );
        }
    }

    /// 资金费率维度：开仓否决 + 持仓退出。对齐参考
    /// `_check_funding_rate_before_open` 与 `_should_close_for_funding_rate`。
    ///
    /// 缺数据时**放行**而不是拦住。费率是收益修正项，不是安全闸门——
    /// 一个所的费率接口挂了就停掉全部开仓，代价比偶尔开一笔费率不利的仓大。
    /// 已持仓侧同理：查不到费率不构成平仓理由。
    fn apply_funding(
        &mut self,
        slot: &str,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        pos: Option<&crate::domain::Position>,
        intent: Intent,
    ) -> Intent {
        let threshold = self
            .live_params()
            .map(|p| p.funding_annual_threshold_pct)
            .unwrap_or(self.cfg.risk.funding_annual_threshold_pct);
        if threshold <= Decimal::ZERO {
            return intent;
        }
        // 费率按**持仓方向**算：多头付正费率、空头收正费率，所以
        // 已持仓时必须用仓位自己的 buy/sell，不能用当前扫描方向。
        let (buy, sell) = match pos {
            Some(p) => (p.buy.clone(), p.sell.clone()),
            None => (net.buy.clone(), net.sell.clone()),
        };
        let (Some(bl), Some(sl)) = (pair.leg(buy.as_str()), pair.leg(sell.as_str())) else {
            return intent;
        };
        let Some(view) = self
            .funding
            .view(&buy, &bl.raw_symbol, &sell, &sl.raw_symbol)
        else {
            self.funding_unfavorable.clear(slot);
            return intent;
        };

        // 净支付且年化超阈值 = 不利。net_annual_pct 带符号，正数是我方净收。
        let unfavorable = view.net_annual_pct < -threshold;
        if !unfavorable {
            self.funding_unfavorable.clear(slot);
            return intent;
        }

        if matches!(intent, Intent::Open { .. }) {
            // 开仓侧不要持续性门：现在就知道要净付钱，没有理由先开进来再等一小时。
            self.panel.stats.bump_skip("funding");
            info!(
                pair = %pair.pair_id,
                buy = %buy, sell = %sell,
                net_annual_pct = %view.net_annual_pct.round_dp(2),
                threshold = %threshold,
                "跳过开仓：资金费率净支付超阈值"
            );
            return Intent::Hold;
        }

        // 持仓侧要持续性门：费率逐周期结算，瞬时读数不利不等于真要付钱，
        // 阈值边缘抖动会把仓位反复开平，磨掉的手续费远超省下的费率。
        let held = self.funding_unfavorable.mark(slot);
        let Some(p) = pos else { return intent };
        if !self
            .funding_unfavorable
            .sustained(slot, self.cfg.risk.funding_unfavorable_duration_minutes)
        {
            return intent;
        }
        // 已经在平了就别覆盖——格子减仓的 qty/grid 是它自己算的，
        // 换成全量平仓会打乱格子状态机。
        if matches!(intent, Intent::Close { .. }) {
            return intent;
        }
        warn!(
            pair = %pair.pair_id,
            buy = %buy, sell = %sell,
            net_annual_pct = %view.net_annual_pct.round_dp(2),
            held_mins = held.as_secs() / 60,
            "资金费率持续不利，平仓离场"
        );
        Intent::Close {
            qty: p.qty,
            grid: p.grid,
            reason: CloseReason::FundingStopLoss,
            round_trip_pct: net.net_pct,
        }
    }

    /// 风险闸门：持仓时长、余额下限、每日次数、数量上限、错误退避。
    ///
    /// 和 `apply_funding` 一样放在 grid 决策**之后**：这些都不是网格状态，
    /// 而是「无论价差怎么走都要生效」的外部约束。顺序上强制平仓优先于
    /// 开仓拦截——余额告急时既要停止开新仓，也要把已有仓位撤出来。
    fn apply_risk_limits(
        &mut self,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        pos: Option<&crate::domain::Position>,
        intent: Intent,
    ) -> Intent {
        // ── 强制平仓维度 ──
        let lp = self.live_params();
        let max_position_hours = lp.as_ref().map(|p| p.max_position_hours).unwrap_or(self.cfg.risk.max_position_hours);
        if let Some(p) = pos.filter(|p| p.qty > Decimal::ZERO) {
            if !matches!(intent, Intent::Close { .. }) {
                if position_expired(p.opened_at.elapsed(), max_position_hours) {
                    warn!(
                        pair = %pair.pair_id,
                        held_hours = p.opened_at.elapsed().as_secs() / 3600,
                        limit_hours = max_position_hours,
                        "持仓超时，强制平仓"
                    );
                    return Intent::Close {
                        qty: p.qty,
                        grid: p.grid,
                        reason: CloseReason::HoldTimeout,
                        round_trip_pct: net.net_pct,
                    };
                }
                // 余额跌破清仓线：任何一腿告急就平——对冲仓两条腿共命，
                // 一腿被强平另一腿立刻裸奔。
                if self.balance_critical.contains(p.buy.as_str())
                    || self.balance_critical.contains(p.sell.as_str())
                {
                    warn!(
                        pair = %pair.pair_id,
                        buy = %p.buy, sell = %p.sell,
                        "余额低于清仓线，强制平仓"
                    );
                    return Intent::Close {
                        qty: p.qty,
                        grid: p.grid,
                        reason: CloseReason::BalanceFloor,
                        round_trip_pct: net.net_pct,
                    };
                }
            }
        }

        let Intent::Open { qty, buy, sell, .. } = &intent else {
            return intent;
        };
        let (qty, buy, sell) = (*qty, buy.clone(), sell.clone());

        // ── 开仓拦截维度 ──
        // 退避是**按所**的：Lighter 在 nonce 退避期不代表 SoDEX 也不能下单。
        // 但对冲仓要两条腿同时可用，任一腿退避中就整条 pair 停开。
        let now = Instant::now();
        for v in [buy.as_str(), sell.as_str()] {
            if self.backoff.is_paused(v, now) {
                let wait = self
                    .backoff
                    .get(v)
                    .map(|s| s.remaining(now).as_secs())
                    .unwrap_or(0);
                self.panel.stats.bump_skip("backoff");
                debug!(pair = %pair.pair_id, venue = v, wait_secs = wait, "跳过开仓：错误退避中");
                return Intent::Hold;
            }
        }
        if !self.daily.allows(
            lp.as_ref()
                .map(|p| p.max_daily_opens)
                .unwrap_or(self.cfg.risk.max_daily_opens),
        ) {
            self.panel.stats.bump_skip("daily_quota");
            debug!(
                pair = %pair.pair_id,
                used = self.daily.count(),
                "跳过开仓：当日开仓次数已达上限"
            );
            return Intent::Hold;
        }
        // 余额低于告警线就停开（清仓线在上面已经触发强制平仓）。
        if self.balance_low.contains(buy.as_str()) || self.balance_low.contains(sell.as_str()) {
            self.panel.stats.bump_skip("balance_low");
            return Intent::Hold;
        }

        // 名义敞口上限：跨币种可比，BTC/ETH/DOGE 一个数管住。
        // 用建仓名义而非实时市值：盘口拿不到时限额判定仍然有效。
        let limits = NotionalLimits {
            per_symbol: lp
                .as_ref()
                .map(|p| p.max_single_token_notional_usdc)
                .unwrap_or(self.cfg.risk.max_single_token_notional_usdc),
            total: lp
                .as_ref()
                .map(|p| p.max_total_notional_usdc)
                .unwrap_or(self.cfg.risk.max_total_notional_usdc),
        };
        // 本笔预估名义 = qty × 中价，拿不到就退回 0（不挡）。
        let mid_price = self
            .book(net.buy.as_str(), &pair.pair_id)
            .filter(|b| b.valid())
            .map(|b| (b.bid + b.ask) / Decimal::TWO)
            .unwrap_or(Decimal::ZERO);
        let add_notional = qty * mid_price;
        if let Err(reason) = limits.check(
            &pair.legs[0].base,
            add_notional,
            self.positions.held_notional_for_pair(&pair.pair_id),
            self.positions.held_notional_total(),
        ) {
            self.panel.stats.bump_skip("notional_limit");
            debug!(pair = %pair.pair_id, %reason, "跳过开仓：名义敞口上限");
            return Intent::Hold;
        }
        intent
    }

    /// 挂单监视：价差跌破门槛或超时 → 置 cancel 标志，后台执行 task 会看到并撤单。
    ///
    /// 平仓单**不**因价差变化撤——平仓要走完，否则会一直留着单腿风险。
    /// 盘口读不到时只按超时判：不能因为暂时看不到价差就一直不撤。
    fn watch_pending_slot(&mut self, pair_i: usize, pair: &Pair, slot: &str) {
        let Some(pending) = self.pending.get(slot).cloned() else {
            return;
        };
        // 看门狗：执行 task 万一 panic 就不会回消息，pending 会永久占住
        // `execution_in_flight()` 把整个决策环卡死。超过所有正常耗时上限后强制清理。
        if pending.since.elapsed() > self.pending_hard_deadline() {
            tracing::error!(
                pair = %pair.pair_id,
                slot,
                elapsed_secs = pending.since.elapsed().as_secs(),
                "pending limit exceeded hard deadline; force-clearing state (check for orphan orders)"
            );
            pending.cancel.store(true, Ordering::Relaxed);
            self.pending.remove(slot);
            self.hedging.remove(slot);
            self.positions.release_pending(slot);
            self.log_plan_record(&pending.plan, "exec_fail", "watchdog_timeout", "");
            return;
        }
        let already = pending.cancel.load(Ordering::Relaxed);
        let timeout = Duration::from_millis(self.cfg.order.limit_timeout_ms);

        let spread = self.pending_spread(pair, &pending);
        let spread_ok = match (&spread, pending.plan.is_open) {
            // 平仓单：价差怎么变都要走完
            (_, false) => true,
            (Some((net, _residual)), true) => {
                let same_dir = pending.plan.buy_venue == net.buy.as_str()
                    && pending.plan.sell_venue == net.sell.as_str();
                same_dir && net.raw_pct >= self.cfg.grid.initial_spread_threshold
            }
            // 开仓单但读不到盘口：不当成「价差没了」，交给超时处理
            (None, true) => true,
        };

        let ui = if already {
            "canceling"
        } else {
            match watch_resting_limit(spread_ok, pending.since.elapsed(), timeout, false) {
                LimitWatch::CancelSpreadGone => {
                    pending.cancel.store(true, Ordering::Relaxed);
                    self.panel.stats.cancel_gone += 1;
                    info!(pair = %pair.pair_id, "resting limit: spread gone, requesting cancel");
                    "canceling"
                }
                LimitWatch::CancelTimeout => {
                    pending.cancel.store(true, Ordering::Relaxed);
                    self.panel.stats.cancel_timeout += 1;
                    info!(pair = %pair.pair_id, "resting limit: timeout, requesting cancel");
                    "canceling"
                }
                _ => "limit",
            }
        };

        let params = self.cfg.grid_for(&pair.legs[0].base, pair.min_qty());
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

    /// 挂单期间按**计划的方向**算净边（不双向取优）。
    /// residual 只给监控行展示；开仓挂单是否还够看毛价差 vs T1。
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
        !self.pending.is_empty() || !self.hedging.is_empty()
    }

    /// 一轮 limit-then-market 的正常耗时上限：
    /// 每轮挂单等待 × 轮数 + 撤单竞态 + 一次写操作的 sidecar 超时，再留一倍余量。
    fn pending_hard_deadline(&self) -> Duration {
        let rounds = u64::from(self.cfg.order.limit_retry_count.max(1));
        let per_round = self.cfg.order.limit_timeout_ms.max(200) + 1_000;
        Duration::from_millis(per_round * rounds) + Duration::from_secs(240)
    }

    async fn snapshot_first_leg_position(&self, leg: &crate::exec::HedgeLeg) -> Option<Decimal> {
        let adapter = self.adapters_by_id.get(&leg.venue)?;
        let positions = adapter.positions().await.ok()?;
        Some(
            positions
                .iter()
                .filter(|p| symbol_matches_symbol(&p.symbol, &leg.symbol, &leg.symbol))
                .map(|p| p.qty)
                .sum(),
        )
    }

    async fn handle_exec_event(&mut self, ev: ExecEvent) {
        match ev {
            ExecEvent::RunPlan(msg) => self.on_run_plan(msg).await,
            ExecEvent::Accounts(msg) => {
                self.balance = msg.balance;
                self.venue_accounts = msg.accounts;
                self.classify_balance_health();
                self.reconcile_exchange_positions(false);
            }
            // 空表不覆盖：两个所都拉失败时保留上一轮数据，让 `is_fresh`
            // 的时效判断决定它还能不能用。直接盖成空会把「暂时拉不到」
            // 变成「确定没有」,开仓门当场全关。
            ExecEvent::Funding(cache) => {
                if !cache.is_empty() {
                    self.funding = *cache;
                }
            }
        }
    }

    /// 按余额把各所分成正常 / 告警 / 清仓三档，落到两个集合里。
    ///
    /// 只在账户快照刷新后跑一次，而不是每 tick 重算：余额本身就是缓存值，
    /// 每 tick 重算只是把同一份数据反复解一遍。
    fn classify_balance_health(&mut self) {
        self.balance_low.clear();
        self.balance_critical.clear();
        let warn = self.cfg.risk.min_balance_warn_usdc;
        let critical = self.cfg.risk.min_balance_close_usdc;
        if warn <= Decimal::ZERO && critical <= Decimal::ZERO {
            return;
        }
        for venue in self.adapters_by_id.keys() {
            // 快照拉不到时 available 是 0，不能当成「余额耗尽」触发全平——
            // 那是拉取失败，不是真没钱。只有拿到过快照的所才参与判定。
            if self.venue_accounts.get(venue).is_none() {
                continue;
            }
            let avail = self.balance.venue_available(venue);
            match balance_health(avail, warn, critical) {
                BalanceHealth::Ok => {}
                BalanceHealth::Low => {
                    if self.balance_low.insert(venue.clone()) {
                        warn!(venue = %venue, available = %avail, floor = %warn,
                            "balance below warning floor; pausing new opens on this venue");
                    }
                }
                BalanceHealth::Critical => {
                    self.balance_low.insert(venue.clone());
                    if self.balance_critical.insert(venue.clone()) {
                        warn!(venue = %venue, available = %avail, floor = %critical,
                            "balance below close floor; force-closing this venue's positions");
                    }
                }
            }
        }
    }

    async fn on_run_plan(&mut self, msg: RunPlanMsg) {
        self.hedging.remove(&msg.slot);
        self.pending.remove(&msg.slot);
        let Some(pair) = self.pairs.get(msg.pair_i).cloned() else {
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
                    "limit-then-market executed"
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
                if result.orphan_order.is_some() {
                    // 两腿都对冲上了，但第一腿还留着一张撤不掉的单。仓位记账是
                    // 对的，可那张单随时可能成交出第三条腿——先停手。
                    let oid = result.orphan_order.clone().unwrap_or_default();
                    self.mark_intervention(
                        &msg.plan,
                        Cause::OrphanOrder,
                        format!(
                            "hedge ok but order {oid} on {} could not be canceled",
                            msg.plan.first.venue
                        ),
                    );
                } else {
                    // 两腿干净成交：连击归零（对齐参考在成功路径上重置计数）。
                    self.intervention.clear_streak(&msg.plan.pair_id);
                }
                self.log_plan_record(
                    &msg.plan,
                    if msg.plan.is_open { "open" } else { "close" },
                    "both_filled",
                    &format!("hedged={hedged} planned={}", msg.plan.qty),
                );
                self.naked_exposures
                    .retain(|n| n.pair_id != msg.plan.pair_id);
                self.apply_fill(&pair, &msg.plan, &result, msg.pair_i);
            }
            Err(err) => {
                // 交易所说「只能减仓」：拉闸挡开仓。要先于下面的分类判断，
                // 因为这条报错常带着 NAKED_FIRST_LEG 等前缀一起来。
                //
                // 平仓失败时置 closing_blocked——连 reduce-only 单都被拒，
                // 说明不只是开仓受限，探针要连平仓能力一起验。
                if is_reduce_only_error(&err) {
                    let already = self.reduce_only.is_blocked(&msg.plan.pair_id);
                    self.reduce_only.mark(
                        &msg.plan.pair_id,
                        &msg.plan.first.venue,
                        &err,
                        !msg.plan.is_open,
                        Instant::now(),
                    );
                    if !already {
                        warn!(
                            pair = %msg.plan.pair_id,
                            venue = %msg.plan.first.venue,
                            closing_blocked = !msg.plan.is_open,
                            error = %err,
                            "venue reports reduce-only; blocking opens for this pair until hourly probe clears it"
                        );
                        self.log_plan_record(&msg.plan, "reduce_only", "blocked", &err);
                    }
                }
                // nonce 冲突 / 限流：登记指数退避，让这个所歇一会儿。
                //
                // 放在所有分类之前：这类错误和下面的裸腿/未知是**正交**的
                // ——一次 nonce 冲突既要退避该所，也可能同时留下裸腿要处理。
                // 两条腿都登记：报错文本一般认不出是哪条腿失败的。
                if let Some(kind) = backoff::classify(&err) {
                    let now = Instant::now();
                    for venue in [&msg.plan.first.venue, &msg.plan.second.venue] {
                        let wait = self.backoff.register(venue, kind, now);
                        warn!(
                            venue = %venue,
                            pair = %msg.plan.pair_id,
                            kind = kind.code(),
                            backoff_ms = wait.as_millis(),
                            "venue error; backing off"
                        );
                    }
                    self.log_plan_record(&msg.plan, "backoff", kind.code(), &err);
                }
                if err.contains("EMERGENCY_CLOSED") {
                    warn!(pair = %msg.plan.pair_id, error = %err, "second leg failed; first emergency closed");
                    self.log_plan_record(&msg.plan, "exec_fail", "emergency_closed", &err);
                    // 紧急平仓**成功**，敞口已经收掉，仓位状态是干净的，
                    // 所以不挂起。但这算一次单腿成交：参考的规则是连续 3 次
                    // 即使每次都补上也要挂起，因为那说明链路有系统性问题。
                    let n = self.intervention.note_single_leg(&msg.plan.pair_id);
                    if n >= SINGLE_LEG_STREAK_LIMIT {
                        self.mark_intervention(
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
                } else if err.contains("NAKED_FIRST_LEG") {
                    warn!(pair = %msg.plan.pair_id, error = %err, "naked first leg");
                    self.log_plan_record(&msg.plan, "exec_fail", "naked", &err);
                    self.record_naked_from_failed_hedge(&msg.plan, msg.plan.qty);
                    // 裸腿且紧急平仓也失败：真实仓位不明，必须停手。
                    self.mark_intervention(
                        &msg.plan,
                        Cause::NakedLegUnrecoverable,
                        format!("naked leg on {} and emergency close failed", msg.plan.first.venue),
                    );
                } else if err.contains("SECOND_LEG_UNKNOWN") {
                    warn!(
                        pair = %msg.plan.pair_id,
                        error = %err,
                        "second leg outcome unknown; first leg left in place on purpose"
                    );
                    self.log_plan_record(&msg.plan, "exec_fail", "second_leg_unknown", &err);
                    // 第一腿确实成交了，第二腿成没成不知道。按裸腿登记以便对账
                    // 能看见它，但绝不自动补——补错方向会变成双倍敞口。
                    self.record_naked_from_failed_hedge(&msg.plan, msg.plan.qty);
                    self.mark_intervention(
                        &msg.plan,
                        Cause::SecondLegUnknown,
                        format!(
                            "second leg on {} unverifiable; check venue before resuming",
                            msg.plan.second.venue
                        ),
                    );
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
                        &msg.plan,
                        Cause::OrphanOrder,
                        format!("uncancelable resting order on {}", msg.plan.first.venue),
                    );
                }
                self.positions.release_pending(&msg.slot);
                // 失败后重新攒持续性，避免立刻再来一遍。
                self.grid.forget(&msg.slot);
            }
        }
    }

    fn log_plan_record(&self, plan: &HedgePlan, action: &str, result: &str, detail: &str) {
        self.log_record(
            &plan.pair_id,
            &plan.buy_venue,
            &plan.sell_venue,
            plan.qty,
            action,
            result,
            detail,
        );
    }

    #[allow(clippy::too_many_arguments)]
    /// 挂起一个 pair 等人工处理。对齐参考 `_mark_manual_intervention`。
    ///
    /// 只在**首次**挂起时打 ERROR + 写 journal；重复触发不重置计时，否则
    /// 反复报错会把 30 分钟自动解除无限推后，等于永久锁死这个币。
    fn mark_intervention(&mut self, plan: &HedgePlan, cause: Cause, detail: String) {
        let first = self.intervention.mark(
            &plan.pair_id,
            cause,
            detail.clone(),
            None,
            Instant::now(),
        );
        if !first {
            return;
        }
        let mins = super::intervention::AUTO_RESUME.as_secs() / 60;
        tracing::error!(
            pair = %plan.pair_id,
            cause = cause.as_str(),
            detail = %detail,
            auto_resume_mins = mins,
            "MANUAL INTERVENTION REQUIRED: pausing this pair (opens and closes both blocked)"
        );
        self.log_plan_record(plan, "intervention", cause.as_str(), &detail);
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
        });
    }

    /// 用**实际成交量**回写持仓，不是计划量：部分成交时用 plan.qty
    /// 会让内存持仓虚高，之后按虚高量平仓就留下尾巴。
    fn apply_fill(&mut self, pair: &Pair, plan: &HedgePlan, result: &ExecResult, pair_i: usize) {
        let qty = result.hedged_qty();
        let entry_net = self.realized_entry_net(plan, result);
        let entry_raw = self.realized_entry_raw(plan, result);
        if plan.is_open {
            let notional = qty
                * self
                    .book(&plan.buy_venue, &pair.pair_id)
                    .and_then(|bb| {
                        self.book(&plan.sell_venue, &pair.pair_id)
                            .and_then(|sb| mid_from_bbo(&bb, &sb))
                    })
                    .unwrap_or(Decimal::ZERO);
            self.positions.record_open(
                &plan.slot,
                &pair.pair_id,
                VenueId::from(plan.buy_venue.as_str()),
                VenueId::from(plan.sell_venue.as_str()),
                qty,
                1,
                notional,
                entry_net,
                entry_raw,
            );
            // 计入当日配额。记在**成交**而不是下单，配额才对应真实开仓次数：
            // 下单被拒/撤单不该吃额度。平仓永远不计——配额是节流开仓的，
            // 不能因为额度用尽就让仓位平不掉。
            self.daily.record();
            info!(
                pair = %pair.pair_id,
                qty = %qty,
                notional_usdc = %notional.round_dp(2),
                entry_net_pct = %entry_net.round_dp(4),
                "position opened"
            );
        } else {
            self.positions.record_close(&plan.slot, qty);
            if self.positions.get(&plan.slot).is_none() {
                self.grid.exit_scalping(&plan.slot);
            }
            info!(pair = %pair.pair_id, qty = %qty, "position closed");
        }
        self.grid.forget(&plan.slot);
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
        let fee = self.cfg.round_leg_fee(
            &VenueId::from(plan.buy_venue.as_str()),
            &VenueId::from(plan.sell_venue.as_str()),
        );
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
        let label = match reason {
            "thin_book" => "深度不足",
            "stale" => "盘口过期",
            "wide_book" => "点差过宽",
            "invalid_bbo" => "盘口非法",
            "no_min_qty" => "无最小量",
            "depeg" => "脱锚",
            other => other,
        };
        if let Some(net) = net {
            self.fill_monitor_row(
                slot,
                pair,
                &net,
                pos,
                label,
                b0,
                b1,
                mid_from_bbo(b0, b1),
            );
        } else {
            self.mark_ui_status(slot, label);
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
        b0: &Bbo,
        b1: &Bbo,
        mid: Option<Decimal>,
    ) {
        let mut params = self.cfg.grid_for(&pair.legs[0].base, pair.min_qty());
        if let Some(mid) = mid {
            if let (Some(buy_leg), Some(sell_leg)) =
                (pair.leg(net.buy.as_str()), pair.leg(net.sell.as_str()))
            {
                let reserved = self
                    .positions
                    .reserved_margin_by_venue(|v| self.cfg.leverage_for(v));
                let (bb, sb) = books_for_direction(&net.buy, &pair.legs[0].venue, b0, b1);
                params.base_qty = preview_segment_qty(
                    &self.cfg.sizing,
                    params.max_segments,
                    self.balance.global_min(),
                    self.leg_margin(&reserved, buy_leg.venue.as_str()),
                    self.leg_margin(&reserved, sell_leg.venue.as_str()),
                    bb,
                    sb,
                    mid,
                    buy_leg,
                    sell_leg,
                );
            }
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
        let entry = params.initial;
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
                entry_pct: api::fmt_pct(entry),
                grid: format!("T{}", pos.map(|p| p.grid).unwrap_or(0)),
                target_qty: api::fmt_qty(params.base_qty),
                actual_qty: api::fmt_qty(actual),
                status: status.to_string(),
            },
        );
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
                    },
                })
                .collect(),
            venue_matches: self.venue_match_rows(),
            stats: api::ApiStats {
                matched_pairs: self.pairs.len(),
                open_positions: self.positions.open_count(),
                best_net_pct: best.map(api::fmt_pct),
            },
            monitor_only: self.cfg.system.monitor_only,
            paper_trading: self.cfg.execution.paper_trading,
            arbitrage_enabled: self
                .control
                .as_ref()
                .and_then(|c| c.lock().ok())
                .map(|c| c.enabled)
                .unwrap_or(self.cfg.execution.enabled),
            matching: self.matching,
            updated_at: now_ts(),
        });
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

fn markets_for_venue(pairs: &[Pair], venue: &VenueId) -> Vec<VenueMarket> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for pair in pairs {
        for leg in &pair.legs {
            if &leg.venue == venue && seen.insert(leg.pair_id.clone()) {
                out.push(leg.clone());
            }
        }
    }
    out
}

fn intent_label(intent: &Intent) -> &'static str {
    match intent {
        Intent::Open { .. } => "open",
        Intent::Close {
            reason: CloseReason::ScalpTakeProfit,
            ..
        } => "scalp_tp",
        Intent::Close { .. } => "close",
        Intent::Hold => "hold",
    }
}
