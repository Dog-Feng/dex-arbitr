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
    grid_step_from_target_bp, is_cross_dex, match_all_pairs, new_books, order_pairs_legs,
    read_book, step_after_qty, Bbo, Books, CloseReason, CloseView, Intent, Pair,
    VenueId,
    VenueMarket, WindowGridEngine, WindowGridParams,
};
use crate::exchange::{make_adapter, ExchangePort};
use crate::exec::{
    best_sequenced_spread, closing_sequenced_spread, plan_hedge, resting_open_spread_ok,
    sequenced_spread, Adapters,
    ExecResult, HedgePlan,
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
    spawn_account_refresher, spawn_naked_hedge, spawn_run_plan, ExecEvent,
    NakedHedgeMsg, RunPlanMsg,
};
use super::positions::{grid_qty_drift, PositionStore};
use super::reconcile::{
    audit_position_qty, counterparty_hedge_is_buy, detect_naked_exposures, exchange_opposite_hedge,
    hedge_grid_step, hedge_qty, memory_hedge_matches_exchange, same_sign_open_positions,
    NakedExposure,
    NakedSource,
};
use super::intervention::{Cause, Gate, InterventionGuard, SINGLE_LEG_STREAK_LIMIT};

use super::burst::BurstSlot;
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
    pub(super) cfg: AppConfig,
    pub(super) adapters: Vec<Arc<dyn ExchangePort>>,
    pub(super) adapters_by_id: Adapters,
    pub(super) pairs: Vec<Pair>,
    /// 鐐广€屽惎鍔ㄥ鍒┿€嶅悗鎸夋墍閫夋墍 + 鐢ㄦ埛濉啓鐨?symbol 鍖归厤鍑虹殑鎵€瀵广€傚惎鍔ㄨ繘绋嬫椂涓虹┖銆?
    pub(super) available_pairs: Vec<Pair>,
    /// 鍚勬墍瀹屾暣姘哥画鍒楄〃锛圫oDEX 璁?allBookTicker 寤哄埆鍚嶇敤锛夈€?
    pub(super) listed_markets: HashMap<String, Vec<VenueMarket>>,
    pub(super) books: Books,
    pub(super) positions: PositionStore,
    pub(super) windows: WindowBook,
    /// 姣忔墍涓€鏉′拱鍗栫偣宸獥鍙ｃ€傞樁娈?1 鎶樹袱鎵€骞冲潎杩?螖锛涢樁娈?2 鍙姌甯備环鎵€涓灑銆?
    pub(super) venue_spreads: VenueSpreadBook,
    pub(super) window_grid: WindowGridEngine,
    pub(super) event_rx: Option<mpsc::UnboundedReceiver<(VenueId, String, Bbo)>>,
    /// 鍚姩濂楀埄鍚庢墠 subscribe锛沚ootstrap 鍏堝缓 channel锛岄伩鍏?sender 鍏ㄦ帀瀵艰嚧鐜€€鍑恒€?
    pub(super) bbo_tx: Option<mpsc::UnboundedSender<(VenueId, String, Bbo)>>,
    /// 宸茬粡鎷夎捣杩囩鏈夌洏鍙?WS 鐨勬墍锛岄伩鍏嶉噸澶?subscribe 鍒峰嚭澶氳矾閲嶈繛銆?
    pub(super) subscribed: HashSet<String>,
    /// 宸茶闃呯殑 (venue, pair_id)銆傜櫧鍚嶅崟鎵╁鏃跺彧缁欐柊甯佸啀鎷変竴璺?WS銆?
    pub(super) subscribed_markets: HashSet<(String, String)>,
    pub(super) matching: bool,
    pub(super) history: Option<HistoryStore>,
    pub(super) panel: LivePanel,
    /// key = slot锛堝竵 + 鎵€瀵癸級
    pub(super) pending: HashMap<String, PendingLimit>,
    pub(super) hedging: HashSet<String>,
    pub(super) exec_tx: mpsc::UnboundedSender<ExecEvent>,
    pub(super) exec_rx: Option<mpsc::UnboundedReceiver<ExecEvent>>,
    pub(super) scan_engine: ScanEngine,
    pub(super) scan_universe: Vec<Pair>,
    pub(super) scan_candidates: Vec<Pair>,
    pub(super) scan_phase: ScanPhase,
    pub(super) scan_error: Option<String>,
    pub(super) scan_venues: Vec<String>,
    pub(super) scan_was_running: bool,
    pub(super) last_coarse_at: Instant,
    pub(super) scan_probe_books: HashMap<(String, String), Bbo>,
    pub(super) scan_probe_queue: Vec<Pair>,
    pub(super) scan_probe_until: Option<Instant>,
    pub(super) balance: BalanceCache,
    pub(super) venue_accounts: VenueAccountCache,
    pub(super) api: Option<Arc<ApiHub>>,
    /// 杩愯鏃跺鍒╁紑鍏筹紙涓?ApiHub 鍏变韩鐨?Arc锛夈€侶TTP API 鍐欙紝鍐崇瓥鐜銆?
    /// `None` 琛ㄧず娌℃湁 HTTP 鏈嶅姟锛坄http.enabled: false`锛夛紝姝ゆ椂濮嬬粓鎸?
    /// `execution.enabled` 鐨勯潤鎬佸€艰繍琛屻€?
    pub(super) control: Option<Arc<std::sync::Mutex<ArbitrageControl>>>,
    pub(super) ui_pairs: HashMap<String, PairRow>,
    pub(super) naked_exposures: Vec<NakedExposure>,
    pub(super) naked_hedging: HashSet<String>,
    pub(super) intervention: InterventionGuard,
    /// 涓婃鎶婂唴瀛樼洏鍙ｆ帹鍒?HTTP 蹇収鐨勬椂闂淬€備簨浠剁幆鎸?WS 鏇存柊锛屼絾鎺ㄩ〉闈㈣鑺傛祦銆?
    pub(super) last_snap_at: Instant,
    /// 娈嬩粨浣庝簬 min_qty 鐨勮捣濮嬫椂鍒汇€傝繛缁?5 鍒嗛挓鎵嶆姤鐏板皹浠撲粙鍏ャ€?
    pub(super) dust_since: HashMap<String, Instant>,
    /// 瀵硅处鏃犳硶鏍℃鏃剁殑鍛婅鑺傛祦锛堝悓涓€妲戒綅涓嶈姣忕鍒?WARN锛夈€?
    pub(super) mismatch_log_at: HashMap<String, Instant>,
    /// 鏈Ы浣嶄笂娆″钩浠撴椂鍒汇€傝处鎴峰揩鐓ф粸鍚庢椂涓嶈绔嬪埢鎸夋棫浠撴妸鍐呭瓨鍐嶅紑鍥炴潵銆?
    pub(super) last_flat_at: HashMap<String, Instant>,
    /// 涓婁竴鎷嶅鍒╁紑鍏筹紝鐢ㄦ潵妫€娴嬨€屽仠姝€嶈竟娌垮苟娓呯┖鎵€瀵瑰垪琛ㄣ€?
    pub(super) was_enabled: bool,
    /// 鏈杩涚▼鍚勬墍鎴愪氦鍚嶄箟锛坬ty 脳 鎴愪氦浠凤級锛屽紑骞抽兘绱銆?
    pub(super) session_volume: HashMap<String, Decimal>,
    /// 闃舵 2 Burst 鐘舵€侊紙鍗?slot / 鍗曞竵绉嶏級銆?
    pub(super) burst_slots: HashMap<String, BurstSlot>,
    /// 濂楀埄寮€鐫€鎵嶅厑璁稿彂甯備环瀵瑰啿 / 绱ф€ュ钩銆傚仠姝㈠悗宸插彂鍑虹殑闄愪环鍙惉鍒版垚浜ゃ€?
    pub(super) orders_live: Arc<AtomicBool>,
    /// 鍚姩鏃跺凡閰嶇閽ョ殑鎵€銆侽pen 鍓嶄袱鑵块兘蹇呴』鍦ㄨ繖閲屻€?
    pub(super) keys_ready: HashSet<String>,
}

#[derive(Clone)]
pub(super) struct PendingLimit {
    pub(super) plan: HedgePlan,
    pub(super) since: Instant,
    pub(super) cancel: Arc<AtomicBool>,
}

impl Controller {
    pub async fn run(cfg: AppConfig) -> Result<()> {
        let mut adapters: Vec<Arc<dyn ExchangePort>> = Vec::new();
        let mut adapters_by_id = HashMap::new();
        let mut keys_ready = HashSet::new();
        for id in &cfg.venues {
            let venue = cfg.load_venue(id)?;
            if venue.keys_ready() {
                keys_ready.insert(id.clone());
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
            // 鐧藉悕鍗曡窡椤甸潰璧帮細閫傞厤鍣ㄥ垪鍑哄叏閮ㄦ案缁紝鍖归厤鏃跺啀鐢?live/yaml 杩囨护銆?
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
            // 鏋勫缓鎵€鐨勫厓鏁版嵁鍒楄〃渚?/api/venues 杩斿洖銆俴eys_ready 鍛婅瘔鍓嶇鍝簺鎵€宸查厤绉侀挜銆?
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
            burst_slots: HashMap::new(),
            orders_live: Arc::new(AtomicBool::new(false)),
            keys_ready,
        };
        this.bootstrap().await?;
        this.publish_api_snapshot();
        // 浣欓缁欑湅鏉跨敤锛氫笁鏉＄幆閮借鎷夈€備箣鍓嶅彧缁戝湪 execution 鐜笂锛?
        // loop_events / loop_scan 涓?LiveSnapshot.balances 涓€鐩寸┖锛岄〉闈㈡樉绀恒€屸€斻€嶃€?
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
        // 鏃?HTTP 闈㈡澘鏃舵病鏈夈€屽惎鍔ㄥ鍒┿€嶆寜閽紝绔嬪埢鎸?yaml 鍚敤鐨勪氦鏄撳婵€娲汇€?
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

        // HTTP 闈㈡澘锛氬繀椤昏蛋缁熶竴鍐崇瓥鐜€倅aml `execution.enabled` 榛樿鏄?false锛?
        // 鍚姩鎸夐挳鍙疆 `control.enabled`锛岃嫢鍥犳钀藉埌 loop_events锛屽垯锛?
        // - 鍚?pair_id 鍙鐞嗙涓€鏉?Pair锛堜笁鎵€涓や袱缁勫悎浼氭紡锛?
        // - 浣欓鍒锋柊鍗″湪 BBO 鍥炶皟涔嬪悗锛岄《鏍忎笉鎸?refresh_balance_secs 鏇存柊
        // - 瑁镐粨琛ュ鍐?/ 鍏堝钩鍚庡紑璋冨害閮戒笉璺?
        if this.cfg.http.enabled || this.cfg.execution.enabled {
            // 绉佹湁 WS 璁㈠崟娴侊細鎴愪氦妫€娴嬮潬瀹冧粠杞鍙樻垚浜嬩欢椹卞姩銆?
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
        // 鍙缓鐩樺彛 channel銆備氦鏄撳鍖归厤鎺ㄨ繜鍒般€屽惎鍔ㄥ鍒┿€嶏紝鎸夊綋鏃跺嬀閫夌殑 DEX 鎷夊競鍦恒€?
        let (tx, rx) = mpsc::unbounded_channel();
        self.bbo_tx = Some(tx);
        self.event_rx = Some(rx);
        self.panel = LivePanel::new(0);
        self.panel.scan_mode = self.cfg.scan.enabled && !self.cfg.execution.enabled;
        Ok(())
    }

    /// 鍖归厤瀹屾垚鍚庣珛鍒昏繘蹇収锛屼环宸洃鎺ч〉鍚姩灏辫兘鐪嬪埌浜ゆ槗瀵癸紝涓嶅繀绛夌洏鍙ｃ€?
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
        // 鎵弿涓嶆敼鎵ц `pairs` 涓嬫爣锛岄鍗曚笉蹇呮尅浣忔壂鎻忓惎鍔ㄣ€?
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

    /// 鏈?HTTP 鏃舵妸椤甸潰鍙傛暟瑕嗙洊杩?`self.cfg`锛涙棤 HTTP 鍒欎繚鎸?yaml锛堢函鍚庣娴嬭瘯锛夈€?
    pub(super) fn sync_page_config(&mut self) {
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

    /// `0鈫捖?` 鍚庡喕 渭锛堥樁娈?1 閿?live锛夈€?
    fn freeze_window(&mut self, slot: &str) {
        self.windows.freeze(slot, false);
    }

    /// 闃舵 1 STEP 鍒ゆ嵁鐢?live 渭銆?
    fn decision_mu(&self, slot: &str) -> Option<Decimal> {
        self.windows.step_mu(slot)
    }

    pub(super) fn slot_is_live(&self, slot: &str) -> bool {
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

    /// 妫€娴嬪惎鍔?鍋滄杈规部銆傚仠姝㈡椂娓呮墍瀵瑰垪琛ㄥ拰绌洪棽绐楀彛锛涘啀鍚姩鏃剁偣宸腑鏋粠绌烘牱鏈噸绠椼€?
    pub(super) fn sync_enabled_edge(&mut self) {
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
        self.cancel_all_resting_limits();
        self.clear_arbitrage_memory();
        let keep = self.drop_idle_windows();
        self.ui_pairs.retain(|k, _| keep.contains(k));
        info!(
            "arbitrage stopped; strategy memory cleared (positions, burst, grid). \
             Venue exchange positions unchanged — reconcile still reports foreign/naked from accounts"
        );
        self.publish_api_snapshot();
    }

    /// 停止套利：清策略内存。所上真实持仓仍在，页面 `exchange_positions` 仍从账户拉取。
    fn clear_arbitrage_memory(&mut self) {
        self.positions.clear_all();
        self.burst_slots.clear();
        self.hedging.clear();
        self.pending.clear();
        self.ui_pairs.clear();
        self.naked_exposures
            .retain(|n| n.source != NakedSource::BotFailure);
        self.naked_hedging.clear();
        self.intervention.clear_all();
        self.dust_since.clear();
        self.mismatch_log_at.clear();
        for pair in &self.pairs {
            let slot = pair.slot_key();
            self.window_grid.forget(&slot);
            self.windows.unfreeze(&slot);
            self.last_flat_at.insert(slot, Instant::now());
        }
        let empty: HashSet<String> = HashSet::new();
        self.windows.drop_except(&empty);
        self.venue_spreads.drop_except_venues(&empty);
    }

    fn on_arbitrage_started(&mut self) {
        self.orders_live.store(true, Ordering::Release);
        self.drop_idle_windows();
        info!("arbitrage started; venue-spread hubs reset except venues with live positions");
    }

    /// 浠呭湪鍚姩濂楀埄鏃惰皟鐢細瀵规墍閫夋墍 `list_perps`锛屽啀鎸夌敤鎴峰～鍐欑殑 symbol 杩囨护銆?
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
        self.available_pairs = order_pairs_legs(match_all_pairs(&listed), &self.cfg.venues)
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

    /// 椤甸潰鐐瑰惎鍔ㄦ垨 rematch 鏃舵墽琛屻€傚彧璁㈤槄閫変腑涓旈厤缃悎娉曠殑瀵广€?
    async fn activate_pairs(&mut self) -> Result<()> {
        self.sync_page_config();
        if self.cfg.burst.enabled {
            let n = self.cfg.pairs.enabled.len();
            if n != 1 {
                anyhow::bail!("burst mode requires exactly one enabled symbol (got {n})");
            }
        }
        let active_venues = self
            .live_params()
            .map(|lp| lp.active_venues.clone())
            .unwrap_or_else(|| self.cfg.venues.clone());
        if active_venues.len() < 2 {
            anyhow::bail!("need at least two selected venues");
        }
        let missing: Vec<&str> = active_venues
            .iter()
            .map(|s| s.as_str())
            .filter(|id| !self.keys_ready.contains(*id))
            .collect();
        if !missing.is_empty() {
            anyhow::bail!(
                "selected venues missing keys: {}",
                missing.join(", ")
            );
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
        if self.cfg.burst.enabled && self.pairs.len() != 1 {
            anyhow::bail!(
                "burst mode requires exactly one venue pair among selected exchanges (got {})",
                self.pairs.len()
            );
        }
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

    /// 鎵弿璁㈤槄锛氬彧璁紶鍏ョ殑 Pair 鑵匡紝涓嶅姩 `self.pairs`锛孲oDEX 涔熶笉鎷挎湭杩囨护鍏ㄩ泦銆?
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
            anyhow::bail!("鑷冲皯涓や釜鎵€鏈?24h 鎴愪氦閲忔暟鎹墠鑳芥壂鎻忥紙缂哄瓧娈电殑鎵€宸插墧闄わ級");
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

    /// 璇昏繍琛屾椂濂楀埄寮€鍏炽€俙None` 鏃跺洖钀藉埌闈欐€?`execution.enabled`銆?
    pub(super) fn arbitrage_enabled(&self) -> bool {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.enabled)
            .unwrap_or(self.cfg.execution.enabled)
    }

    /// 璇昏繍琛屾椂鍙儹鏀瑰弬鏁板揩鐓с€傛瘡娆″喅绛栬皟鐢ㄤ竴娆★紝閬垮厤閿佸湪鏁翠釜鍐崇瓥杩囩▼涓寔鏈夈€?
    pub(super) fn live_params(&self) -> Option<ArbitrageParams> {
        self.control
            .as_ref()
            .and_then(|c| c.lock().ok())
            .map(|c| c.params.clone())
    }

    /// 鍚姩鍖归厤鍚庢墦涓€琛岋細鐩爣 bp銆佸弽鎺ㄧ殑 螖銆佸洓鑵垮競浠疯垂銆佷袱鎵€鐐瑰樊涓灑銆?
    /// 鐐瑰樊绐楁湭婊℃椂 C=0锛屾弧绐楀悗姣忔媿鐢?live C 閲嶇畻 螖銆?
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
                "window-step 螖 derived from target_bp"
            );
        }
    }

    /// 闃舵 1锛氬洓鑵?taker + 涓ゆ墍鐐瑰樊涓灑骞冲潎銆?
    fn pair_delta_inputs(&self, v0: &VenueId, v1: &VenueId) -> (Decimal, Decimal, Option<Decimal>) {
        let c0 = self.venue_spreads.live_mu(v0.as_str());
        let c1 = self.venue_spreads.live_mu(v1.as_str());
        let both = c0.zip(c1);
        let fee = self.cfg.market_round_trip_taker(v0, v1);
        let avg = both.map(|(a, b)| pair_spread_hub_avg(a, b));
        (fee, avg.unwrap_or(Decimal::ZERO), avg)
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

    pub(super) fn position_mid(&self, pos: &crate::domain::Position) -> Option<Decimal> {
        let bb = self.book(pos.buy.as_str(), &pos.pair_id)?;
        let sb = self.book(pos.sell.as_str(), &pos.pair_id)?;
        mid_from_bbo(&bb, &sb)
    }

    pub(super) fn book(&self, venue: &str, pair_id: &str) -> Option<Bbo> {
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
        if !self.arbitrage_enabled() {
            return;
        }
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

    /// 鍐呭瓨鎸佷粨 vs 浜ゆ槗鎵€瀹炵洏鐨勬暟閲忓璐︺€?
    /// 涓よ吙鍙嶅悜鏃舵寜閲嶅彔瀵瑰啿閲忔牎姝ｏ細瀹炵洏灏戝垯缂╁唴瀛橈紝瀹炵洏澶氬垯鍦ㄤ笂闄愬唴鎶唴瀛橈紝
    /// 鍚庣画骞充粨鎵嶆寜鐪熷疄瀵瑰啿閲忚蛋銆傝烦鍙樿繃澶т笉鎶粨锛岃妭娴佸憡璀︺€?
    /// 鍙湁涓€鑵胯繘璐︺€佹垨鏈Ы浣嶈繕鍦ㄥ鍐蹭腑锛氫笉鍔ㄥ唴瀛樸€?
    fn audit_memory_positions(&mut self) {
        if !self.arbitrage_enabled() {
            return;
        }
        let mut fixes = Vec::new();
        let mut same_sign = Vec::new();
        for pair in &self.pairs {
            let slot = pair.slot_key();
            if self.slot_audit_inflight(&slot) {
                continue;
            }
            let Some(pos) = self.positions.get(&slot) else {
                continue;
            };
            if same_sign_open_positions(pair, &self.venue_accounts) {
                same_sign.push((pair.pair_id.clone(), slot));
                continue;
            }
            if let Some((mem, exch)) = audit_position_qty(pair, &self.venue_accounts, pos.qty) {
                fixes.push((slot, pair.pair_id.clone(), mem, exch));
            }
        }
        for (pair_id, slot) in same_sign {
            self.mark_intervention_for(
                &pair_id,
                &slot,
                Cause::SameSignPositions,
                "both venues hold same-sign inventory; skip auto-reconcile".into(),
            );
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
                    if let Some(pos) = self.positions.get(&slot) {
                        let min_qty = self
                            .pairs
                            .iter()
                            .find(|p| p.pair_id == pair_id)
                            .map(|p| p.min_qty())
                            .unwrap_or(Decimal::ZERO);
                        let drift = grid_qty_drift(pos);
                        if min_qty > Decimal::ZERO && drift > min_qty {
                            warn!(
                                pair = %pair_id,
                                grid = pos.grid,
                                qty = %pos.qty,
                                drift = %drift,
                                min_qty = %min_qty,
                                "reconciled qty drifts from |grid|脳base_qty"
                            );
                        }
                    }
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

    /// 鍐呭瓨宸茬┖浣嗕袱鎵€浠嶆湁鍙嶅悜浠擄細鎸夐噸鍙犻噺鎶?STEP 鎹″洖鏉ワ紝閬垮厤褰撶┖浠撶户缁寕閭绘。銆?
    fn restore_memory_from_exchange(&mut self) {
        if !self.arbitrage_enabled() {
            return;
        }
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
            self.cancel_all_resting_limits();
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
            self.freeze_window(&slot);
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

    /// 鎴愪氦璁や笉鍒版椂鐧昏瑁镐粨锛坄SecondLegUnknown`锛夈€備笉杩涜嚜鍔ㄨˉ瀵瑰啿闃熷垪銆?
    fn record_unknown_naked(
        &mut self,
        pair_id: &str,
        venue: &str,
        signed_qty: Decimal,
        counterparty: &str,
    ) {
        if signed_qty == Decimal::ZERO {
            return;
        }
        if self
            .naked_exposures
            .iter()
            .any(|n| n.pair_id == pair_id && n.venue == venue)
        {
            return;
        }
        let exposure = NakedExposure {
            pair_id: pair_id.to_string(),
            venue: venue.to_string(),
            qty: signed_qty,
            counterparty: counterparty.to_string(),
            source: NakedSource::SecondLegUnknown,
        };
        warn!(
            pair = %exposure.pair_id,
            venue = %exposure.venue,
            qty = %exposure.qty,
            "second leg unknown — manual check required before resuming"
        );
        self.log_naked_journal(&exposure, "second_leg_unknown");
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
        // 鈶?鏈夋寔浠撶殑鍏堣窇锛堝厛骞冲悗寮€锛夛紝鈶?鎸傚崟/瀵瑰啿涓殑蹇呰窇锛堝惁鍒欑洃瑙嗕細鍋滐級锛?
        // 鈶?鍓╀笅鐨勬墠鑰冭檻寮€鏂颁粨锛屽彈 in-flight 涓茶闄愬埗銆傛湭鍚姩鍒欑涓夋璺宠繃銆?
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
            // 鍏堟媿鍐呭瓨鐩樺彛锛屽啀 list_perps銆傚埛鏂伴噺浼氬崱浣忓嚑绉掞紝鑻ュ厛鎷夊競鍦哄啀璇荤洏鍙ｏ紝
            // 鍏ㄤ細瓒呰繃 data_freshness_ms锛岀矖绛涙妸鏈弧绐楃殑鍊欓€夊叏閮ㄨ涪鎺夊苟閫€璁€?
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
        // 鍊欓€夋墍瀵逛箣澶栫殑 DEX 鍒楋細鍙鍚屽竵鏈夌洏鍙ｅ氨鍗曠嫭鍏ョ獥锛岄伩鍏?Lighter脳RH 鍗犳弧鍊欓€夊悗
        // SoDEX / Entropy 鏁村垪閮芥槸 鈥斻€?
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
                    // WS 鐩樺彛宸茬粡鍐欒繘鍐呭瓨骞惰窇瀹屾湰杞喅绛栵紱100ms 鎺ㄤ竴娆＄粰椤甸潰锛?
                    // 閬垮厤姣忎釜 BBO 閮藉簭鍒楀寲蹇収銆?
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

    pub(super) fn set_spread(&mut self, pair_i: usize, lines: [String; 2]) {
        let slot = self.spread_slot(pair_i);
        self.panel.set(slot, lines[0].clone());
        self.panel.set(slot + 1, lines[1].clone());
    }

    async fn process_pair(&mut self, pair_i: usize) {
        self.sync_page_config();
        self.sync_enabled_edge();
        let pair = self.pairs[pair_i].clone();
        let slot = pair.slot_key();

        // 鍋滄鍚庣┖闂叉Ы涓嶅啀鍏ョ獥銆佷笉绠?STEP銆佷笉鍒风洃鎺ц銆傛湁浠?鎸傚崟/瀵瑰啿浠嶈蛋锛屾柟渚垮钩浠撱€?
        if !self.arbitrage_enabled() && !self.slot_is_live(&slot) {
            self.ui_pairs.remove(&slot);
            self.windows.drop_slot(&slot);
            self.window_grid.forget(&slot);
            return;
        }

        // 鎸傚崟鐩戣鎺掑湪鎵€鏈夌洏鍙ｉ棬妲?*涔嬪墠**锛氬崟瀛愪竴鏃︽寕鍑哄幓灏卞繀椤荤洴鍒版挙鍗曟垨鎴愪氦銆?
        if self.cfg.burst.enabled {
            self.process_pair_burst(pair_i);
            return;
        }

        if self.slot_has_pending(&slot) {
            self.watch_pending_slot(pair_i, &pair, &slot);
            return;
        }
        // 涓よ吙甯備环娌℃湁 pending锛屽彧鏈?hedging銆備笉鑳界┖ return锛氬惁鍒欑洃鎺ц鍋滃湪
        // 銆屽紑浠撱€嶄笖浠峰樊/鎸佷粨鏁磋鍐讳綇锛岀洿鍒版垚浜ゅ洖璋冦€?
        if self.hedging.contains(&slot) {
            self.paint_inflight_slot(pair_i, &pair, &slot);
            return;
        }

        // 娲昏穬鎵€杩囨护锛氬彧鏈変袱鑵块兘鍦?active_venues 閲岀殑 pair 鎵嶈兘寮€浠撱€?
        // 骞充粨涓嶅彈姝ら檺鈥斺€斿凡鏈夋寔浠撶殑 pair 涓嶇鎵€鏄惁杩樺湪鍒楄〃閲岄兘缁х画骞炽€?
        // 绌哄垪琛?= 鏈€夋墍锛屼笉寮€鏂颁粨锛堥〉闈㈤粯璁や笉鍕?DEX锛夈€?
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
        // `base_qty` 鏄?*鍗曟牸**鏁伴噺銆傛湁浠撴椂鐢?Position 閲屽喕缁撶殑灏猴紝
        // 缁濅笉鑳芥嬁鎬绘寔浠撻噺瑕嗙洊锛屽惁鍒?3 鏍间細琚畻鎴?1 鏍笺€?
        if let Some(p) = pos.as_ref().filter(|p| p.base_qty > Decimal::ZERO) {
            params.base_qty = p.base_qty;
        }

        // 鍏ョ獥鍙姹傜洏鍙ｆ柊椴滃悎娉曘€傚帤搴︿笉澶熶粛瑕侀噰 渭锛屽惁鍒欒杽鐩樺彛姘歌繙鍑戜笉婊＄獥鍙ｃ€?
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
        // 閲嶅惎鍚庡唴瀛樼獥鍙ｆ槸绌虹殑锛屽喕 渭 涔熶涪浜嗐€傛湁浠撲笖绐楀凡婊″垯鍐诲綋鍓?live 渭锛?
        // 閬垮厤鎸佷粨鏈?STEP 璺熺潃婊戝姩鍧囧€兼紓绉汇€備笉鏄缓浠撴椂鐨?渭锛屼絾鏄兘鎷垮埌鐨勬渶濂借繎浼笺€?
        if pos
            .as_ref()
            .is_some_and(|p| p.qty > Decimal::ZERO)
            && !self.windows.is_frozen(&slot)
            && self.windows.live_mu(&slot).is_some()
        {
            self.freeze_window(&slot);
        }

        // 鏈変粨锛氭柊椴滃害 + 鍚堟硶 BBO銆傛牸瀛愬噺鏍肩殑涓€妗ｅ帤搴﹀湪浣滃嚭 Close 涔嬪悗鎸夋湰绗?qty 鏍￠獙銆?
        // 绌轰粨锛氭暟鎹川閲?+ 涓€妗ｆ繁搴﹂兘瑕佽繃銆?
        let gate = if pos.is_some() {
            books_quality_ok(&self.cfg, &b0, &b1)
        } else {
            books_tradable(&self.cfg, &pair, &b0, &b1, params.base_qty)
        };
        if let Err(reason) = gate {
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), reason);
            if !has_pos {
                self.forget_persist(&slot);
            }
            return;
        }

        // L = legs[0]锛孯 = legs[1]銆傛 STEP = 绌?L 澶?R銆傚喅绛栫敤鍙墽琛屼环宸€?

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

        // 骞充粨瑙嗚锛氫拱鍥炲師 sell 鎵€鐨?Ask銆佸崠鍥炲師 buy 鎵€鐨?Bid锛岀敤褰撳墠鐩樺彛閲嶇畻銆?
        //
        // qty 浼?0锛氬厛绠楀嚭浠峰樊锛岃鏍煎瓙鑳藉垽鏂€岃涓嶈鍑忋€嶃€傜湡姝ｄ笅鍗曞墠鍐?
        // 鐢ㄦ湰绗斿钩浠撻噺鍋氫竴妗ｆ牎楠岋紝涓嶅灏变涪鎺夊钩浠撴剰鍥撅紙瑙佷笅鏂?thin_book锛夈€?
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
        let mu = self.decision_mu(&slot);
        let forced = pos.as_ref().and_then(|p| self.force_exit_intent(p));

        let mut intent = if let Some(forced) = forced {
            forced
        } else {
            match (mu, s_plus, s_minus, spread_rt) {
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
                    &format!("閲囨牱 {n}/{cap}"),
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
                    &format!("鐐瑰樊 {n0}/{cap} {n1}/{cap}"),
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

        // 瀹归噺鏍￠獙鍙嫤 Open锛欳lose 缁濅笉鑳借淇濊瘉閲?娣卞害鎷︿綇锛屽惁鍒欎粨浣嶅钩涓嶆帀銆?
        // 绌轰粨寮€浠撲笌鍔犱粨璧板悓涓€鏉¤矾寰勩€傛湰鍦扮偣宸湪鍏ュ彛 `books_quality_ok` 宸叉煡杩囥€?
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
                    "娣卞害涓嶈冻",
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
        if matches!(intent, Intent::Open { .. })
            && (self.pair_has_naked(&pair.pair_id) || self.pair_naked_inflight(&pair.pair_id))
        {
            intent = Intent::Hold;
        }
        if matches!(intent, Intent::Open { .. }) && !self.pair_keys_ready(&pair) {
            intent = Intent::Hold;
        }
        let open_skip = if want_open && matches!(intent, Intent::Hold) {
            if !self.arbitrage_enabled() {
                Some("未启动")
            } else if self.pair_has_naked(&pair.pair_id) || self.pair_naked_inflight(&pair.pair_id)
            {
                Some("鍗曡竟鏁炲彛")
            } else if !self.pair_keys_ready(&pair) {
                Some("缺密钥")
            } else {
                None
            }
        } else {
            None
        };

        // 涓€妗ｆ拺涓嶄綇鏈瑪骞充粨閲?鈫?涓㈡帀鏍煎瓙骞充粨鎰忓浘銆?
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
            self.mark_ui_status(&slot, "寮€浠撲腑");
            return;
        }
        // 鏈Ы宸插湪鍏ュ彛鍥?pending/hedging 杩斿洖銆傝繖閲岀殑 in_flight 鍙彲鑳芥槸**鍒殑妲?*銆?
        // 骞充粨涓嶈兘琚埆鐨勫竵鍗′綇锛涙柊寮€/鍔犱粨浠嶇瓑锛岄伩鍏嶅瀵瑰悓鏃跺崰淇濊瘉閲戝拰涓嬪崟閫氶亾銆?
        if matches!(intent, Intent::Open { .. }) && self.execution_in_flight() {
            self.panel.stats.bump_skip("in_flight");
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "in_flight");
            return;
        }
        // 浜哄伐浠嬪叆绛夊緟锛氬紑浠撳拰骞充粨**閮芥尅**锛堝榻愬弬鑰?`should_block` 鍦ㄥ紑浠?
        // 涓庡钩浠撲袱鏉¤矾寰勪笂閮芥煡锛夈€傝繖璺熶笅闈㈢殑 reduce-only 鐔旀柇鐩稿弽锛屾槸鍒绘剰鐨勶細
        // reduce-only 鏃朵粨浣嶆槸宸茬煡鐨勶紝鎸″钩浠撶瓑浜庨攣姝讳粨浣嶏紱鑰屼粙鍏ユ€佹剰鍛崇潃
        // 鍐呭瓨閲岀殑浠撲綅鏈韩涓嶅彲淇★紝鎸夐敊鐨勯噺鍘诲钩浼氭妸鏁炲彛鏀惧ぇ銆?
        // 鍏滃簳鏄?30 鍒嗛挓鑷姩瑙ｉ櫎鍜屾牸鏁板彉鍖栬В闄わ紝涓嶄細姘镐箙閿佹銆?
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
                let flatten_ok = matches!(intent, Intent::Close { .. })
                    && cause.allows_reduce_only_close()
                    && pos.as_ref().is_some_and(|p| {
                        memory_hedge_matches_exchange(&pair, p, &self.venue_accounts)
                    });
                if flatten_ok {
                    if let Some(p) = pos.as_ref() {
                        intent = Intent::Close {
                            qty: p.qty,
                            grid: 0,
                            reason: CloseReason::GridReduce,
                            round_trip_pct: close_view
                                .map(|cv| p.entry_net_pct + cv.exit_net_pct)
                                .unwrap_or(Decimal::ZERO),
                        };
                    }
                    info!(
                        pair = %pair.pair_id,
                        cause = cause.as_str(),
                        "intervention flatten-only: memory matches exchange; reduce to 0"
                    );
                } else {
                // 鎸傝捣閭ｄ竴鍒诲凡缁忔墦杩?ERROR 骞跺啓浜?journal锛岃繖閲屾瘡杞彧璁?skip 璁℃暟锛?
                // 鐢?debug 閬垮厤鍒峰睆銆傞潰鏉夸笂鑳界湅鍒?`intervention` 鐨勮烦杩囨暟銆?
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
        }
        let Some(mut plan) = plan_hedge(&pair, &intent, pos.as_ref(), &self.cfg) else {
            self.paint_skip_with_books(&slot, &pair, &v0, &v1, &b0, &b1, pos.as_ref(), "no_plan");
            return;
        };
        plan.decision_net_pct = net.net_pct;
        plan.decision_raw_pct = net.raw_pct;
        // 鍥哄寲寮€浠撴椂鐨勫崟鏍兼暟閲忥紝渚?Position.base_qty 浣跨敤銆傚钩浠撴椂 params.base_qty
        // 鐢辨寔浠撹嚜韬惡甯︼紝涓嶉渶瑕佷粠 plan 浼犲叆锛屾墍浠ュ彧鍦?is_open 鏃跺啓鏈夋剰涔夌殑鍊笺€?
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
            let held = pos.as_ref().map(|p| p.qty).unwrap_or(Decimal::ZERO);
            let after = if plan.is_open {
                held + plan.qty
            } else {
                (held - plan.qty).max(Decimal::ZERO)
            };
            let base = if plan.base_qty > Decimal::ZERO {
                plan.base_qty
            } else {
                pos.as_ref()
                    .map(|p| p.base_qty)
                    .unwrap_or(params.base_qty)
            };
            plan.grid_to = step_after_qty(plan.grid_from, plan.grid_to, after, base);
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

    /// 鎸傚崟鐩戣锛氬紑浠撳崟浠峰樊璺屽嚭鎸佹湁鍖?鈫?缃?cancel锛屽悗鍙版墽琛?task 鎾ゅ崟銆?
    ///
    /// 骞充粨鍗?*涓?*鍥犱环宸彉鍖栨挙鈥斺€斿钩浠撹璧板畬锛屽惁鍒欎細涓€鐩寸暀鐫€鍗曡吙椋庨櫓銆?
    /// 鍗曡疆瓒呮椂鐢辨墽琛屽櫒鑷繁绠★紱杩欓噷鑻ユ寜鏁磋疆璁″垝璧风偣瓒呮椂骞剁疆 cancel锛?
    /// `limit_retry_count` 鐨勫悗缁噸鎸備細琚洿鎺ヨ烦杩囥€?
    pub(super) fn watch_pending_slot(&mut self, pair_i: usize, pair: &Pair, slot: &str) {
        if self.pending.contains_key(slot) {
            self.watch_one_pending(pair_i, pair, slot, slot);
        }
    }

    fn watch_one_pending(&mut self, pair_i: usize, pair: &Pair, slot: &str, key: &str) {
        let Some(pending) = self.pending.get(key).cloned() else {
            return;
        };
        let deadline = self.pending_hard_deadline();
        if pending.since.elapsed() > deadline {
            let already = pending.cancel.load(Ordering::Relaxed);
            pending.cancel.store(true, Ordering::Release);
            if !already {
                tracing::error!(
                    pair = %pair.pair_id,
                    slot,
                    elapsed_secs = pending.since.elapsed().as_secs(),
                    "pending limit exceeded hard deadline; requesting cancel (waiting for exec ack)"
                );
                self.mark_intervention_for(
                    &pair.pair_id,
                    slot,
                    Cause::WatchdogTimeout,
                    format!(
                        "pending on {} exceeded {}s; cancel requested, waiting for ack",
                        pending.plan.first.venue,
                        pending.since.elapsed().as_secs()
                    ),
                );
                self.log_plan_record(&pending.plan, "exec_fail", "watchdog_timeout", "");
            }
            // 瓒呮椂鍙疆 cancel锛岀瓑鍒?on_run_plan 鍥炴墽鍐嶆憳 pending銆?
        }
        if pending.plan.burst {
            let ui = if pending.cancel.load(Ordering::Relaxed) {
                "撤单中"
            } else {
                "Burst挂单"
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
            // 骞充粨鍗曪細浠峰樊鎬庝箞鍙橀兘瑕佽蛋瀹?
            (_, false) => true,
            (Some((net, _residual)), true) => {
                let same_dir = pending.plan.buy_venue == net.buy.as_str()
                    && pending.plan.sell_venue == net.sell.as_str();
                resting_open_spread_ok(net.raw_pct, same_dir, floor)
            }
            // 寮€浠撳崟浣嗚涓嶅埌鐩樺彛锛氫笉褰撴垚銆屼环宸病浜嗐€嶏紝浜ょ粰鎵ц鍣ㄦ湰杞秴鏃?
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

    /// 涓よ吙甯備环瀵瑰啿杩涜涓細缁х画鐢ㄥ綋鍓嶇洏鍙ｅ埛鐩戞帶琛岋紝鐘舵€佹爣鎴愬紑浠撲腑/骞充粨涓€?
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

    /// 鎸傚崟鏈熼棿鎸?*璁″垝鐨勬柟鍚?*绠楀噣杈癸紙涓嶅弻鍚戝彇浼橈級銆?
    /// residual 鍙粰鐩戞帶琛屽睍绀猴紱寮€浠撴寕鍗曟槸鍚﹁繕澶熺湅姣涗环宸?vs 螖脳婊炲悗銆?
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

    pub(super) fn execution_in_flight(&self) -> bool {
        !self.hedging.is_empty() || !self.pending.is_empty()
    }

    /// 涓€杞?limit-then-market 鐨勬甯歌€楁椂涓婇檺锛?
    /// 姣忚疆鎸傚崟绛夊緟 脳 杞暟 + 鎾ゅ崟绔炴€?+ 涓€娆″啓鎿嶄綔鐨?sidecar 瓒呮椂锛屽啀鐣欎竴鍊嶄綑閲忋€?
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
        if msg.plan.burst {
            self.on_burst_run_plan(msg);
            return;
        }
        if !self.arbitrage_enabled() {
            self.hedging.remove(&msg.slot);
            self.pending.remove(&msg.slot);
            self.positions.release_pending(&msg.slot);
            info!(
                pair = %msg.plan.pair_id,
                "exec finished after arbitrage stopped; not updating memory"
            );
            return;
        }
        self.hedging.remove(&msg.slot);
        self.pending.remove(&msg.slot);
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
                    // 涓よ吙骞插噣鎴愪氦锛氳繛鍑诲綊闆讹紙瀵归綈鍙傝€冨湪鎴愬姛璺緞涓婇噸缃鏁帮級銆?
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
                // 绗簩鑵垮皯鎴愪氦鐨勯儴鍒嗘槸鐪熷疄鍗曡竟鏁炲彛銆傚繀椤绘帓鍦?retain 涔嬪悗锛?
                // 鍚﹀垯鍒氱櫥璁板氨琚繖涓€琛屾竻鎺夈€?
                if result.unhedged_qty > Decimal::ZERO {
                    self.record_naked_from_failed_hedge(&msg.plan, result.unhedged_qty);
                }
                // 涓よ吙閮藉鍐蹭笂浜嗭紝浣嗙涓€鑵胯繕鐣欑潃涓€寮犳挙涓嶆帀鐨勫崟銆備粨浣嶈璐︽槸
                // 瀵圭殑锛屽彲閭ｅ紶鍗曢殢鏃跺彲鑳芥垚浜ゅ嚭绗笁鏉¤吙鈥斺€斿厛鍋滄墜銆?
                // 蹇呴』鎺掑湪 `apply_fill` 涔嬪悗锛氭寕璧疯璁扮殑鏄湰绗旀垚浜ゅ悗鐨勬牸鏁般€?
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
            }
            Err(err) => {
                if err.contains("EMERGENCY_CLOSED") {
                    warn!(pair = %msg.plan.pair_id, error = %err, "dual-market unhedged leg closed");
                    self.log_plan_record(&msg.plan, "exec_fail", "emergency_closed", &err);
                    // 绱ф€ュ钩浠?*鎴愬姛**锛屾暈鍙ｅ凡缁忔敹鎺夛紝浠撲綅鐘舵€佹槸骞插噣鐨勶紝
                    // 鎵€浠ヤ笉鎸傝捣銆備絾杩欑畻涓€娆″崟鑵挎垚浜わ細鍙傝€冪殑瑙勫垯鏄繛缁?3 娆?
                    // 鍗充娇姣忔閮借ˉ涓婁篃瑕佹寕璧凤紝鍥犱负閭ｈ鏄庨摼璺湁绯荤粺鎬ч棶棰樸€?
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
                    let qty = qty_from_exec_err(&err, msg.plan.qty);
                    warn!(
                        pair = %msg.plan.pair_id,
                        error = %err,
                        "dual-market fill unverifiable; not sending more"
                    );
                    self.log_plan_record(&msg.plan, "exec_fail", "second_leg_unknown", &err);
                    self.record_unknown_naked(
                        &msg.plan.pair_id,
                        &msg.plan.first.venue,
                        if msg.plan.first.is_buy { qty } else { -qty },
                        &msg.plan.second.venue,
                    );
                    self.record_unknown_naked(
                        &msg.plan.pair_id,
                        &msg.plan.second.venue,
                        if msg.plan.second.is_buy { qty } else { -qty },
                        &msg.plan.first.venue,
                    );
                    self.mark_intervention(
                        &msg.slot,
                        &msg.plan,
                        Cause::SecondLegUnknown,
                        "dual-market fill unverifiable; check both venues before resuming".into(),
                    );
                    } else if err.contains("NAKED_FIRST_LEG") {
                    warn!(pair = %msg.plan.pair_id, error = %err, "naked first leg");
                    self.log_plan_record(&msg.plan, "exec_fail", "naked", &err);
                    let naked_qty = qty_from_exec_err(&err, msg.plan.qty);
                    self.record_naked_from_failed_hedge(&msg.plan, naked_qty);
                    // 绱ф€ュ钩澶辫触 vs 鍏跺畠瑁歌吙锛氶潰鏉?/ journal 鍘熷洜鍒嗗紑锛屼究浜庝汉宸ュ垎娴併€?
                    let cause = if err.to_ascii_lowercase().contains("close failed")
                        || err.contains("emergency")
                    {
                        Cause::EmergencyCloseFailed
                    } else {
                        Cause::NakedLegUnrecoverable
                    };
                    self.mark_intervention(
                        &msg.slot,
                        &msg.plan,
                        cause,
                        format!("naked leg on {} ({err})", msg.plan.first.venue),
                    );
                } else if err.contains("QUOTE_LOST_RACE") {
                    info!(pair = %msg.plan.pair_id, "quote lost race; extra fill closed");
                    self.log_plan_record(&msg.plan, "cancel", "quote_lost_race", &err);
                } else if err.contains("ARB_STOPPED") {
                    info!(
                        pair = %msg.plan.pair_id,
                        "adjacent fill after stop; booking any hedged qty already returned as Ok"
                    );
                    self.log_plan_record(&msg.plan, "cancel", "arb_stopped", &err);
                    let leftover = qty_from_exec_err(&err, Decimal::ZERO);
                    if leftover > Decimal::ZERO {
                        self.record_naked_from_failed_hedge(&msg.plan, leftover);
                    }
                    self.positions.release_pending(&msg.slot);
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
                    // 鎾や笉鎺夌殑鎸傚崟鍙兘绋嶅悗鎴愪氦锛屽眾鏃朵細鍑┖澶氬嚭涓€鏉¤吙銆?
                    // 鍦ㄦ悶娓呮瀹冨埌搴曟垚娌℃垚涔嬪墠涓嶈兘缁х画浜ゆ槗杩欎釜甯併€?
                    self.mark_intervention(
                        &msg.slot,
                        &msg.plan,
                        Cause::OrphanOrder,
                        format!("uncancelable resting order on {}", msg.plan.first.venue),
                    );
                }
                self.positions.release_pending(&msg.slot);
                self.forget_persist(&msg.slot);
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
    /// 鎸傝捣涓€涓?pair 绛変汉宸ュ鐞嗐€傚榻愬弬鑰?`_mark_manual_intervention`銆?
    ///
    /// 鍙湪**棣栨**鎸傝捣鏃舵墦 ERROR + 鍐?journal锛涢噸澶嶈Е鍙戜笉閲嶇疆璁℃椂锛屽惁鍒?
    /// 鍙嶅鎶ラ敊浼氭妸 30 鍒嗛挓鑷姩瑙ｉ櫎鏃犻檺鎺ㄥ悗锛岀瓑浜庢案涔呴攣姝昏繖涓竵銆?
    /// 鎸傝捣鏃剁偣鐨勬寔浠撴牸鏁帮紝渚?`should_block` 鐨勩€屾牸鏁板彉鍖?鈫?瑙ｉ櫎銆嶇敤銆?
    ///
    /// 绌轰粨鎴栧崟鏍奸噺鏈煡鏃惰繑鍥?`None`锛氳В闄ゅ垽瀹氳姹傛寕璧锋椂鍜屽綋鍓嶉兘鏄?`Some`锛?
    /// 鎷夸笉鍑嗗氨鍙蛋 30 鍒嗛挓瓒呮椂锛屼笉鐚溿€?
    fn current_grid_level(&self, slot: &str) -> Option<i32> {
        let pos = self.positions.get(slot)?;
        if pos.base_qty <= Decimal::ZERO {
            return None;
        }
        Some(pos.grid)
    }

    /// 鎸傝捣涓€涓竵瀵圭瓑寰呬汉宸ヤ粙鍏ャ€?
    ///
    /// `slot` 鐢ㄦ潵璇?*璁拌处瀹屾垚鍚?*鐨勬牸鏁扳€斺€旇皟鐢ㄧ偣蹇呴』鍦?`apply_fill` 涔嬪悗锛?
    /// 鍚﹀垯璁颁笅鐨勬槸鎴愪氦鍓嶇殑鏃ф牸鏁帮紝涓嬩竴杞?`should_block` 浼氭妸杩欐鎴愪氦鏈韩
    /// 閫犳垚鐨勬牸鏁板彉鍖栧綋鎴愩€岃鎯呮崲鍖洪棿銆嶏紝鍒氭寕璧峰氨鑷姩瑙ｉ櫎銆?
    fn mark_intervention(&mut self, slot: &str, plan: &HedgePlan, cause: Cause, detail: String) {
        self.mark_intervention_for(&plan.pair_id, slot, cause, detail);
    }

    pub(super) fn mark_intervention_for(&mut self, pair_id: &str, slot: &str, cause: Cause, detail: String) {
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

    /// 鐢?*瀹為檯鎴愪氦閲?*鍥炲啓鎸佷粨锛屼笉鏄鍒掗噺锛氶儴鍒嗘垚浜ゆ椂鐢?plan.qty
    /// 浼氳鍐呭瓨鎸佷粨铏氶珮锛屼箣鍚庢寜铏氶珮閲忓钩浠撳氨鐣欎笅灏惧反銆?
    pub(super) fn apply_fill(&mut self, pair: &Pair, plan: &HedgePlan, result: &ExecResult, pair_i: usize) {
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
                self.freeze_window(&plan.slot);
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
            self.positions.record_close(&plan.slot, qty, plan.grid_to);
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

    /// 寤轰粨鍑€杈逛紭鍏堟寜涓よ吙鐪熷疄鎴愪氦浠风畻锛涙垚浜や环鎷夸笉鍒帮紙甯備环鑵?sidecar 涓嶅洖
    /// avg_price锛夋椂閫€鍥炲喅绛栨椂鐨勫噣杈广€?*涓嶆墸 nat**锛歯at 鏄粨鏋勬€у熀宸紝
    /// 骞充粨鏃朵細瀵圭О鍦拌繕鍥炴潵锛屾墸浜嗕細浣庝及寰€杩斿噣鍒┿€?
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

    /// 涓や釜鏂瑰悜閮介噰鏍峰啀鍙栬鏂瑰悜鐨?nat銆?
    /// 鍙噰銆屽綋杞渶浼樻柟鍚戙€嶄細璁╂牱鏈彉鎴愭潯浠跺垎甯冿紝涓綅鏁扮郴缁熸€у亸楂橈紝
    /// residual 琚暱鏈熷帇浣庡埌姘歌繙寮€涓嶄簡浠撱€俷at 鍙璺?DEX 鏈夋剰涔夈€?
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

    pub(super) fn mark_ui_status(&mut self, slot: &str, status: &str) {
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
        let mu = self.decision_mu(slot);
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

    fn cancel_all_resting_limits(&mut self) {
        for p in self.pending.values() {
            p.cancel.store(true, Ordering::Release);
        }
    }

    pub(super) fn slot_has_pending(&self, slot: &str) -> bool {
        self.pending.contains_key(slot)
    }

    /// 宸叉湁鍗曡竟鏁炲彛鏃朵笉鍐嶆寕寮€浠撻偦妗ｏ紝閬垮厤鍦ㄦ湭瀵瑰啿鐨?RH/lighter 浠撲笂缁х画鍔犵爜銆?
    pub(super) fn pair_has_naked(&self, pair_id: &str) -> bool {
        self.naked_exposures
            .iter()
            .any(|n| n.pair_id == pair_id && n.qty.abs() > Decimal::ZERO)
    }

    pub(super) fn pair_naked_inflight(&self, pair_id: &str) -> bool {
        self.naked_hedging
            .iter()
            .any(|k| k.split('|').next() == Some(pair_id))
    }

    pub(super) fn pair_keys_ready(&self, pair: &Pair) -> bool {
        self.keys_ready.contains(pair.legs[0].venue.as_str())
            && self.keys_ready.contains(pair.legs[1].venue.as_str())
    }

    /// 寮哄埗绂诲満锛氭寔浠撹秴鏃?/ 浣欓瑙﹀簳銆備竴娆″钩鍒?0锛屼笉鍙?卤1 / persistence銆?
    /// `FundingStopLoss` 闇€瑕佽祫閲戣垂鐜囩紦瀛橈紝灏氭湭鎺ュ叆銆?
    fn force_exit_intent(&self, pos: &crate::domain::Position) -> Option<Intent> {
        if pos.qty <= Decimal::ZERO {
            return None;
        }
        if self.cfg.grid.max_hold_secs > 0
            && pos.held_for(Instant::now()) >= Duration::from_secs(self.cfg.grid.max_hold_secs)
        {
            return Some(Intent::Close {
                qty: pos.qty,
                grid: 0,
                reason: CloseReason::HoldTimeout,
                round_trip_pct: Decimal::ZERO,
            });
        }
        let floor = self.cfg.sizing.balance_floor_usdc;
        if floor > Decimal::ZERO {
            let buy_av = self.balance.venue_available(pos.buy.as_str());
            let sell_av = self.balance.venue_available(pos.sell.as_str());
            if (buy_av > Decimal::ZERO && buy_av < floor)
                || (sell_av > Decimal::ZERO && sell_av < floor)
            {
                return Some(Intent::Close {
                    qty: pos.qty,
                    grid: 0,
                    reason: CloseReason::BalanceFloor,
                    round_trip_pct: Decimal::ZERO,
                });
            }
        }
        None
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
        "thin_book" => "娣卞害涓嶈冻".into(),
        "stale" => "鐩樺彛杩囨湡".into(),
        "invalid_bbo" => "鐩樺彛闈炴硶".into(),
        "no_min_qty" => "鏃犳渶灏忛噺".into(),
        "no_margin" | "no_capacity" => "保证金不足".into(),
        "no_mid" => "无中价".into(),
        "no_size" => "鏁伴噺鏃犳晥".into(),
        "in_flight" => "鎺掗槦".into(),
        "intervention" => "浜哄伐浠嬪叆".into(),
        "naked_exposure" => "鍗曡竟鏁炲彛".into(),
        "keys_missing" => "缺密钥".into(),
        "concurrent_limit" => "执行占用中".into(),
        "no_plan" => "鏃犳硶瑙勫垝".into(),
        "no_baseline" => "无底价".into(),
        other => other.to_string(),
    }
}

fn naked_key(n: &NakedExposure) -> String {
    format!("{}|{}", n.pair_id, n.venue)
}

/// 浠庢墽琛岄敊璇瓧绗︿覆閲屾娊鍑?`unhedged=` / `qty=`锛屾病鏈夊垯鐢?fallback銆?
fn qty_from_exec_err(err: &str, fallback: Decimal) -> Decimal {
    for key in ["unhedged=", "qty="] {
        if let Some(rest) = err.split(key).nth(1) {
            let tok = rest
                .split(|c: char| c == ' ' || c == ';' || c == ',' || c == ')')
                .next()
                .unwrap_or("");
            if let Ok(d) = tok.parse::<Decimal>() {
                if d > Decimal::ZERO {
                    return d;
                }
            }
        }
    }
    fallback
}

/// 鎸変拱鎵€鏄笉鏄?legs[0] 鍐冲畾 (buy_book, sell_book)銆?
pub(super) fn books_for_direction<'a>(
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
