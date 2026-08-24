use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{
    is_cross_dex, match_all_pairs, Bbo, GridEngine, Intent, Pair, VenueId, VenueMarket,
};
use crate::domain::spread::raw_spread_pct;
use crate::exec::{
    best_sequenced_spread, plan_hedge, watch_resting_limit, HedgeExecutor, HedgePlan, LimitWatch,
};
use crate::exchange::{make_adapter, ExchangePort};
use crate::infra::api::{self, ApiHub, ExchangePositionRow, LiveSnapshot, PairRow, PositionRow, VenueBalanceRow};
use crate::infra::dashboard::{self, LivePanel};
use crate::infra::journal::{ExecJournal, ExecRecord, now_ts};
use crate::infra::history::{residual_net, HistoryStore};

use super::balance::{refresh_balances, refresh_venue_accounts, BalanceCache, VenueAccountCache};
use super::exec_worker::{
    spawn_hedge_second_leg, spawn_post_first_leg, spawn_run_plan, ExecEvent, HedgeSecondMsg,
    PostFirstMsg, RunPlanMsg,
};
use super::positions::PositionStore;
use super::risk::{books_tradable, stable_ok};
use super::scan::OpportunityTracker;
use super::sizing::{mid_from_bbo, resolve_qty, BindingLeg, LegMargin};

pub struct Controller {
    cfg: AppConfig,
    adapters: Vec<Arc<dyn ExchangePort>>,
    adapters_by_id: HashMap<String, Arc<dyn ExchangePort>>,
    pairs: Vec<Pair>,
    books: HashMap<(String, String), Bbo>,
    positions: PositionStore,
    grid: GridEngine,
    event_rx: Option<mpsc::UnboundedReceiver<(VenueId, String, Bbo)>>,
    history: Option<HistoryStore>,
    panel: LivePanel,
    pending: HashMap<String, PendingLimit>,
    hedging_pairs: HashSet<String>,
    exec_tx: mpsc::UnboundedSender<ExecEvent>,
    exec_rx: Option<mpsc::UnboundedReceiver<ExecEvent>>,
    scanner: OpportunityTracker,
    last_token_log: HashMap<String, LoggedToken>,
    balance: BalanceCache,
    venue_accounts: VenueAccountCache,
    api: Option<Arc<ApiHub>>,
    ui_pairs: HashMap<String, PairRow>,
}

struct LoggedToken {
    pair_id: String,
    buy: String,
    sell: String,
    raw: Decimal,
    residual: Decimal,
    at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingFlight {
    None,
    PostingFirst,
}

#[derive(Clone)]
struct PendingLimit {
    plan: HedgePlan,
    since: Instant,
    order_id: Option<String>,
    flight: PendingFlight,
}

impl Controller {
    pub async fn run(cfg: AppConfig) -> Result<()> {
        let mut adapters: Vec<Arc<dyn ExchangePort>> = Vec::new();
        let mut adapters_by_id = HashMap::new();
        for id in &cfg.venues {
            let venue = cfg.load_venue(id)?;
            if venue.keys_ready() {
                if venue.id == "sodex" {
                    tracing::info!(
                        venue = id,
                        account_id = venue.account_index,
                        api_key_name = %venue.api_key_name,
                        "sodex keys loaded"
                    );
                } else {
                    tracing::info!(
                        venue = id,
                        account_index = venue.account_index,
                        api_key_index = venue.api_key_index,
                        "signing keys loaded"
                    );
                }
            } else {
                tracing::info!(venue = id, "no signing keys; monitor_only still works");
            }
            let adapter = make_adapter(venue, cfg.pairs.whitelist.clone());
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
            let hub = Arc::new(ApiHub::new(
                cfg.live_test.journal_path.clone(),
                PathBuf::from(&cfg.http.web_root),
                cfg.http.auth_token.clone(),
            ));
            hub.clone().spawn(&cfg.http.bind);
            Some(hub)
        } else {
            None
        };
        let (exec_tx, exec_rx) = mpsc::unbounded_channel();
        let mut this = Self {
            cfg,
            adapters,
            adapters_by_id,
            pairs: Vec::new(),
            books: HashMap::new(),
            positions: PositionStore::default(),
            grid: GridEngine::default(),
            event_rx: None,
            history,
            panel: LivePanel::new(0),
            pending: HashMap::new(),
            hedging_pairs: HashSet::new(),
            exec_tx,
            exec_rx: Some(exec_rx),
            scanner: OpportunityTracker::default(),
            last_token_log: HashMap::new(),
            balance: BalanceCache::default(),
            venue_accounts: VenueAccountCache::default(),
            api,
            ui_pairs: HashMap::new(),
        };
        this.bootstrap().await?;
        if this.cfg.execution.enabled {
            this.refresh_balances_now().await?;
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
        let mut books = Vec::new();
        for adapter in &self.adapters {
            books.push(adapter.list_perps().await?);
        }
        self.pairs = match_all_pairs(&books);
        info!(
            n = self.pairs.len(),
            venues = self.adapters.len(),
            scan = self.cfg.scan.enabled,
            execution = self.cfg.execution.enabled,
            whitelist = ?self.cfg.pairs.whitelist,
            "matched perp pairs"
        );
        for i in 0..self.adapters.len() {
            for j in (i + 1)..self.adapters.len() {
                let left = self.adapters[i].id();
                let right = self.adapters[j].id();
                let n = self
                    .pairs
                    .iter()
                    .filter(|p| {
                        let a = p.legs[0].venue.as_str();
                        let b = p.legs[1].venue.as_str();
                        (a == left.as_str() && b == right.as_str())
                            || (a == right.as_str() && b == left.as_str())
                    })
                    .count();
                info!(
                    left = left.as_str(),
                    right = right.as_str(),
                    n,
                    "venue pair intersection"
                );
            }
        }
        let (tx, rx) = mpsc::unbounded_channel();
        for adapter in &self.adapters {
            let mkts = markets_for_venue(&self.pairs, &adapter.id());
            adapter.subscribe_bbo(&mkts, tx.clone()).await?;
        }
        self.event_rx = Some(rx);
        let panel_pairs = if self.cfg.execution.enabled || !self.cfg.scan.enabled {
            self.pairs.len() * self.pair_stride()
        } else {
            0
        };
        self.panel = LivePanel::new(panel_pairs);
        self.panel.scan_mode = self.cfg.scan.enabled && !self.cfg.execution.enabled;
        Ok(())
    }

    async fn refresh_balances_now(&mut self) -> Result<()> {
        self.balance = refresh_balances(&self.adapters, &self.cfg.sizing).await?;
        self.venue_accounts = refresh_venue_accounts(&self.adapters).await?;
        Ok(())
    }

    async fn maybe_refresh_balances(&mut self) {
        let due = self.balance.last_refresh.elapsed()
            >= Duration::from_secs(self.cfg.sizing.refresh_balance_secs.max(5));
        if due {
            if let Err(err) = self.refresh_balances_now().await {
                warn!(error = %err, "balance refresh failed");
            }
        }
    }

    async fn loop_unified(&mut self) -> Result<()> {
        let mut rx = self.event_rx.take().expect("bootstrap must run first");
        let mut exec_rx = self.exec_rx.take().expect("exec channel");
        let mut tick = tokio::time::interval(Duration::from_millis(
            self.cfg.execution.loop_interval_ms.max(10),
        ));
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some((venue, pair_id, bbo)) = msg else {
                        break;
                    };
                    self.books
                        .insert((venue.as_str().to_string(), pair_id), bbo);
                }
                Some(ev) = exec_rx.recv() => {
                    self.handle_exec_event(ev).await;
                }
                _ = tick.tick() => {
                    self.maybe_refresh_balances().await;
                    self.tick_execution().await;
                    self.panel.flush();
                }
            }
        }
        Ok(())
    }

    async fn tick_execution(&mut self) {
        let close_first: Vec<usize> = self
            .pairs
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                self.positions
                    .get(&p.pair_id)
                    .map(|pos| pos.qty > Decimal::ZERO)
                    .unwrap_or(false)
            })
            .map(|(i, _)| i)
            .collect();
        let close_set: HashSet<usize> = close_first.iter().copied().collect();
        for &pi in &close_first {
            self.process_pair(pi).await;
        }
        for pi in 0..self.pairs.len() {
            if close_set.contains(&pi) {
                continue;
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
        let mut tick = tokio::time::interval(Duration::from_millis(
            self.cfg.scan.analysis_interval_ms.max(10),
        ));
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some((venue, pair_id, bbo)) = msg else {
                        break;
                    };
                    self.books
                        .insert((venue.as_str().to_string(), pair_id), bbo);
                }
                _ = tick.tick() => {
                    self.paint_scan();
                }
            }
        }
        Ok(())
    }

    fn paint_scan(&mut self) {
        self.sample_scan_history();
        let mut round = self.scanner.evaluate(
            &self.pairs,
            &self.books,
            self.cfg.system.data_freshness_ms,
            self.cfg.scan.min_spread_pct,
            Instant::now(),
        );
        super::scan::OpportunityTracker::apply_cross_natural(
            &mut round,
            self.history.as_ref(),
            self.cfg.scan.min_spread_pct,
            self.cfg.scan.cross_use_natural,
        );
        self.emit_token_lines(&round);
    }

    fn sample_scan_history(&self) {
        let Some(store) = &self.history else {
            return;
        };
        for pair in &self.pairs {
            let v0 = pair.legs[0].venue.as_str();
            let v1 = pair.legs[1].venue.as_str();
            let Some(b0) = self.books.get(&(v0.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            let Some(b1) = self.books.get(&(v1.to_string(), pair.pair_id.clone())) else {
                continue;
            };
            if !b0.is_fresh(self.cfg.system.data_freshness_ms)
                || !b1.is_fresh(self.cfg.system.data_freshness_ms)
                || !b0.valid()
                || !b1.valid()
            {
                continue;
            }
            if !is_cross_dex(v0, v1) {
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
        while let Some((venue, pair_id, bbo)) = rx.recv().await {
            self.books
                .insert((venue.as_str().to_string(), pair_id.clone()), bbo.clone());
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
        let pair = self.pairs[pair_i].clone();
        let v0 = pair.legs[0].venue.clone();
        let v1 = pair.legs[1].venue.clone();
        let Some(b0) = self.books.get(&(v0.as_str().to_string(), pair.pair_id.clone())) else {
            self.panel.stats.bump_skip("wait");
            return;
        };
        let Some(b1) = self.books.get(&(v1.as_str().to_string(), pair.pair_id.clone())) else {
            self.panel.stats.bump_skip("wait");
            return;
        };
        let b0 = b0.clone();
        let b1 = b1.clone();

        let mid = match mid_from_bbo(&b0, &b1) {
            Some(m) => m,
            None => {
                self.panel.stats.bump_skip("no_mid");
                return;
            }
        };
        let base = &pair.legs[0].base;
        let min_probe = self
            .cfg
            .min_book_qty(base)
            .max(self.cfg.grid_for(base).base_qty);

        if let Err(reason) = books_tradable(&self.cfg, &pair, &b0, &b1, min_probe) {
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
            return;
        }
        if !stable_ok(&self.cfg, Decimal::ONE) {
            self.panel.stats.bump_skip("depeg");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "depeg"));
            return;
        }

        let reserved = self
            .positions
            .reserved_margin_by_venue(|v| self.cfg.leverage_for(v));
        let global_min = self.balance.global_min();
        let mut params = self.cfg.grid_for(base);
        let probe = resolve_qty(
            &self.cfg.sizing,
            global_min,
            self.leg_margin(&reserved, v0.as_str()),
            self.leg_margin(&reserved, v1.as_str()),
            self.positions.active_slots() as u32,
            &b0,
            &b1,
            mid,
            &pair.legs[0],
            &pair.legs[1],
        );
        if let Some(r) = probe {
            params.base_qty = r.qty;
        }

        let pos = self.positions.get(&pair.pair_id).cloned();

        let Some(mut net) = best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, params.base_qty)
        else {
            self.panel.stats.bump_skip("no_spread");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_spread"));
            return;
        };

        let (buy_leg, sell_leg, buy_book, sell_book) =
            legs_and_books(&pair, &net.buy, &net.sell, &v0, &v1, &b0, &b1);
        if let Some(r) = resolve_qty(
            &self.cfg.sizing,
            global_min,
            self.leg_margin(&reserved, buy_leg.venue.as_str()),
            self.leg_margin(&reserved, sell_leg.venue.as_str()),
            self.positions.active_slots() as u32,
            buy_book,
            sell_book,
            mid,
            buy_leg,
            sell_leg,
        ) {
            if pos.is_none() {
                params.base_qty = r.qty;
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
                    qty = %r.qty,
                    "sized by min-margin leg; same qty on both venues"
                );
                if let Some(net2) =
                    best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, r.qty)
                {
                    net = net2;
                }
            }
        } else if pos.is_none() {
            self.panel.stats.bump_skip("no_size");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_size"));
            return;
        }

        let mut natural = None;
        if let Some(store) = &self.history {
            match store.maybe_sample(
                &pair.pair_id,
                net.buy.as_str(),
                net.sell.as_str(),
                net.raw_pct,
                net.net_pct,
            ) {
                Ok(v) => natural = v,
                Err(err) => warn!(error = %err, "history sample failed"),
            }
            if natural.is_none() {
                natural = store.natural(&pair.pair_id, net.buy.as_str(), net.sell.as_str());
            }
        }

        let mut decide_net = net.clone();
        if let Some(nat) = &natural {
            decide_net.net_pct = residual_net(net.net_pct, nat.value);
        } else if self.history.is_some() && pos.is_none() {
            decide_net.net_pct = Decimal::ZERO;
        }

        let intent = self.grid.decide(
            &pair.pair_id,
            &decide_net,
            pos.as_ref(),
            &params,
            Instant::now(),
        );

        let residual = decide_net.net_pct.round_dp(6);
        let label = intent_label(&intent);
        let min_pts = self.cfg.history.min_points;
        let pts = natural.as_ref().map(|n| n.points).unwrap_or_else(|| {
            self.history
                .as_ref()
                .map(|s| s.window_points(&pair.pair_id, net.buy.as_str(), net.sell.as_str()))
                .unwrap_or(0)
        });

        if let Some(pending) = self.pending.get(&pair.pair_id).cloned() {
            if self.hedging_pairs.contains(&pair.pair_id) {
                self.record_ui_pair(&pair, &net, &params, pos.as_ref(), "hedging");
                self.set_spread(
                    pair_i,
                    dashboard::spread_lines(
                        &pair.pair_id,
                        net.buy.as_str(),
                        net.sell.as_str(),
                        net.raw_pct,
                        net.net_pct,
                        net.slip_pct,
                        natural.as_ref().map(|n| n.value),
                        residual,
                        pts,
                        min_pts,
                        "hedging",
                    ),
                );
                return;
            }
            if pending.flight == PendingFlight::PostingFirst {
                self.record_ui_pair(&pair, &net, &params, pos.as_ref(), "posting");
                self.set_spread(
                    pair_i,
                    dashboard::spread_lines(
                        &pair.pair_id,
                        net.buy.as_str(),
                        net.sell.as_str(),
                        net.raw_pct,
                        net.net_pct,
                        net.slip_pct,
                        natural.as_ref().map(|n| n.value),
                        residual,
                        pts,
                        min_pts,
                        "posting",
                    ),
                );
                return;
            }
            let same_dir = pending.plan.buy_venue == net.buy.as_str()
                && pending.plan.sell_venue == net.sell.as_str();
            let still_valid = same_dir && residual >= params.initial;
            let paper = self.cfg.execution.paper_trading;
            let first_filled;
            let hedge_qty;
            if paper {
                first_filled = pending.since.elapsed() >= Duration::from_millis(100) && still_valid;
                hedge_qty = pending.plan.qty;
            } else if let Some(ref oid) = pending.order_id {
                match HedgeExecutor::poll_first_leg(
                    &self.adapters_by_id,
                    &pending.plan.first,
                    pending.plan.qty,
                    oid,
                )
                .await
                {
                    Ok(Some(f)) => {
                        first_filled = f.qty > Decimal::ZERO;
                        hedge_qty = if f.qty > Decimal::ZERO {
                            f.qty
                        } else {
                            pending.plan.qty
                        };
                    }
                    Ok(None) => {
                        first_filled = false;
                        hedge_qty = pending.plan.qty;
                    }
                    Err(err) => {
                        warn!(pair = %pair.pair_id, error = %err, "poll first leg");
                        first_filled = false;
                        hedge_qty = pending.plan.qty;
                    }
                }
            } else {
                first_filled = false;
                hedge_qty = pending.plan.qty;
            };
            let action = watch_resting_limit(
                still_valid,
                pending.since.elapsed(),
                Duration::from_millis(self.cfg.order.limit_timeout_ms),
                first_filled,
            );
            match action {
                LimitWatch::StillWait => {
                    self.record_ui_pair(&pair, &net, &params, pos.as_ref(), "limit");
                    self.set_spread(
                        pair_i,
                        dashboard::spread_lines(
                            &pair.pair_id,
                            net.buy.as_str(),
                            net.sell.as_str(),
                            net.raw_pct,
                            net.net_pct,
                            net.slip_pct,
                            natural.as_ref().map(|n| n.value),
                            residual,
                            pts,
                            min_pts,
                            "limit",
                        ),
                    );
                    return;
                }
                LimitWatch::CancelSpreadGone => {
                    if self
                        .cancel_resting_or_hedge(&pair, pair_i, &pending, paper)
                        .await
                    {
                        return;
                    }
                    self.panel.stats.cancel_gone += 1;
                    self.log_exec(&pending.plan, "cancel", "spread_gone", "");
                    info!(
                        pair = %pair.pair_id,
                        first = %pending.plan.first.venue,
                        "cancel resting limit: spread gone"
                    );
                }
                LimitWatch::CancelTimeout => {
                    if self
                        .cancel_resting_or_hedge(&pair, pair_i, &pending, paper)
                        .await
                    {
                        return;
                    }
                    self.panel.stats.cancel_timeout += 1;
                    self.log_exec(&pending.plan, "cancel", "timeout", "");
                    info!(
                        pair = %pair.pair_id,
                        first = %pending.plan.first.venue,
                        "cancel resting limit: timeout"
                    );
                }
                LimitWatch::FilledHedgeNow => {
                    self.pending.remove(&pair.pair_id);
                    self.panel.stats.late_hedge += 1;
                    if !self.cfg.system.monitor_only {
                        if paper {
                            self.spawn_execute_plan(&pair, &pending.plan, pair_i);
                        } else {
                            self.spawn_hedge_second(&pair, &pending.plan, pair_i, hedge_qty);
                        }
                    }
                    return;
                }
            }
        }

        self.panel.stats.bump_intent(label);
        self.record_ui_pair(&pair, &net, &params, pos.as_ref(), label);
        self.set_spread(
            pair_i,
            dashboard::spread_lines(
                &pair.pair_id,
                net.buy.as_str(),
                net.sell.as_str(),
                net.raw_pct,
                net.net_pct,
                net.slip_pct,
                natural.as_ref().map(|n| n.value),
                residual,
                pts,
                min_pts,
                label,
            ),
        );

        if matches!(intent, Intent::Hold) {
            return;
        }
        if self.pending.contains_key(&pair.pair_id)
            || self.positions.is_pending(&pair.pair_id)
            || self.hedging_pairs.contains(&pair.pair_id)
        {
            return;
        }
        if matches!(intent, Intent::Open { .. }) && !self.positions.can_open(self.cfg.sizing.max_concurrent_pairs) {
            self.panel.stats.bump_skip("slots");
            return;
        }
        let Some(mut plan) = plan_hedge(&pair, &intent, pos.as_ref(), &self.cfg) else {
            return;
        };
        if self.cfg.live_test.dex_test_mode
            && !self.cfg.execution.paper_trading
            && plan.qty > self.cfg.live_test.max_qty
        {
            plan.qty = self.cfg.live_test.max_qty;
        }
        self.pending.insert(
            pair.pair_id.clone(),
            PendingLimit {
                plan: plan.clone(),
                since: Instant::now(),
                order_id: None,
                flight: PendingFlight::None,
            },
        );
        if matches!(intent, Intent::Open { .. }) {
            self.positions.reserve_open(&pair.pair_id);
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
        if self.cfg.system.monitor_only {
            self.panel.stats.skip_send += 1;
            return;
        }
        if self.cfg.execution.paper_trading {
            return;
        }
        if matches!(self.cfg.order.style, OrderStyle::LimitThenMarket) {
            if let Some(entry) = self.pending.get_mut(&pair.pair_id) {
                entry.flight = PendingFlight::PostingFirst;
            }
            self.spawn_post_first(&pair, &plan, pair_i);
        } else {
            self.pending.remove(&pair.pair_id);
            self.spawn_execute_plan(&pair, &plan, pair_i);
        }
    }

    /// 撤单前先查成交；已成交或撤单后成交则对冲。返回 true 表示已转去对冲。
    async fn cancel_resting_or_hedge(
        &mut self,
        pair: &Pair,
        pair_i: usize,
        pending: &PendingLimit,
        paper: bool,
    ) -> bool {
        if paper {
            self.pending.remove(&pair.pair_id);
            self.positions.release_pending(&pair.pair_id);
            return false;
        }
        if let Some(ref oid) = pending.order_id {
            if let Some(qty) = self
                .poll_first_fill(&pending.plan, oid)
                .await
                .filter(|q| *q > Decimal::ZERO)
            {
                self.pending.remove(&pair.pair_id);
                self.spawn_hedge_second(pair, &pending.plan, pair_i, qty);
                return true;
            }
            if let Err(err) = HedgeExecutor::cancel_resting(
                &self.adapters_by_id,
                &pending.plan.first,
                oid,
            )
            .await
            {
                warn!(pair = %pair.pair_id, error = %err, "cancel resting failed");
            }
            if let Some(qty) = self
                .poll_first_fill(&pending.plan, oid)
                .await
                .filter(|q| *q > Decimal::ZERO)
            {
                warn!(
                    pair = %pair.pair_id,
                    qty = %qty,
                    "first leg filled around cancel; hedging second leg"
                );
                self.pending.remove(&pair.pair_id);
                self.spawn_hedge_second(pair, &pending.plan, pair_i, qty);
                return true;
            }
        }
        self.pending.remove(&pair.pair_id);
        self.positions.release_pending(&pair.pair_id);
        false
    }

    async fn poll_first_fill(&self, plan: &HedgePlan, order_id: &str) -> Option<Decimal> {
        match HedgeExecutor::poll_first_leg(
            &self.adapters_by_id,
            &plan.first,
            plan.qty,
            order_id,
        )
        .await
        {
            Ok(Some(f)) if f.qty > Decimal::ZERO => Some(f.qty),
            Ok(Some(_)) | Ok(None) => None,
            Err(err) => {
                warn!(pair = %plan.pair_id, error = %err, "poll first leg before cancel");
                None
            }
        }
    }

    fn spawn_post_first(&self, pair: &Pair, plan: &HedgePlan, pair_i: usize) {
        spawn_post_first_leg(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair_i,
            plan.clone(),
        );
        let _ = pair;
    }

    fn spawn_hedge_second(
        &mut self,
        pair: &Pair,
        plan: &HedgePlan,
        pair_i: usize,
        hedge_qty: Decimal,
    ) {
        self.hedging_pairs.insert(plan.pair_id.clone());
        spawn_hedge_second_leg(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair_i,
            plan.clone(),
            hedge_qty,
        );
        let _ = pair;
    }

    fn spawn_execute_plan(&mut self, pair: &Pair, plan: &HedgePlan, pair_i: usize) {
        self.hedging_pairs.insert(plan.pair_id.clone());
        spawn_run_plan(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair_i,
            plan.clone(),
            self.cfg.execution.paper_trading,
        );
        let _ = pair;
    }

    async fn handle_exec_event(&mut self, ev: ExecEvent) {
        match ev {
            ExecEvent::PostFirst(msg) => self.on_post_first(msg).await,
            ExecEvent::HedgeSecond(msg) => self.on_hedge_second(msg).await,
            ExecEvent::RunPlan(msg) => self.on_run_plan(msg).await,
        }
    }

    async fn on_post_first(&mut self, msg: PostFirstMsg) {
        let pair_id = msg.pair_id.clone();
        match msg.result {
            Ok(post) => {
                if let Some(entry) = self.pending.get_mut(&pair_id) {
                    entry.order_id = post.order_id.clone();
                    entry.flight = PendingFlight::None;
                    if post.resting {
                        entry.since = Instant::now();
                    }
                }
                if !post.resting {
                    self.pending.remove(&pair_id);
                    let qty = if post.first.qty > Decimal::ZERO {
                        post.first.qty
                    } else {
                        msg.plan.qty
                    };
                    if let Some(pair) = self.pairs.get(msg.pair_i).cloned() {
                        self.spawn_hedge_second(&pair, &msg.plan, msg.pair_i, qty);
                    }
                }
            }
            Err(err) => {
                warn!(pair = %pair_id, error = %err, "post first leg failed");
                self.log_exec(&msg.plan, "post_fail", "error", &err);
                self.pending.remove(&pair_id);
                self.positions.release_pending(&pair_id);
            }
        }
    }

    async fn on_hedge_second(&mut self, msg: HedgeSecondMsg) {
        self.hedging_pairs.remove(&msg.pair_id);
        let Some(pair) = self.pairs.get(msg.pair_i).cloned() else {
            return;
        };
        match msg.result {
            Ok(result) => {
                info!(
                    pair = %msg.plan.pair_id,
                    first = %result.first.venue,
                    second = %result.second.venue,
                    qty = %result.first.qty,
                    "hedge second leg executed"
                );
                self.log_exec(
                    &msg.plan,
                    if msg.plan.is_open { "open" } else { "close" },
                    "both_filled",
                    "",
                );
                self.apply_fill(&pair, &msg.plan, msg.pair_i).await;
            }
            Err(err) => {
                warn!(pair = %msg.plan.pair_id, error = %err, "hedge second failed");
                self.log_exec(&msg.plan, "exec_fail", "error", &err);
                self.positions.release_pending(&msg.plan.pair_id);
            }
        }
    }

    async fn on_run_plan(&mut self, msg: RunPlanMsg) {
        self.hedging_pairs.remove(&msg.pair_id);
        let Some(pair) = self.pairs.get(msg.pair_i).cloned() else {
            return;
        };
        match msg.result {
            Ok(result) => {
                info!(
                    pair = %msg.plan.pair_id,
                    first = %result.first.venue,
                    second = %result.second.venue,
                    qty = %result.first.qty,
                    "hedge executed"
                );
                self.log_exec(
                    &msg.plan,
                    if msg.plan.is_open { "open" } else { "close" },
                    "both_filled",
                    "",
                );
                self.apply_fill(&pair, &msg.plan, msg.pair_i).await;
            }
            Err(err) => {
                warn!(pair = %msg.plan.pair_id, error = %err, "execute failed");
                self.log_exec(&msg.plan, "exec_fail", "error", &err);
                self.positions.release_pending(&msg.plan.pair_id);
            }
        }
    }

    fn log_exec(&self, plan: &HedgePlan, action: &str, result: &str, detail: &str) {
        let Some(path) = &self.cfg.live_test.journal_path else {
            return;
        };
        match ExecJournal::open(path) {
            Ok(j) => {
                if let Err(err) = j.append(&ExecRecord {
                    ts: now_ts(),
                    pair_id: plan.pair_id.clone(),
                    action: action.to_string(),
                    buy_venue: plan.buy_venue.clone(),
                    sell_venue: plan.sell_venue.clone(),
                    qty: plan.qty,
                    net_pct: None,
                    result: result.to_string(),
                    detail: detail.to_string(),
                }) {
                    warn!(
                        pair = %plan.pair_id,
                        error = %err,
                        "execution journal append failed"
                    );
                }
            }
            Err(err) => warn!(error = %err, "execution journal open failed"),
        }
    }

    async fn apply_fill(&mut self, pair: &Pair, plan: &HedgePlan, pair_i: usize) {
        let entry_notional = plan
            .qty
            * self
                .books
                .get(&(plan.buy_venue.clone(), pair.pair_id.clone()))
                .and_then(|bb| {
                    self.books
                        .get(&(plan.sell_venue.clone(), pair.pair_id.clone()))
                        .and_then(|sb| mid_from_bbo(bb, sb))
                })
                .unwrap_or(Decimal::ZERO);
        if plan.is_open {
            self.positions.record_open(
                &plan.pair_id,
                VenueId::from(plan.buy_venue.as_str()),
                VenueId::from(plan.sell_venue.as_str()),
                plan.qty,
                1,
                entry_notional,
            );
        } else {
            self.positions.record_close(&plan.pair_id, plan.qty);
        }
        let pos = self.positions.get(&pair.pair_id).cloned();
        let label = if plan.is_open { "filled_open" } else { "filled_close" };
        self.set_spread(
            pair_i,
            dashboard::spread_lines(
                &pair.pair_id,
                plan.buy_venue.as_str(),
                plan.sell_venue.as_str(),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                0,
                self.cfg.history.min_points,
                label,
            ),
        );
        let _ = pos;
    }

    fn leg_margin(
        &self,
        reserved: &HashMap<String, Decimal>,
        venue: &str,
    ) -> LegMargin {
        LegMargin {
            available_usdc: self.balance.venue_available(venue),
            leverage: self.cfg.leverage_for(venue),
            reserved_usdc: reserved.get(venue).copied().unwrap_or(Decimal::ZERO),
        }
    }

    fn record_ui_pair(
        &mut self,
        pair: &Pair,
        net: &crate::domain::NetSpread,
        params: &crate::domain::GridParams,
        pos: Option<&crate::domain::Position>,
        status: &str,
    ) {
        let actual = pos.map(|p| p.qty).unwrap_or(Decimal::ZERO);
        self.ui_pairs.insert(
            pair.pair_id.clone(),
            PairRow {
                pair_id: pair.pair_id.clone(),
                buy: net.buy.to_string(),
                sell: net.sell.to_string(),
                raw_pct: api::fmt_pct(net.raw_pct),
                net_pct: api::fmt_pct(net.net_pct),
                grid: format!("T{}", pos.map(|p| p.grid).unwrap_or(0)),
                target_qty: params.base_qty.to_string(),
                actual_qty: actual.to_string(),
                status: status.to_string(),
            },
        );
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
        hub.publish(LiveSnapshot {
            pairs: self.ui_pairs.values().cloned().collect(),
            positions,
            balances,
            exchange_positions,
            stats: api::ApiStats {
                matched_pairs: self.pairs.len(),
                open_positions: self.positions.open_count(),
                best_net_pct: best.map(api::fmt_pct),
            },
            monitor_only: self.cfg.system.monitor_only,
            paper_trading: self.cfg.execution.paper_trading,
            updated_at: now_ts(),
        });
    }
}

fn legs_and_books<'a>(
    pair: &'a Pair,
    buy: &VenueId,
    sell: &VenueId,
    v0: &VenueId,
    _v1: &VenueId,
    b0: &'a Bbo,
    b1: &'a Bbo,
) -> (&'a crate::domain::VenueMarket, &'a crate::domain::VenueMarket, &'a Bbo, &'a Bbo) {
    let buy_leg = pair
        .legs
        .iter()
        .find(|l| &l.venue == buy)
        .expect("buy leg");
    let sell_leg = pair
        .legs
        .iter()
        .find(|l| &l.venue == sell)
        .expect("sell leg");
    let buy_book = if buy == v0 { b0 } else { b1 };
    let sell_book = if sell == v0 { b0 } else { b1 };
    (buy_leg, sell_leg, buy_book, sell_book)
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
        Intent::Close { .. } => "close",
        Intent::Hold => "hold",
    }
}
