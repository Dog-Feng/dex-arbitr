use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::domain::{
    is_cross_dex, match_all_pairs, Bbo, GridEngine, Intent, Pair, Position, VenueId, VenueMarket,
};
use crate::domain::spread::raw_spread_pct;
use crate::exec::{plan_hedge, watch_resting_limit, LimitWatch, best_sequenced_spread, HedgePlan};
use crate::exchange::{make_adapter, ExchangePort};
use crate::infra::dashboard::{self, LivePanel};
use crate::infra::history::{residual_net, HistoryStore};

use super::risk::{books_tradable, stable_ok};
use super::scan::OpportunityTracker;

pub struct Controller {
    cfg: AppConfig,
    adapters: Vec<Arc<dyn ExchangePort>>,
    pairs: Vec<Pair>,
    books: HashMap<(String, String), Bbo>,
    positions: HashMap<String, Position>,
    grid: GridEngine,
    event_rx: Option<mpsc::UnboundedReceiver<(VenueId, String, Bbo)>>,
    history: Option<HistoryStore>,
    panel: LivePanel,
    pending: HashMap<String, PendingLimit>,
    scanner: OpportunityTracker,
    last_token_log: HashMap<String, LoggedToken>,
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
}

impl Controller {
    pub async fn run(cfg: AppConfig) -> Result<()> {
        let mut adapters: Vec<Arc<dyn ExchangePort>> = Vec::new();
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
            adapters.push(make_adapter(venue, cfg.pairs.whitelist.clone()));
        }
        let history = if cfg.history.enabled {
            let store = HistoryStore::open(cfg.history.clone())?;
            info!(path = %cfg.history.db_path, "spread history sqlite ready");
            Some(store)
        } else {
            None
        };
        let mut this = Self {
            cfg,
            adapters,
            pairs: Vec::new(),
            books: HashMap::new(),
            positions: HashMap::new(),
            grid: GridEngine::default(),
            event_rx: None,
            history,
            panel: LivePanel::new(0),
            pending: HashMap::new(),
            scanner: OpportunityTracker::default(),
            last_token_log: HashMap::new(),
        };
        this.bootstrap().await?;
        if this.cfg.scan.enabled {
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
        if self.cfg.scan.enabled {
            self.panel = LivePanel::new(0);
        } else {
            self.panel = LivePanel::new(self.pairs.len() * self.pair_stride());
        }
        self.panel.scan_mode = self.cfg.scan.enabled;
        Ok(())
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
            self.cfg.history.min_points,
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
            for (buy, sell, bb, sb) in [
                (v0, v1, b0, b1),
                (v1, v0, b1, b0),
            ] {
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
                let pair = self.pairs[pi].clone();
                self.on_pair(&pair, pi);
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

    fn on_pair(&mut self, pair: &Pair, pair_i: usize) {
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
        if let Err(reason) = books_tradable(&self.cfg, pair, b0, b1) {
            self.panel.stats.bump_skip(reason);
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, reason));
            return;
        }
        if !stable_ok(&self.cfg, Decimal::ONE) {
            self.panel.stats.bump_skip("depeg");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "depeg"));
            return;
        }

        let pos = self.positions.get(&pair.pair_id).cloned();
        let params = self.cfg.grid_for(&pair.legs[0].base);
        let Some(net) = best_sequenced_spread(
            &self.cfg,
            &v0,
            &v1,
            b0,
            b1,
            params.base_qty,
        ) else {
            self.panel.stats.bump_skip("no_spread");
            self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, "no_spread"));
            return;
        };

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
            let same_dir =
                pending.plan.buy_venue == net.buy.as_str() && pending.plan.sell_venue == net.sell.as_str();
            let still_valid = same_dir && residual >= params.initial;
            let action = watch_resting_limit(
                still_valid,
                pending.since.elapsed(),
                Duration::from_millis(self.cfg.order.limit_timeout_ms),
                false,
            );
            match action {
                LimitWatch::StillWait => {
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
                    self.pending.remove(&pair.pair_id);
                    self.panel.stats.cancel_gone += 1;
                    info!(
                        pair = %pair.pair_id,
                        first = %pending.plan.first.venue,
                        "cancel resting limit: spread gone; if already filled, market-hedge other leg"
                    );
                }
                LimitWatch::CancelTimeout => {
                    self.pending.remove(&pair.pair_id);
                    self.panel.stats.cancel_timeout += 1;
                    info!(
                        pair = %pair.pair_id,
                        first = %pending.plan.first.venue,
                        "cancel resting limit: timeout; if already filled, market-hedge other leg"
                    );
                }
                LimitWatch::FilledHedgeNow => {
                    self.pending.remove(&pair.pair_id);
                    self.panel.stats.late_hedge += 1;
                    info!(
                        pair = %pair.pair_id,
                        second = %pending.plan.second.venue,
                        "first leg filled; market hedge second even if spread is gone"
                    );
                }
            }
        }

        self.panel.stats.bump_intent(label);
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
        if self.pending.contains_key(&pair.pair_id) {
            return;
        }
        let Some(plan) = plan_hedge(pair, &intent, pos.as_ref(), &self.cfg) else {
            return;
        };
        self.pending.insert(
            pair.pair_id.clone(),
            PendingLimit {
                plan: plan.clone(),
                since: Instant::now(),
            },
        );
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            first_style = plan.first.style.as_str(),
            first_buy = plan.first.is_buy,
            second = %plan.second.venue,
            second_style = plan.second.style.as_str(),
            qty = %plan.qty,
            "limit-then-market: post high-fee venue first"
        );
        if self.cfg.system.monitor_only {
            self.panel.stats.skip_send += 1;
            return;
        }
        warn!("live send is disabled until signer is wired");
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
        Intent::Close { .. } => "close",
        Intent::Hold => "hold",
    }
}
