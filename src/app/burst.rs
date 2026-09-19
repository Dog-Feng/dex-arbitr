//! 阶段 2 Burst：L1 限价循环 + 市价对冲（与阶段 1 STEP 完全分离）。

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use rust_decimal::Decimal;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::domain::{Bbo, Pair, VenueId};
use crate::exec::{best_sequenced_spread, plan_burst, LimitMarketRun};
use crate::infra::dashboard;

use super::controller::PendingLimit;
use super::controller::Controller;
use super::exec_worker::spawn_limit_market;
use super::intervention::Gate;
use super::risk::{books_quality_ok, books_tradable};
use super::sizing::{check_capacity, mid_from_bbo, LegMargin};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BurstPhase {
    Opening,
    RepPause,
    Cooldown,
    Closing,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PauseKind {
    AfterOpenRep,
    AfterCloseRep,
}

#[derive(Debug, Clone)]
pub struct BurstSlot {
    pub phase: BurstPhase,
    pub open_reps_done: u32,
    pub wait_until: Option<Instant>,
    pub peak_capacity_checked: bool,
    pause_kind: Option<PauseKind>,
}

impl Default for BurstSlot {
    fn default() -> Self {
        Self {
            phase: BurstPhase::Opening,
            open_reps_done: 0,
            wait_until: None,
            peak_capacity_checked: false,
            pause_kind: None,
        }
    }
}

impl Controller {
    pub(super) fn process_pair_burst(&mut self, pair_i: usize) {
        self.sync_page_config();
        self.sync_enabled_edge();
        let pair = self.pairs[pair_i].clone();
        let slot = pair.slot_key();
        let v0 = pair.legs[0].venue.clone();
        let v1 = pair.legs[1].venue.clone();

        if !self.arbitrage_enabled() && !self.slot_is_live(&slot) {
            self.ui_pairs.remove(&slot);
            return;
        }

        if self.slot_has_pending(&slot) || self.hedging.contains(&slot) {
            self.paint_burst_status(pair_i, &pair, &slot, "执行中");
            return;
        }

        let mut st = self.burst_slots.entry(slot.clone()).or_default().clone();

        if st.phase == BurstPhase::Stopped {
            self.paint_burst_status(pair_i, &pair, &slot, "已停止(需人工)");
            return;
        }

        if let Some(until) = st.wait_until {
            if Instant::now() < until {
                let left = until.duration_since(Instant::now()).as_secs();
                let label = match st.phase {
                    BurstPhase::RepPause => format!("rep 暂停 {left}s"),
                    BurstPhase::Cooldown => format!("冷却 {left}s"),
                    _ => format!("等待 {left}s"),
                };
                self.paint_burst_status(pair_i, &pair, &slot, &label);
                return;
            }
            st.wait_until = None;
            match st.phase {
                BurstPhase::RepPause => {
                    st.phase = match st.pause_kind {
                        Some(PauseKind::AfterOpenRep) => {
                            if st.open_reps_done >= self.cfg.burst.open_repeats {
                                BurstPhase::Cooldown
                            } else {
                                BurstPhase::Opening
                            }
                        }
                        Some(PauseKind::AfterCloseRep) => BurstPhase::Closing,
                        None => BurstPhase::Opening,
                    };
                    if st.phase == BurstPhase::Cooldown {
                        st.wait_until =
                            Some(Instant::now() + burst_duration(&self.cfg, false));
                    }
                    st.pause_kind = None;
                }
                BurstPhase::Cooldown => {
                    let held = self
                        .positions
                        .get(&slot)
                        .map(|p| p.qty)
                        .unwrap_or(Decimal::ZERO);
                    if held > Decimal::ZERO {
                        st.phase = BurstPhase::Closing;
                    } else {
                        st.phase = BurstPhase::Opening;
                        st.open_reps_done = 0;
                        st.peak_capacity_checked = false;
                    }
                }
                _ => {}
            }
            self.burst_slots.insert(slot.clone(), st);
        }

        let st = self.burst_slots.get(&slot).cloned().unwrap_or_default();
        if st.phase == BurstPhase::Stopped {
            return;
        }
        if !self.arbitrage_enabled() {
            self.paint_burst_status(pair_i, &pair, &slot, "未启动");
            return;
        }
        if !self.pair_keys_ready(&pair) {
            self.paint_burst_status(pair_i, &pair, &slot, "缺密钥");
            return;
        }

        let Some(b0) = self.book(v0.as_str(), &pair.pair_id) else {
            self.paint_burst_status(pair_i, &pair, &slot, "缺盘口");
            return;
        };
        let Some(b1) = self.book(v1.as_str(), &pair.pair_id) else {
            self.paint_burst_status(pair_i, &pair, &slot, "缺盘口");
            return;
        };
        if books_quality_ok(&self.cfg, &b0, &b1).is_err() {
            self.paint_burst_status(pair_i, &pair, &slot, "盘口过期");
            return;
        }

        let base = pair.legs[0].base.clone();
        let Some(setting) = self.cfg.pair_setting(&base) else {
            self.paint_burst_status(pair_i, &pair, &slot, "未配置");
            return;
        };
        let base_qty = setting.base_qty;
        if base_qty <= Decimal::ZERO {
            self.paint_burst_status(pair_i, &pair, &slot, "无 base_qty");
            return;
        }

        let held = self
            .positions
            .get(&slot)
            .map(|p| p.qty)
            .unwrap_or(Decimal::ZERO);
        let st = self.burst_slots.get(&slot).cloned().unwrap_or_default();

        match st.phase {
            BurstPhase::Opening => {
                if held <= Decimal::ZERO {
                    if self.pair_has_naked(&pair.pair_id) || self.pair_naked_inflight(&pair.pair_id)
                    {
                        self.paint_burst_status(pair_i, &pair, &slot, "单边敞口");
                        return;
                    }
                    if let Some(lp) = self.live_params() {
                        if lp.active_venues.len() < 2
                            || !lp.active_venues.iter().any(|v| v == v0.as_str())
                            || !lp.active_venues.iter().any(|v| v == v1.as_str())
                        {
                            self.paint_burst_status(pair_i, &pair, &slot, "未选所");
                            return;
                        }
                    }
                }
                if st.open_reps_done >= self.cfg.burst.open_repeats {
                    let mut st = st;
                    st.phase = BurstPhase::Cooldown;
                    st.wait_until =
                        Some(Instant::now() + burst_duration(&self.cfg, false));
                    self.burst_slots.insert(slot.clone(), st);
                    self.paint_burst_status(pair_i, &pair, &slot, "开满→冷却");
                    return;
                }
                if !st.peak_capacity_checked {
                    let peak = base_qty * Decimal::from(self.cfg.burst.open_repeats);
                    let mid = mid_from_bbo(&b0, &b1).unwrap_or(Decimal::ZERO);
                    let reserved = self.positions.reserved_margin_by_venue(
                        |v| self.cfg.leverage_for(v),
                        |p| self.position_mid(p),
                    );
                    if let Some(net) =
                        best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, peak)
                    {
                        let (bb, sb) = super::controller::books_for_direction(
                            &net.buy, &v0, &b0, &b1,
                        );
                        if check_capacity(
                            &self.cfg.sizing,
                            peak,
                            burst_leg_margin(self, &reserved, net.buy.as_str()),
                            burst_leg_margin(self, &reserved, net.sell.as_str()),
                            bb,
                            sb,
                            mid,
                        )
                        .is_err()
                        {
                            self.paint_burst_status(pair_i, &pair, &slot, "容量不足");
                            return;
                        }
                    }
                    let mut st = st;
                    st.peak_capacity_checked = true;
                    self.burst_slots.insert(slot.clone(), st);
                }
                let Some(net) =
                    best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, base_qty)
                else {
                    self.paint_burst_status(pair_i, &pair, &slot, "无价差");
                    return;
                };
                if books_tradable(&self.cfg, &pair, &b0, &b1, base_qty).is_err() {
                    self.paint_burst_status(pair_i, &pair, &slot, "盘口过薄");
                    return;
                }
                let (buy_book, sell_book) = super::controller::books_for_direction(
                    &net.buy, &v0, &b0, &b1,
                );
                self.spawn_burst_rep(
                    pair_i,
                    &pair,
                    &slot,
                    true,
                    &net.buy,
                    &net.sell,
                    buy_book,
                    sell_book,
                    base_qty,
                );
            }
            BurstPhase::Closing => {
                if held <= Decimal::ZERO {
                    let mut st = st;
                    st.phase = BurstPhase::Cooldown;
                    st.wait_until =
                        Some(Instant::now() + burst_duration(&self.cfg, false));
                    self.burst_slots.insert(slot.clone(), st);
                    self.paint_burst_status(pair_i, &pair, &slot, "平完→冷却");
                    return;
                }
                let (buy, sell) = {
                    let p = self.positions.get(&slot).unwrap();
                    (p.buy.clone(), p.sell.clone())
                };
                let close_qty = base_qty.min(held);
                let (buy_book, sell_book) =
                    super::controller::books_for_direction(&buy, &v0, &b0, &b1);
                self.spawn_burst_rep(
                    pair_i,
                    &pair,
                    &slot,
                    false,
                    &buy,
                    &sell,
                    buy_book,
                    sell_book,
                    close_qty,
                );
            }
            BurstPhase::RepPause | BurstPhase::Cooldown | BurstPhase::Stopped => {}
        }
    }

    fn spawn_burst_rep(
        &mut self,
        pair_i: usize,
        pair: &Pair,
        slot: &str,
        is_open: bool,
        buy: &VenueId,
        sell: &VenueId,
        buy_book: &Bbo,
        sell_book: &Bbo,
        qty: Decimal,
    ) {
        if self.execution_in_flight() {
            return;
        }
        let cur_grid = self.positions.get(slot).map(|p| p.grid);
        if matches!(
            self.intervention
                .should_block(&pair.pair_id, cur_grid, Instant::now()),
            Gate::Block { .. }
        ) {
            self.paint_burst_status(pair_i, pair, slot, "人工介入");
            return;
        }

        let Some(mut plan) =
            plan_burst(pair, &self.cfg, qty, is_open, buy, sell, buy_book, sell_book)
        else {
            return;
        };
        plan.base_qty = qty;
        plan.grid_from = self.positions.get(slot).map(|p| p.grid).unwrap_or(0);
        plan.grid_to = if is_open {
            plan.grid_from + 1
        } else {
            (plan.grid_from - 1).max(0)
        };

        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.pending.insert(
            slot.to_string(),
            PendingLimit {
                plan: plan.clone(),
                since: Instant::now(),
                cancel: cancel.clone(),
            },
        );
        if is_open {
            self.positions.reserve_open(slot);
        }
        self.hedging.insert(slot.to_string());
        info!(
            pair = %pair.pair_id,
            open = is_open,
            qty = %qty,
            first = %plan.first.venue,
            px = ?plan.first.limit_price,
            "burst: spawn limit+market rep"
        );
        spawn_limit_market(
            self.exec_tx.clone(),
            self.cfg.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair_i,
            plan,
            LimitMarketRun {
                baseline: Decimal::ZERO,
                min_qty: pair.min_qty().max(Decimal::new(1, 8)),
                cancel,
                orders_live: Arc::clone(&self.orders_live),
            },
        );
    }

    pub(super) fn on_burst_run_plan(&mut self, msg: super::exec_worker::RunPlanMsg) {
        self.hedging.remove(&msg.slot);
        self.pending.remove(&msg.slot);
        if !self.arbitrage_enabled() {
            self.positions.release_pending(&msg.slot);
            info!(
                pair = %msg.plan.pair_id,
                "burst exec finished after arbitrage stopped; not updating memory"
            );
            return;
        }
        let pair_i = msg.pair_i;
        let slot = msg.slot.clone();
        let Some(pair) = self.pairs.get(pair_i).cloned() else {
            self.positions.release_pending(&slot);
            return;
        };

        match msg.result {
            Ok(result) => {
                let hedged = result.hedged_qty();
                if hedged <= Decimal::ZERO {
                    warn!(pair = %msg.plan.pair_id, "burst exec zero hedged");
                    self.positions.release_pending(&slot);
                    return;
                }
                self.apply_fill(&pair, &msg.plan, &result, pair_i);
                self.intervention.clear_streak(&msg.plan.pair_id);
                let mut st = self.burst_slots.get(&slot).cloned().unwrap_or_default();
                if msg.plan.is_open {
                    st.open_reps_done += 1;
                    st.pause_kind = Some(PauseKind::AfterOpenRep);
                } else {
                    st.pause_kind = Some(PauseKind::AfterCloseRep);
                }
                st.phase = BurstPhase::RepPause;
                st.wait_until = Some(Instant::now() + burst_duration(&self.cfg, true));
                self.burst_slots.insert(slot.clone(), st);
                info!(
                    pair = %msg.plan.pair_id,
                    open = msg.plan.is_open,
                    hedged = %hedged,
                    "burst rep completed"
                );
            }
            Err(err) => {
                error!(pair = %msg.plan.pair_id, error = %err, "burst rep failed");
                self.positions.release_pending(&slot);
                if err.contains("BURST_HEDGE_FAILED") || err.contains("BURST_ZERO_FILL") {
                    self.burst_stop(&pair, &slot, &err);
                }
            }
        }
    }

    fn burst_stop(&mut self, pair: &Pair, slot: &str, detail: &str) {
        self.orders_live.store(false, Ordering::Release);
        let mut st = self.burst_slots.get(slot).cloned().unwrap_or_default();
        st.phase = BurstPhase::Stopped;
        st.wait_until = None;
        self.burst_slots.insert(slot.to_string(), st);
        self.mark_intervention_for(
            &pair.pair_id,
            slot,
            super::intervention::Cause::NakedLegUnrecoverable,
            format!("burst stopped: {detail}"),
        );
        warn!(pair = %pair.pair_id, "burst: orders_live=false; manual intervention required");
    }

    fn paint_burst_status(&mut self, pair_i: usize, pair: &Pair, slot: &str, status: &str) {
        let st = self.burst_slots.get(slot).cloned().unwrap_or_default();
        let extra = match st.phase {
            BurstPhase::Opening => {
                format!("开 {}/{}", st.open_reps_done, self.cfg.burst.open_repeats)
            }
            BurstPhase::Closing => "平仓中".into(),
            BurstPhase::Stopped => "停止".into(),
            _ => String::new(),
        };
        let ui = if extra.is_empty() {
            status.to_string()
        } else {
            format!("{status} · {extra}")
        };
        self.mark_ui_status(slot, &ui);
        self.set_spread(pair_i, dashboard::skip_lines(&pair.pair_id, &ui));
    }
}

fn burst_leg_margin(
    ctrl: &Controller,
    reserved: &HashMap<String, Decimal>,
    venue: &str,
) -> LegMargin {
    LegMargin {
        available_usdc: ctrl.balance.venue_available(venue),
        leverage: ctrl.cfg.leverage_for(venue),
        reserved_usdc: reserved.get(venue).copied().unwrap_or(Decimal::ZERO),
    }
}

fn burst_duration(cfg: &AppConfig, pause: bool) -> std::time::Duration {
    let ms = if pause {
        cfg.burst.random_pause_ms()
    } else {
        cfg.burst.random_cooldown_ms()
    };
    std::time::Duration::from_millis(ms)
}
