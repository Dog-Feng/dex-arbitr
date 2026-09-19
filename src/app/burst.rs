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
use super::exec_worker::spawn_burst_post_close_exchange_flatten;
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
    /// 配置的大循环次数已跑满（开+平算一轮），正常结束，非故障停手。
    Completed,
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
    /// 本宏观周期锁定的开仓次数目标（与 `total_rounds` 无关；周期开始时快照，避免热改提前平仓）。
    pub open_repeats_goal: u32,
    /// 已完成的大循环数（每轮 = open_repeats 开满 + 平到 0）。
    pub rounds_completed: u32,
    pub wait_until: Option<Instant>,
    pub peak_capacity_checked: bool,
    pause_kind: Option<PauseKind>,
}

impl Default for BurstSlot {
    fn default() -> Self {
        Self {
            phase: BurstPhase::Opening,
            open_reps_done: 0,
            open_repeats_goal: 0,
            rounds_completed: 0,
            wait_until: None,
            peak_capacity_checked: false,
            pause_kind: None,
        }
    }
}

fn burst_open_repeats_goal(st: &BurstSlot, burst: &crate::config::BurstConfig) -> u32 {
    if st.open_repeats_goal > 0 {
        st.open_repeats_goal
    } else {
        burst.open_repeats
    }
}

fn burst_status_suffix(burst: &crate::config::BurstConfig, st: &BurstSlot) -> String {
    let open_goal = burst_open_repeats_goal(st, burst);
    let mut parts = vec![format!("小循环 {}/{}", st.open_reps_done, open_goal)];
    let macro_total = burst.total_rounds;
    if macro_total > 0 {
        parts.push(format!(
            "大循环 {}/{}",
            st.rounds_completed.min(macro_total),
            macro_total
        ));
    } else if st.rounds_completed > 0 {
        parts.push(format!("大循环已完成 {}", st.rounds_completed));
    }
    parts.join(" · ")
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
            // 与阶段 1 一致：挂单期间仍跑 watchdog（Burst 原先直接 return，超时永不 cancel）。
            self.watch_pending_slot(pair_i, &pair, &slot);
            self.paint_burst_status(pair_i, &pair, &slot, "执行中");
            return;
        }

        let mut st = self.burst_slots.entry(slot.clone()).or_default().clone();

        if st.phase == BurstPhase::Stopped {
            self.paint_burst_status(pair_i, &pair, &slot, "已停止(需人工)");
            return;
        }
        if st.phase == BurstPhase::Completed {
            self.paint_burst_status(pair_i, &pair, &slot, "大循环已完成");
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
                            let goal = burst_open_repeats_goal(&st, &self.cfg.burst);
                            if st.open_reps_done >= goal {
                                info!(
                                    pair = %pair.pair_id,
                                    open_reps_done = st.open_reps_done,
                                    open_repeats_goal = goal,
                                    cfg_open_repeats = self.cfg.burst.open_repeats,
                                    total_rounds = self.cfg.burst.total_rounds,
                                    "burst: 小循环开满 → 冷却（与 total_rounds 无关）"
                                );
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
                    } else if !self.cfg.burst.should_run_another_round(st.rounds_completed) {
                        st.phase = BurstPhase::Completed;
                        info!(
                            pair = %pair.pair_id,
                            rounds = st.rounds_completed,
                            total = self.cfg.burst.total_rounds,
                            "burst: all macro rounds completed"
                        );
                    } else {
                        st.phase = BurstPhase::Opening;
                        st.open_reps_done = 0;
                        st.open_repeats_goal = self.cfg.burst.open_repeats.max(1);
                        st.peak_capacity_checked = false;
                    }
                }
                _ => {}
            }
            self.burst_slots.insert(slot.clone(), st);
        }

        let st = self.burst_slots.get(&slot).cloned().unwrap_or_default();
        if matches!(st.phase, BurstPhase::Stopped | BurstPhase::Completed) {
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
                if st.open_repeats_goal == 0 {
                    let mut st = st;
                    st.open_repeats_goal = self.cfg.burst.open_repeats.max(1);
                    self.burst_slots.insert(slot.clone(), st);
                }
                let st = self.burst_slots.get(&slot).cloned().unwrap_or_default();
                let open_goal = burst_open_repeats_goal(&st, &self.cfg.burst);
                if !self.cfg.burst.should_run_another_round(st.rounds_completed) {
                    let mut st = st;
                    st.phase = BurstPhase::Completed;
                    self.burst_slots.insert(slot.clone(), st);
                    self.paint_burst_status(pair_i, &pair, &slot, "大循环已完成");
                    return;
                }
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
                if st.open_reps_done >= open_goal {
                    let mut st = st;
                    info!(
                        pair = %pair.pair_id,
                        open_reps_done = st.open_reps_done,
                        open_repeats_goal = open_goal,
                        total_rounds = self.cfg.burst.total_rounds,
                        "burst: 小循环开满 → 冷却"
                    );
                    st.phase = BurstPhase::Cooldown;
                    st.wait_until =
                        Some(Instant::now() + burst_duration(&self.cfg, false));
                    self.burst_slots.insert(slot.clone(), st);
                    self.paint_burst_status(pair_i, &pair, &slot, "开满→冷却");
                    return;
                }
                if !st.peak_capacity_checked {
                    let peak = base_qty * Decimal::from(open_goal);
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
                // 已有对锁仓时方向必须跟内存 buy/sell 一致；否则下一 rep 可能 flip
                // best_sequenced_spread，在 buy 腿挂 sell = 所上「平多」但内存仍记 open。
                let (open_buy, open_sell, buy_book, sell_book) = if held > Decimal::ZERO {
                    let p = self.positions.get(&slot).unwrap();
                    let (bb, sb) = super::controller::books_for_direction(
                        &p.buy, &v0, &b0, &b1,
                    );
                    (p.buy.clone(), p.sell.clone(), bb, sb)
                } else {
                    let Some(net) =
                        best_sequenced_spread(&self.cfg, &v0, &v1, &b0, &b1, base_qty)
                    else {
                        self.paint_burst_status(pair_i, &pair, &slot, "无价差");
                        return;
                    };
                    let (bb, sb) = super::controller::books_for_direction(
                        &net.buy, &v0, &b0, &b1,
                    );
                    (net.buy.clone(), net.sell.clone(), bb, sb)
                };
                if books_tradable(&self.cfg, &pair, &b0, &b1, base_qty).is_err() {
                    self.paint_burst_status(pair_i, &pair, &slot, "盘口过薄");
                    return;
                }
                if held > Decimal::ZERO {
                    info!(
                        pair = %pair.pair_id,
                        buy = %open_buy,
                        sell = %open_sell,
                        "burst open: direction locked to memory (ignore spread flip)"
                    );
                }
                self.spawn_burst_rep(
                    pair_i,
                    &pair,
                    &slot,
                    true,
                    &open_buy,
                    &open_sell,
                    buy_book,
                    sell_book,
                    base_qty,
                );
            }
            BurstPhase::Closing => {
                if held <= Decimal::ZERO {
                    let mut st = st;
                    st.rounds_completed = st.rounds_completed.saturating_add(1);
                    info!(
                        pair = %pair.pair_id,
                        round = st.rounds_completed,
                        total = self.cfg.burst.total_rounds,
                        "burst macro round completed (open+close)"
                    );
                    st.phase = BurstPhase::Cooldown;
                    st.wait_until =
                        Some(Instant::now() + burst_duration(&self.cfg, false));
                    self.burst_slots.insert(slot.clone(), st);
                    self.paint_burst_status(pair_i, &pair, &slot, "平完→冷却");
                    return;
                }
                // 平仓方向与 plan_hedge(Intent::Close) 一致：在原 sell 所买回、原 buy 所卖出。
                let (close_buy, close_sell) = {
                    let p = self.positions.get(&slot).unwrap();
                    (p.sell.clone(), p.buy.clone())
                };
                let close_qty = base_qty.min(held);
                let (buy_book, sell_book) = super::controller::books_for_direction(
                    &close_buy,
                    &v0,
                    &b0,
                    &b1,
                );
                self.spawn_burst_rep(
                    pair_i,
                    &pair,
                    &slot,
                    false,
                    &close_buy,
                    &close_sell,
                    buy_book,
                    sell_book,
                    close_qty,
                );
            }
            BurstPhase::RepPause
            | BurstPhase::Cooldown
            | BurstPhase::Stopped
            | BurstPhase::Completed => {}
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
                if !msg.plan.is_open {
                    let held = self
                        .positions
                        .get(&slot)
                        .map(|p| p.qty)
                        .unwrap_or(Decimal::ZERO);
                    if held <= Decimal::ZERO {
                        self.burst_flatten_exchange_after_close(&pair);
                    }
                }
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

    /// 小循环全部平完（内存 qty=0）后：暂停 3–5s 再拉所上持仓，仍有尾差则市价 reduce-only。
    /// 单笔 close rep 后内存仍有余仓时不得 flatten，否则会按所上全仓市价平掉。
    fn burst_flatten_exchange_after_close(&mut self, pair: &Pair) {
        let settle_ms = self.cfg.burst.random_flatten_settle_ms();
        spawn_burst_post_close_exchange_flatten(
            self.cfg.clone(),
            self.adapters.clone(),
            self.adapters_by_id.clone(),
            self.books.clone(),
            pair.clone(),
            settle_ms,
        );
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
            BurstPhase::Closing => "平仓中".into(),
            BurstPhase::Stopped => "停止".into(),
            BurstPhase::Completed => "结束".into(),
            _ => String::new(),
        };
        let round_tag = burst_status_suffix(&self.cfg.burst, &st);
        let ui = match (extra.is_empty(), round_tag.is_empty()) {
            (true, true) => status.to_string(),
            (false, true) => format!("{status} · {extra}"),
            (true, false) => format!("{status} · {round_tag}"),
            (false, false) => format!("{status} · {extra} · {round_tag}"),
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

#[cfg(test)]
mod round_label_tests {
    use super::*;
    use crate::config::BurstConfig;

    #[test]
    fn status_distinguishes_small_and_macro_cycles() {
        let burst = BurstConfig {
            total_rounds: 3,
            enabled: true,
            open_repeats: 10,
            pause_ms_min: 3000,
            pause_ms_max: 10000,
            cooldown_ms_min: 60_000,
            cooldown_ms_max: 300_000,
            limit_rehang_timeout_ms: 2000,
            hedge_max_attempts: 20,
        };
        let st = BurstSlot {
            open_reps_done: 3,
            open_repeats_goal: 10,
            rounds_completed: 0,
            phase: BurstPhase::Opening,
            ..BurstSlot::default()
        };
        assert_eq!(
            burst_status_suffix(&burst, &st),
            "小循环 3/10 · 大循环 0/3"
        );
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
