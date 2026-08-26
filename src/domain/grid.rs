use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::{NetSpread, Position, VenueId};

#[derive(Debug, Clone)]
pub struct GridParams {
    /// T1：第一格开仓阈值（%）。第 n 格 = T1 + (n−1) × step。
    pub initial: Decimal,
    pub step: Decimal,
    pub max_segments: u32,
    pub persistence: Duration,
    /// 单格数量。目标持仓 = 格数 × base_qty。
    pub base_qty: Decimal,
    /// 保留字段：参考统一引擎没有往返净利止损，热路径不再读它。
    pub close_stop_loss: Decimal,
    /// T0 = T1 × 该系数，第一格的平仓阈值。对齐参考 `_build_grid_thresholds`。
    pub t0_ratio: Decimal,
    /// 单笔最大开/平仓量（拆单）。0 = 不拆，一次下完该加或该减的量。
    pub split_order_size: Decimal,
    /// 两腿 min_qty 的较大者。拆单不能切出低于它的尾巴，
    /// 否则最后一笔必被执行层的 `limit_min_qty_unmet` 拒掉，尾仓永远平不掉。
    pub min_qty: Decimal,
    /// 对齐参考 `scalping_enabled`。默认关。
    pub scalping_enabled: bool,
    /// 开仓视角格子 ≥ 该值时进入剥头皮。格子按 T1/step **不封顶**，
    /// 所以触发格可以大于 `max_segments`（参考默认 10）。
    pub scalping_trigger_segment: u32,
    /// 剥头皮止盈：建仓均毛价差 − 当前剩余毛价差 ≥ 该值才按目标格减仓。
    pub scalping_profit_threshold: Decimal,
}

impl GridParams {
    /// 开仓阈值序列：`[T1, T1+step, T1+2*step, ...]`，长度 = max_segments。
    /// 对齐参考 `_build_grid_thresholds`。
    pub fn open_thresholds(&self) -> Vec<Decimal> {
        if self.initial <= Decimal::ZERO || self.step < Decimal::ZERO || self.max_segments == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.max_segments as usize);
        let mut cur = self.initial;
        for _ in 0..self.max_segments {
            out.push(cur);
            cur += self.step;
        }
        out
    }

    /// 平仓阈值序列：`[T0, T1, T2, ...]`，即第 n 格回落到 T(n−1) 以下才减仓。
    /// T0 = T1 × t0_ratio。对齐参考 `close_thresholds = [t0] + open[:-1]`。
    pub fn close_thresholds(&self) -> Vec<Decimal> {
        let open = self.open_thresholds();
        if open.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(open.len());
        out.push(self.initial * self.t0_ratio);
        out.extend_from_slice(&open[..open.len() - 1]);
        out
    }
}

/// 本次开/平仓下单量。对齐参考 `_calculate_order_quantity` 的拆单部分：
/// 单笔不超过 `split_order_size`，剩余留给后续 tick 重新决策。
///
/// 开仓缺口不另记账：每轮用 `目标格 − 已持格` 重算，吃不满或价差再涨
/// 到更高格，下一轮只要 raw 还够就会继续补。
///
/// 与参考的一处**刻意分歧**：参考对平仓不做 min_qty 检查（开仓才有
/// `pending_open_shortfall` 累积机制），因为它的执行层容得下小单。本项目
/// `hedge_second_leg` 有硬门槛，低于 `min_qty` 的单会被拒并触发紧急平仓，
/// 所以尾巴不能留——切完若剩余不足 min_qty，就把尾巴并进这一笔。
/// 否则 delta=0.0025 / split=0.001 / min=0.0006 会切出 0.001+0.001+0.0005，
/// 最后 0.0005 永远下不出去。
fn split_order_qty(delta: Decimal, params: &GridParams) -> Decimal {
    if delta <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    if params.split_order_size <= Decimal::ZERO {
        return delta;
    }
    let qty = delta.min(params.split_order_size);
    let tail = delta - qty;
    // 尾巴既非 0 又下不出去 → 本笔直接吃掉整个 delta。
    if tail > Decimal::ZERO && tail < params.min_qty {
        return delta;
    }
    qty
}

/// 阈值列表里满足 `value >= thresholds[i]` 的最高格数。
/// 对齐参考 `_count_segments_by_threshold`。
fn count_segments(value: Decimal, thresholds: &[Decimal]) -> u32 {
    for (idx, th) in thresholds.iter().enumerate().rev() {
        if value >= *th {
            return idx as u32 + 1;
        }
    }
    0
}

/// 开仓视角当前格子，**不封顶**。对齐参考 `_calculate_current_grid`
/// （`int((spread-T1)/step)+1`）。剥头皮触发格可以大于 `max_segments`。
fn open_grid_level(spread: Decimal, params: &GridParams) -> u32 {
    if params.initial <= Decimal::ZERO || spread < params.initial {
        return 0;
    }
    if params.step <= Decimal::ZERO {
        return 1;
    }
    let n = ((spread - params.initial) / params.step).floor() + Decimal::ONE;
    u32::try_from(n.trunc().mantissa().max(0)).unwrap_or(u32::MAX)
}

/// 按当前价差算目标持仓格数。对齐参考 `_calculate_target_position_by_spread`。
///
/// 滞后逻辑：价差涨到 Tn 就加仓到 n 格；但回落时要跌破 T(n−1) 才减一格，
/// 于是「开一格 → 回落一格才平」，避免在阈值附近来回开平。
pub fn target_segments(relative_spread: Decimal, current_segments: u32, params: &GridParams) -> u32 {
    let open_th = params.open_thresholds();
    if open_th.is_empty() {
        return 0;
    }
    let close_th = params.close_thresholds();

    // 价差允许开到的最高格
    let open_segments = count_segments(relative_spread, &open_th);
    // 价差允许继续持有的格数（按 T(n−1) 判），不能超过已持有格数
    let keep_segments = count_segments(relative_spread, &close_th).min(current_segments);

    let target = if open_segments > current_segments {
        open_segments
    } else {
        keep_segments
    };
    target.min(params.max_segments)
}

/// 平仓视角。两个字段都来自**当前**盘口，不是把开仓价差取负。
#[derive(Debug, Clone, Copy)]
pub struct CloseView {
    /// 平仓视角的**毛价差**（%）：买回原 sell 所 Ask1、卖回原 buy 所 Bid1。
    /// 格子判据用它（对齐参考 `spread_data.spread_pct`，不扣手续费）。
    pub exit_raw_pct: Decimal,
    /// 平仓视角**净边**（已扣平仓那一次手续费）。仅用于止损和记账。
    pub exit_net_pct: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// 价差回落到 T(n−1) 以下，按格减仓。
    GridReduce,
    /// 往返净利止损。参考统一引擎没有；热路径已关掉，变体留给旧 journal。
    StopLoss,
    /// 资金费率长期不利：价差没走坏，但持仓成本在持续流血。
    /// 对齐参考 `_should_close_for_funding_rate`。
    FundingStopLoss,
    /// 持仓时长超过上限。对齐参考 `auto_close_on_timeout`。
    HoldTimeout,
    /// 某腿余额跌破清仓线。对齐参考 `min_balance_close_position`。
    BalanceFloor,
    /// 剥头皮止盈：价差已收敛且盈利达标，减到当前格子的目标仓。
    /// 对齐参考 `_check_scalping_close`。
    ScalpTakeProfit,
}

impl CloseReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GridReduce => "grid_reduce",
            Self::StopLoss => "stop_loss",
            Self::FundingStopLoss => "funding_stop_loss",
            Self::HoldTimeout => "hold_timeout",
            Self::BalanceFloor => "balance_floor",
            Self::ScalpTakeProfit => "scalp_tp",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Intent {
    Open {
        qty: Decimal,
        buy: VenueId,
        sell: VenueId,
        grid: u32,
    },
    Close {
        qty: Decimal,
        grid: u32,
        reason: CloseReason,
        /// 决策时算出的往返净利（%），写 journal 用。
        round_trip_pct: Decimal,
    },
    Hold,
}

#[derive(Debug, Default)]
pub struct GridEngine {
    /// key 是 `slot_key`（币 + 所对），不是 pair_id。三所两两组合各自计时。
    persist: HashMap<String, Persist>,
    /// 剥头皮中的槽。仓位归零才退出，成交 `forget` 不清。
    scalping: HashSet<String>,
}

#[derive(Debug, Clone)]
struct Persist {
    kind: PersistKind,
    since: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistKind {
    Open,
    Close,
}

impl GridEngine {
    /// `slot` 必须是 `slot_key(pair_id, buy, sell)`。
    /// `open_net`：空仓时是双向最优；有仓时必须锁定持仓方向。
    /// `close`：有仓时必给，缺失则只能 Hold（宁可不动也不拿开仓价差当平仓依据）。
    pub fn decide(
        &mut self,
        slot: &str,
        open_net: &NetSpread,
        close: Option<CloseView>,
        pos: Option<&Position>,
        params: &GridParams,
        now: Instant,
    ) -> Intent {
        if let Some(pos) = pos.filter(|p| p.qty > Decimal::ZERO) {
            return self.decide_held(slot, open_net, close, pos, params, now);
        }
        self.exit_scalping(slot);

        if params.base_qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        // 空仓：毛价差落在第几格就开几格。对齐参考的
        // `target = open_segments × base_qty`。拆单打开时第一笔只下一截，
        // 其余留给后续 tick（有仓后走 decide_held 补到目标格）。
        // 开仓看 raw，不扣手续费、不扣天然基差——和参考格子同一套判据。
        let open_th = params.open_thresholds();
        let segments = count_segments(open_net.raw_pct, &open_th);
        self.maybe_activate_scalping(slot, open_grid_level(open_net.raw_pct, params), params);
        if segments == 0 {
            self.clear(slot);
            return Intent::Hold;
        }
        if !self.held(slot, PersistKind::Open, params.persistence, now) {
            return Intent::Hold;
        }
        let qty = split_order_qty(params.base_qty * Decimal::from(segments), params);
        if qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        Intent::Open {
            qty,
            buy: open_net.buy.clone(),
            sell: open_net.sell.clone(),
            grid: segments,
        }
    }

    /// 有仓：价差还够就补到目标格，回落则按格减。对齐参考
    /// `_calculate_target_position_by_spread`（加）+ `_check_grid_close`（减）。
    ///
    /// 加仓看**开仓视角毛价差**（和空仓同一条 raw）：吃不满或涨到更高格，
    /// 只要 raw 还在 Tn 以上就会继续补。减仓看**平仓视角毛价差**对
    /// `T0/T(n−1)`，不是往返净利——见 `docs/平仓判据备选方案B.md`。
    fn decide_held(
        &mut self,
        slot: &str,
        open_net: &NetSpread,
        close: Option<CloseView>,
        pos: &Position,
        params: &GridParams,
        now: Instant,
    ) -> Intent {
        if params.base_qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }

        // 往返净利止损：参考 UnifiedDecisionEngine 没有。曾经是
        // round_trip <= -close_stop_loss 全平且不等持续性，已按对齐关掉。
        // if let Some(view) = close {
        //     let round_trip = pos.entry_net_pct + view.exit_net_pct;
        //     if params.close_stop_loss > Decimal::ZERO && round_trip <= -params.close_stop_loss {
        //         return Intent::Close {
        //             qty: pos.qty,
        //             grid: 0,
        //             reason: CloseReason::StopLoss,
        //             round_trip_pct: round_trip,
        //         };
        //     }
        // }
        let _ = params.close_stop_loss;

        let current_segments = self.segments_held(pos.qty, params.base_qty);
        self.maybe_activate_scalping(
            slot,
            open_grid_level(open_net.raw_pct, params).max(current_segments),
            params,
        );

        // 加仓：开仓视角 raw 允许的格数高于已持格。吃不满（当前格 < 目标格）
        // 或价差涨到更高格，都走这里。方向锁在持仓上，不再双向取优。
        let add_to = count_segments(open_net.raw_pct, &params.open_thresholds());
        if add_to > current_segments {
            let open_delta = params.base_qty * Decimal::from(add_to - current_segments);
            if !self.held(slot, PersistKind::Open, params.persistence, now) {
                return Intent::Hold;
            }
            let qty = split_order_qty(open_delta, params);
            if qty <= Decimal::ZERO
                || (params.min_qty > Decimal::ZERO && qty < params.min_qty)
            {
                self.clear(slot);
                return Intent::Hold;
            }
            return Intent::Open {
                qty,
                buy: pos.buy.clone(),
                sell: pos.sell.clone(),
                grid: add_to,
            };
        }

        let Some(view) = close else {
            self.clear(slot);
            return Intent::Hold;
        };

        if self.is_scalping(slot) {
            return self.decide_scalp_close(slot, view, pos, params, current_segments);
        }

        // 方向归一：持仓方向上「价差还剩多少」。平仓视角的毛价差取负即为
        // 当前开仓方向的毛价差（对齐参考 `relative_spread = -closing_spread_pct`）。
        let relative = -view.exit_raw_pct;
        let target_segments = target_segments(relative, current_segments, params);

        if target_segments >= current_segments {
            self.clear(slot);
            return Intent::Hold;
        }

        let close_delta = pos.qty - params.base_qty * Decimal::from(target_segments);
        if close_delta <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        if !self.held(slot, PersistKind::Close, params.persistence, now) {
            return Intent::Hold;
        }
        let qty = split_order_qty(close_delta, params);
        if qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        Intent::Close {
            qty,
            grid: target_segments,
            reason: CloseReason::GridReduce,
            round_trip_pct: pos.entry_net_pct + view.exit_net_pct,
        }
    }

    /// 剥头皮平仓。对齐 `_check_scalping_close`：关掉格子滞后减仓，
    /// 价差收敛（目标格 < 已持格）且 `建仓均毛价差 − 当前剩余毛价差`
    /// 达到阈值才减到目标格。盈利不够就锁仓等。
    ///
    /// 参考这条路径**没有**持续性门：利润达标就按目标格减，不等
    /// `persistence_ms`。统一引擎也没有往返净利止损。
    fn decide_scalp_close(
        &mut self,
        slot: &str,
        view: CloseView,
        pos: &Position,
        params: &GridParams,
        current_segments: u32,
    ) -> Intent {
        let relative = -view.exit_raw_pct;
        let target_segments = count_segments(relative, &params.open_thresholds());
        if target_segments >= current_segments {
            self.clear(slot);
            return Intent::Hold;
        }
        let profit = pos.entry_raw_pct - relative;
        if profit < params.scalping_profit_threshold {
            self.clear(slot);
            return Intent::Hold;
        }
        let close_delta = pos.qty - params.base_qty * Decimal::from(target_segments);
        if close_delta <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        let qty = split_order_qty(close_delta, params);
        if qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        Intent::Close {
            qty,
            grid: target_segments,
            reason: CloseReason::ScalpTakeProfit,
            round_trip_pct: pos.entry_net_pct + view.exit_net_pct,
        }
    }

    fn maybe_activate_scalping(&mut self, slot: &str, grid: u32, params: &GridParams) {
        if !params.scalping_enabled || self.scalping.contains(slot) {
            return;
        }
        if params.scalping_trigger_segment > 0 && grid >= params.scalping_trigger_segment {
            self.scalping.insert(slot.to_string());
        }
    }

    pub fn is_scalping(&self, slot: &str) -> bool {
        self.scalping.contains(slot)
    }

    pub fn exit_scalping(&mut self, slot: &str) {
        self.scalping.remove(slot);
    }

    /// 当前持仓相当于几格（向上取整，对齐参考 `ROUND_CEILING`）。
    pub fn segments_held(&self, qty: Decimal, base_qty: Decimal) -> u32 {
        if base_qty <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        let ratio = qty / base_qty;
        let ceil = ratio.ceil();
        u32::try_from(ceil.trunc().mantissa().max(0)).unwrap_or(u32::MAX)
    }

    fn held(&mut self, slot: &str, kind: PersistKind, need: Duration, now: Instant) -> bool {
        match self.persist.get(slot) {
            Some(p) if p.kind == kind => now.duration_since(p.since) >= need,
            _ => {
                self.persist
                    .insert(slot.to_string(), Persist { kind, since: now });
                need.is_zero()
            }
        }
    }

    fn clear(&mut self, slot: &str) {
        self.persist.remove(slot);
    }

    /// 成交后清持续性。剥头皮状态故意不清：仓位归零才退出。
    pub fn forget(&mut self, slot: &str) {
        self.persist.remove(slot);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::VenueId;
    use rust_decimal_macros::dec;

    /// T1=0.03, step=0.03, 3 格 → 开仓 [0.03, 0.06, 0.09]，平仓 [0.012, 0.03, 0.06]
    fn params() -> GridParams {
        GridParams {
            initial: dec!(0.03),
            step: dec!(0.03),
            max_segments: 3,
            persistence: Duration::from_millis(0),
            base_qty: dec!(0.001),
            close_stop_loss: dec!(0.1),
            t0_ratio: dec!(0.4),
            split_order_size: dec!(0),
            min_qty: dec!(0),
            scalping_enabled: false,
            scalping_trigger_segment: 10,
            scalping_profit_threshold: dec!(0.02),
        }
    }

    fn net(pct: Decimal) -> NetSpread {
        NetSpread {
            buy: VenueId::from("lighter"),
            sell: VenueId::from("lighter_rh"),
            raw_pct: pct,
            fee_pct: dec!(0),
            slip_pct: dec!(0),
            net_pct: pct,
        }
    }

    fn pos_with(qty: Decimal, entry_net: Decimal) -> Position {
        Position {
            pair_id: "BTC-USD-PERP".into(),
            buy: VenueId::from("lighter"),
            sell: VenueId::from("lighter_rh"),
            qty,
            grid: 1,
            entry_notional_usdc: dec!(100),
            entry_net_pct: entry_net,
            entry_raw_pct: entry_net,
            opened_at: std::time::Instant::now(),
        }
    }

    /// 平仓视角：raw 用于格子判据，net 用于止损。
    /// `relative = -exit_raw`，所以持仓方向剩余价差 x 对应 exit_raw = -x。
    fn close_at(relative: Decimal, exit_net: Decimal) -> Option<CloseView> {
        Some(CloseView {
            exit_raw_pct: -relative,
            exit_net_pct: exit_net,
        })
    }

    const SLOT: &str = "BTC-USD-PERP|lighter|lighter_rh";

    // ── 阈值序列 ────────────────────────────────────────────────

    #[test]
    fn thresholds_match_reference_formula() {
        let p = params();
        assert_eq!(p.open_thresholds(), vec![dec!(0.03), dec!(0.06), dec!(0.09)]);
        // close = [T0, T1, T2]，T0 = 0.03 × 0.4 = 0.012
        assert_eq!(
            p.close_thresholds(),
            vec![dec!(0.012), dec!(0.03), dec!(0.06)]
        );
    }

    #[test]
    fn count_segments_picks_highest_met() {
        let th = vec![dec!(0.03), dec!(0.06), dec!(0.09)];
        assert_eq!(count_segments(dec!(0.02), &th), 0);
        assert_eq!(count_segments(dec!(0.03), &th), 1);
        assert_eq!(count_segments(dec!(0.05), &th), 1);
        assert_eq!(count_segments(dec!(0.06), &th), 2);
        assert_eq!(count_segments(dec!(0.20), &th), 3);
    }

    // ── 开仓 ────────────────────────────────────────────────────

    #[test]
    fn opens_one_segment_at_t1() {
        let mut eng = GridEngine::default();
        match eng.decide(SLOT, &net(dec!(0.04)), None, None, &params(), Instant::now()) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 1);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("{other:?}"),
        }
    }

    /// 价差直接落在第 3 格 → 一次开 3 格的量。
    #[test]
    fn opens_multiple_segments_when_spread_is_wide() {
        let mut eng = GridEngine::default();
        match eng.decide(SLOT, &net(dec!(0.10)), None, None, &params(), Instant::now()) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 3);
                assert_eq!(qty, dec!(0.003));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn holds_below_t1() {
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.01)), None, None, &params(), Instant::now()),
            Intent::Hold
        ));
    }

    /// 对齐参考：开仓看毛价差。扣完费后的净边低于 T1 仍开。
    #[test]
    fn opens_on_raw_even_when_net_is_below_t1() {
        let mut eng = GridEngine::default();
        let mut n = net(dec!(0.04));
        n.net_pct = dec!(0.01);
        match eng.decide(SLOT, &n, None, None, &params(), Instant::now()) {
            Intent::Open { grid, .. } => assert_eq!(grid, 1),
            other => panic!("{other:?}"),
        }
    }

    /// 有仓 1 格、毛价差涨到 T3 → 补 2 格。
    #[test]
    fn adds_up_to_n_when_spread_widens() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.10)),
            close_at(dec!(0.10), dec!(-0.02)),
            Some(&pos_with(dec!(0.001), dec!(0.04))),
            &params(),
            Instant::now(),
        ) {
            Intent::Open { qty, grid, buy, sell } => {
                assert_eq!(grid, 3);
                assert_eq!(qty, dec!(0.002));
                assert_eq!(buy.as_str(), "lighter");
                assert_eq!(sell.as_str(), "lighter_rh");
            }
            other => panic!("{other:?}"),
        }
    }

    /// 吃不满：目标 3 格只成交了 1 格，价差还在 T3 → 把剩下 2 格补上。
    #[test]
    fn tops_up_shortfall_while_spread_still_at_target() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.10)),
            None,
            Some(&pos_with(dec!(0.001), dec!(0.04))),
            &params(),
            Instant::now(),
        ) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 3);
                assert_eq!(qty, dec!(0.002));
            }
            other => panic!("{other:?}"),
        }
    }

    /// 已持 2 格、价差只在 T2（未到 T3）→ 不加。
    #[test]
    fn does_not_add_when_raw_does_not_justify_more_grids() {
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.07)),
                close_at(dec!(0.07), dec!(-0.02)),
                Some(&pos_with(dec!(0.002), dec!(0.06))),
                &params(),
                Instant::now(),
            ),
            Intent::Hold
        ));
    }

    /// 拆单：目标再补 2 格但 split 限制单笔 1 格，下一轮再补。
    #[test]
    fn split_order_size_caps_each_open() {
        let mut p = params();
        p.split_order_size = dec!(0.001);
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.10)),
            None,
            Some(&pos_with(dec!(0.001), dec!(0.04))),
            &p,
            Instant::now(),
        ) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 3);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("{other:?}"),
        }
    }

    // ── 按格减仓（方案 A 核心）────────────────────────────────

    /// 持有 3 格，价差回落到 0.05%（< T2=0.06）→ 减到 2 格，平掉 1 格。
    #[test]
    fn reduces_one_segment_when_spread_falls_below_prev_threshold() {
        let mut eng = GridEngine::default();
        let intent = eng.decide(
            SLOT,
            &net(dec!(0.05)),
            close_at(dec!(0.05), dec!(-0.02)),
            Some(&pos_with(dec!(0.003), dec!(0.09))),
            &params(),
            Instant::now(),
        );
        match intent {
            Intent::Close {
                qty, grid, reason, ..
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 2, "目标应为 2 格");
                assert_eq!(qty, dec!(0.001), "只减 1 格");
            }
            other => panic!("{other:?}"),
        }
    }

    /// 滞后：持有 3 格、价差 0.07%（仍 ≥ T2=0.06）→ 不减仓。
    /// 这正是「开一格→回落一格才平」，防止阈值附近来回开平。
    #[test]
    fn hysteresis_holds_until_below_prev_threshold() {
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.07)),
                close_at(dec!(0.07), dec!(-0.02)),
                Some(&pos_with(dec!(0.003), dec!(0.09))),
                &params(),
                Instant::now(),
            ),
            Intent::Hold
        ));
    }

    /// 价差跌破 T0=0.012 → 目标 0 格，全平。
    #[test]
    fn full_close_below_t0() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.02)),
            Some(&pos_with(dec!(0.003), dec!(0.09))),
            &params(),
            Instant::now(),
        ) {
            Intent::Close {
                qty, grid, reason, ..
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 0);
                assert_eq!(qty, dec!(0.003), "全部 3 格都要平");
            }
            other => panic!("{other:?}"),
        }
    }

    /// 拆单：一次要减 3 格但 split_order_size 限制单笔 0.001。
    #[test]
    fn split_order_size_caps_each_close() {
        let mut p = params();
        p.split_order_size = dec!(0.001);
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.02)),
            Some(&pos_with(dec!(0.003), dec!(0.09))),
            &p,
            Instant::now(),
        ) {
            Intent::Close { qty, .. } => assert_eq!(qty, dec!(0.001), "单笔被拆单上限截断"),
            other => panic!("{other:?}"),
        }
    }

    /// 拆单后的尾巴低于 min_qty 时并进本笔，否则那点残仓永远平不掉
    /// （第二腿会因低于最小下单量被拒）。
    #[test]
    fn split_merges_tail_below_min_qty() {
        let mut eng = GridEngine::default();
        let close_once = |eng: &mut GridEngine, min_qty: Decimal| -> Decimal {
            let mut p = params();
            p.split_order_size = dec!(0.001);
            p.min_qty = min_qty;
            match eng.decide(
                SLOT,
                &net(dec!(0.005)),
                close_at(dec!(0.005), dec!(-0.02)),
                Some(&pos_with(dec!(0.0015), dec!(0.09))),
                &p,
                Instant::now(),
            ) {
                Intent::Close { qty, .. } => qty,
                other => panic!("{other:?}"),
            }
        };
        // delta=0.0015 切 0.001 → 尾巴 0.0005 < min 0.0006，整笔平掉。
        assert_eq!(
            close_once(&mut eng, dec!(0.0006)),
            dec!(0.0015),
            "尾巴下不出去就并进本笔"
        );
        // 尾巴 0.0005 >= min 0.0002，可以留给下一轮，按拆单上限走。
        assert_eq!(
            close_once(&mut eng, dec!(0.0002)),
            dec!(0.001),
            "尾巴能独立成单就正常拆"
        );
    }

    // ── 往返净利止损已关掉（对齐参考统一引擎）────────────────

    /// 即使 round_trip 越线、params.close_stop_loss 仍是 0.1，也不全平；
    /// 价差够高时照常补仓。
    #[test]
    fn round_trip_loss_does_not_flatten() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.20)),
            close_at(dec!(0.20), dec!(-0.2)),
            Some(&pos_with(dec!(0.001), dec!(0.03))),
            &params(),
            Instant::now(),
        ) {
            Intent::Open { grid, .. } => assert_eq!(grid, 3),
            other => panic!("不应再走止损全平: {other:?}"),
        }
    }

    /// 已持满格时，往返亏损也不会在价差仍高时强平。
    #[test]
    fn round_trip_loss_does_not_close_at_high_spread() {
        let mut eng = GridEngine::default();
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.20)),
                    close_at(dec!(0.20), dec!(-0.2)),
                    Some(&pos_with(dec!(0.003), dec!(0.03))),
                    &params(),
                    Instant::now(),
                ),
                Intent::Hold
            ),
            "统一引擎没有往返止损"
        );
    }

    /// 按格减仓要等持续性。
    #[test]
    fn grid_reduce_waits_for_persistence() {
        let mut p = params();
        p.persistence = Duration::from_millis(1000);
        let t0 = Instant::now();

        let mut eng = GridEngine::default();
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.05)),
                    close_at(dec!(0.05), dec!(-0.02)),
                    Some(&pos_with(dec!(0.003), dec!(0.09))),
                    &p,
                    t0,
                ),
                Intent::Hold
            ),
            "按格减仓要先攒持续性"
        );
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.05)),
                close_at(dec!(0.05), dec!(-0.02)),
                Some(&pos_with(dec!(0.003), dec!(0.09))),
                &p,
                t0 + Duration::from_millis(1100),
            ),
            Intent::Close {
                reason: CloseReason::GridReduce,
                ..
            }
        ));
    }

    /// 拿不到平仓视角时不能减仓。已持满目标格时只能 Hold。
    /// 加仓不依赖平仓视角（见 `tops_up_shortfall_while_spread_still_at_target`）。
    #[test]
    fn missing_close_view_does_not_reduce() {
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.5)),
                None,
                Some(&pos_with(dec!(0.003), dec!(0.09))),
                &params(),
                Instant::now()
            ),
            Intent::Hold
        ));
    }

    /// 持续性计时按槽位隔离。
    #[test]
    fn persistence_is_per_slot() {
        let mut p = params();
        p.persistence = Duration::from_millis(50);
        let mut eng = GridEngine::default();
        let t0 = Instant::now();
        assert!(matches!(
            eng.decide("BTC|lighter|sodex", &net(dec!(0.04)), None, None, &p, t0),
            Intent::Hold
        ));
        let later = t0 + Duration::from_millis(60);
        assert!(matches!(
            eng.decide("BTC|lighter|sodex", &net(dec!(0.04)), None, None, &p, later),
            Intent::Open { .. }
        ));
        assert!(matches!(
            eng.decide(
                "BTC|lighter|lighter_rh",
                &net(dec!(0.04)),
                None,
                None,
                &p,
                later
            ),
            Intent::Hold
        ));
    }

    /// 持仓格数向上取整，对齐参考 ROUND_CEILING。
    #[test]
    fn segments_held_rounds_up() {
        let eng = GridEngine::default();
        assert_eq!(eng.segments_held(dec!(0.003), dec!(0.001)), 3);
        assert_eq!(
            eng.segments_held(dec!(0.0025), dec!(0.001)),
            3,
            "部分格向上取整"
        );
        assert_eq!(eng.segments_held(dec!(0), dec!(0.001)), 0);
    }

    fn scalp_params() -> GridParams {
        let mut p = params();
        p.scalping_enabled = true;
        p.scalping_trigger_segment = 2;
        p
    }

    fn decide_held(
        eng: &mut GridEngine,
        raw: Decimal,
        relative: Decimal,
        exit_net: Decimal,
        pos: &Position,
        p: &GridParams,
    ) -> Intent {
        eng.decide(
            SLOT,
            &net(raw),
            close_at(relative, exit_net),
            Some(pos),
            p,
            Instant::now(),
        )
    }

    /// 开仓视角格子不封顶，所以触发格可以大于 max_segments。
    #[test]
    fn open_grid_level_is_uncapped() {
        assert_eq!(open_grid_level(dec!(0.02), &params()), 0);
        assert_eq!(open_grid_level(dec!(0.03), &params()), 1);
        assert_eq!(open_grid_level(dec!(0.06), &params()), 2);
        assert_eq!(open_grid_level(dec!(0.09), &params()), 3);
        assert_eq!(open_grid_level(dec!(0.30), &params()), 10);
    }

    /// 持 2 格且 trigger=2 → 进入剥头皮。默认关着时即使持满也不进。
    #[test]
    fn activates_scalping_at_trigger_segment() {
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.06));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &scalp_params());
        assert!(eng.is_scalping(SLOT));

        let mut off = GridEngine::default();
        let _ = decide_held(&mut off, dec!(0.10), dec!(0.10), dec!(-0.10), &pos, &params());
        assert!(!off.is_scalping(SLOT));
    }

    /// 不封顶格子 ≥ 触发格（即使已持仓被 max_segments 封顶）也会进入。
    #[test]
    fn activates_when_uncapped_grid_hits_trigger_above_max_segments() {
        let mut p = params();
        p.scalping_enabled = true;
        p.scalping_trigger_segment = 10;
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.003), dec!(0.09));
        let _ = decide_held(&mut eng, dec!(0.30), dec!(0.30), dec!(-0.01), &pos, &p);
        assert!(eng.is_scalping(SLOT));
    }

    /// 剥头皮关掉滞后减仓：价差回落到会触发普通减仓、但利润不够 → Hold。
    #[test]
    fn scalping_holds_when_lag_would_reduce_but_profit_is_thin() {
        let p = scalp_params();
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.06));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &p);
        assert!(eng.is_scalping(SLOT));

        // relative=0.05：普通网格因滞后仍持 2 格；剥头皮目标格=1，
        // 但 profit = 0.06 − 0.05 = 0.01 < 0.02 → Hold。
        assert!(matches!(
            decide_held(&mut eng, dec!(0.05), dec!(0.05), dec!(-0.05), &pos, &p),
            Intent::Hold
        ));
    }

    /// 利润够：按目标格减，理由是 scalp_tp，且第一 tick 就出（无持续性门）。
    #[test]
    fn scalping_takes_profit_to_target_grid() {
        let p = scalp_params();
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.08));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &p);
        assert!(eng.is_scalping(SLOT));

        // relative=0.05 → 目标 1 格；profit = 0.08 − 0.05 = 0.03 ≥ 0.02。
        match decide_held(&mut eng, dec!(0.05), dec!(0.05), dec!(-0.05), &pos, &p) {
            Intent::Close {
                qty,
                grid,
                reason,
                ..
            } => {
                assert_eq!(qty, dec!(0.001));
                assert_eq!(grid, 1);
                assert_eq!(reason, CloseReason::ScalpTakeProfit);
            }
            other => panic!("{other:?}"),
        }
    }

    /// 剥头皮不因往返净利亏损而全平（统一引擎没有这条止损）。
    #[test]
    fn scalping_does_not_flatten_on_round_trip_loss() {
        let p = scalp_params();
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.06));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &p);
        assert!(eng.is_scalping(SLOT));

        assert!(matches!(
            decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.20), &pos, &p),
            Intent::Hold
        ));
    }

    /// 剥头皮中价差再涨仍可加仓。
    #[test]
    fn scalping_still_allows_add_on() {
        let p = scalp_params();
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.06));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &p);
        assert!(eng.is_scalping(SLOT));

        match decide_held(&mut eng, dec!(0.10), dec!(0.10), dec!(-0.10), &pos, &p) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 3);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("{other:?}"),
        }
    }

    /// 空仓退出剥头皮；forget 只清持续性。
    #[test]
    fn empty_exits_scalping_forget_does_not() {
        let p = scalp_params();
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.002), dec!(0.06));
        let _ = decide_held(&mut eng, dec!(0.06), dec!(0.06), dec!(-0.06), &pos, &p);
        assert!(eng.is_scalping(SLOT));

        eng.forget(SLOT);
        assert!(eng.is_scalping(SLOT), "forget 不应清剥头皮");

        let _ = eng.decide(SLOT, &net(dec!(0.02)), None, None, &p, Instant::now());
        assert!(!eng.is_scalping(SLOT));
    }
}
