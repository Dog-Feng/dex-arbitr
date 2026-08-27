use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::{NetSpread, Position, VenueId};

/// 持续性模式。`Window` 是原逻辑，保留可切回。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceMode {
    /// 从第一次达标起连续满 `persistence_ms`，掉线 `clear`。
    Window,
    /// 参考统一引擎：宽松按 unix 风格秒桶累加，严格按墙钟窗口内全样本达标。
    Bucket,
}

#[derive(Debug, Clone)]
pub struct GridParams {
    /// T1：第一格开仓阈值（%）。第 n 格 = T1 + (n−1) × step。
    pub initial: Decimal,
    pub step: Decimal,
    pub max_segments: u32,
    pub persistence: Duration,
    pub persistence_mode: PersistenceMode,
    pub spread_persistence_seconds: u32,
    pub strict_persistence_check: bool,
    /// 单格数量。目标持仓 = 格数 × base_qty。
    pub base_qty: Decimal,
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
    /// 格子加减仓看开仓方向 raw；这里只给往返记录和一档厚度校验。
#[derive(Debug, Clone, Copy)]
pub struct CloseView {
    /// 平仓视角的**毛价差**（%）：买回原 sell 所 Ask1、卖回原 buy 所 Bid1。
    /// 格子用它算目标格数（对齐参考 `spread_data.spread_pct`）。
    pub exit_raw_pct: Decimal,
    /// 平仓视角**净边**（已扣平仓那一次手续费）。journal / 面板记录往返用。
    pub exit_net_pct: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// 价差回落到 T(n−1) 以下，按格减仓。盈利性由阈值参数自己扛（D1-c）。
    GridReduce,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistCfg {
    mode: PersistenceMode,
    window: Duration,
    seconds: u32,
    strict: bool,
}

#[derive(Debug, Default)]
pub struct GridEngine {
    /// key 是 `slot_key`（币 + 所对），不是 pair_id。三所两两组合各自计时。
    persist: HashMap<String, Persist>,
    /// 参考秒桶状态，和 `persist` 分开，切回 window 时原逻辑不受影响。
    buckets: HashMap<String, BucketState>,
    /// 秒桶的本地时钟原点。用 Instant 差值模拟 unix 秒，测试可注入。
    clock_origin: Option<Instant>,
    /// 上次用过的持续性参数。热切换 mode/秒数/严格时丢掉旧进度，
    /// 否则 bucket→window→bucket 会把 count=2 接着用。
    persist_cfg: Option<PersistCfg>,
    /// 剥头皮中的槽。仓位归零才退出，成交 `forget` 不清。
    scalping: HashSet<String>,
}

#[derive(Debug, Clone)]
struct Persist {
    kind: PersistKind,
    since: Instant,
}

#[derive(Debug, Clone)]
struct BucketState {
    kind: PersistKind,
    last_bucket: Option<i64>,
    count: u32,
    strict_start: Option<Instant>,
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
        if !self.persist_ready(slot, PersistKind::Open, params, now) {
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

    /// 有仓：价差还够就补到目标格，回落则按格减。
    ///
    /// 加仓、减仓都看**开仓视角毛价差**（和页面「毛价差」同一条）。
    /// 减仓格数仍按 `T0/T(n−1)` 滞后。盈利性由阈值参数自己扛（D1-c）。
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

        let current_segments = self.segments_held(pos.qty, params.base_qty);
        if !params.scalping_enabled {
            self.exit_scalping(slot);
        }
        self.maybe_activate_scalping(
            slot,
            open_grid_level(open_net.raw_pct, params).max(current_segments),
            params,
        );

        // 加仓用量比较：`segments_held` 向上取整会把「欠了半格」算作满格，
        // 格数比较发现不了欠仓。缺口不足一次 min_qty 时静默跳过（不 clear），
        // 让流程继续到减仓判断，避免精度残差每轮清掉持续性。
        let add_to = count_segments(open_net.raw_pct, &params.open_thresholds());
        let target_qty = params.base_qty * Decimal::from(add_to);
        if target_qty > pos.qty {
            let open_delta = target_qty - pos.qty;
            let qty = split_order_qty(open_delta, params);
            if qty > Decimal::ZERO
                && (params.min_qty <= Decimal::ZERO || qty >= params.min_qty)
            {
                if !self.persist_ready(slot, PersistKind::Open, params, now) {
                    return Intent::Hold;
                }
                return Intent::Open {
                    qty,
                    buy: pos.buy.clone(),
                    sell: pos.sell.clone(),
                    grid: add_to,
                };
            }
        }

        let Some(view) = close else {
            self.clear(slot);
            return Intent::Hold;
        };

        if self.is_scalping(slot) {
            return self.decide_scalp_close(
                slot,
                view,
                pos,
                params,
                current_segments,
                open_net.raw_pct,
            );
        }

        // 减格用**开仓方向毛价差**（和页面「毛价差」同一条），不是平仓视角取负。
        // 平仓视角吃的是对面一档，两边买卖点差会把「剩余价差」抬正：
        // 页面已经是负值时，平仓视角仍可能 > T0，格子会一直 Hold。
        // 平仓视角只留给往返记录和一档厚度校验。
        let relative = open_net.raw_pct;
        let target_segments = target_segments(relative, current_segments, params);

        if target_segments >= current_segments {
            self.clear(slot);
            return Intent::Hold;
        }

        // 每次最多减 1 格。若这一格的量低于 min_qty（半格残量），继续往下减
        // 直到能下单或落到 target，避免残仓把整条减仓路径卡死。
        let mut step_to = current_segments.saturating_sub(1).max(target_segments);
        let mut close_delta = pos.qty - params.base_qty * Decimal::from(step_to);
        if params.min_qty > Decimal::ZERO
            && close_delta > Decimal::ZERO
            && close_delta < params.min_qty
        {
            let mut t = step_to;
            while t > target_segments {
                t -= 1;
                close_delta = pos.qty - params.base_qty * Decimal::from(t);
                if close_delta >= params.min_qty {
                    step_to = t;
                    break;
                }
            }
        }
        if close_delta <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }

        let round_trip = pos.entry_net_pct + view.exit_net_pct;
        if !self.persist_ready(slot, PersistKind::Close, params, now) {
            return Intent::Hold;
        }
        let qty = split_order_qty(close_delta, params);
        if qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        // 量太小发不出去：不 clear。价差继续回落会让 close_delta 变大，
        // 届时用已攒好的持续性立刻发单。全平且残仓 < min_qty 由 controller 报灰尘仓。
        if params.min_qty > Decimal::ZERO && qty < params.min_qty {
            return Intent::Hold;
        }
        Intent::Close {
            qty,
            grid: step_to,
            reason: CloseReason::GridReduce,
            round_trip_pct: round_trip,
        }
    }

    /// 剥头皮平仓。对齐 `_check_scalping_close`：关掉格子滞后减仓，
    /// 价差收敛（目标格 < 已持格）且 `建仓均毛价差 − 当前剩余毛价差`
    /// 达到阈值才减到目标格。盈利不够就锁仓等。
    ///
    /// 参考这条路径**没有**持续性门：利润达标就按目标格减，不等
    /// `persistence_ms`。往返净利不再做闸门（D1-c）。
    fn decide_scalp_close(
        &mut self,
        slot: &str,
        view: CloseView,
        pos: &Position,
        params: &GridParams,
        current_segments: u32,
        open_raw: Decimal,
    ) -> Intent {
        let relative = open_raw;
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
        let round_trip = pos.entry_net_pct + view.exit_net_pct;
        let qty = split_order_qty(close_delta, params);
        if qty <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }
        if params.min_qty > Decimal::ZERO && qty < params.min_qty {
            return Intent::Hold;
        }
        Intent::Close {
            qty,
            grid: target_segments,
            reason: CloseReason::ScalpTakeProfit,
            round_trip_pct: round_trip,
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

    fn persist_ready(
        &mut self,
        slot: &str,
        kind: PersistKind,
        params: &GridParams,
        now: Instant,
    ) -> bool {
        let cfg = PersistCfg {
            mode: params.persistence_mode,
            window: params.persistence,
            seconds: params.spread_persistence_seconds,
            strict: params.strict_persistence_check,
        };
        if self.persist_cfg != Some(cfg) {
            self.reset_persist();
            self.persist_cfg = Some(cfg);
        }
        match params.persistence_mode {
            PersistenceMode::Window => self.held(slot, kind, params.persistence, now),
            PersistenceMode::Bucket => self.held_bucket(slot, kind, params, now),
        }
    }

    /// 原逻辑：从第一次达标起连续满 `need`，掉线由调用方 `clear`。
    fn held(&mut self, slot: &str, kind: PersistKind, need: Duration, now: Instant) -> bool {
        match self.persist.get(slot) {
            Some(p) if p.kind == kind => now.saturating_duration_since(p.since) >= need,
            _ => {
                self.persist
                    .insert(slot.to_string(), Persist { kind, since: now });
                need.is_zero()
            }
        }
    }

    /// 参考 `_check_spread_persistence`：`seconds <= 1` 立刻放行。
    fn held_bucket(
        &mut self,
        slot: &str,
        kind: PersistKind,
        params: &GridParams,
        now: Instant,
    ) -> bool {
        let need = params.spread_persistence_seconds;
        if need <= 1 {
            self.buckets.remove(slot);
            return true;
        }
        if params.strict_persistence_check {
            self.held_strict(slot, kind, need, now)
        } else {
            self.held_loose(slot, kind, need, now)
        }
    }

    fn sec_bucket(&mut self, now: Instant) -> i64 {
        let origin = *self.clock_origin.get_or_insert(now);
        now.saturating_duration_since(origin).as_secs() as i64
    }

    fn bucket_state(&mut self, slot: &str, kind: PersistKind) -> &mut BucketState {
        let reset = self
            .buckets
            .get(slot)
            .map(|s| s.kind != kind)
            .unwrap_or(true);
        if reset {
            self.buckets.insert(
                slot.to_string(),
                BucketState {
                    kind,
                    last_bucket: None,
                    count: 0,
                    strict_start: None,
                },
            );
        }
        self.buckets.get_mut(slot).expect("just inserted")
    }

    /// 宽松：每个秒桶至少一次达标，相邻桶 count+1，中间空秒则重置。
    fn held_loose(&mut self, slot: &str, kind: PersistKind, need: u32, now: Instant) -> bool {
        let bucket = self.sec_bucket(now);
        let st = self.bucket_state(slot, kind);
        match st.last_bucket {
            None => {
                st.count = 1;
                st.last_bucket = Some(bucket);
            }
            Some(last) if last == bucket => {}
            Some(last) if bucket == last + 1 => {
                st.count = st.count.saturating_add(1);
                st.last_bucket = Some(bucket);
            }
            Some(_) => {
                st.count = 1;
                st.last_bucket = Some(bucket);
            }
        }
        st.count >= need
    }

    /// 严格：墙钟窗口内每次调用都达标（失败由 `clear` 清掉起点）。
    fn held_strict(&mut self, slot: &str, kind: PersistKind, need: u32, now: Instant) -> bool {
        let st = self.bucket_state(slot, kind);
        if st.strict_start.is_none() {
            st.strict_start = Some(now);
        }
        let start = st.strict_start.expect("just set");
        now.saturating_duration_since(start) >= Duration::from_secs(u64::from(need))
    }

    fn clear(&mut self, slot: &str) {
        self.persist.remove(slot);
        self.buckets.remove(slot);
    }

    /// 清全部槽的持续性（不含剥头皮）。热改 mode/秒数时由 `persist_ready` 调用。
    pub fn reset_persist(&mut self) {
        self.persist.clear();
        self.buckets.clear();
        self.clock_origin = None;
    }

    /// 成交后清持续性。剥头皮状态故意不清：仓位归零才退出。
    /// 决策环在「本轮没能评估价差」（没盘口 / 无中价 / 价差算不出）时也走这里，
    /// 否则 window/严格秒桶会把空洞算进持续时间内。
    pub fn forget(&mut self, slot: &str) {
        self.persist.remove(slot);
        self.buckets.remove(slot);
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
            persistence_mode: PersistenceMode::Window,
            spread_persistence_seconds: 1,
            strict_persistence_check: true,
            base_qty: dec!(0.001),
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
            entry_buy_px: Decimal::ZERO,
            entry_sell_px: Decimal::ZERO,
            base_qty: dec!(0.001),
            opened_at: std::time::Instant::now(),
        }
    }

    /// 构造平仓视角。格子减仓已改用开仓方向 raw；这里的 relative
    /// 只影响 `exit_raw_pct` / 往返记录。多数单测让它和开仓 raw 相同。
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

    /// yaml 没给该币 base_qty 时（实盘 SNDK 就是 0），有仓也绝不能加格。
    /// 这是控制器必须把 Position.base_qty 填进 params 的原因。
    #[test]
    fn held_with_zero_base_qty_never_adds() {
        let mut p = params();
        p.base_qty = Decimal::ZERO;
        p.initial = dec!(0.015);
        p.step = dec!(0.015);
        let mut eng = GridEngine::default();
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.04)),
                    close_at(dec!(0.04), dec!(-0.02)),
                    Some(&pos_with(dec!(0.01), dec!(0.02))),
                    &p,
                    Instant::now(),
                ),
                Intent::Hold
            ),
            "base_qty=0 时即使 raw 过 T2 也必须 Hold"
        );
    }

    /// T1=0.015、step=0.015：已持 1 格、raw≥0.03 → 加到第 2 格。
    #[test]
    fn adds_second_grid_at_t1_plus_step() {
        let mut p = params();
        p.initial = dec!(0.015);
        p.step = dec!(0.015);
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.031)),
            close_at(dec!(0.031), dec!(-0.02)),
            Some(&pos_with(dec!(0.001), dec!(0.02))),
            &p,
            Instant::now(),
        ) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 2);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("raw≥T2 应加第 2 格: {other:?}"),
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

    /// 欠仓补齐：pos.qty = 0.5 × base_qty、价差仍在 T1 → 补到 1 格整。
    #[test]
    fn tops_up_partial_fill_by_quantity() {
        let mut p = params();
        p.base_qty = dec!(1.0);
        let mut eng = GridEngine::default();
        let pos = pos_with(dec!(0.5), dec!(0.04));
        match eng.decide(
            SLOT,
            &net(dec!(0.04)),
            close_at(dec!(0.04), dec!(-0.02)),
            Some(&pos),
            &p,
            Instant::now(),
        ) {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(grid, 1);
                assert_eq!(qty, dec!(0.5));
            }
            other => panic!("欠半格应补仓: {other:?}"),
        }
    }

    // ── 按格减仓（方案 A：格数按 T0 滞后，盈利性由阈值自己扛）──

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

    /// 价差跌破 T0=0.012 → 目标 0 格，但每笔只减 1 格（3→2）。
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
                assert_eq!(grid, 2, "跌破 T0 也先减到 2 格");
                assert_eq!(qty, dec!(0.001), "每次只平 1 格");
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
        // 单格尺 0.002、仓 0.0015 = 1 格。跌破 T0 要全平 0.0015。
        let mut p = params();
        p.base_qty = dec!(0.002);
        p.split_order_size = dec!(0.001);
        p.min_qty = dec!(0.0006);
        let pos = pos_with(dec!(0.0015), dec!(0.09));
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.02)),
            Some(&pos),
            &p,
            Instant::now(),
        ) {
            Intent::Close { qty, .. } => {
                assert_eq!(qty, dec!(0.0015), "尾巴下不出去就并进本笔")
            }
            other => panic!("{other:?}"),
        }
        p.min_qty = dec!(0.0002);
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.02)),
            Some(&pos),
            &p,
            Instant::now(),
        ) {
            Intent::Close { qty, .. } => {
                assert_eq!(qty, dec!(0.001), "尾巴能独立成单就正常拆")
            }
            other => panic!("{other:?}"),
        }
    }

    /// 格子说该减（跌破 T2），往返仍亏也按格减——盈利性由阈值自己扛（D1-c）。
    #[test]
    fn grid_reduce_fires_even_when_round_trip_is_negative() {
        let mut eng = GridEngine::default();
        // 3 格、relative=0.05 < T2，该减到 2 格；0.03 + (-0.04) = -0.01
        match eng.decide(
            SLOT,
            &net(dec!(0.05)),
            close_at(dec!(0.05), dec!(-0.04)),
            Some(&pos_with(dec!(0.003), dec!(0.03))),
            &params(),
            Instant::now(),
        ) {
            Intent::Close {
                qty,
                grid,
                reason,
                round_trip_pct,
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 2);
                assert_eq!(qty, dec!(0.001));
                assert_eq!(round_trip_pct, dec!(-0.01));
            }
            other => panic!("D1-c 无止盈闸，该减就减: {other:?}"),
        }
    }

    /// 开仓视角已为负，平仓视角因买卖点差仍为正 → 仍按开仓视角减格。
    #[test]
    fn grid_reduce_uses_open_raw_not_close_bounce() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(-0.02)),
            close_at(dec!(0.05), dec!(-0.04)),
            Some(&pos_with(dec!(0.001), dec!(0.03))),
            &params(),
            Instant::now(),
        ) {
            Intent::Close {
                reason, grid, qty, ..
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 0);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("开仓视角为负应减格: {other:?}"),
        }
    }

    /// 持 2 格：页面 0.025% 已低于 T1=0.03，即使平仓剩余仍 0.06% 也减到 1 格。
    #[test]
    fn grid_reduce_when_open_raw_below_t1_despite_close_bounce() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.025)),
            close_at(dec!(0.06), dec!(-0.02)),
            Some(&pos_with(dec!(0.002), dec!(0.06))),
            &params(),
            Instant::now(),
        ) {
            Intent::Close {
                reason, grid, qty, ..
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 1);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("开仓视角低于 T1 应减 1 格: {other:?}"),
        }
    }

    /// 跌破 T0 → 1 格仓全平。round_trip_pct 仍记录。
    #[test]
    fn grid_reduce_fires_when_below_t0() {
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.024)),
            Some(&pos_with(dec!(0.001), dec!(0.03))),
            &params(),
            Instant::now(),
        ) {
            Intent::Close {
                qty,
                grid,
                reason,
                round_trip_pct,
            } => {
                assert_eq!(reason, CloseReason::GridReduce);
                assert_eq!(grid, 0);
                assert_eq!(qty, dec!(0.001));
                assert_eq!(round_trip_pct, dec!(0.006));
            }
            other => panic!("{other:?}"),
        }
    }

    /// 刚开完的执行成本（往返已亏）不挡加仓：raw 仍在 T2。
    #[test]
    fn execution_cost_does_not_block_add() {
        let mut p = params();
        p.initial = dec!(0.015);
        p.step = dec!(0.015);
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.031)),
            close_at(dec!(0.031), dec!(-0.054)),
            Some(&pos_with(dec!(0.001), dec!(-0.095))),
            &p,
            Instant::now(),
        ) {
            Intent::Open { grid, .. } => assert_eq!(grid, 2),
            other => panic!("刚开完的执行成本不应挡加仓: {other:?}"),
        }
    }

    /// 价差够高时仍可补仓（方案 A 没有往返止损）。
    #[test]
    fn high_spread_still_adds() {
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
            other => panic!("价差够高应照常补仓: {other:?}"),
        }
    }

    /// 已持满格、往返亏损且价差仍高 → Hold（不减，因为还没跌破 T(n−1)）。
    #[test]
    fn high_spread_holds_until_below_lag_threshold() {
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
            "未跌破滞后阈值 → Hold"
        );
    }

    /// close_delta 低于 min_qty 时 Hold，不发必拒单。
    #[test]
    fn close_delta_below_min_qty_holds() {
        let mut p = params();
        p.min_qty = dec!(0.0001); // 0.1 × base_qty
        let mut eng = GridEngine::default();
        // pos = 1.02 格 → segments_held 向上取整 = 2；relative=0.02 在 T0..T1 → 目标 1 格
        // close_delta = 0.00002 < min_qty
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.02)),
                    close_at(dec!(0.02), dec!(-0.01)),
                    Some(&pos_with(dec!(0.00102), dec!(0.04))),
                    &p,
                    Instant::now(),
                ),
                Intent::Hold
            ),
            "减仓量低于 min_qty 必须 Hold"
        );
    }

    /// 目标已是 0 格时 close_delta = pos.qty，只要 ≥ min_qty 就能发出。
    #[test]
    fn close_delta_grows_when_target_drops() {
        let mut p = params();
        p.min_qty = dec!(0.0001);
        let mut eng = GridEngine::default();
        match eng.decide(
            SLOT,
            &net(dec!(0.005)),
            close_at(dec!(0.005), dec!(-0.01)),
            Some(&pos_with(dec!(0.00102), dec!(0.04))),
            &p,
            Instant::now(),
        ) {
            Intent::Close { qty, grid, .. } => {
                assert_eq!(grid, 0);
                assert_eq!(qty, dec!(0.00102));
            }
            other => panic!("全平且量够 min_qty 应发出: {other:?}"),
        }
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

    fn bucket_params(seconds: u32, strict: bool) -> GridParams {
        let mut p = params();
        p.persistence_mode = PersistenceMode::Bucket;
        p.spread_persistence_seconds = seconds;
        p.strict_persistence_check = strict;
        p.persistence = Duration::from_millis(10_000);
        p
    }

    /// 参考：seconds <= 1 不累计，达标即放行。
    #[test]
    fn bucket_seconds_one_fires_immediately() {
        let p = bucket_params(1, true);
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, Instant::now()),
            Intent::Open { grid: 1, .. }
        ));
    }

    /// 宽松：连续三个秒桶各至少一次达标才开。
    #[test]
    fn bucket_loose_needs_consecutive_seconds() {
        let p = bucket_params(3, false);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(1)),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(2)),
            Intent::Open { grid: 1, .. }
        ));
    }

    /// 宽松：中间空掉一整秒则进度清零。
    #[test]
    fn bucket_loose_gap_resets() {
        let p = bucket_params(3, false);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0);
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(2));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(3)),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(4)),
            Intent::Open { grid: 1, .. }
        ));
    }

    /// 严格：墙钟满 N 秒且中间没有掉线。
    #[test]
    fn bucket_strict_waits_wall_clock() {
        let p = bucket_params(3, true);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_millis(2900)),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(3)),
            Intent::Open { grid: 1, .. }
        ));
    }

    /// 严格：掉线 clear 后重新计时。
    #[test]
    fn bucket_strict_drop_resets() {
        let p = bucket_params(3, true);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0);
        let _ = eng.decide(SLOT, &net(dec!(0.01)), None, None, &p, t0 + Duration::from_secs(1));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(3)),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0 + Duration::from_secs(6)),
            Intent::Open { grid: 1, .. }
        ));
    }

    /// 热切 mode 必须丢掉旧秒桶进度，否则 bucket→window→bucket 会把 count=2 接着累到 3。
    #[test]
    fn persist_mode_switch_drops_stale_bucket_count() {
        let mut p = bucket_params(3, false);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0);
        let _ = eng.decide(
            SLOT,
            &net(dec!(0.03)),
            None,
            None,
            &p,
            t0 + Duration::from_secs(1),
        );

        p.persistence_mode = PersistenceMode::Window;
        p.persistence = Duration::from_secs(10);
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.03)),
                None,
                None,
                &p,
                t0 + Duration::from_secs(1)
            ),
            Intent::Hold
        ));

        p = bucket_params(3, false);
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.03)),
                    None,
                    None,
                    &p,
                    t0 + Duration::from_secs(2)
                ),
                Intent::Hold
            ),
            "切回秒桶不能复用切换前的 count=2"
        );
    }

    /// 改秒数等同换了一套门槛，已攒的 count 作废。
    #[test]
    fn persist_seconds_change_restarts_count() {
        let mut p = bucket_params(3, false);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0);
        let _ = eng.decide(
            SLOT,
            &net(dec!(0.03)),
            None,
            None,
            &p,
            t0 + Duration::from_secs(1),
        );
        p.spread_persistence_seconds = 2;
        assert!(
            matches!(
                eng.decide(
                    SLOT,
                    &net(dec!(0.03)),
                    None,
                    None,
                    &p,
                    t0 + Duration::from_secs(2)
                ),
                Intent::Hold
            ),
            "秒数从 3 改成 2 不能立刻用旧 count 放行"
        );
    }

    /// 本轮没评估到价差（决策环会 forget）必须打断 window / 严格秒桶。
    #[test]
    fn forget_breaks_strict_bucket() {
        let p = bucket_params(3, true);
        let t0 = Instant::now();
        let mut eng = GridEngine::default();
        let _ = eng.decide(SLOT, &net(dec!(0.03)), None, None, &p, t0);
        eng.forget(SLOT);
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.03)),
                None,
                None,
                &p,
                t0 + Duration::from_secs(3)
            ),
            Intent::Hold
        ));
        assert!(matches!(
            eng.decide(
                SLOT,
                &net(dec!(0.03)),
                None,
                None,
                &p,
                t0 + Duration::from_secs(6)
            ),
            Intent::Open { grid: 1, .. }
        ));
    }
}
