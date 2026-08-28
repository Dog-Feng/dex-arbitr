//! 滑动窗口有符号 STEP：相对 μ 的偏离、滞后、每拍 ±1、过零必经 0。
//!
//! μ 由上层窗口模块提供；本模块不读窗口。

use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::{CloseReason, GridParams, Intent, VenueId};

#[derive(Debug, Clone)]
pub struct WindowGridParams {
    pub step: Decimal,
    pub max_step: i32,
    pub hysteresis: Decimal,
    pub persistence: Duration,
    pub persistence_min_hits: u32,
    pub base_qty: Decimal,
    pub split_order_size: Decimal,
    pub min_qty: Decimal,
}

impl WindowGridParams {
    pub fn from_grid(p: &GridParams, hysteresis: Decimal) -> Self {
        Self {
            step: p.step,
            max_step: (p.max_segments as i32).max(1),
            hysteresis: hysteresis.max(Decimal::ZERO),
            persistence: p.persistence,
            persistence_min_hits: p.persistence_min_hits,
            base_qty: p.base_qty,
            split_order_size: p.split_order_size,
            min_qty: p.min_qty,
        }
    }

    fn window_hit_count(&self) -> bool {
        self.persistence_min_hits > 0
    }
}

fn split_order_qty(delta: Decimal, params: &WindowGridParams) -> Decimal {
    if delta <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    if params.split_order_size <= Decimal::ZERO {
        return delta;
    }
    let qty = delta.min(params.split_order_size);
    let tail = delta - qty;
    if tail > Decimal::ZERO && tail < params.min_qty {
        return delta;
    }
    qty
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistCfg {
    window: Duration,
    min_hits: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistKind {
    Plus,
    Minus,
}

#[derive(Debug, Clone)]
struct Persist {
    kind: PersistKind,
    since: Instant,
}

#[derive(Debug, Clone)]
struct HitWindow {
    kind: PersistKind,
    hits: VecDeque<Instant>,
}

#[derive(Debug, Default)]
pub struct WindowGridEngine {
    persist: HashMap<String, Persist>,
    hits: HashMap<String, HitWindow>,
    persist_cfg: Option<PersistCfg>,
}

impl WindowGridEngine {
    /// `s_plus` / `s_minus`：本拍可执行价差（%）。`k` 当前有符号 STEP。
    /// `held_qty` 用于最后一格平仓吃掉残量。
    pub fn decide(
        &mut self,
        slot: &str,
        k: i32,
        s_plus: Decimal,
        s_minus: Decimal,
        mu: Decimal,
        left: &VenueId,
        right: &VenueId,
        held_qty: Decimal,
        params: &WindowGridParams,
        now: Instant,
    ) -> Intent {
        if params.base_qty <= Decimal::ZERO || params.step <= Decimal::ZERO {
            self.clear(slot);
            return Intent::Hold;
        }

        let h = params.hysteresis;
        let n = params.max_step;
        let raw_plus = (s_plus - mu) / params.step;
        let raw_minus = (s_minus - mu) / params.step;

        let want_plus = k < n && raw_plus >= Decimal::from(k + 1) - h;
        let want_minus = k > -n && raw_minus <= Decimal::from(k - 1) + h;

        let kind = match (want_plus, want_minus) {
            (true, false) => PersistKind::Plus,
            (false, true) => PersistKind::Minus,
            _ => {
                self.persist_fail(slot, params, now);
                return Intent::Hold;
            }
        };

        if !self.persist_ready(slot, kind, params, now) {
            return Intent::Hold;
        }

        let next = match kind {
            PersistKind::Plus => k + 1,
            PersistKind::Minus => k - 1,
        };
        self.emit(k, next, left, right, held_qty, params)
    }

    fn emit(
        &self,
        k: i32,
        next: i32,
        left: &VenueId,
        right: &VenueId,
        held_qty: Decimal,
        params: &WindowGridParams,
    ) -> Intent {
        let one = split_order_qty(params.base_qty, params);
        if one <= Decimal::ZERO {
            return Intent::Hold;
        }
        if params.min_qty > Decimal::ZERO && one < params.min_qty {
            return Intent::Hold;
        }

        if next > k {
            if k >= 0 {
                Intent::Open {
                    qty: one,
                    buy: right.clone(),
                    sell: left.clone(),
                    grid: next,
                }
            } else {
                let qty = close_qty(next, held_qty, one);
                if qty <= Decimal::ZERO {
                    return Intent::Hold;
                }
                Intent::Close {
                    qty,
                    grid: next,
                    reason: CloseReason::GridReduce,
                    round_trip_pct: Decimal::ZERO,
                }
            }
        } else if next < k {
            if k <= 0 {
                Intent::Open {
                    qty: one,
                    buy: left.clone(),
                    sell: right.clone(),
                    grid: next,
                }
            } else {
                let qty = close_qty(next, held_qty, one);
                if qty <= Decimal::ZERO {
                    return Intent::Hold;
                }
                Intent::Close {
                    qty,
                    grid: next,
                    reason: CloseReason::GridReduce,
                    round_trip_pct: Decimal::ZERO,
                }
            }
        } else {
            Intent::Hold
        }
    }

    fn persist_ready(
        &mut self,
        slot: &str,
        kind: PersistKind,
        params: &WindowGridParams,
        now: Instant,
    ) -> bool {
        self.sync_persist_cfg(params);
        if params.window_hit_count() {
            self.held_hits(slot, kind, params, now)
        } else {
            self.held(slot, kind, params.persistence, now)
        }
    }

    fn persist_cfg_of(params: &WindowGridParams) -> PersistCfg {
        PersistCfg {
            window: params.persistence,
            min_hits: params.persistence_min_hits,
        }
    }

    fn sync_persist_cfg(&mut self, params: &WindowGridParams) {
        let cfg = Self::persist_cfg_of(params);
        if self.persist_cfg != Some(cfg) {
            self.reset_persist();
            self.persist_cfg = Some(cfg);
        }
    }

    fn persist_fail(&mut self, slot: &str, params: &WindowGridParams, now: Instant) {
        self.sync_persist_cfg(params);
        if params.window_hit_count() {
            self.expire_hits(slot, params.persistence, now);
        } else {
            self.clear(slot);
        }
    }

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

    fn held_hits(
        &mut self,
        slot: &str,
        kind: PersistKind,
        params: &WindowGridParams,
        now: Instant,
    ) -> bool {
        let need = params.persistence_min_hits;
        let window = params.persistence;
        let reset = self.hits.get(slot).map(|h| h.kind != kind).unwrap_or(true);
        if reset {
            self.hits.insert(
                slot.to_string(),
                HitWindow {
                    kind,
                    hits: VecDeque::new(),
                },
            );
            self.persist.remove(slot);
        }
        let st = self.hits.get_mut(slot).expect("just inserted");
        st.hits.push_back(now);
        Self::expire_deque(&mut st.hits, window, now);
        while st.hits.len() > 64 {
            st.hits.pop_front();
        }
        (st.hits.len() as u32) >= need
    }

    fn expire_hits(&mut self, slot: &str, window: Duration, now: Instant) {
        if let Some(st) = self.hits.get_mut(slot) {
            Self::expire_deque(&mut st.hits, window, now);
        }
    }

    fn expire_deque(hits: &mut VecDeque<Instant>, window: Duration, now: Instant) {
        while let Some(t) = hits.front() {
            if now.saturating_duration_since(*t) <= window {
                break;
            }
            hits.pop_front();
        }
    }

    fn clear(&mut self, slot: &str) {
        self.persist.remove(slot);
        self.hits.remove(slot);
    }

    pub fn reset_persist(&mut self) {
        self.persist.clear();
        self.hits.clear();
    }

    pub fn forget(&mut self, slot: &str) {
        self.persist.remove(slot);
        self.hits.remove(slot);
    }
}

fn close_qty(next: i32, held_qty: Decimal, one: Decimal) -> Decimal {
    if next == 0 {
        held_qty
    } else {
        one.min(held_qty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const SLOT: &str = "BTC-USD-PERP|lighter|entropy";

    fn params() -> WindowGridParams {
        WindowGridParams {
            step: dec!(0.5),
            max_step: 3,
            hysteresis: dec!(0.25),
            persistence: Duration::from_millis(0),
            persistence_min_hits: 0,
            base_qty: dec!(0.001),
            split_order_size: Decimal::ZERO,
            min_qty: Decimal::ZERO,
        }
    }

    fn left() -> VenueId {
        VenueId::from("lighter")
    }
    fn right() -> VenueId {
        VenueId::from("entropy")
    }

    fn decide(
        eng: &mut WindowGridEngine,
        k: i32,
        s_plus: Decimal,
        s_minus: Decimal,
        held: Decimal,
        now: Instant,
    ) -> Intent {
        eng.decide(
            SLOT,
            k,
            s_plus,
            s_minus,
            Decimal::ZERO,
            &left(),
            &right(),
            held,
            &params(),
            now,
        )
    }

    #[test]
    fn empty_below_075_delta_holds() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // raw = 0.74/0.5 = 1.48? wait 0.74 of delta is 0.37. threshold 0.75
        // s = 0.74 * 0.5 = 0.37
        assert!(matches!(
            decide(&mut eng, 0, dec!(0.37), dec!(0.37), Decimal::ZERO, t),
            Intent::Hold
        ));
    }

    #[test]
    fn empty_at_075_delta_opens_plus_one() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // s = 0.75 * 0.5 = 0.375 → raw=0.75 ≥ 1-0.25
        match decide(&mut eng, 0, dec!(0.375), dec!(0), Decimal::ZERO, t) {
            Intent::Open {
                qty,
                buy,
                sell,
                grid,
            } => {
                assert_eq!(qty, dec!(0.001));
                assert_eq!(buy, right());
                assert_eq!(sell, left());
                assert_eq!(grid, 1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn empty_negative_opens_minus_one() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        match decide(&mut eng, 0, dec!(0), dec!(-0.375), Decimal::ZERO, t) {
            Intent::Open {
                qty,
                buy,
                sell,
                grid,
            } => {
                assert_eq!(qty, dec!(0.001));
                assert_eq!(buy, left());
                assert_eq!(sell, right());
                assert_eq!(grid, -1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn add_one_at_a_time_no_skip() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // raw=3 够到 +3，但从 0 每拍只发 ±1
        match decide(&mut eng, 0, dec!(1.5), dec!(1.6), Decimal::ZERO, t) {
            Intent::Open { grid, .. } => assert_eq!(grid, 1),
            other => panic!("{other:?}"),
        }
        match decide(&mut eng, 1, dec!(1.5), dec!(1.6), dec!(0.001), t) {
            Intent::Open { grid, .. } => assert_eq!(grid, 2),
            other => panic!("{other:?}"),
        }
        match decide(&mut eng, 2, dec!(1.5), dec!(1.6), dec!(0.002), t) {
            Intent::Open { grid, .. } => assert_eq!(grid, 3),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            decide(&mut eng, 3, dec!(1.5), dec!(1.6), dec!(0.003), t),
            Intent::Hold
        ));
    }

    #[test]
    fn reduce_positive_step_one_at_a_time() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // k=3 → 2: raw_minus ≤ 2.25. s_minus = 0
        match decide(&mut eng, 3, dec!(0), dec!(0), dec!(0.003), t) {
            Intent::Close { grid, qty, .. } => {
                assert_eq!(grid, 2);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("{other:?}"),
        }
        match decide(&mut eng, 1, dec!(0), dec!(0), dec!(0.001), t) {
            Intent::Close { grid, qty, .. } => {
                assert_eq!(grid, 0);
                assert_eq!(qty, dec!(0.001));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn cannot_cross_zero_in_one_tick() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // k=-1, s_plus huge would want +1, but only k+1 = 0
        match decide(&mut eng, -1, dec!(2), dec!(0), dec!(0.001), t) {
            Intent::Close { grid, .. } => assert_eq!(grid, 0),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hysteresis_avoids_churn_near_boundary() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        // k=1, to add need raw≥1.75; to reduce need raw≤0.25
        // raw=1.0 → hold
        assert!(matches!(
            decide(&mut eng, 1, dec!(0.5), dec!(0.5), dec!(0.001), t),
            Intent::Hold
        ));
    }

    #[test]
    fn hits_need_several_crosses() {
        let mut p = params();
        p.persistence = Duration::from_millis(1000);
        p.persistence_min_hits = 5;
        let mut eng = WindowGridEngine::default();
        let t0 = Instant::now();
        for i in 0..4 {
            let intent = eng.decide(
                SLOT,
                0,
                dec!(0.375),
                dec!(0),
                Decimal::ZERO,
                &left(),
                &right(),
                Decimal::ZERO,
                &p,
                t0 + Duration::from_millis(i * 100),
            );
            assert!(matches!(intent, Intent::Hold), "hit {}", i + 1);
        }
        let intent = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0 + Duration::from_millis(400),
        );
        assert!(matches!(intent, Intent::Open { grid: 1, .. }));
    }

    #[test]
    fn miss_does_not_clear_hit_window() {
        let mut p = params();
        p.persistence = Duration::from_millis(1000);
        p.persistence_min_hits = 3;
        let mut eng = WindowGridEngine::default();
        let t0 = Instant::now();
        let _ = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0,
        );
        let _ = eng.decide(
            SLOT,
            0,
            dec!(0),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0 + Duration::from_millis(100),
        );
        let _ = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0 + Duration::from_millis(200),
        );
        let intent = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0 + Duration::from_millis(300),
        );
        assert!(matches!(intent, Intent::Open { grid: 1, .. }));
    }

    #[test]
    fn forget_resets_hits() {
        let mut p = params();
        p.persistence = Duration::from_millis(1000);
        p.persistence_min_hits = 2;
        let mut eng = WindowGridEngine::default();
        let t0 = Instant::now();
        let _ = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0,
        );
        eng.forget(SLOT);
        let intent = eng.decide(
            SLOT,
            0,
            dec!(0.375),
            dec!(0),
            Decimal::ZERO,
            &left(),
            &right(),
            Decimal::ZERO,
            &p,
            t0 + Duration::from_millis(100),
        );
        assert!(matches!(intent, Intent::Hold));
    }

    #[test]
    fn cap_still_allows_reduce() {
        let mut eng = WindowGridEngine::default();
        let t = Instant::now();
        assert!(matches!(
            decide(&mut eng, 3, dec!(10), dec!(10), dec!(0.003), t),
            Intent::Hold
        ));
        match decide(&mut eng, 3, dec!(10), dec!(0), dec!(0.003), t) {
            Intent::Close { grid, .. } => assert_eq!(grid, 2),
            other => panic!("{other:?}"),
        }
    }
}
