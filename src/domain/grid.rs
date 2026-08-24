use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::{NetSpread, Position, VenueId};

#[derive(Debug, Clone)]
pub struct GridParams {
    pub initial: Decimal,
    pub step: Decimal,
    pub max_segments: u32,
    pub persistence: Duration,
    pub base_qty: Decimal,
}

impl GridParams {
    pub fn t0(&self) -> Decimal {
        self.initial * Decimal::new(4, 1)
    }

    pub fn open_thresholds(&self) -> Vec<Decimal> {
        (0..self.max_segments)
            .map(|i| self.initial + self.step * Decimal::from(i))
            .collect()
    }

    pub fn close_thresholds(&self) -> Vec<Decimal> {
        let mut out = vec![self.t0()];
        let opens = self.open_thresholds();
        if opens.len() > 1 {
            out.extend(opens.iter().take(opens.len() - 1).cloned());
        }
        out
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
    },
    Hold,
}

#[derive(Debug, Default)]
pub struct GridEngine {
    persist: HashMap<String, Persist>,
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
    pub fn decide(
        &mut self,
        pair_id: &str,
        net: &NetSpread,
        pos: Option<&Position>,
        params: &GridParams,
        now: Instant,
    ) -> Intent {
        let actual = pos.map(|p| p.qty).unwrap_or(Decimal::ZERO);
        let target_open = if net.net_pct >= params.initial {
            params.base_qty * Decimal::from(params.max_segments.min(1).max(1))
        } else {
            Decimal::ZERO
        };

        if actual > Decimal::ZERO {
            let reverse = pos
                .map(|p| p.buy != net.buy || p.sell != net.sell)
                .unwrap_or(false);
            if reverse || net.net_pct < params.t0() {
                return self.maybe_close(pair_id, reverse, actual, params, now);
            }
            self.clear(pair_id);
            return Intent::Hold;
        }

        if target_open <= Decimal::ZERO || net.net_pct < params.initial {
            self.clear(pair_id);
            return Intent::Hold;
        }

        if !self.held(pair_id, PersistKind::Open, params.persistence, now) {
            return Intent::Hold;
        }
        Intent::Open {
            qty: params.base_qty.min(target_open),
            buy: net.buy.clone(),
            sell: net.sell.clone(),
            grid: 1,
        }
    }

    fn maybe_close(
        &mut self,
        pair_id: &str,
        reverse: bool,
        actual: Decimal,
        params: &GridParams,
        now: Instant,
    ) -> Intent {
        let _ = reverse;
        if !self.held(pair_id, PersistKind::Close, params.persistence, now) {
            return Intent::Hold;
        }
        Intent::Close {
            qty: actual,
            grid: 0,
        }
    }

    fn held(&mut self, pair_id: &str, kind: PersistKind, need: Duration, now: Instant) -> bool {
        match self.persist.get(pair_id) {
            Some(p) if p.kind == kind => now.duration_since(p.since) >= need,
            _ => {
                self.persist.insert(
                    pair_id.to_string(),
                    Persist { kind, since: now },
                );
                need.is_zero()
            }
        }
    }

    fn clear(&mut self, pair_id: &str) {
        self.persist.remove(pair_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::VenueId;
    use rust_decimal_macros::dec;

    fn params() -> GridParams {
        GridParams {
            initial: dec!(0.03),
            step: dec!(0.03),
            max_segments: 1,
            persistence: Duration::from_millis(0),
            base_qty: dec!(0.001),
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

    #[test]
    fn t0_is_forty_percent_of_t1() {
        assert_eq!(params().t0(), dec!(0.012));
    }

    #[test]
    fn opens_at_t1_when_flat() {
        let mut eng = GridEngine::default();
        let intent = eng.decide("BTC-USD-PERP", &net(dec!(0.04)), None, &params(), Instant::now());
        match intent {
            Intent::Open { qty, grid, .. } => {
                assert_eq!(qty, dec!(0.001));
                assert_eq!(grid, 1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn holds_below_t1() {
        let mut eng = GridEngine::default();
        let intent = eng.decide("BTC-USD-PERP", &net(dec!(0.01)), None, &params(), Instant::now());
        assert!(matches!(intent, Intent::Hold));
    }

    #[test]
    fn closes_below_t0() {
        let mut eng = GridEngine::default();
        let pos = Position {
            pair_id: "BTC-USD-PERP".into(),
            buy: VenueId::from("lighter"),
            sell: VenueId::from("lighter_rh"),
            qty: dec!(0.001),
            grid: 1,
            entry_notional_usdc: dec!(100),
        };
        let intent = eng.decide(
            "BTC-USD-PERP",
            &net(dec!(0.005)),
            Some(&pos),
            &params(),
            Instant::now(),
        );
        match intent {
            Intent::Close { qty, .. } => assert_eq!(qty, dec!(0.001)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn reverse_direction_closes() {
        let mut eng = GridEngine::default();
        let pos = Position {
            pair_id: "BTC-USD-PERP".into(),
            buy: VenueId::from("lighter"),
            sell: VenueId::from("lighter_rh"),
            qty: dec!(0.001),
            grid: 1,
            entry_notional_usdc: dec!(100),
        };
        let mut reverse = net(dec!(0.05));
        reverse.buy = VenueId::from("lighter_rh");
        reverse.sell = VenueId::from("lighter");
        let intent = eng.decide("BTC-USD-PERP", &reverse, Some(&pos), &params(), Instant::now());
        assert!(matches!(intent, Intent::Close { .. }));
    }
}
