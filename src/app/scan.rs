//! 复刻参考项目 `OpportunityFinder`：毛价差过线、按 raw 排序、同 key 续 age。

use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::domain::{is_cross_dex, spread::raw_spread_pct, Bbo, Pair, VenueId};
use crate::infra::history::{residual_net, HistoryStore};

#[derive(Debug, Clone)]
pub struct ScanOpportunity {
    pub pair_id: String,
    pub buy: String,
    pub sell: String,
    pub raw_pct: Decimal,
    pub nat_pct: Option<Decimal>,
    pub residual_pct: Decimal,
    pub cross_dex: bool,
    pub price_buy: Decimal,
    pub price_sell: Decimal,
    pub first_seen: Instant,
}

impl ScanOpportunity {
    pub fn key(&self) -> String {
        opportunity_key(&self.pair_id, &self.buy, &self.sell)
    }

    pub fn age_secs(&self) -> f64 {
        self.first_seen.elapsed().as_secs_f64()
    }
}

#[derive(Debug, Default, Clone)]
pub struct ScanRound {
    pub universe: usize,
    pub ready: usize,
    pub wait: usize,
    pub stale: usize,
    pub invalid: usize,
    pub cross_hold: usize,
    pub opportunities: Vec<ScanOpportunity>,
}

pub struct OpportunityTracker {
    live: HashMap<String, Instant>,
}

impl Default for OpportunityTracker {
    fn default() -> Self {
        Self {
            live: HashMap::new(),
        }
    }
}

impl OpportunityTracker {
    /// 两边合法盘口、两个方向的 raw；≥ `min_spread_pct` 的记入机会并按 raw 降序。
    pub fn evaluate(
        &mut self,
        pairs: &[Pair],
        books: &HashMap<(String, String), Bbo>,
        freshness_ms: u64,
        min_spread_pct: Decimal,
        now: Instant,
    ) -> ScanRound {
        let mut round = ScanRound {
            universe: pairs.len(),
            ..ScanRound::default()
        };
        let mut current_keys = HashSet::new();

        for pair in pairs {
            let v0 = pair.legs[0].venue.as_str();
            let v1 = pair.legs[1].venue.as_str();
            let Some(b0) = books.get(&(v0.to_string(), pair.pair_id.clone())) else {
                round.wait += 1;
                continue;
            };
            let Some(b1) = books.get(&(v1.to_string(), pair.pair_id.clone())) else {
                round.wait += 1;
                continue;
            };
            if !b0.is_fresh(freshness_ms) || !b1.is_fresh(freshness_ms) {
                round.stale += 1;
                continue;
            }
            if !b0.valid() || !b1.valid() {
                round.invalid += 1;
                continue;
            }
            round.ready += 1;

            push_if_hit(
                &mut round.opportunities,
                &mut current_keys,
                &mut self.live,
                &pair.pair_id,
                &pair.legs[0].venue,
                &pair.legs[1].venue,
                b0,
                b1,
                min_spread_pct,
                now,
            );
            push_if_hit(
                &mut round.opportunities,
                &mut current_keys,
                &mut self.live,
                &pair.pair_id,
                &pair.legs[1].venue,
                &pair.legs[0].venue,
                b1,
                b0,
                min_spread_pct,
                now,
            );
        }

        self.live.retain(|k, _| current_keys.contains(k));
        round
            .opportunities
            .sort_by(|a, b| b.residual_pct.cmp(&a.residual_pct));
        round
    }

    /// 跨 DEX：库里有 nat 快照（或窗口已满）才按 residual = raw − max(nat,0) 上榜；同协议仍看 raw。
    pub fn apply_cross_natural(
        round: &mut ScanRound,
        history: Option<&HistoryStore>,
        min_spread_pct: Decimal,
        enabled: bool,
    ) {
        if !enabled {
            return;
        }
        let mut kept = Vec::new();
        for mut o in round.opportunities.drain(..) {
            if !o.cross_dex {
                kept.push(o);
                continue;
            }
            let Some(store) = history else {
                round.cross_hold += 1;
                continue;
            };
            let Some(nat) = store.natural(&o.pair_id, &o.buy, &o.sell) else {
                round.cross_hold += 1;
                continue;
            };
            o.nat_pct = Some(nat.value);
            o.residual_pct = residual_net(o.raw_pct, nat.value);
            if o.residual_pct >= min_spread_pct {
                kept.push(o);
            }
        }
        kept.sort_by(|a, b| b.residual_pct.cmp(&a.residual_pct));
        round.opportunities = kept;
    }
}

fn opportunity_key(pair_id: &str, buy: &str, sell: &str) -> String {
    format!("{pair_id}|{buy}|{sell}")
}

fn push_if_hit(
    out: &mut Vec<ScanOpportunity>,
    current_keys: &mut HashSet<String>,
    live: &mut HashMap<String, Instant>,
    pair_id: &str,
    buy: &VenueId,
    sell: &VenueId,
    buy_book: &Bbo,
    sell_book: &Bbo,
    min_spread_pct: Decimal,
    now: Instant,
) {
    let Some(raw) = raw_spread_pct(buy_book.ask, sell_book.bid) else {
        return;
    };
    if raw < min_spread_pct {
        return;
    }
    let key = opportunity_key(pair_id, buy.as_str(), sell.as_str());
    current_keys.insert(key.clone());
    let first_seen = *live.entry(key).or_insert(now);
    let cross = is_cross_dex(buy.as_str(), sell.as_str());
    out.push(ScanOpportunity {
        pair_id: pair_id.to_string(),
        buy: buy.as_str().to_string(),
        sell: sell.as_str().to_string(),
        raw_pct: raw,
        nat_pct: None,
        residual_pct: raw,
        cross_dex: cross,
        price_buy: buy_book.ask,
        price_sell: sell_book.bid,
        first_seen,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Pair, VenueId, VenueMarket};
    use rust_decimal_macros::dec;
    use std::time::Duration;

    fn mkt(venue: &str, pair_id: &str) -> VenueMarket {
        VenueMarket {
            venue: VenueId::from(venue),
            raw_symbol: pair_id.to_string(),
            pair_id: pair_id.to_string(),
            base: "SOL".into(),
            market_index: 1,
            qty_precision: 4,
            min_qty: dec!(0.01),
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> Bbo {
        Bbo {
            bid,
            ask,
            bid_qty: dec!(1),
            ask_qty: dec!(1),
            bids: vec![(bid, dec!(1))],
            asks: vec![(ask, dec!(1))],
            ts: Instant::now(),
        }
    }

    #[test]
    fn ranks_raw_and_keeps_age() {
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [mkt("lighter", "SOL-USD-PERP"), mkt("lighter_rh", "SOL-USD-PERP")],
        };
        let mut books = HashMap::new();
        books.insert(
            ("lighter".into(), "SOL-USD-PERP".into()),
            book(dec!(100), dec!(100.01)),
        );
        books.insert(
            ("lighter_rh".into(), "SOL-USD-PERP".into()),
            book(dec!(100.20), dec!(100.21)),
        );
        let mut tracker = OpportunityTracker::default();
        let t0 = Instant::now();
        let first = tracker.evaluate(&[pair.clone()], &books, 3000, dec!(0.1), t0);
        assert_eq!(first.ready, 1);
        assert_eq!(first.opportunities.len(), 1);
        assert_eq!(first.opportunities[0].buy, "lighter");
        assert!(first.opportunities[0].raw_pct > dec!(0.1));

        std::thread::sleep(Duration::from_millis(20));
        let second = tracker.evaluate(&[pair], &books, 3000, dec!(0.1), Instant::now());
        assert_eq!(second.opportunities.len(), 1);
        assert!(second.opportunities[0].age_secs() >= 0.015);
    }

    #[test]
    fn drops_when_raw_falls() {
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [mkt("lighter", "SOL-USD-PERP"), mkt("lighter_rh", "SOL-USD-PERP")],
        };
        let mut books = HashMap::new();
        books.insert(
            ("lighter".into(), "SOL-USD-PERP".into()),
            book(dec!(100), dec!(100.01)),
        );
        books.insert(
            ("lighter_rh".into(), "SOL-USD-PERP".into()),
            book(dec!(100.20), dec!(100.21)),
        );
        let mut tracker = OpportunityTracker::default();
        let now = Instant::now();
        assert_eq!(
            tracker
                .evaluate(&[pair.clone()], &books, 3000, dec!(0.1), now)
                .opportunities
                .len(),
            1
        );
        books.insert(
            ("lighter_rh".into(), "SOL-USD-PERP".into()),
            book(dec!(100.02), dec!(100.03)),
        );
        assert!(tracker
            .evaluate(&[pair], &books, 3000, dec!(0.1), Instant::now())
            .opportunities
            .is_empty());
    }
}
