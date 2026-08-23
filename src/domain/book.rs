use rust_decimal::Decimal;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Bbo {
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_qty: Decimal,
    pub ask_qty: Decimal,
    /// Best bid first.
    pub bids: Vec<(Decimal, Decimal)>,
    /// Best ask first.
    pub asks: Vec<(Decimal, Decimal)>,
    pub ts: Instant,
}

impl Bbo {
    pub fn is_fresh(&self, freshness_ms: u64) -> bool {
        self.ts.elapsed().as_millis() as u64 <= freshness_ms
    }

    pub fn valid(&self) -> bool {
        self.bid > Decimal::ZERO && self.ask > Decimal::ZERO && self.ask >= self.bid
    }

    pub fn bid_depth(&self) -> Decimal {
        self.bids.iter().map(|(_, q)| *q).sum()
    }

    pub fn ask_depth(&self) -> Decimal {
        self.asks.iter().map(|(_, q)| *q).sum()
    }

    /// 吃 ask `qty` 的成交均价。深度不够返回 None。
    pub fn buy_vwap(&self, qty: Decimal) -> Option<Decimal> {
        vwap(&self.asks, qty)
    }

    /// 吃 bid `qty` 的成交均价。深度不够返回 None。
    pub fn sell_vwap(&self, qty: Decimal) -> Option<Decimal> {
        vwap(&self.bids, qty)
    }

    /// 相对 Ask1 的买入滑点，单位 %。
    pub fn buy_slip_pct(&self, qty: Decimal) -> Option<Decimal> {
        slip_from_best(self.buy_vwap(qty)?, self.ask)
    }

    /// 相对 Bid1 的卖出滑点，单位 %。
    pub fn sell_slip_pct(&self, qty: Decimal) -> Option<Decimal> {
        let vwap = self.sell_vwap(qty)?;
        if self.bid <= Decimal::ZERO {
            return None;
        }
        Some((self.bid - vwap) / self.bid * Decimal::from(100))
    }
}

fn vwap(levels: &[(Decimal, Decimal)], qty: Decimal) -> Option<Decimal> {
    if qty <= Decimal::ZERO {
        return None;
    }
    let mut left = qty;
    let mut notional = Decimal::ZERO;
    for (px, sz) in levels {
        if *px <= Decimal::ZERO || *sz <= Decimal::ZERO {
            continue;
        }
        let take = left.min(*sz);
        notional += take * *px;
        left -= take;
        if left <= Decimal::ZERO {
            break;
        }
    }
    if left > Decimal::ZERO {
        return None;
    }
    Some(notional / qty)
}

fn slip_from_best(vwap: Decimal, best: Decimal) -> Option<Decimal> {
    if best <= Decimal::ZERO {
        return None;
    }
    Some((vwap - best) / best * Decimal::from(100))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn book(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Bbo {
        let (bid, bid_qty) = bids[0];
        let (ask, ask_qty) = asks[0];
        Bbo {
            bid,
            ask,
            bid_qty,
            ask_qty,
            bids,
            asks,
            ts: Instant::now(),
        }
    }

    #[test]
    fn l1_enough_is_zero_slip() {
        let b = book(vec![(dec!(100), dec!(1))], vec![(dec!(100.1), dec!(1))]);
        assert_eq!(b.buy_slip_pct(dec!(0.001)).unwrap(), dec!(0));
        assert_eq!(b.sell_slip_pct(dec!(0.001)).unwrap(), dec!(0));
    }

    #[test]
    fn walks_second_level() {
        let b = book(
            vec![(dec!(100), dec!(0.0004)), (dec!(99), dec!(1))],
            vec![(dec!(101), dec!(0.0004)), (dec!(102), dec!(1))],
        );
        let buy = b.buy_slip_pct(dec!(0.001)).unwrap();
        // vwap = (0.0004*101 + 0.0006*102) / 0.001 = 101.6; slip = 0.6/101*100
        assert_eq!(buy.round_dp(6), dec!(0.594059));
        let sell = b.sell_slip_pct(dec!(0.001)).unwrap();
        // vwap = (0.0004*100 + 0.0006*99) / 0.001 = 99.4; slip = 0.6/100*100
        assert_eq!(sell, dec!(0.6));
    }

    #[test]
    fn none_when_depth_short() {
        let b = book(vec![(dec!(100), dec!(0.0002))], vec![(dec!(101), dec!(0.0002))]);
        assert!(b.buy_slip_pct(dec!(0.001)).is_none());
        assert!(b.sell_slip_pct(dec!(0.001)).is_none());
    }
}
