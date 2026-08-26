use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// `(venue, pair_id)` → 最新盘口。
pub type BookMap = HashMap<(String, String), Bbo>;

/// 共享盘口。执行 task 直接读最新盘口，不再克隆整张表当快照——
/// 挂单价必须用「此刻」的盘口算，否则重试和限价定价都是在拿旧价下单。
pub type Books = Arc<RwLock<BookMap>>;

pub fn new_books() -> Books {
    Arc::new(RwLock::new(BookMap::new()))
}

/// 读一条盘口的副本。锁只在克隆期间持有，不跨 await。
pub fn read_book(books: &Books, venue: &str, pair_id: &str) -> Option<Bbo> {
    books
        .read()
        .ok()?
        .get(&(venue.to_string(), pair_id.to_string()))
        .cloned()
}

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

    /// 对齐参考 `SpreadCalculator` 的盘口校验：要求 Bid < Ask。
    /// 锁定盘口（ask == bid）是数据异常，不当合法报价。
    pub fn valid(&self) -> bool {
        self.bid > Decimal::ZERO && self.ask > Decimal::ZERO && self.ask > self.bid
    }

    /// 本所自身买卖点差（%）。过宽说明该所报价不可信 / 流动性极差，
    /// 而「先挂后吃」的 maker 腿恰恰要挂在这个所的盘口上。
    pub fn own_spread_pct(&self) -> Option<Decimal> {
        if self.ask <= Decimal::ZERO {
            return None;
        }
        Some((self.ask - self.bid) / self.ask * Decimal::from(100))
    }

    /// 最小报价变动单位。两所 WS 都按市场价格精度推字符串，所以取
    /// bid/ask 的小数位即为 tick。用于把 maker 单挂进点差内侧。
    pub fn price_tick(&self) -> Decimal {
        let scale = self.bid.scale().max(self.ask.scale());
        Decimal::new(1, scale)
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
    fn locked_book_is_invalid() {
        let b = book(vec![(dec!(100), dec!(1))], vec![(dec!(100), dec!(1))]);
        assert!(!b.valid());
    }

    #[test]
    fn tick_from_quote_scale() {
        let b = book(vec![(dec!(100.01), dec!(1))], vec![(dec!(100.05), dec!(1))]);
        assert_eq!(b.price_tick(), dec!(0.01));
        assert_eq!(b.own_spread_pct().unwrap().round_dp(4), dec!(0.0400));
    }

    #[test]
    fn none_when_depth_short() {
        let b = book(vec![(dec!(100), dec!(0.0002))], vec![(dec!(101), dec!(0.0002))]);
        assert!(b.buy_slip_pct(dec!(0.001)).is_none());
        assert!(b.sell_slip_pct(dec!(0.001)).is_none());
    }
}
