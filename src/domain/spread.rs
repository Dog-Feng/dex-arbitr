use rust_decimal::Decimal;

use super::{Bbo, VenueId};

#[derive(Debug, Clone)]
pub struct NetSpread {
    pub buy: VenueId,
    pub sell: VenueId,
    pub raw_pct: Decimal,
    pub fee_pct: Decimal,
    pub slip_pct: Decimal,
    pub net_pct: Decimal,
}

/// Executable spread: buy Ask, sell Bid. Result is percent.
pub fn raw_spread_pct(buy_ask: Decimal, sell_bid: Decimal) -> Option<Decimal> {
    if buy_ask <= Decimal::ZERO {
        return None;
    }
    Some((sell_bid - buy_ask) / buy_ask * Decimal::from(100))
}

pub fn net_spread(
    buy: VenueId,
    sell: VenueId,
    buy_book: &Bbo,
    sell_book: &Bbo,
    fee_pct: Decimal,
    slip_pct: Decimal,
) -> Option<NetSpread> {
    let raw_pct = raw_spread_pct(buy_book.ask, sell_book.bid)?;
    Some(NetSpread {
        buy,
        sell,
        raw_pct,
        fee_pct,
        slip_pct,
        net_pct: raw_pct - fee_pct - slip_pct,
    })
}

/// 双吃：买所走 ask + 卖所走 bid，深度不够返回 None。
pub fn hedge_slip_pct(buy_book: &Bbo, sell_book: &Bbo, qty: Decimal) -> Option<Decimal> {
    Some(buy_book.buy_slip_pct(qty)? + sell_book.sell_slip_pct(qty)?)
}

/// Both directions; better net wins. Tie keeps first (x buy / y sell).
/// `book_slip` 为 false 时（限价挂单）滑点按 0。`max_slip_pct` > 0 时超过则丢掉该方向。
pub fn best_open_spread(
    venue_x: &VenueId,
    venue_y: &VenueId,
    book_x: &Bbo,
    book_y: &Bbo,
    fee_xy: Decimal,
    fee_yx: Decimal,
    qty: Decimal,
    max_slip_pct: Decimal,
    book_slip: bool,
) -> Option<NetSpread> {
    let a = direction(venue_x, venue_y, book_x, book_y, fee_xy, qty, max_slip_pct, book_slip);
    let b = direction(venue_y, venue_x, book_y, book_x, fee_yx, qty, max_slip_pct, book_slip);
    match (a, b) {
        (Some(l), Some(r)) => {
            if r.net_pct > l.net_pct {
                Some(r)
            } else {
                Some(l)
            }
        }
        (Some(v), None) | (None, Some(v)) => Some(v),
        (None, None) => None,
    }
}

fn direction(
    buy: &VenueId,
    sell: &VenueId,
    buy_book: &Bbo,
    sell_book: &Bbo,
    fee_pct: Decimal,
    qty: Decimal,
    max_slip_pct: Decimal,
    book_slip: bool,
) -> Option<NetSpread> {
    let slip_pct = if book_slip {
        hedge_slip_pct(buy_book, sell_book, qty)?
    } else {
        Decimal::ZERO
    };
    if max_slip_pct > Decimal::ZERO && slip_pct > max_slip_pct {
        return None;
    }
    net_spread(buy.clone(), sell.clone(), buy_book, sell_book, fee_pct, slip_pct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::time::Instant;

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
    fn uses_ask_bid_not_mid() {
        let pct = raw_spread_pct(dec!(100), dec!(100.05)).unwrap();
        assert_eq!(pct, dec!(0.05));
    }

    #[test]
    fn subtracts_fee_and_slip() {
        let buy = book(dec!(99), dec!(100));
        let sell = book(dec!(100.10), dec!(100.20));
        let net = net_spread(
            VenueId::from("a"),
            VenueId::from("b"),
            &buy,
            &sell,
            dec!(0.02),
            dec!(0.01),
        )
        .unwrap();
        assert_eq!(net.raw_pct, dec!(0.10));
        assert_eq!(net.net_pct, dec!(0.07));
    }

    #[test]
    fn picks_better_direction() {
        let cheap = book(dec!(100), dec!(100.01));
        let rich = book(dec!(100.08), dec!(100.09));
        let best = best_open_spread(
            &VenueId::from("lighter"),
            &VenueId::from("lighter_rh"),
            &cheap,
            &rich,
            dec!(0),
            dec!(0),
            dec!(0.001),
            dec!(0),
            true,
        )
        .unwrap();
        assert_eq!(best.buy.as_str(), "lighter");
        assert_eq!(best.sell.as_str(), "lighter_rh");
        assert!(best.net_pct > dec!(0));
        assert_eq!(best.slip_pct, dec!(0));
    }

    #[test]
    fn book_walk_slip_enters_net() {
        let cheap = Bbo {
            bid: dec!(99),
            ask: dec!(100),
            bid_qty: dec!(1),
            ask_qty: dec!(0.0004),
            bids: vec![(dec!(99), dec!(1))],
            asks: vec![(dec!(100), dec!(0.0004)), (dec!(100.2), dec!(1))],
            ts: Instant::now(),
        };
        let rich = Bbo {
            bid: dec!(100.10),
            ask: dec!(100.20),
            bid_qty: dec!(1),
            ask_qty: dec!(1),
            bids: vec![(dec!(100.10), dec!(1))],
            asks: vec![(dec!(100.20), dec!(1))],
            ts: Instant::now(),
        };
        let best = best_open_spread(
            &VenueId::from("cheap"),
            &VenueId::from("rich"),
            &cheap,
            &rich,
            dec!(0),
            dec!(0),
            dec!(0.001),
            dec!(1),
            true,
        )
        .unwrap();
        assert_eq!(best.buy.as_str(), "cheap");
        assert_eq!(best.slip_pct, dec!(0.12));
        assert_eq!(best.raw_pct, dec!(0.10));
        assert_eq!(best.net_pct, dec!(-0.02));
    }
}
