use rust_decimal::Decimal;
use std::time::Duration;

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{spread::net_spread, Bbo, NetSpread, VenueId};

/// 挂单未成且价差没了 → 撤单。
/// 已经成交（含撤单前成交）→ 另一边仍市价对冲，避免单边。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitWatch {
    StillWait,
    CancelSpreadGone,
    CancelTimeout,
    FilledHedgeNow,
}

pub fn watch_resting_limit(
    spread_still_valid: bool,
    elapsed: Duration,
    timeout: Duration,
    first_filled: bool,
) -> LimitWatch {
    if first_filled {
        return LimitWatch::FilledHedgeNow;
    }
    if !spread_still_valid {
        return LimitWatch::CancelSpreadGone;
    }
    if elapsed >= timeout && !timeout.is_zero() {
        return LimitWatch::CancelTimeout;
    }
    LimitWatch::StillWait
}

/// 吃单费率更高的那所先挂限价，另一所后市价。
/// taker 相同：挂在 maker 更低的一侧。maker 也相同：两边都市价（返回 None）。
pub fn first_limit_venue<'a>(
    cfg: &AppConfig,
    buy: &'a VenueId,
    sell: &'a VenueId,
) -> Option<(&'a VenueId, &'a VenueId)> {
    let buy_t = cfg.taker_fee(buy);
    let sell_t = cfg.taker_fee(sell);
    if buy_t > sell_t {
        return Some((buy, sell));
    }
    if sell_t > buy_t {
        return Some((sell, buy));
    }
    let buy_m = cfg.maker_fee(buy);
    let sell_m = cfg.maker_fee(sell);
    if buy_m < sell_m {
        return Some((buy, sell));
    }
    if sell_m < buy_m {
        return Some((sell, buy));
    }
    None
}

pub fn sequenced_fee(cfg: &AppConfig, buy: &VenueId, sell: &VenueId) -> Decimal {
    match first_limit_venue(cfg, buy, sell) {
        Some((first, second)) => cfg.maker_fee(first) + cfg.taker_fee(second),
        None => cfg.taker_fee(buy) + cfg.taker_fee(sell),
    }
}

/// 只走市价那一腿的盘口滑点。
pub fn sequenced_slip(
    cfg: &AppConfig,
    buy: &VenueId,
    sell: &VenueId,
    buy_book: &Bbo,
    sell_book: &Bbo,
    qty: Decimal,
) -> Option<Decimal> {
    if !matches!(cfg.order.style, OrderStyle::LimitThenMarket) {
        return Some(buy_book.buy_slip_pct(qty)? + sell_book.sell_slip_pct(qty)?);
    }
    match first_limit_venue(cfg, buy, sell) {
        Some((first, _)) if first == buy => sell_book.sell_slip_pct(qty),
        Some(_) => buy_book.buy_slip_pct(qty),
        None => Some(buy_book.buy_slip_pct(qty)? + sell_book.sell_slip_pct(qty)?),
    }
}

pub fn sequenced_spread(
    cfg: &AppConfig,
    buy: &VenueId,
    sell: &VenueId,
    buy_book: &Bbo,
    sell_book: &Bbo,
    qty: Decimal,
) -> Option<NetSpread> {
    let slip = sequenced_slip(cfg, buy, sell, buy_book, sell_book, qty)?;
    let max_slip = cfg.cost.default_slip_pct;
    if max_slip > Decimal::ZERO && slip > max_slip {
        return None;
    }
    let fee = if matches!(cfg.order.style, OrderStyle::LimitThenMarket) {
        sequenced_fee(cfg, buy, sell)
    } else {
        cfg.exec_fee(buy) + cfg.exec_fee(sell)
    };
    net_spread(buy.clone(), sell.clone(), buy_book, sell_book, fee, slip)
}

pub fn best_sequenced_spread(
    cfg: &AppConfig,
    x: &VenueId,
    y: &VenueId,
    book_x: &Bbo,
    book_y: &Bbo,
    qty: Decimal,
) -> Option<NetSpread> {
    let a = sequenced_spread(cfg, x, y, book_x, book_y, qty);
    let b = sequenced_spread(cfg, y, x, book_y, book_x, qty);
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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    #[test]
    fn late_fill_still_hedges() {
        assert_eq!(
            watch_resting_limit(false, Duration::from_secs(1), Duration::from_secs(3), true),
            LimitWatch::FilledHedgeNow
        );
    }

    #[test]
    fn cancel_when_spread_gone_and_unfilled() {
        assert_eq!(
            watch_resting_limit(false, Duration::from_millis(200), Duration::from_secs(3), false),
            LimitWatch::CancelSpreadGone
        );
    }

    #[test]
    fn timeout_cancels_unfilled() {
        assert_eq!(
            watch_resting_limit(true, Duration::from_secs(5), Duration::from_secs(2), false),
            LimitWatch::CancelTimeout
        );
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
    fn higher_taker_is_first() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("lighter_rh");
        let (first, second) = first_limit_venue(&cfg, &buy, &sell).expect("rh taker higher");
        assert_eq!(first.as_str(), "lighter_rh");
        assert_eq!(second.as_str(), "lighter");
        assert_eq!(book(dec!(100), dec!(100.01)).ask, dec!(100.01));
    }
}
