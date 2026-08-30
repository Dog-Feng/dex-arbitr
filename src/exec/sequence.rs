use rust_decimal::Decimal;
use std::time::Duration;

use crate::config::AppConfig;
use crate::domain::{
    spread::{decide_spread, realized_slip_pct},
    Bbo, NetSpread, VenueId,
};

/// 挂单未成且价差没了 → 撤单。
/// 已经成交（含撤单前成交）→ 另一边仍市价对冲，避免单边。
///
/// `timeout` 只应覆盖**本轮**挂单等待，由执行器自己计。看门狗若按整轮计划
/// 起点计时并置全局 cancel，会把 `limit_retry_count` 后面几轮一并掐掉。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitWatch {
    StillWait,
    CancelSpreadGone,
    CancelTimeout,
    FilledHedgeNow,
}

/// 开仓挂单还够不够：方向没反，且毛价差仍不低于给定下限（阶段 1 用 Δ×滞后）。
pub fn resting_open_spread_ok(raw_pct: Decimal, same_dir: bool, min_raw: Decimal) -> bool {
    same_dir && raw_pct >= min_raw
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

/// 成交后相对决策价的实际滑点。超过 `cost.default_slip_pct`（>0）则视为 overrun。
pub fn fill_slip_overrun(
    cfg: &AppConfig,
    is_buy: bool,
    expected: Decimal,
    fill: Decimal,
) -> Option<Decimal> {
    let slip = realized_slip_pct(is_buy, expected, fill)?;
    let max = cfg.cost.default_slip_pct;
    if max > Decimal::ZERO && slip > max {
        Some(slip)
    } else {
        None
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
    let fee = cfg.exec_fee(buy) + cfg.exec_fee(sell);
    decide_spread(buy.clone(), sell.clone(), buy_book, sell_book, fee, qty)
}

/// 平仓视角净边：买回原 sell 所（吃它 Ask1）、卖回原 buy 所（吃它 Bid1），
/// 用**当前**盘口重算，手续费按双腿 taker。
///
/// 对齐参考 `build_closing_spread_from_orderbooks`：不能把开仓价差取负，也不能
/// 只调换 price_buy / price_sell 字段。正常返回负数——现在平仓要吐回一部分价差。
pub fn closing_sequenced_spread(
    cfg: &AppConfig,
    pos_buy: &VenueId,
    pos_sell: &VenueId,
    pos_buy_book: &Bbo,
    pos_sell_book: &Bbo,
    qty: Decimal,
) -> Option<NetSpread> {
    sequenced_spread(cfg, pos_sell, pos_buy, pos_sell_book, pos_buy_book, qty)
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

    #[test]
    fn resting_open_keeps_order_above_floor() {
        let floor = dec!(0.05) * dec!(0.25);
        assert!(resting_open_spread_ok(dec!(0.040), true, floor));
        assert!(resting_open_spread_ok(dec!(0.013), true, floor));
        assert!(!resting_open_spread_ok(dec!(0.005), true, floor));
        assert!(!resting_open_spread_ok(dec!(0.040), false, floor));
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
    fn equal_taker_and_maker_has_no_limit_first() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("lighter_rh");
        // 当前 yaml 两所 maker/taker 都是 0，先挂后吃没有优先腿。
        assert!(first_limit_venue(&cfg, &buy, &sell).is_none());
        assert_eq!(book(dec!(100), dec!(100.01)).ask, dec!(100.01));
    }

    #[test]
    fn decide_uses_l1_and_no_pre_slip() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("sodex");
        let cheap = book(dec!(100), dec!(100.00));
        let rich = book(dec!(100.20), dec!(100.21));
        let net = sequenced_spread(&cfg, &buy, &sell, &cheap, &rich, dec!(0.001)).unwrap();
        assert_eq!(net.slip_pct, dec!(0));
        assert_eq!(net.net_pct, net.raw_pct - net.fee_pct);
        assert!(fill_slip_overrun(&cfg, true, dec!(100), dec!(100.05)).is_some());
        assert!(fill_slip_overrun(&cfg, true, dec!(100), dec!(100.005)).is_none());
    }

    /// 平仓视角必须用「原 sell 所的 Ask」和「原 buy 所的 Bid」，
    /// 结果不等于开仓价差取负。
    #[test]
    fn closing_view_reads_the_other_side_of_both_books() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("sodex");
        let buy_book = book(dec!(100.00), dec!(100.02));
        let sell_book = book(dec!(100.20), dec!(100.24));

        let open = sequenced_spread(&cfg, &buy, &sell, &buy_book, &sell_book, dec!(0.001)).unwrap();
        // 开仓：(100.20 − 100.02) / 100.02
        assert_eq!(open.raw_pct.round_dp(4), dec!(0.1800));

        let close =
            closing_sequenced_spread(&cfg, &buy, &sell, &buy_book, &sell_book, dec!(0.001)).unwrap();
        // 平仓：(100.00 − 100.24) / 100.24，与 −0.18% 不同
        assert_eq!(close.raw_pct.round_dp(4), dec!(-0.2394));
        assert_eq!(close.buy.as_str(), "sodex");
        assert_eq!(close.sell.as_str(), "lighter");
        assert_ne!(close.raw_pct, -open.raw_pct);
    }

    /// 盘口薄到撑不住本笔平仓量时，带 qty 的平仓视角返回 None。
    /// 上层据此丢掉格子平仓意图。价差本身仍用 qty=0 算。
    #[test]
    fn closing_view_with_qty_is_blocked_by_thin_book() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("sodex");
        // 两边都只有 0.0001 的一档量，持仓 0.5
        let thin = Bbo {
            bid: dec!(100.00),
            ask: dec!(100.02),
            bid_qty: dec!(0.0001),
            ask_qty: dec!(0.0001),
            bids: vec![(dec!(100.00), dec!(0.0001))],
            asks: vec![(dec!(100.02), dec!(0.0001))],
            ts: Instant::now(),
        };
        let held = dec!(0.5);
        assert!(
            closing_sequenced_spread(&cfg, &buy, &sell, &thin, &thin, held).is_none(),
            "带 qty 时厚度不足会吞掉平仓视角"
        );
        assert!(
            closing_sequenced_spread(&cfg, &buy, &sell, &thin, &thin, Decimal::ZERO).is_some(),
            "qty=0 仍能算出平仓净边，供格子判断该不该减"
        );
    }
}
