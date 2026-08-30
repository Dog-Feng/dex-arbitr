use rust_decimal::Decimal;
use std::cmp::Ordering;
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

/// 比较「A 挂限价（付 maker）+ B 市价（付 taker 并吃 B 点差）」和反过来哪边更便宜。
///
/// `Less` = A 做第一腿更便宜；`Greater` = B 做第一腿更便宜；`Equal` 打平。
pub fn limit_then_hedge_cost_cmp(
    a_maker: Decimal,
    a_taker: Decimal,
    a_spread: Decimal,
    b_maker: Decimal,
    b_taker: Decimal,
    b_spread: Decimal,
) -> Ordering {
    let a_makes = a_maker + b_taker + b_spread.max(Decimal::ZERO);
    let b_makes = b_maker + a_taker + a_spread.max(Decimal::ZERO);
    a_makes.cmp(&b_makes)
}

/// 限价挂在「自己做 maker、对面 taker」总成本更低的一侧；打平返回 None。
pub fn first_limit_venue<'a>(
    cfg: &AppConfig,
    buy: &'a VenueId,
    sell: &'a VenueId,
) -> Option<(&'a VenueId, &'a VenueId)> {
    match limit_then_hedge_cost_cmp(
        cfg.maker_fee(buy),
        cfg.taker_fee(buy),
        Decimal::ZERO,
        cfg.maker_fee(sell),
        cfg.taker_fee(sell),
        Decimal::ZERO,
    ) {
        Ordering::Less => Some((buy, sell)),
        Ordering::Greater => Some((sell, buy)),
        Ordering::Equal => None,
    }
}

pub fn first_limit_venue_or_left<'a>(
    cfg: &AppConfig,
    buy: &'a VenueId,
    sell: &'a VenueId,
    left: &'a VenueId,
) -> (&'a VenueId, &'a VenueId) {
    first_limit_venue(cfg, buy, sell).unwrap_or_else(|| fallback_left(buy, sell, left))
}

/// 对称挂单：比较两种挂法的单边成本，取更便宜的。
///
/// 成本 = 挂单所 maker + 市价所 taker + 市价所点差中枢（限价所点差不进）。
/// `*_spread_pct` 两侧都有才把点差加进比较；缺一侧或打平则回退 [`first_limit_venue`]。
pub fn first_limit_venue_all_in<'a>(
    cfg: &AppConfig,
    buy: &'a VenueId,
    sell: &'a VenueId,
    buy_spread_pct: Option<Decimal>,
    sell_spread_pct: Option<Decimal>,
) -> Option<(&'a VenueId, &'a VenueId)> {
    if let (Some(cb), Some(cs)) = (buy_spread_pct, sell_spread_pct) {
        match limit_then_hedge_cost_cmp(
            cfg.maker_fee(buy),
            cfg.taker_fee(buy),
            cb,
            cfg.maker_fee(sell),
            cfg.taker_fee(sell),
            cs,
        ) {
            Ordering::Less => return Some((buy, sell)),
            Ordering::Greater => return Some((sell, buy)),
            Ordering::Equal => {}
        }
    }
    first_limit_venue(cfg, buy, sell)
}

/// 阶段 2 一格开平：\(F=2\times(\mathrm{maker}_{挂}+\mathrm{taker}_{市})\)，\(C=\) 市价所点差中枢。
///
/// 返回 `(F, 市价所 C)`。点差未满时 C 为 `None`（Δ 里当 0）。
pub fn symmetric_grid_costs(
    cfg: &AppConfig,
    left: &VenueId,
    right: &VenueId,
    left_spread_pct: Option<Decimal>,
    right_spread_pct: Option<Decimal>,
) -> (Decimal, Option<Decimal>) {
    let (first, second) = first_limit_venue_all_in_or_left(
        cfg,
        left,
        right,
        left,
        left_spread_pct,
        right_spread_pct,
    );
    let fee = (cfg.maker_fee(first) + cfg.taker_fee(second)) * Decimal::from(2);
    let hedge_c = if second.as_str() == left.as_str() {
        left_spread_pct
    } else {
        right_spread_pct
    };
    (fee, hedge_c)
}

pub fn first_limit_venue_all_in_or_left<'a>(
    cfg: &AppConfig,
    buy: &'a VenueId,
    sell: &'a VenueId,
    left: &'a VenueId,
    buy_spread_pct: Option<Decimal>,
    sell_spread_pct: Option<Decimal>,
) -> (&'a VenueId, &'a VenueId) {
    first_limit_venue_all_in(cfg, buy, sell, buy_spread_pct, sell_spread_pct)
        .unwrap_or_else(|| fallback_left(buy, sell, left))
}

fn fallback_left<'a>(
    buy: &'a VenueId,
    sell: &'a VenueId,
    left: &'a VenueId,
) -> (&'a VenueId, &'a VenueId) {
    if buy.as_str() == left.as_str() {
        (buy, sell)
    } else {
        (sell, buy)
    }
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
    use std::cmp::Ordering;
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
    fn all_in_prefers_wider_own_spread_when_fees_equal() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("lighter_rh");
        assert!(first_limit_venue(&cfg, &buy, &sell).is_none());
        let (first, second) = first_limit_venue_all_in(
            &cfg,
            &buy,
            &sell,
            Some(dec!(0.01)),
            Some(dec!(0.04)),
        )
        .unwrap();
        assert_eq!(first.as_str(), "lighter_rh");
        assert_eq!(second.as_str(), "lighter");
    }

    #[test]
    fn all_in_missing_spread_falls_back_to_fee() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let buy = VenueId::from("lighter");
        let sell = VenueId::from("lighter_rh");
        assert!(first_limit_venue_all_in(&cfg, &buy, &sell, Some(dec!(0.04)), None).is_none());
        let left = VenueId::from("lighter");
        let (first, _) = first_limit_venue_all_in_or_left(
            &cfg,
            &buy,
            &sell,
            &left,
            Some(dec!(0.04)),
            None,
        );
        assert_eq!(first.as_str(), "lighter");
    }

    #[test]
    fn all_in_spread_can_override_fee_rank() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let cheap = VenueId::from("lighter");
        let rich = VenueId::from("sodex");
        // 费率单独比：挂 sodex（maker）+ 吃 lighter（taker）更便宜。
        let (first, _) = first_limit_venue(&cfg, &cheap, &rich).unwrap();
        assert_eq!(first.as_str(), "sodex");
        // 市价吃 lighter 的点差中枢大到超过费率差 → 改挂 lighter、吃 sodex。
        let (first, second) = first_limit_venue_all_in(
            &cfg,
            &cheap,
            &rich,
            Some(dec!(0.05)),
            Some(dec!(0.01)),
        )
        .unwrap();
        assert_eq!(first.as_str(), "lighter");
        assert_eq!(second.as_str(), "sodex");
    }

    #[test]
    fn limit_then_hedge_uses_maker_on_posting_side() {
        // A taker 0.03 / maker 0.01；B taker 0.009 / maker 0.005。
        // A 挂：0.01 + 0.009 = 0.019；B 挂：0.005 + 0.03 = 0.035 → 挂 A。
        assert_eq!(
            limit_then_hedge_cost_cmp(
                dec!(0.01),
                dec!(0.03),
                Decimal::ZERO,
                dec!(0.005),
                dec!(0.009),
                Decimal::ZERO,
            ),
            Ordering::Less
        );
        // 开平一格 F = 2 × 0.019 = 0.038（不再用 2 × (0.03+0.009)=0.078）。
        let one_way = dec!(0.01) + dec!(0.009);
        assert_eq!(one_way * Decimal::from(2), dec!(0.038));
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
