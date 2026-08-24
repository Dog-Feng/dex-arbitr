use rust_decimal::Decimal;

use crate::config::{SizingConfig, SizingMode};
use crate::domain::{Bbo, VenueMarket};

/// 单所可用保证金上下文（已扣占用、已乘利用率）。
#[derive(Debug, Clone, Copy)]
pub struct LegMargin {
    pub available_usdc: Decimal,
    pub leverage: Decimal,
    pub reserved_usdc: Decimal,
}

impl LegMargin {
    pub fn free_notional(&self, utilization_pct: Decimal) -> Decimal {
        let util = utilization_pct / Decimal::from(100);
        let free = (self.available_usdc - self.reserved_usdc).max(Decimal::ZERO);
        free * util * self.leverage
    }
}

/// 定仓以哪条腿的可开名义为上限（保证金更紧的一所）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingLeg {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub struct ResolveQtyResult {
    pub qty: Decimal,
    pub notional_usdc: Decimal,
    /// 本笔仓位由该腿保证金瓶颈决定；A/B 两腿下 **同一 qty**。
    pub binding: BindingLeg,
}

/// 定仓：以 buy/sell **可开名义较小** 的一腿为准（等价于保证金短板），
/// 再与全局每对预算、深度、精度对齐。不会 A 大 B 小各下一档。
pub fn resolve_qty(
    cfg: &SizingConfig,
    global_min_available: Decimal,
    buy: LegMargin,
    sell: LegMargin,
    active_pairs: u32,
    buy_book: &Bbo,
    sell_book: &Bbo,
    mid_price: Decimal,
    buy_leg: &VenueMarket,
    sell_leg: &VenueMarket,
) -> Option<ResolveQtyResult> {
    if mid_price <= Decimal::ZERO {
        return None;
    }
    let slots = cfg.max_concurrent_pairs.max(1);
    if active_pairs >= slots {
        return None;
    }

    let buy_notional = buy.free_notional(cfg.margin_utilization_pct);
    let sell_notional = sell.free_notional(cfg.margin_utilization_pct);
    let (pair_cap, binding) = if buy_notional <= sell_notional {
        (buy_notional, BindingLeg::Buy)
    } else {
        (sell_notional, BindingLeg::Sell)
    };

    let global_lev = cfg.leverage_multiplier;
    let mut notional = match cfg.mode {
        SizingMode::Fixed => {
            if pair_cap < cfg.fixed_notional_usdc {
                return None;
            }
            cfg.fixed_notional_usdc
        }
        SizingMode::Margin => {
            let global_per_pair = global_min_available * global_lev / Decimal::from(slots);
            if pair_cap < cfg.min_notional_usdc {
                return None;
            }
            let mut n = pair_cap
                .min(global_per_pair)
                .min(cfg.max_notional_usdc)
                .max(cfg.min_notional_usdc);
            if n > pair_cap {
                n = pair_cap;
            }
            n
        }
    };

    let depth_cap = buy_book
        .ask_qty
        .min(sell_book.bid_qty)
        * cfg.depth_pct
        / Decimal::from(100);
    if depth_cap > Decimal::ZERO {
        notional = notional.min(depth_cap * mid_price);
    }

    let precision = buy_leg.qty_precision.min(sell_leg.qty_precision);
    let min_qty = buy_leg.min_qty.max(sell_leg.min_qty);
    let qty = (notional / mid_price).round_dp(precision);
    if qty < min_qty {
        return None;
    }
    Some(ResolveQtyResult {
        qty,
        notional_usdc: notional,
        binding,
    })
}

pub fn mid_from_bbo(b0: &Bbo, b1: &Bbo) -> Option<Decimal> {
    let a = (b0.bid + b0.ask) / Decimal::from(2);
    let b = (b1.bid + b1.ask) / Decimal::from(2);
    if a <= Decimal::ZERO || b <= Decimal::ZERO {
        return None;
    }
    Some((a + b) / Decimal::from(2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::VenueId;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn cfg() -> SizingConfig {
        SizingConfig {
            mode: SizingMode::Margin,
            fixed_notional_usdc: dec!(20),
            max_concurrent_pairs: 1,
            leverage_multiplier: dec!(2),
            min_notional_usdc: dec!(20),
            max_notional_usdc: dec!(500),
            depth_pct: dec!(50),
            refresh_balance_secs: 60,
            fallback_available_usdc: None,
            margin_utilization_pct: dec!(100),
            leverage_by_venue: Default::default(),
        }
    }

    fn leg(venue: &str, min_qty: Decimal, precision: u32) -> VenueMarket {
        VenueMarket {
            venue: VenueId::from(venue),
            raw_symbol: "BTC".into(),
            pair_id: "BTC-USD-PERP".into(),
            base: "BTC".into(),
            market_index: 1,
            qty_precision: precision,
            min_qty,
        }
    }

    fn book(bid: Decimal, ask: Decimal, qty: Decimal) -> Bbo {
        Bbo {
            bid,
            ask,
            bid_qty: qty,
            ask_qty: qty,
            bids: vec![(bid, qty)],
            asks: vec![(ask, qty)],
            ts: Instant::now(),
        }
    }

    fn margin(avail: Decimal, reserved: Decimal) -> LegMargin {
        LegMargin {
            available_usdc: avail,
            leverage: dec!(2),
            reserved_usdc: reserved,
        }
    }

    #[test]
    fn a200_b100_sizes_by_b_not_a() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        let a_leg = leg("lighter", dec!(0.0001), 5);
        let b_leg = leg("sodex", dec!(0.0001), 5);
        // A 200×2=400 名义，B 100×2=200 名义 → 按 B 的 100 保证金定仓
        let r = resolve_qty(
            &cfg(),
            dec!(100),
            margin(dec!(200), dec!(0)),
            margin(dec!(100), dec!(0)),
            0,
            &b,
            &b,
            dec!(100_000),
            &a_leg,
            &b_leg,
        )
        .unwrap();
        assert_eq!(r.binding, BindingLeg::Sell);
        assert_eq!(r.notional_usdc, dec!(200));
        assert_eq!(r.qty, dec!(0.002));
    }

    #[test]
    fn pair_cap_uses_shorter_leg() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        let buy_leg = leg("lighter", dec!(0.0001), 5);
        let sell_leg = leg("sodex", dec!(0.0001), 5);
        let r = resolve_qty(
            &cfg(),
            dec!(100),
            margin(dec!(500), dec!(0)),
            margin(dec!(100), dec!(0)),
            0,
            &b,
            &b,
            dec!(100_000),
            &buy_leg,
            &sell_leg,
        )
        .unwrap();
        assert_eq!(r.qty, dec!(0.002));
        assert_eq!(r.binding, BindingLeg::Sell);
    }

    #[test]
    fn reserved_margin_reduces_cap() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        let buy_leg = leg("lighter", dec!(0.0001), 5);
        let sell_leg = leg("sodex", dec!(0.0001), 5);
        let r = resolve_qty(
            &cfg(),
            dec!(500),
            margin(dec!(500), dec!(0)),
            margin(dec!(100), dec!(80)),
            0,
            &b,
            &b,
            dec!(100_000),
            &buy_leg,
            &sell_leg,
        )
        .unwrap();
        assert_eq!(r.qty, dec!(0.0004));
    }

    #[test]
    fn fixed_mode_uses_constant_notional() {
        let mut cfg = cfg();
        cfg.mode = SizingMode::Fixed;
        cfg.fixed_notional_usdc = dec!(20);
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        let buy_leg = leg("lighter", dec!(0.0001), 5);
        let sell_leg = leg("sodex", dec!(0.0001), 5);
        let r = resolve_qty(
            &cfg,
            dec!(500),
            margin(dec!(500), dec!(0)),
            margin(dec!(500), dec!(0)),
            0,
            &b,
            &b,
            dec!(100_000),
            &buy_leg,
            &sell_leg,
        )
        .unwrap();
        assert_eq!(r.notional_usdc, dec!(20));
        assert_eq!(r.qty, dec!(0.0002));
    }

    #[test]
    fn fixed_mode_skips_when_margin_insufficient() {
        let mut cfg = cfg();
        cfg.mode = SizingMode::Fixed;
        cfg.fixed_notional_usdc = dec!(20);
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        let buy_leg = leg("lighter", dec!(0.0001), 5);
        let sell_leg = leg("sodex", dec!(0.0001), 5);
        assert!(resolve_qty(
            &cfg,
            dec!(5),
            margin(dec!(5), dec!(0)),
            margin(dec!(5), dec!(0)),
            0,
            &b,
            &b,
            dec!(100_000),
            &buy_leg,
            &sell_leg,
        )
        .is_none());
    }

    #[test]
    fn respects_coarser_precision() {
        let b = book(dec!(100), dec!(100), dec!(100));
        let buy_leg = leg("a", dec!(0.01), 2);
        let sell_leg = leg("b", dec!(0.01), 1);
        let r = resolve_qty(
            &cfg(),
            dec!(10_000),
            margin(dec!(10_000), dec!(0)),
            margin(dec!(10_000), dec!(0)),
            0,
            &b,
            &b,
            dec!(100),
            &buy_leg,
            &sell_leg,
        )
        .unwrap();
        assert_eq!(r.qty, dec!(5.0));
    }
}
