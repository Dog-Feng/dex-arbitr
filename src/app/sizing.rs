use rust_decimal::Decimal;

use crate::config::SizingConfig;
use crate::domain::Bbo;

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

/// 本次要开/加 `add_qty` 币，两腿保证金与盘口深度是否撑得住。
/// 不再返回数量——数量由配置决定，这里只做否决。
pub fn check_capacity(
    cfg: &SizingConfig,
    add_qty: Decimal,
    buy: LegMargin,
    sell: LegMargin,
    buy_book: &Bbo,
    sell_book: &Bbo,
    mid_price: Decimal,
) -> Result<(), &'static str> {
    if mid_price <= Decimal::ZERO {
        return Err("no_mid");
    }
    if add_qty <= Decimal::ZERO {
        return Err("no_size");
    }
    let need = add_qty * mid_price;

    let util = cfg.margin_utilization_pct;
    if buy.free_notional(util) < need || sell.free_notional(util) < need {
        return Err("no_margin");
    }

    if cfg.depth_pct > Decimal::ZERO {
        let cap = buy_book.ask_qty.min(sell_book.bid_qty) * cfg.depth_pct / Decimal::from(100);
        if cap > Decimal::ZERO && add_qty > cap {
            return Err("thin_book");
        }
    }
    Ok(())
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
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn cfg() -> SizingConfig {
        SizingConfig {
            leverage_multiplier: dec!(5),
            depth_pct: dec!(50),
            refresh_balance_secs: 60,
            fallback_available_usdc: None,
            margin_utilization_pct: dec!(100),
            leverage_by_venue: Default::default(),
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
    fn ok_when_both_legs_cover_notional() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        assert!(check_capacity(
            &cfg(),
            dec!(0.001),
            margin(dec!(200), dec!(0)),
            margin(dec!(200), dec!(0)),
            &b,
            &b,
            dec!(100_000),
        )
        .is_ok());
    }

    #[test]
    fn no_margin_when_short_leg_cannot_cover() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        // 0.001 × 100000 = 100 名义；B 可用 20 × 2 杠杆 = 40 < 100
        assert_eq!(
            check_capacity(
                &cfg(),
                dec!(0.001),
                margin(dec!(200), dec!(0)),
                margin(dec!(20), dec!(0)),
                &b,
                &b,
                dec!(100_000),
            ),
            Err("no_margin")
        );
    }

    #[test]
    fn reserved_margin_reduces_free_notional() {
        let b = book(dec!(100_000), dec!(100_000), dec!(10));
        assert_eq!(
            check_capacity(
                &cfg(),
                dec!(0.001),
                margin(dec!(200), dec!(0)),
                margin(dec!(100), dec!(80)),
                &b,
                &b,
                dec!(100_000),
            ),
            Err("no_margin")
        );
    }

    #[test]
    fn thin_book_when_add_qty_exceeds_depth_pct() {
        let b = book(dec!(100), dec!(100), dec!(0.001));
        // depth cap = 0.001 × 50% = 0.0005 < 0.001
        assert_eq!(
            check_capacity(
                &cfg(),
                dec!(0.001),
                margin(dec!(10_000), dec!(0)),
                margin(dec!(10_000), dec!(0)),
                &b,
                &b,
                dec!(100),
            ),
            Err("thin_book")
        );
    }

    #[test]
    fn depth_pct_zero_skips_depth_check() {
        let mut c = cfg();
        c.depth_pct = Decimal::ZERO;
        let b = book(dec!(100), dec!(100), dec!(0.0001));
        assert!(check_capacity(
            &c,
            dec!(0.001),
            margin(dec!(10_000), dec!(0)),
            margin(dec!(10_000), dec!(0)),
            &b,
            &b,
            dec!(100),
        )
        .is_ok());
    }

    #[test]
    fn no_mid_when_price_invalid() {
        let b = book(dec!(100), dec!(100), dec!(10));
        assert_eq!(
            check_capacity(
                &cfg(),
                dec!(0.001),
                margin(dec!(200), dec!(0)),
                margin(dec!(200), dec!(0)),
                &b,
                &b,
                Decimal::ZERO,
            ),
            Err("no_mid")
        );
    }

    #[test]
    fn no_size_when_qty_zero() {
        let b = book(dec!(100), dec!(100), dec!(10));
        assert_eq!(
            check_capacity(
                &cfg(),
                Decimal::ZERO,
                margin(dec!(200), dec!(0)),
                margin(dec!(200), dec!(0)),
                &b,
                &b,
                dec!(100),
            ),
            Err("no_size")
        );
    }
}
