use rust_decimal::Decimal;

use crate::config::AppConfig;
use crate::domain::{Bbo, Pair};

/// 新鲜度 + 合法 BBO。
pub fn books_fresh_ok(cfg: &AppConfig, buy: &Bbo, sell: &Bbo) -> Result<(), &'static str> {
    if !buy.is_fresh(cfg.system.data_freshness_ms) || !sell.is_fresh(cfg.system.data_freshness_ms) {
        return Err("stale");
    }
    if !buy.valid() || !sell.valid() {
        return Err("invalid_bbo");
    }
    Ok(())
}

/// 数据质量门槛：新鲜度、合法 BBO。
pub fn books_quality_ok(cfg: &AppConfig, buy: &Bbo, sell: &Bbo) -> Result<(), &'static str> {
    books_fresh_ok(cfg, buy, sell)
}

/// 开仓门槛：数据质量 + 一档深度（按每格数量 / 两腿 min_qty）。
pub fn books_tradable(
    cfg: &AppConfig,
    pair: &Pair,
    buy: &Bbo,
    sell: &Bbo,
    probe_qty: Decimal,
) -> Result<(), &'static str> {
    books_quality_ok(cfg, buy, sell)?;

    let base = &pair.legs[0].base;
    let min_qty = cfg
        .grid_for(
            base,
            pair.legs[0].venue.as_str(),
            pair.legs[1].venue.as_str(),
            pair.min_qty(),
        )
        .map(|p| p.base_qty)
        .unwrap_or(Decimal::ZERO)
        .max(probe_qty)
        .max(pair.min_qty());
    if min_qty <= Decimal::ZERO {
        return Err("no_min_qty");
    }
    if buy.ask_qty < min_qty
        || buy.bid_qty < min_qty
        || sell.ask_qty < min_qty
        || sell.bid_qty < min_qty
    {
        return Err("thin_book");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{VenueId, VenueMarket};
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn pair() -> Pair {
        let leg = |v: &str| VenueMarket {
            venue: VenueId::from(v),
            raw_symbol: "BTC".into(),
            pair_id: "BTC-USD-PERP".into(),
            base: "BTC".into(),
            market_index: 1,
            qty_precision: 5,
            min_qty: dec!(0.0002),
        };
        Pair {
            pair_id: "BTC-USD-PERP".into(),
            legs: [leg("lighter"), leg("sodex")],
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

    fn cfg() -> AppConfig {
        AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap()
    }

    /// probe_qty 为 0 时仍要按两腿 min_qty 校验厚度。
    #[test]
    fn thin_check_falls_back_to_leg_min_qty() {
        let thin = book(dec!(100.00), dec!(100.02), dec!(0.0001));
        assert_eq!(
            books_tradable(&cfg(), &pair(), &thin, &thin, Decimal::ZERO),
            Err("thin_book")
        );
    }

    #[test]
    fn rejects_locked_book() {
        let locked = book(dec!(100), dec!(100), dec!(1));
        assert_eq!(
            books_tradable(&cfg(), &pair(), &locked, &locked, dec!(0.001)),
            Err("invalid_bbo")
        );
    }
}
