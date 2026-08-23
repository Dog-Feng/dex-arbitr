use rust_decimal::Decimal;

use crate::config::AppConfig;
use crate::domain::{Bbo, Pair};

pub fn books_tradable(cfg: &AppConfig, pair: &Pair, buy: &Bbo, sell: &Bbo) -> Result<(), &'static str> {
    if !buy.is_fresh(cfg.system.data_freshness_ms) || !sell.is_fresh(cfg.system.data_freshness_ms) {
        return Err("stale");
    }
    if !buy.valid() || !sell.valid() {
        return Err("invalid_bbo");
    }
    let min_qty = cfg
        .min_book_qty(&pair.legs[0].base)
        .max(cfg.grid_for(&pair.legs[0].base).base_qty);
    if buy.ask_depth() < min_qty
        || buy.bid_depth() < min_qty
        || sell.ask_depth() < min_qty
        || sell.bid_depth() < min_qty
    {
        return Err("thin_book");
    }
    Ok(())
}

/// P1: stables treated 1:1. Hook stays so a later feed can trip the fuse.
pub fn stable_ok(cfg: &AppConfig, usdc_usdg: Decimal) -> bool {
    let one = Decimal::ONE;
    let max = Decimal::from(cfg.system.stable_depeg_bps) / Decimal::from(10_000);
    let dev = if usdc_usdg > one {
        usdc_usdg - one
    } else {
        one - usdc_usdg
    };
    dev <= max
}
