use rust_decimal::Decimal;

use crate::app::balance::VenueAccountCache;
use crate::domain::{Pair, VenueMarket};

/// 交易所真实持仓与内存不一致时的单边敞口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NakedExposure {
    pub pair_id: String,
    pub venue: String,
    pub qty: Decimal,
    pub counterparty: String,
    /// 仅 BotFailure 来源会触发自动补对冲；Foreign 为启动前已有仓位，只告警。
    pub source: NakedSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NakedSource {
    Foreign,
    BotFailure,
}

/// 从各所 account 快照检测「仅一边有仓」的单边敞口（含启动前已有仓位）。
pub fn detect_naked_exposures(pairs: &[Pair], accounts: &VenueAccountCache) -> Vec<NakedExposure> {
    detect_with_source(pairs, accounts, NakedSource::Foreign)
}

pub fn detect_bot_failure_exposures(
    pairs: &[Pair],
    accounts: &VenueAccountCache,
) -> Vec<NakedExposure> {
    detect_with_source(pairs, accounts, NakedSource::BotFailure)
}

fn detect_with_source(
    pairs: &[Pair],
    accounts: &VenueAccountCache,
    source: NakedSource,
) -> Vec<NakedExposure> {
    let mut out = Vec::new();
    for pair in pairs {
        let q0 = venue_position_qty(accounts, &pair.legs[0]);
        let q1 = venue_position_qty(accounts, &pair.legs[1]);
        if q0.is_zero() && q1.is_zero() {
            continue;
        }
        let tol = pair.legs[0].min_qty.max(pair.legs[1].min_qty);
        if !q0.is_zero() && q1.is_zero() {
            out.push(NakedExposure {
                pair_id: pair.pair_id.clone(),
                venue: pair.legs[0].venue.as_str().to_string(),
                qty: q0,
                counterparty: pair.legs[1].venue.as_str().to_string(),
                source,
            });
        } else if q0.is_zero() && !q1.is_zero() {
            out.push(NakedExposure {
                pair_id: pair.pair_id.clone(),
                venue: pair.legs[1].venue.as_str().to_string(),
                qty: q1,
                counterparty: pair.legs[0].venue.as_str().to_string(),
                source,
            });
        } else if q0.is_sign_positive() == q1.is_sign_positive() {
            // 同向：两边都多或都空，无法自动配对
            let larger = if q0.abs() >= q1.abs() {
                (0, q0)
            } else {
                (1, q1)
            };
            out.push(NakedExposure {
                pair_id: pair.pair_id.clone(),
                venue: pair.legs[larger.0].venue.as_str().to_string(),
                qty: larger.1,
                counterparty: pair.legs[1 - larger.0].venue.as_str().to_string(),
                source,
            });
        } else {
            let imbalance = (q0 + q1).abs();
            if imbalance > tol {
                let (leg_i, qty) = if q0.abs() > q1.abs() {
                    (0, q0 + q1)
                } else {
                    (1, q0 + q1)
                };
                if !qty.is_zero() {
                    out.push(NakedExposure {
                        pair_id: pair.pair_id.clone(),
                        venue: pair.legs[leg_i].venue.as_str().to_string(),
                        qty,
                        counterparty: pair.legs[1 - leg_i].venue.as_str().to_string(),
                        source,
                    });
                }
            }
        }
    }
    out
}

pub fn leg_position_qty(accounts: &VenueAccountCache, leg: &VenueMarket) -> Decimal {
    venue_position_qty(accounts, leg)
}

pub fn symbol_matches_symbol(pos_symbol: &str, leg_symbol: &str, leg_base: &str) -> bool {
    let s = pos_symbol.trim().to_ascii_uppercase();
    let raw = leg_symbol.trim().to_ascii_uppercase();
    let base = leg_base.trim().to_ascii_uppercase();
    s == raw || s == base || s.starts_with(&format!("{base}-"))
}

/// 相对 baseline 的第一腿新增成交量（限价成交检测）。
pub fn first_leg_fill_delta(
    baseline: Decimal,
    current: Decimal,
    is_buy: bool,
    plan_qty: Decimal,
    min_qty: Decimal,
) -> Option<Decimal> {
    let delta = if is_buy {
        current - baseline
    } else {
        baseline - current
    };
    if delta >= min_qty {
        Some(delta.min(plan_qty))
    } else {
        None
    }
}

pub fn counterparty_hedge_is_buy(naked_qty: Decimal) -> bool {
    naked_qty.is_sign_negative()
}

pub fn hedge_qty(naked_qty: Decimal) -> Decimal {
    naked_qty.abs()
}

fn venue_position_qty(accounts: &VenueAccountCache, leg: &VenueMarket) -> Decimal {
    let Some(acct) = accounts.get(leg.venue.as_str()) else {
        return Decimal::ZERO;
    };
    acct.positions
        .iter()
        .filter(|p| symbol_matches(&p.symbol, leg))
        .map(|p| p.qty)
        .sum()
}

fn symbol_matches(pos_symbol: &str, leg: &VenueMarket) -> bool {
    symbol_matches_symbol(pos_symbol, &leg.raw_symbol, &leg.base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::balance::{VenueAccountSnapshot, VenueExchangePosition};
    use crate::domain::VenueId;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn mon_pair() -> Pair {
        Pair {
            pair_id: "MON-USD-PERP".into(),
            legs: [
                VenueMarket {
                    venue: VenueId::from("sodex"),
                    raw_symbol: "MON".into(),
                    pair_id: "MON-USD-PERP".into(),
                    base: "MON".into(),
                    market_index: 1,
                    qty_precision: 0,
                    min_qty: dec!(1),
                },
                VenueMarket {
                    venue: VenueId::from("lighter"),
                    raw_symbol: "MON".into(),
                    pair_id: "MON-USD-PERP".into(),
                    base: "MON".into(),
                    market_index: 2,
                    qty_precision: 0,
                    min_qty: dec!(1),
                },
            ],
        }
    }

    #[test]
    fn detects_single_venue_long() {
        let accounts = VenueAccountCache {
            venues: vec![VenueAccountSnapshot {
                venue: "sodex".into(),
                available: dec!(100),
                total: dec!(100),
                positions: vec![VenueExchangePosition {
                    symbol: "MON".into(),
                    qty: dec!(676),
                    entry_price: None,
                }],
            }],
            last_refresh: Instant::now(),
        };
        let naked = detect_naked_exposures(&[mon_pair()], &accounts);
        assert_eq!(naked.len(), 1);
        assert_eq!(naked[0].venue, "sodex");
        assert_eq!(naked[0].qty, dec!(676));
        assert_eq!(naked[0].counterparty, "lighter");
        assert_eq!(naked[0].source, NakedSource::Foreign);
    }

    #[test]
    fn fill_delta_detects_buy_fill() {
        assert_eq!(
            first_leg_fill_delta(dec!(0), dec!(32.43), true, dec!(32.43), dec!(1)),
            Some(dec!(32.43))
        );
    }

    #[test]
    fn fill_delta_detects_sell_fill() {
        assert_eq!(
            first_leg_fill_delta(dec!(0), dec!(-0.5), false, dec!(0.5), dec!(0.01)),
            Some(dec!(0.5))
        );
    }

    #[test]
    fn hedge_direction_for_long() {
        assert!(!counterparty_hedge_is_buy(dec!(676)));
        assert!(counterparty_hedge_is_buy(dec!(-676)));
    }
}
