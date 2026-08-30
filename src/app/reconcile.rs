use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::app::balance::VenueAccountCache;
use crate::domain::{Pair, VenueMarket};

/// 交易所真实持仓与内存不一致时的单边敞口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NakedExposure {
    pub pair_id: String,
    pub venue: String,
    /// 带符号：正 = 多头敞口，负 = 空头敞口。
    pub qty: Decimal,
    pub counterparty: String,
    /// 仅 BotFailure 来源会触发自动补对冲；Foreign 为启动前已有仓位，只告警。
    pub source: NakedSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NakedSource {
    Foreign,
    BotFailure,
    /// 第二腿结果不可知：不触发自动补对冲，必须人工核查后再 resume。
    /// 自动补对冲会在第二腿实际已成交的情况下制造双倍敞口。
    SecondLegUnknown,
}

/// 单边敞口检测。
///
/// 按 **pair_id 聚合各所净持仓**，而不是逐条 `Pair` 判断：三所两两组合下同一个
/// pair_id 有 C(3,2) 条 Pair，逐条判会把一笔单边仓位报成多条（counterparty 还各不
/// 相同）。真正要看的是「这个币在我们交易的这些所上加总是否中性」。
///
/// 账户快照不完整时返回空：`positions` 为空只代表查询失败，不代表没有仓位，
/// 拿它去判「单边」会误报甚至误触发补单。
pub fn detect_naked_exposures(pairs: &[Pair], accounts: &VenueAccountCache) -> Vec<NakedExposure> {
    if !accounts.all_fresh() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (pair_id, legs) in group_by_pair(pairs) {
        let tol = legs
            .iter()
            .map(|(_, l)| l.min_qty)
            .max()
            .unwrap_or(Decimal::ZERO);
        let held: Vec<(String, Decimal)> = legs
            .iter()
            .map(|(venue, leg)| (venue.clone(), venue_position_qty(accounts, leg)))
            .collect();
        let net: Decimal = held.iter().map(|(_, q)| *q).sum();
        if net.abs() <= tol {
            continue;
        }
        // 敞口方向与净额同向、绝对值最大的那一所是主要来源。
        let Some((venue, _)) = held
            .iter()
            .filter(|(_, q)| q.is_sign_positive() == net.is_sign_positive() && !q.is_zero())
            .max_by_key(|(_, q)| q.abs())
        else {
            continue;
        };
        // 对手方：优先挑仓位为 0 的所（补上去就中性），否则任意另一所。
        let counterparty = held
            .iter()
            .filter(|(v, _)| v != venue)
            .min_by_key(|(_, q)| q.abs())
            .map(|(v, _)| v.clone());
        let Some(counterparty) = counterparty else {
            continue;
        };
        out.push(NakedExposure {
            pair_id,
            venue: venue.clone(),
            qty: net,
            counterparty,
            source: NakedSource::Foreign,
        });
    }
    out
}

/// 内存持仓 vs 交易所实盘的数量偏差。返回 `(内存量, 实盘重叠对冲量)`。
///
/// 只有一腿非零时不当成「对冲量 = 0」：第二腿账户快照经常晚几秒，
/// 这时缩内存会把刚成交的仓抹掉。两腿同号且都非零时对冲量视为 0。
pub fn audit_position_qty(
    pair: &Pair,
    accounts: &VenueAccountCache,
    memory_qty: Decimal,
) -> Option<(Decimal, Decimal)> {
    if !accounts.all_fresh() || memory_qty <= Decimal::ZERO {
        return None;
    }
    let a = venue_position_qty(accounts, &pair.legs[0]);
    let b = venue_position_qty(accounts, &pair.legs[1]);
    if a.is_zero() ^ b.is_zero() {
        return None;
    }
    if !a.is_zero() && !b.is_zero() && a.is_sign_positive() == b.is_sign_positive() {
        return Some((memory_qty, Decimal::ZERO));
    }
    let hedged = a.abs().min(b.abs());
    let tol = pair.min_qty();
    if (memory_qty - hedged).abs() <= tol {
        return None;
    }
    Some((memory_qty, hedged))
}

/// 两所已有反向仓时的重叠对冲量（内存为空时用来把库存捡回来）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OppositeHedge {
    pub qty: Decimal,
    pub buy: String,
    pub sell: String,
    pub buy_px: Decimal,
    pub sell_px: Decimal,
}

pub fn exchange_opposite_hedge(
    pair: &Pair,
    accounts: &VenueAccountCache,
) -> Option<OppositeHedge> {
    if !accounts.all_fresh() {
        return None;
    }
    let (a_qty, a_px) = venue_leg(accounts, &pair.legs[0]);
    let (b_qty, b_px) = venue_leg(accounts, &pair.legs[1]);
    if a_qty.is_zero() || b_qty.is_zero() {
        return None;
    }
    if a_qty.is_sign_positive() == b_qty.is_sign_positive() {
        return None;
    }
    let qty = a_qty.abs().min(b_qty.abs());
    if qty <= Decimal::ZERO {
        return None;
    }
    let (buy, sell, buy_px, sell_px) = if a_qty > Decimal::ZERO {
        (
            pair.legs[0].venue.to_string(),
            pair.legs[1].venue.to_string(),
            a_px,
            b_px,
        )
    } else {
        (
            pair.legs[1].venue.to_string(),
            pair.legs[0].venue.to_string(),
            b_px,
            a_px,
        )
    };
    Some(OppositeHedge {
        qty,
        buy,
        sell,
        buy_px,
        sell_px,
    })
}

/// 按重叠量反推有符号 STEP。至少 ±1，不超过 `max_step`。
pub fn hedge_grid_step(qty: Decimal, base_qty: Decimal, max_step: i32, plus: bool) -> i32 {
    let cap = max_step.max(1);
    let steps = if base_qty > Decimal::ZERO {
        (qty / base_qty).round()
    } else {
        Decimal::ONE
    };
    let n = i32::try_from(steps.trunc().mantissa().max(1)).unwrap_or(1);
    let k = n.clamp(1, cap);
    if plus {
        k
    } else {
        -k
    }
}

fn group_by_pair(pairs: &[Pair]) -> Vec<(String, Vec<(String, VenueMarket)>)> {
    let mut map: BTreeMap<String, Vec<(String, VenueMarket)>> = BTreeMap::new();
    for pair in pairs {
        let entry = map.entry(pair.pair_id.clone()).or_default();
        for leg in &pair.legs {
            let venue = leg.venue.as_str().to_string();
            if !entry.iter().any(|(v, _)| *v == venue) {
                entry.push((venue, leg.clone()));
            }
        }
    }
    map.into_iter().collect()
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
    if delta >= min_qty && delta > Decimal::ZERO {
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
    venue_leg(accounts, leg).0
}

fn venue_leg(accounts: &VenueAccountCache, leg: &VenueMarket) -> (Decimal, Decimal) {
    let Some(acct) = accounts.get(leg.venue.as_str()) else {
        return (Decimal::ZERO, Decimal::ZERO);
    };
    let mut qty = Decimal::ZERO;
    let mut px_qty = Decimal::ZERO;
    let mut px_notional = Decimal::ZERO;
    for p in acct
        .positions
        .iter()
        .filter(|p| symbol_matches(&p.symbol, leg))
    {
        qty += p.qty;
        if let Some(px) = p.entry_price.filter(|x| *x > Decimal::ZERO) {
            let w = p.qty.abs();
            px_qty += w;
            px_notional += px * w;
        }
    }
    let px = if px_qty > Decimal::ZERO {
        px_notional / px_qty
    } else {
        Decimal::ZERO
    };
    (qty, px)
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

    fn mkt(venue: &str) -> VenueMarket {
        VenueMarket {
            venue: VenueId::from(venue),
            raw_symbol: "MON".into(),
            pair_id: "MON-USD-PERP".into(),
            base: "MON".into(),
            market_index: 1,
            qty_precision: 0,
            min_qty: dec!(1),
            volume_24h_usdc: None,
        }
    }

    fn pair(a: &str, b: &str) -> Pair {
        Pair {
            pair_id: "MON-USD-PERP".into(),
            legs: [mkt(a), mkt(b)],
        }
    }

    fn snap(venue: &str, qty: Option<Decimal>, fresh: bool) -> VenueAccountSnapshot {
        VenueAccountSnapshot {
            venue: venue.into(),
            available: dec!(100),
            total: dec!(100),
            positions: qty
                .map(|q| {
                    vec![VenueExchangePosition {
                        symbol: "MON".into(),
                        qty: q,
                        entry_price: None,
                        realized_pnl: None,
                    }]
                })
                .unwrap_or_default(),
            fresh,
        }
    }

    fn cache(venues: Vec<VenueAccountSnapshot>) -> VenueAccountCache {
        VenueAccountCache {
            venues,
            last_refresh: Instant::now(),
        }
    }

    #[test]
    fn detects_single_venue_long() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(676)), true),
            snap("lighter", None, true),
            snap("lighter_rh", None, true),
        ]);
        let naked = detect_naked_exposures(&[pair("sodex", "lighter")], &accounts);
        assert_eq!(naked.len(), 1);
        assert_eq!(naked[0].venue, "sodex");
        assert_eq!(naked[0].qty, dec!(676));
        assert_eq!(naked[0].counterparty, "lighter");
    }

    /// 三所两两组合下，一笔单边仓位只报一条，不是每条 Pair 报一次。
    #[test]
    fn one_exposure_per_pair_id_across_venue_combos() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(676)), true),
            snap("lighter", None, true),
            snap("lighter_rh", None, true),
        ]);
        let pairs = [
            pair("lighter", "lighter_rh"),
            pair("lighter", "sodex"),
            pair("lighter_rh", "sodex"),
        ];
        let naked = detect_naked_exposures(&pairs, &accounts);
        assert_eq!(naked.len(), 1);
        assert_eq!(naked[0].venue, "sodex");
    }

    /// 已经对冲好的仓位不算敞口。
    #[test]
    fn hedged_pair_is_not_naked() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(676)), true),
            snap("lighter", Some(dec!(-676)), true),
        ]);
        assert!(detect_naked_exposures(&[pair("sodex", "lighter")], &accounts).is_empty());
    }

    /// 快照不完整时不做判定，避免把「查不到」当成「没有仓位」。
    #[test]
    fn stale_snapshot_reports_nothing() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(676)), true),
            snap("lighter", None, false),
        ]);
        assert!(detect_naked_exposures(&[pair("sodex", "lighter")], &accounts).is_empty());
    }

    #[test]
    fn audit_flags_memory_over_exchange() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(600)), true),
            snap("lighter", Some(dec!(-600)), true),
        ]);
        let p = pair("sodex", "lighter");
        assert_eq!(audit_position_qty(&p, &accounts, dec!(676)), Some((dec!(676), dec!(600))));
        assert_eq!(audit_position_qty(&p, &accounts, dec!(600)), None);
    }

    #[test]
    fn audit_flags_memory_under_exchange() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(45)), true),
            snap("lighter", Some(dec!(-45)), true),
        ]);
        let p = pair("sodex", "lighter");
        assert_eq!(
            audit_position_qty(&p, &accounts, dec!(30)),
            Some((dec!(30), dec!(45)))
        );
    }

    #[test]
    fn audit_same_direction_is_zero_hedge() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(10)), true),
            snap("lighter", Some(dec!(5)), true),
        ]);
        let p = pair("sodex", "lighter");
        assert_eq!(
            audit_position_qty(&p, &accounts, dec!(5)),
            Some((dec!(5), Decimal::ZERO))
        );
    }

    #[test]
    fn audit_skips_one_legged_snapshot() {
        let accounts = cache(vec![
            snap("sodex", Some(dec!(0.0005)), true),
            snap("lighter", None, true),
        ]);
        let p = pair("sodex", "lighter");
        assert_eq!(audit_position_qty(&p, &accounts, dec!(0.0005)), None);
    }

    #[test]
    fn opposite_hedge_uses_overlap() {
        let accounts = cache(vec![
            snap("lighter", Some(dec!(0.0005)), true),
            snap("lighter_rh", Some(dec!(-0.00056)), true),
        ]);
        let p = pair("lighter", "lighter_rh");
        let h = exchange_opposite_hedge(&p, &accounts).unwrap();
        assert_eq!(h.qty, dec!(0.0005));
        assert_eq!(h.buy, "lighter");
        assert_eq!(h.sell, "lighter_rh");
        assert_eq!(hedge_grid_step(h.qty, dec!(0.0005), 3, false), -1);
    }

    #[test]
    fn fill_delta_detects_both_sides() {
        assert_eq!(
            first_leg_fill_delta(dec!(0), dec!(32.43), true, dec!(32.43), dec!(1)),
            Some(dec!(32.43))
        );
        assert_eq!(
            first_leg_fill_delta(dec!(0), dec!(-0.5), false, dec!(0.5), dec!(0.01)),
            Some(dec!(0.5))
        );
        // 没有新成交时不能返回 0
        assert_eq!(
            first_leg_fill_delta(dec!(5), dec!(5), true, dec!(1), Decimal::ZERO),
            None
        );
    }

    #[test]
    fn hedge_direction_for_long() {
        assert!(!counterparty_hedge_is_buy(dec!(676)));
        assert!(counterparty_hedge_is_buy(dec!(-676)));
    }
}
