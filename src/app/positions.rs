use std::collections::{HashMap, HashSet};
use std::time::Instant;

use rust_decimal::Decimal;

use crate::domain::{Position, VenueId};

/// 内存持仓 + 挂单占槽。
///
/// **key 是 `slot_key`（币 + 所对），不是 pair_id**：三所两两组合下同一个
/// `pair_id` 会出现在 C(3,2) 条 `Pair` 里，只按 pair_id 索引会让它们共用一份
/// 持仓，另外两条组合看到方向不符就误判成反向仓。
#[derive(Default)]
pub struct PositionStore {
    positions: HashMap<String, Position>,
    pending_opens: HashSet<String>,
}

impl PositionStore {
    pub fn get(&self, slot: &str) -> Option<&Position> {
        self.positions.get(slot)
    }

    pub fn open_count(&self) -> usize {
        self.positions
            .values()
            .filter(|p| p.qty > Decimal::ZERO)
            .count()
    }

    pub fn active_slots(&self) -> usize {
        let mut n = self.open_count();
        for slot in &self.pending_opens {
            if !self
                .positions
                .get(slot)
                .is_some_and(|p| p.qty > Decimal::ZERO)
            {
                n += 1;
            }
        }
        n
    }

    pub fn can_open(&self, max_concurrent_pairs: u32) -> bool {
        self.active_slots() < max_concurrent_pairs.max(1) as usize
    }

    pub fn reserve_open(&mut self, slot: &str) {
        self.pending_opens.insert(slot.to_string());
    }

    pub fn release_pending(&mut self, slot: &str) {
        self.pending_opens.remove(slot);
    }

    /// 各所已占用保证金（名义 / 该所杠杆），buy/sell 两腿各计一份。
    pub fn reserved_margin_by_venue(
        &self,
        leverage: impl Fn(&str) -> Decimal,
    ) -> HashMap<String, Decimal> {
        let mut out: HashMap<String, Decimal> = HashMap::new();
        for pos in self.positions.values() {
            if pos.qty <= Decimal::ZERO || pos.entry_notional_usdc <= Decimal::ZERO {
                continue;
            }
            let buy_margin = pos.entry_notional_usdc / leverage(pos.buy.as_str()).max(Decimal::ONE);
            let sell_margin =
                pos.entry_notional_usdc / leverage(pos.sell.as_str()).max(Decimal::ONE);
            *out.entry(pos.buy.as_str().to_string()).or_default() += buy_margin;
            *out.entry(pos.sell.as_str().to_string()).or_default() += sell_margin;
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_open(
        &mut self,
        slot: &str,
        pair_id: &str,
        buy: VenueId,
        sell: VenueId,
        qty: Decimal,
        grid: u32,
        entry_notional_usdc: Decimal,
        entry_net_pct: Decimal,
        entry_raw_pct: Decimal,
    ) {
        self.pending_opens.remove(slot);
        if qty <= Decimal::ZERO {
            return;
        }
        // 同槽位补仓：按名义加权平均建仓净边，平仓才算得对往返净利。
        if let Some(prev) = self.positions.get(slot).filter(|p| p.qty > Decimal::ZERO) {
            if prev.buy == buy && prev.sell == sell {
                let total_qty = prev.qty + qty;
                let total_notional = prev.entry_notional_usdc + entry_notional_usdc;
                let weighted_net = if total_notional > Decimal::ZERO {
                    (prev.entry_net_pct * prev.entry_notional_usdc
                        + entry_net_pct * entry_notional_usdc)
                        / total_notional
                } else {
                    entry_net_pct
                };
                let weighted_raw = if total_notional > Decimal::ZERO {
                    (prev.entry_raw_pct * prev.entry_notional_usdc
                        + entry_raw_pct * entry_notional_usdc)
                        / total_notional
                } else {
                    entry_raw_pct
                };
                self.positions.insert(
                    slot.to_string(),
                    Position {
                        pair_id: pair_id.to_string(),
                        buy,
                        sell,
                        qty: total_qty,
                        grid: grid.max(prev.grid),
                        entry_notional_usdc: total_notional,
                        entry_net_pct: weighted_net,
                        entry_raw_pct: weighted_raw,
                        // 保留最早的建仓时刻，补仓不给仓位「续命」。
                        opened_at: prev.opened_at,
                    },
                );
                return;
            }
        }
        self.positions.insert(
            slot.to_string(),
            Position {
                pair_id: pair_id.to_string(),
                buy,
                sell,
                qty,
                grid,
                entry_notional_usdc,
                entry_net_pct,
                entry_raw_pct,
                opened_at: Instant::now(),
            },
        );
    }

    pub fn record_close(&mut self, slot: &str, qty: Decimal) {
        self.pending_opens.remove(slot);
        let Some(pos) = self.positions.get_mut(slot) else {
            return;
        };
        let before = pos.qty;
        pos.qty = (before - qty).max(Decimal::ZERO);
        if before > Decimal::ZERO {
            // 名义按比例缩减，剩余仓位的保证金占用才不会一直按开仓全量算。
            pos.entry_notional_usdc = pos.entry_notional_usdc * pos.qty / before;
        }
        if pos.qty.is_zero() {
            self.positions.remove(slot);
        }
    }

    /// 交易所实盘持仓与内存不一致时按实盘校正（只缩不放，避免放大敞口）。
    pub fn reconcile_qty(&mut self, slot: &str, exchange_qty: Decimal) -> Option<(Decimal, Decimal)> {
        let pos = self.positions.get_mut(slot)?;
        let before = pos.qty;
        if before <= Decimal::ZERO || exchange_qty >= before {
            return None;
        }
        pos.qty = exchange_qty.max(Decimal::ZERO);
        if before > Decimal::ZERO {
            pos.entry_notional_usdc = pos.entry_notional_usdc * pos.qty / before;
        }
        if pos.qty.is_zero() {
            self.positions.remove(slot);
        }
        Some((before, exchange_qty))
    }

    pub fn is_pending(&self, slot: &str) -> bool {
        self.pending_opens.contains(slot)
    }

    /// 该 pair_id 的持仓名义之和（USDC，建仓名义）。
    ///
    /// 按 pair_id 而不是 slot 汇总：三所两两组合下同一个币可能同时持有
    /// C(3,2) 条仓位，单币限额管的是这个币的**总敞口**，只看一条会漏。
    pub fn held_notional_for_pair(&self, pair_id: &str) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.pair_id == pair_id && p.qty > Decimal::ZERO)
            .map(|p| p.entry_notional_usdc)
            .sum()
    }

    /// 全部持仓名义之和（USDC，建仓名义）。
    pub fn held_notional_total(&self) -> Decimal {
        self.positions
            .values()
            .filter(|p| p.qty > Decimal::ZERO)
            .map(|p| p.entry_notional_usdc)
            .sum()
    }

    pub fn all_open(&self) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.qty > Decimal::ZERO)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn lev(_v: &str) -> Decimal {
        dec!(2)
    }

    const BTC_LS: &str = "BTC-USD-PERP|lighter|sodex";
    const BTC_LR: &str = "BTC-USD-PERP|lighter|lighter_rh";

    fn open(store: &mut PositionStore, slot: &str, qty: Decimal, notional: Decimal, net: Decimal) {
        store.record_open(
            slot,
            "BTC-USD-PERP",
            VenueId::from("lighter"),
            VenueId::from("sodex"),
            qty,
            1,
            notional,
            net,
            net,
        );
    }

    /// 开仓后槽位仍被占着：max_concurrent_pairs=1 时不能再开第二条。
    #[test]
    fn slot_limit_counts_open_and_pending() {
        let mut store = PositionStore::default();
        assert!(store.can_open(1));
        store.reserve_open(BTC_LS);
        assert!(!store.can_open(1));
        open(&mut store, BTC_LS, dec!(0.001), dec!(100), dec!(0.05));
        assert!(!store.can_open(1));
        assert_eq!(store.active_slots(), 1);
        assert!(store.can_open(2));
    }

    /// 已有仓再补仓：挂单占用同一槽，不额外占 max_concurrent_pairs。
    #[test]
    fn topping_up_existing_slot_does_not_consume_another() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.001), dec!(100), dec!(0.05));
        store.reserve_open(BTC_LS);
        assert_eq!(store.active_slots(), 1);
        assert!(!store.can_open(1));
        assert!(store.can_open(2));
    }

    /// 同一 pair_id 的不同所对是两条互不影响的仓位。
    #[test]
    fn same_pair_different_venue_combo_is_separate() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.001), dec!(100), dec!(0.05));
        assert!(store.get(BTC_LR).is_none());
        assert_eq!(store.get(BTC_LS).unwrap().qty, dec!(0.001));
        assert_eq!(store.open_count(), 1);
    }

    #[test]
    fn reserved_margin_per_venue() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.01), dec!(400), dec!(0.05));
        let reserved = store.reserved_margin_by_venue(lev);
        assert_eq!(reserved.get("lighter"), Some(&dec!(200)));
        assert_eq!(reserved.get("sodex"), Some(&dec!(200)));
    }

    /// 部分平仓后名义按比例缩，保证金占用不再按开仓全量算。
    #[test]
    fn partial_close_scales_notional() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.01), dec!(400), dec!(0.05));
        store.record_close(BTC_LS, dec!(0.0075));
        let pos = store.get(BTC_LS).unwrap();
        assert_eq!(pos.qty, dec!(0.0025));
        assert_eq!(pos.entry_notional_usdc, dec!(100));
    }

    #[test]
    fn add_on_averages_entry_net() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.001), dec!(100), dec!(0.06));
        open(&mut store, BTC_LS, dec!(0.001), dec!(100), dec!(0.04));
        let pos = store.get(BTC_LS).unwrap();
        assert_eq!(pos.qty, dec!(0.002));
        assert_eq!(pos.entry_net_pct, dec!(0.05));
        assert_eq!(pos.entry_raw_pct, dec!(0.05));
    }

    #[test]
    fn reconcile_shrinks_to_exchange_qty() {
        let mut store = PositionStore::default();
        open(&mut store, BTC_LS, dec!(0.01), dec!(400), dec!(0.05));
        assert_eq!(
            store.reconcile_qty(BTC_LS, dec!(0.004)),
            Some((dec!(0.01), dec!(0.004)))
        );
        assert_eq!(store.get(BTC_LS).unwrap().qty, dec!(0.004));
        // 实盘比内存多时不动（宁可少记，避免放大敞口）
        assert_eq!(store.reconcile_qty(BTC_LS, dec!(0.02)), None);
    }
}
