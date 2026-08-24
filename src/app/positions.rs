use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;

use crate::domain::{Position, VenueId};

/// 内存持仓 + 挂单占槽，对齐参考项目 per-symbol 槽位计数。
#[derive(Default)]
pub struct PositionStore {
    positions: HashMap<String, Position>,
    pending_opens: HashSet<String>,
}

impl PositionStore {
    pub fn get(&self, pair_id: &str) -> Option<&Position> {
        self.positions.get(pair_id)
    }

    pub fn get_mut(&mut self, pair_id: &str) -> Option<&mut Position> {
        self.positions.get_mut(pair_id)
    }

    pub fn open_count(&self) -> usize {
        self.positions
            .values()
            .filter(|p| p.qty > Decimal::ZERO)
            .count()
    }

    pub fn active_slots(&self) -> usize {
        self.open_count() + self.pending_opens.len()
    }

    pub fn can_open(&self, max_concurrent_pairs: u32) -> bool {
        self.active_slots() < max_concurrent_pairs.max(1) as usize
    }

    pub fn reserve_open(&mut self, pair_id: &str) {
        self.pending_opens.insert(pair_id.to_string());
    }

    pub fn release_pending(&mut self, pair_id: &str) {
        self.pending_opens.remove(pair_id);
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

    pub fn record_open(
        &mut self,
        pair_id: &str,
        buy: VenueId,
        sell: VenueId,
        qty: Decimal,
        grid: u32,
        entry_notional_usdc: Decimal,
    ) {
        self.pending_opens.remove(pair_id);
        self.positions.insert(
            pair_id.to_string(),
            Position {
                pair_id: pair_id.to_string(),
                buy,
                sell,
                qty,
                grid,
                entry_notional_usdc,
            },
        );
    }

    pub fn record_close(&mut self, pair_id: &str, qty: Decimal) {
        self.pending_opens.remove(pair_id);
        if let Some(pos) = self.positions.get_mut(pair_id) {
            pos.qty = (pos.qty - qty).max(Decimal::ZERO);
            if pos.qty.is_zero() {
                self.positions.remove(pair_id);
            }
        }
    }

    pub fn pairs_with_position(&self) -> Vec<String> {
        self.positions
            .keys()
            .filter(|k| {
                self.positions
                    .get(*k)
                    .map(|p| p.qty > Decimal::ZERO)
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    pub fn is_pending(&self, pair_id: &str) -> bool {
        self.pending_opens.contains(pair_id)
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

    fn lev(v: &str) -> Decimal {
        if v == "sodex" {
            dec!(2)
        } else {
            dec!(2)
        }
    }

    #[test]
    fn slot_limit_counts_pending() {
        let mut store = PositionStore::default();
        assert!(store.can_open(1));
        store.reserve_open("BTC-USD-PERP");
        assert!(!store.can_open(1));
        store.record_open(
            "BTC-USD-PERP",
            VenueId::from("lighter"),
            VenueId::from("lighter_rh"),
            dec!(0.001),
            1,
            dec!(100),
        );
        assert!(store.can_open(1));
    }

    #[test]
    fn reserved_margin_per_venue() {
        let mut store = PositionStore::default();
        store.record_open(
            "BTC-USD-PERP",
            VenueId::from("lighter"),
            VenueId::from("sodex"),
            dec!(0.01),
            1,
            dec!(400),
        );
        let reserved = store.reserved_margin_by_venue(lev);
        assert_eq!(reserved.get("lighter"), Some(&dec!(200)));
        assert_eq!(reserved.get("sodex"), Some(&dec!(200)));
    }
}
