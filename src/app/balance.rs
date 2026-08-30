use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rust_decimal::Decimal;
use tracing::{debug, warn};

use crate::config::SizingConfig;
use crate::exchange::{Balance, ExchangePort};

const STABLES: &[&str] = &["USDC", "USDG", "USDT", "USD", "VUSDC", "VUSD"];

#[derive(Debug, Clone)]
pub struct VenueExchangePosition {
    pub symbol: String,
    pub qty: Decimal,
    pub entry_price: Option<Decimal>,
    pub realized_pnl: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct VenueAccountSnapshot {
    pub venue: String,
    pub available: Decimal,
    pub total: Decimal,
    pub positions: Vec<VenueExchangePosition>,
    /// account() 调用失败时为 false：此时 positions 为空只代表「不知道」，
    /// 不能当成「该所没有仓位」。
    pub fresh: bool,
}

#[derive(Debug, Clone)]
pub struct VenueAccountCache {
    pub venues: Vec<VenueAccountSnapshot>,
    pub last_refresh: Instant,
}

impl Default for VenueAccountCache {
    fn default() -> Self {
        Self {
            venues: Vec::new(),
            last_refresh: Instant::now(),
        }
    }
}

impl VenueAccountCache {
    pub fn get(&self, venue: &str) -> Option<&VenueAccountSnapshot> {
        self.venues.iter().find(|v| v.venue == venue)
    }

    /// 是否所有所都拿到了真实快照。没拿全就不要做单边敞口判定。
    pub fn all_fresh(&self) -> bool {
        !self.venues.is_empty() && self.venues.iter().all(|v| v.fresh)
    }

    /// 某一所本轮 `account()` 失败时保留上一轮成功快照。
    /// 否则顶栏余额会被刷成 0，`all_fresh` 也长期为 false，裸仓补对冲停住。
    pub fn absorb(&mut self, incoming: Self) {
        if self.venues.is_empty() {
            *self = incoming;
            return;
        }
        let previous = std::mem::replace(self, incoming);
        for prev in previous.venues {
            if let Some(cur) = self.venues.iter_mut().find(|v| v.venue == prev.venue) {
                if !cur.fresh && prev.fresh {
                    *cur = prev;
                }
            }
        }
    }

    pub fn to_balance_map(&self) -> HashMap<String, Decimal> {
        self.venues
            .iter()
            .filter(|v| v.available > Decimal::ZERO)
            .map(|v| (v.venue.clone(), v.available))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct BalanceCache {
    pub by_venue: HashMap<String, Decimal>,
    pub last_refresh: Instant,
}

impl Default for BalanceCache {
    fn default() -> Self {
        Self {
            by_venue: HashMap::new(),
            last_refresh: Instant::now(),
        }
    }
}

impl BalanceCache {
    pub fn venue_available(&self, venue: &str) -> Decimal {
        self.by_venue.get(venue).copied().unwrap_or(Decimal::ZERO)
    }

    pub fn global_min(&self) -> Decimal {
        self.by_venue
            .values()
            .copied()
            .filter(|v| *v > Decimal::ZERO)
            .min()
            .unwrap_or(Decimal::ZERO)
    }
}

/// 一次 `account()` 同时产出余额和持仓快照。
///
/// 旧实现分别调 `balances()` 和 `positions()`，而 trait 默认的 `account()` 又是
/// 二者相加，等于每所 3 次 sidecar 进程 + 3 次全量 REST。
///
/// fallback 逐所生效：某一所拿不到余额时只给那一所兜底，而不是「所有所都为空」
/// 才兜。否则单所失败会让涉及该所的全部 pair 永久 `no_size`。
pub async fn refresh_accounts(
    adapters: &[Arc<dyn ExchangePort>],
    cfg: &SizingConfig,
) -> (BalanceCache, VenueAccountCache) {
    let fallback = cfg.fallback_available_usdc;
    let futs = adapters.iter().map(|adapter| {
        let adapter = Arc::clone(adapter);
        async move {
            let id = adapter.id().as_str().to_string();
            let (snap, fresh) = match adapter.account().await {
                Ok(s) => (s, true),
                Err(err) => {
                    warn!(venue = %id, error = %err, "account refresh failed");
                    (Default::default(), false)
                }
            };
            (id, snap, fresh)
        }
    });
    let rows = futures_util::future::join_all(futs).await;

    let mut by_venue = HashMap::new();
    let mut venues = Vec::new();
    for (id, snap, fresh) in rows {
        let mut available = stable_available(&snap.balances);
        let total = stable_total(&snap.balances);
        if available <= Decimal::ZERO {
            if let Some(fallback) = fallback {
                warn!(
                    venue = %id,
                    fallback = %fallback,
                    fresh,
                    "no stable balance reported; using sizing.fallback_available_usdc"
                );
                available = fallback;
            }
        }
        if available > Decimal::ZERO {
            by_venue.insert(id.clone(), available);
        }
        venues.push(VenueAccountSnapshot {
            venue: id,
            available,
            total: total.max(available),
            positions: snap
                .positions
                .into_iter()
                .map(|p| VenueExchangePosition {
                    symbol: p.symbol,
                    qty: p.qty,
                    entry_price: p.entry_price,
                    realized_pnl: p.realized_pnl,
                })
                .collect(),
            fresh,
        });
    }
    debug!(venues = ?by_venue, "balance refresh");
    let now = Instant::now();
    (
        BalanceCache {
            by_venue,
            last_refresh: now,
        },
        VenueAccountCache {
            venues,
            last_refresh: now,
        },
    )
}

fn stable_total(bals: &[Balance]) -> Decimal {
    bals.iter()
        .filter(|b| STABLES.iter().any(|s| b.asset.eq_ignore_ascii_case(s)))
        .map(|b| b.total.unwrap_or(b.available))
        .sum()
}

fn stable_available(bals: &[Balance]) -> Decimal {
    bals.iter()
        .filter(|b| STABLES.iter().any(|s| b.asset.eq_ignore_ascii_case(s)))
        .map(|b| b.available)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn snap(venue: &str, available: Decimal, fresh: bool) -> VenueAccountSnapshot {
        VenueAccountSnapshot {
            venue: venue.into(),
            available,
            total: available,
            positions: vec![],
            fresh,
        }
    }

    #[test]
    fn absorb_keeps_last_fresh_on_failure() {
        let mut cache = VenueAccountCache {
            venues: vec![snap("lighter", dec!(100), true)],
            last_refresh: Instant::now(),
        };
        cache.absorb(VenueAccountCache {
            venues: vec![snap("lighter", dec!(0), false)],
            last_refresh: Instant::now(),
        });
        let v = cache.get("lighter").unwrap();
        assert!(v.fresh);
        assert_eq!(v.available, dec!(100));
        assert_eq!(cache.to_balance_map().get("lighter").copied(), Some(dec!(100)));
    }
}
