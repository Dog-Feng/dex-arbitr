use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use rust_decimal::Decimal;
use tracing::debug;

use crate::config::SizingConfig;
use crate::exchange::{Balance, ExchangePort};

const STABLES: &[&str] = &["USDC", "USDG", "USDT", "USD", "VUSDC", "VUSD"];

#[derive(Debug, Clone)]
pub struct VenueExchangePosition {
    pub symbol: String,
    pub qty: Decimal,
    pub entry_price: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct VenueAccountSnapshot {
    pub venue: String,
    pub available: Decimal,
    pub total: Decimal,
    pub positions: Vec<VenueExchangePosition>,
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

pub async fn refresh_balances(
    adapters: &[Arc<dyn ExchangePort>],
    cfg: &SizingConfig,
) -> Result<BalanceCache> {
    let mut by_venue = HashMap::new();
    for adapter in adapters {
        let id = adapter.id().as_str().to_string();
        let bals = adapter.balances().await?;
        let avail = stable_available(&bals);
        if avail > Decimal::ZERO {
            by_venue.insert(id.clone(), avail);
        }
    }
    if by_venue.is_empty() {
        if let Some(fallback) = cfg.fallback_available_usdc {
            for adapter in adapters {
                by_venue.insert(adapter.id().as_str().to_string(), fallback);
            }
        }
    }
    debug!(venues = ?by_venue, "balance refresh");
    Ok(BalanceCache {
        by_venue,
        last_refresh: Instant::now(),
    })
}

pub async fn refresh_venue_accounts(
    adapters: &[Arc<dyn ExchangePort>],
) -> Result<VenueAccountCache> {
    let mut venues = Vec::new();
    for adapter in adapters {
        let id = adapter.id().as_str().to_string();
        let snap = match adapter.account().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let available = stable_available(&snap.balances);
        let total = stable_total(&snap.balances);
        let positions = snap
            .positions
            .into_iter()
            .map(|p| VenueExchangePosition {
                symbol: p.symbol,
                qty: p.qty,
                entry_price: p.entry_price,
            })
            .collect();
        venues.push(VenueAccountSnapshot {
            venue: id,
            available,
            total,
            positions,
        });
    }
    debug!(n = venues.len(), "venue account refresh");
    Ok(VenueAccountCache {
        venues,
        last_refresh: Instant::now(),
    })
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
