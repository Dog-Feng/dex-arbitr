//! 资金费率缓存与不利持续时间跟踪。
//!
//! 费率走后台周期刷新，和余额同理：单次 sidecar 调用可能十几秒，
//! 放进 100ms 决策环会把环拖停。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rust_decimal::Decimal;

use crate::domain::funding::{funding_view, FundingView, VenueFunding};
use crate::domain::VenueId;
use crate::exchange::ExchangePort;

/// 一个所的费率表：symbol（大写）→ (费率, 结算周期秒)。
type VenueRates = HashMap<String, (Decimal, u32)>;

#[derive(Default)]
pub struct FundingCache {
    by_venue: HashMap<String, VenueRates>,
    last_refresh: Option<Instant>,
}

impl FundingCache {
    /// 费率数据是否足够新。过期就当没有数据——用小时前的费率判风控
    /// 比不判更危险，因为它看起来是有效值。
    pub fn is_fresh(&self, max_age: Duration) -> bool {
        self.last_refresh
            .map(|t| t.elapsed() <= max_age)
            .unwrap_or(false)
    }

    pub fn is_empty(&self) -> bool {
        self.by_venue.is_empty()
    }

    fn rate(&self, venue: &VenueId, symbol: &str) -> Option<(Decimal, u32)> {
        let key = symbol.trim().to_uppercase();
        self.by_venue.get(venue.as_str())?.get(&key).copied()
    }

    /// 取一对腿的费率维度。任一腿缺数据返回 None——由调用方决定
    /// 「缺数据」怎么处理，不在这里替它选。
    pub fn view(
        &self,
        buy: &VenueId,
        buy_symbol: &str,
        sell: &VenueId,
        sell_symbol: &str,
    ) -> Option<FundingView> {
        let (buy_rate, buy_interval) = self.rate(buy, buy_symbol)?;
        let (sell_rate, sell_interval) = self.rate(sell, sell_symbol)?;
        funding_view(
            &VenueFunding {
                venue: buy.clone(),
                rate: buy_rate,
                interval_secs: buy_interval,
            },
            &VenueFunding {
                venue: sell.clone(),
                rate: sell_rate,
                interval_secs: sell_interval,
            },
        )
    }
}

/// 拉一轮所有所的费率。单所失败不影响其他所——那一所的 pair
/// 会因为缺数据走 `view() == None`,而不是整轮丢弃。
pub async fn refresh_funding(adapters: &[Arc<dyn ExchangePort>]) -> FundingCache {
    let mut by_venue = HashMap::new();
    for a in adapters {
        let Ok(rates) = a.funding().await else { continue };
        if rates.is_empty() {
            continue;
        }
        let mut table = VenueRates::new();
        for r in rates {
            table.insert(
                r.symbol.trim().to_uppercase(),
                (r.rate, r.interval_secs),
            );
        }
        by_venue.insert(a.id().as_str().to_string(), table);
    }
    FundingCache {
        by_venue,
        last_refresh: Some(Instant::now()),
    }
}

/// 费率对某仓不利的持续时长跟踪。对齐参考
/// `funding_rate_unfavorable_duration`：不利才起表，转有利就清零。
#[derive(Default)]
pub struct UnfavorableTracker {
    since: HashMap<String, Instant>,
}

impl UnfavorableTracker {
    /// 标记该 slot 当前不利，返回已持续时长。首次标记返回 0。
    pub fn mark(&mut self, slot: &str) -> Duration {
        let now = Instant::now();
        let start = self.since.entry(slot.to_string()).or_insert(now);
        now.saturating_duration_since(*start)
    }

    pub fn clear(&mut self, slot: &str) {
        self.since.remove(slot);
    }

    /// 不利已持续到阈值。`minutes == 0` 表示不要求持续性，立即成立。
    pub fn sustained(&mut self, slot: &str, minutes: u64) -> bool {
        let held = self.mark(slot);
        held >= Duration::from_secs(minutes * 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn cache() -> FundingCache {
        let mut by_venue = HashMap::new();
        by_venue.insert(
            "lighter".to_string(),
            VenueRates::from([("BTC".to_string(), (dec!(0.0002), 3600))]),
        );
        by_venue.insert(
            "sodex".to_string(),
            VenueRates::from([("BTC".to_string(), (dec!(0.0001), 3600))]),
        );
        FundingCache {
            by_venue,
            last_refresh: Some(Instant::now()),
        }
    }

    #[test]
    fn view_orients_net_by_leg_direction() {
        let c = cache();
        let lighter = VenueId::from("lighter");
        let sodex = VenueId::from("sodex");
        // 买 lighter（付 0.02%）、卖 sodex（收 0.01%）→ 净付 0.01%。
        let v = c.view(&lighter, "BTC", &sodex, "BTC").unwrap();
        assert_eq!(v.net_pct, dec!(-0.01));
        // 反过来定向 → 净收。
        let v = c.view(&sodex, "BTC", &lighter, "BTC").unwrap();
        assert_eq!(v.net_pct, dec!(0.01));
    }

    #[test]
    fn symbol_lookup_is_case_insensitive() {
        let c = cache();
        assert!(c
            .view(
                &VenueId::from("lighter"),
                "btc",
                &VenueId::from("sodex"),
                "Btc"
            )
            .is_some());
    }

    #[test]
    fn missing_leg_yields_none() {
        let c = cache();
        assert!(c
            .view(
                &VenueId::from("lighter"),
                "ETH",
                &VenueId::from("sodex"),
                "ETH"
            )
            .is_none());
    }

    #[test]
    fn stale_cache_is_not_fresh() {
        let c = FundingCache::default();
        assert!(!c.is_fresh(Duration::from_secs(300)));
        assert!(cache().is_fresh(Duration::from_secs(300)));
    }

    /// 持续性门：首次标记不该立刻成立，否则费率抖一下就平仓。
    #[test]
    fn tracker_requires_duration() {
        let mut t = UnfavorableTracker::default();
        assert!(!t.sustained("btc:l:s", 60));
        // 阈值 0 = 不要求持续性。
        assert!(t.sustained("btc:l:s", 0));
    }

    #[test]
    fn tracker_clear_restarts_clock() {
        let mut t = UnfavorableTracker::default();
        t.mark("s");
        t.clear("s");
        assert!(!t.since.contains_key("s"));
    }
}
