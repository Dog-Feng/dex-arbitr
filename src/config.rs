use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::domain::{GridParams, VenueId};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub system: SystemConfig,
    pub venues: Vec<String>,
    pub pairs: PairsConfig,
    pub grid: GridConfig,
    pub order: OrderConfig,
    pub cost: CostConfig,
    pub history: HistoryConfig,
    pub risk: RiskConfig,
    #[serde(skip)]
    pub venue_fees: HashMap<String, VenueFees>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemConfig {
    pub monitor_only: bool,
    pub data_freshness_ms: u64,
    pub stable_depeg_bps: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PairsConfig {
    pub whitelist: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GridConfig {
    pub initial_spread_threshold: Decimal,
    pub grid_step: Decimal,
    pub max_segments: u32,
    pub persistence_ms: u64,
    pub base_qty: HashMap<String, Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    pub style: OrderStyle,
    /// 第一腿限价最长等待。超时未成交则撤。
    #[serde(default = "default_limit_timeout_ms")]
    pub limit_timeout_ms: u64,
}

fn default_limit_timeout_ms() -> u64 {
    2000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStyle {
    LimitMaker,
    MarketTaker,
    /// 吃单费率高的一所先挂限价，成交后再对另一所市价。
    LimitThenMarket,
}

impl OrderStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LimitMaker => "limit_maker",
            Self::MarketTaker => "market_taker",
            Self::LimitThenMarket => "limit_then_market",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CostConfig {
    pub default_slip_pct: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryConfig {
    pub enabled: bool,
    pub db_path: String,
    pub sample_interval_secs: u64,
    pub window_hours: u64,
    pub min_points: usize,
    pub max_age_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueFees {
    pub maker: Decimal,
    pub taker: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub min_book_qty: HashMap<String, Decimal>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueFile {
    pub id: String,
    pub rest: String,
    pub ws: String,
    pub chain_id: u64,
    pub quote: String,
    pub fees: VenueFees,
    #[serde(default)]
    pub account_index: i64,
    #[serde(default)]
    pub api_key_index: i32,
    #[serde(default)]
    pub api_key_private_key: String,
}

impl VenueFile {
    pub fn auth(&self) -> Option<VenueAuth> {
        let key = self.api_key_private_key.trim();
        if key.is_empty() || key == "0x" {
            return None;
        }
        Some(VenueAuth {
            account_index: self.account_index,
            api_key_index: self.api_key_index,
            api_key_private_key: key.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct VenueAuth {
    pub account_index: i64,
    pub api_key_index: i32,
    pub api_key_private_key: String,
}

impl std::fmt::Debug for VenueAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VenueAuth")
            .field("account_index", &self.account_index)
            .field("api_key_index", &self.api_key_index)
            .field("api_key_private_key", &"<redacted>")
            .finish()
    }
}

impl VenueAuth {
    pub fn is_ready(&self) -> bool {
        !self.api_key_private_key.trim().is_empty()
            && self.api_key_private_key.trim() != "0x"
    }
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        Self::load_from(Path::new("config/default.yaml"))
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        let mut cfg: AppConfig = serde_yaml::from_str(&raw).context("parse default.yaml")?;
        cfg.hydrate_fees()?;
        Ok(cfg)
    }

    pub fn load_venue(&self, id: &str) -> Result<VenueFile> {
        let path = PathBuf::from("config/venues").join(venue_file_name(id));
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read venue {}", path.display()))?;
        serde_yaml::from_str(&raw).context("parse venue yaml")
    }

    pub fn grid_for(&self, base: &str) -> GridParams {
        let qty = self
            .grid
            .base_qty
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(base))
            .map(|(_, v)| *v)
            .unwrap_or(Decimal::ZERO);
        GridParams {
            initial: self.grid.initial_spread_threshold,
            step: self.grid.grid_step,
            max_segments: self.grid.max_segments,
            persistence: Duration::from_millis(self.grid.persistence_ms),
            base_qty: qty,
        }
    }

    fn hydrate_fees(&mut self) -> Result<()> {
        self.venue_fees.clear();
        for id in self.venues.clone() {
            let venue = self.load_venue(&id)?;
            self.venue_fees.insert(id, venue.fees);
        }
        Ok(())
    }

    pub fn maker_fee(&self, venue: &VenueId) -> Decimal {
        self.venue_fees
            .get(venue.as_str())
            .map(|f| f.maker)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn taker_fee(&self, venue: &VenueId) -> Decimal {
        self.venue_fees
            .get(venue.as_str())
            .map(|f| f.taker)
            .unwrap_or(Decimal::ZERO)
    }

    /// Fee used on the hot path: taker when eating, maker when posting.
    pub fn exec_fee(&self, venue: &VenueId) -> Decimal {
        match self.order.style {
            OrderStyle::LimitMaker => self.maker_fee(venue),
            OrderStyle::MarketTaker | OrderStyle::LimitThenMarket => self.taker_fee(venue),
        }
    }

    pub fn min_book_qty(&self, base: &str) -> Decimal {
        self.risk
            .min_book_qty
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(base))
            .map(|(_, v)| *v)
            .unwrap_or(Decimal::ZERO)
    }
}

fn venue_file_name(id: &str) -> String {
    match id {
        "lighter_rh" => "lighter_robinhood.yaml".into(),
        other => format!("{other}.yaml"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn loads_default_yaml() {
        let cfg = AppConfig::load_from(Path::new("config/default.yaml")).unwrap();
        assert!(cfg.system.monitor_only);
        assert_eq!(cfg.venues, ["lighter", "lighter_rh"]);
        assert_eq!(cfg.grid.initial_spread_threshold, dec!(0.03));
        assert_eq!(cfg.grid_for("BTC").base_qty, dec!(0.001));
        let venue = cfg.load_venue("lighter_rh").unwrap();
        assert_eq!(venue.chain_id, 466324);
        assert_eq!(venue.quote, "USDG");
        assert_eq!(cfg.order.style, OrderStyle::LimitThenMarket);
        assert!(cfg.system.monitor_only);
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter")), dec!(0.005));
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter_rh")), dec!(0.012));
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter")), dec!(0.005));
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter_rh")), dec!(0.035));
        assert_eq!(
            cfg.exec_fee(&VenueId::from("lighter")) + cfg.exec_fee(&VenueId::from("lighter_rh")),
            dec!(0.040)
        );
        assert!(cfg.history.enabled);
        assert_eq!(cfg.history.min_points, 10);
        assert!(venue.auth().is_none());
    }
}
