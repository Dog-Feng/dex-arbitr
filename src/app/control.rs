//! 运行时套利开关与可热改参数。
//!
//! [`ArbitrageControl`] 包在 `Arc<Mutex<>>` 里，由 HTTP API 写入、由决策环
//! 同步到 [`AppConfig`] 后再跑策略。`default.yaml` 只做两件事：
//! - HTTP 开着：给页面反显默认值（`GET /api/config`）
//! - HTTP 关着：纯后端测试，整条策略直接读 yaml
//!
//! 状态纯内存，重启后回到 yaml 的初始值。

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::config::{AppConfig, PairDefaults, PairSetting};

fn default_window_samples() -> usize {
    10_000
}

fn default_sample_interval_ms() -> u64 {
    1000
}

fn default_step_hysteresis() -> Decimal {
    Decimal::new(25, 2)
}

fn default_second_leg_verify_ms() -> u64 {
    2000
}

/// default.yaml 里**全部**可在 UI 热改的参数。
/// 字段名与 YAML 路径一一对应，方便前端展示与编辑。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageParams {
    // ═══ 交易所选择 ═══
    /// 参与套利的所（cfg.venues 的子集）。空 = 全部已加载所。
    #[serde(default)]
    pub active_venues: Vec<String>,

    // ═══ system ═══
    pub monitor_only: bool,
    pub data_freshness_ms: u64,

    // ═══ pairs ═══
    #[serde(default)]
    pub pair_defaults: PairDefaults,
    #[serde(default)]
    pub pairs: Vec<PairSetting>,

    // ═══ execution ═══
    pub paper_trading: bool,
    pub loop_interval_ms: u64,
    pub hedge_failed_legs: bool,

    // ═══ sizing ═══
    pub max_concurrent_pairs: u32,
    pub leverage_multiplier: Decimal,
    pub depth_pct: Decimal,
    pub refresh_balance_secs: u64,
    pub margin_utilization_pct: Decimal,
    pub fallback_available_usdc: Decimal,

    // ═══ scan ═══
    pub scan_enabled: bool,
    pub min_spread_pct: Decimal,
    pub analysis_interval_ms: u64,
    pub watch_top: u32,
    pub cross_use_natural: bool,
    pub scan_log_interval_secs: u64,

    // ═══ grid persistence（阈值在 pair_defaults / pairs）═══
    pub persistence_ms: u64,
    /// 1 秒内至少几次达标。0 = 连续不掉线。
    #[serde(default)]
    pub persistence_min_hits: u32,
    #[serde(default = "default_window_samples")]
    pub window_samples: usize,
    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,
    #[serde(default = "default_step_hysteresis")]
    pub step_hysteresis: Decimal,

    // ═══ order ═══
    pub limit_timeout_ms: u64,
    #[serde(default = "default_second_leg_verify_ms")]
    pub second_leg_verify_ms: u64,
    pub maker_inside_ticks: u32,
    pub limit_retry_count: u32,

    // ═══ cost ═══
    pub default_slip_pct: Decimal,
    pub max_slippage_pct: Decimal,
    pub emergency_slippage_multiplier: Decimal,

    // ═══ history ═══
    pub history_enabled: bool,
    pub history_window_hours: u64,
    pub history_min_points: u32,
    pub history_sample_interval_secs: u64,
    pub history_refresh_interval_secs: u64,
}

impl ArbitrageParams {
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self {
            // 页面默认不勾 DEX；yaml `venues` 只决定连哪些适配器。
            active_venues: Vec::new(),

            monitor_only: cfg.system.monitor_only,
            data_freshness_ms: cfg.system.data_freshness_ms,

            pair_defaults: cfg.pairs.defaults.clone(),
            pairs: cfg.pairs.enabled.clone(),

            paper_trading: cfg.execution.paper_trading,
            loop_interval_ms: cfg.execution.loop_interval_ms,
            hedge_failed_legs: cfg.execution.hedge_failed_legs,

            max_concurrent_pairs: cfg.sizing.max_concurrent_pairs,
            leverage_multiplier: cfg.sizing.leverage_multiplier,
            depth_pct: cfg.sizing.depth_pct,
            refresh_balance_secs: cfg.sizing.refresh_balance_secs,
            margin_utilization_pct: cfg.sizing.margin_utilization_pct,
            fallback_available_usdc: cfg.sizing.fallback_available_usdc.unwrap_or_default(),

            scan_enabled: cfg.scan.enabled,
            min_spread_pct: cfg.scan.min_spread_pct,
            analysis_interval_ms: cfg.scan.analysis_interval_ms,
            watch_top: cfg.scan.watch_top as u32,
            cross_use_natural: cfg.scan.cross_use_natural,
            scan_log_interval_secs: cfg.scan.log_interval_secs,

            persistence_ms: cfg.grid.persistence_ms,
            persistence_min_hits: cfg.grid.persistence_min_hits,
            window_samples: cfg.grid.window_samples,
            sample_interval_ms: cfg.grid.sample_interval_ms,
            step_hysteresis: cfg.grid.step_hysteresis,

            limit_timeout_ms: cfg.order.limit_timeout_ms,
            second_leg_verify_ms: cfg.order.second_leg_verify_ms,
            maker_inside_ticks: cfg.order.maker_inside_ticks,
            limit_retry_count: cfg.order.limit_retry_count,

            default_slip_pct: cfg.cost.default_slip_pct,
            max_slippage_pct: cfg.cost.max_slippage_pct,
            emergency_slippage_multiplier: cfg.cost.emergency_slippage_multiplier,

            history_enabled: cfg.history.enabled,
            history_window_hours: cfg.history.window_hours,
            history_min_points: cfg.history.min_points as u32,
            history_sample_interval_secs: cfg.history.sample_interval_secs,
            history_refresh_interval_secs: cfg.history.refresh_interval_secs,
        }
    }

    /// 把页面（或 API）当前参数写进 `AppConfig`。费率、密钥、`venues` 列表
    /// 仍来自 yaml：页面改不了私钥，所列表是进程启动时装进适配器的。
    pub fn apply_to(&self, cfg: &mut AppConfig) {
        cfg.system.monitor_only = self.monitor_only;
        cfg.system.data_freshness_ms = self.data_freshness_ms;

        cfg.pairs.defaults = self.pair_defaults.clone();
        cfg.pairs.enabled = self.pairs.clone();

        cfg.execution.paper_trading = self.paper_trading;
        cfg.execution.loop_interval_ms = self.loop_interval_ms;
        cfg.execution.hedge_failed_legs = self.hedge_failed_legs;

        cfg.sizing.max_concurrent_pairs = self.max_concurrent_pairs;
        cfg.sizing.leverage_multiplier = self.leverage_multiplier;
        cfg.sizing.depth_pct = self.depth_pct;
        cfg.sizing.refresh_balance_secs = self.refresh_balance_secs;
        cfg.sizing.margin_utilization_pct = self.margin_utilization_pct;
        cfg.sizing.fallback_available_usdc = if self.fallback_available_usdc > Decimal::ZERO {
            Some(self.fallback_available_usdc)
        } else {
            None
        };

        cfg.scan.enabled = self.scan_enabled;
        cfg.scan.min_spread_pct = self.min_spread_pct;
        cfg.scan.analysis_interval_ms = self.analysis_interval_ms;
        cfg.scan.watch_top = self.watch_top as usize;
        cfg.scan.cross_use_natural = self.cross_use_natural;
        cfg.scan.log_interval_secs = self.scan_log_interval_secs;

        cfg.grid.persistence_ms = self.persistence_ms;
        cfg.grid.persistence_min_hits = self.persistence_min_hits;
        cfg.grid.window_samples = self.window_samples.max(1);
        cfg.grid.sample_interval_ms = self.sample_interval_ms.max(1);
        cfg.grid.step_hysteresis = self.step_hysteresis.max(Decimal::ZERO);

        cfg.order.limit_timeout_ms = self.limit_timeout_ms;
        cfg.order.second_leg_verify_ms = self.second_leg_verify_ms;
        cfg.order.maker_inside_ticks = self.maker_inside_ticks;
        cfg.order.limit_retry_count = self.limit_retry_count.max(1);

        cfg.cost.default_slip_pct = self.default_slip_pct;
        cfg.cost.max_slippage_pct = self.max_slippage_pct;
        cfg.cost.emergency_slippage_multiplier = self.emergency_slippage_multiplier;

        cfg.history.enabled = self.history_enabled;
        cfg.history.window_hours = self.history_window_hours;
        cfg.history.min_points = self.history_min_points as usize;
        cfg.history.sample_interval_secs = self.history_sample_interval_secs;
        cfg.history.refresh_interval_secs = self.history_refresh_interval_secs;
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ValidationResult {
    pub ok: bool,
    pub errors: Vec<String>,
}

pub fn validate(p: &ArbitrageParams) -> ValidationResult {
    let mut errors = Vec::new();

    if p.pair_defaults.target_bp <= Decimal::ZERO {
        errors.push("pair_defaults.target_bp 必须 > 0".into());
    }
    if p.pair_defaults.max_segments == 0 {
        errors.push("pair_defaults.max_segments 必须 >= 1".into());
    }
    if p.window_samples == 0 {
        errors.push("window_samples 必须 >= 1".into());
    }
    if p.sample_interval_ms == 0 {
        errors.push("sample_interval_ms 必须 >= 1".into());
    }
    if p.step_hysteresis < Decimal::ZERO {
        errors.push("step_hysteresis 不能为负".into());
    }
    if p.step_hysteresis >= Decimal::new(5, 1) {
        errors.push("step_hysteresis 必须 < 0.5，否则一格锁不住价差".into());
    }
    if p.max_concurrent_pairs == 0 {
        errors.push("max_concurrent_pairs 必须 >= 1".into());
    }
    for s in &p.pairs {
        if s.symbol.trim().is_empty() {
            errors.push("pairs.symbol 不能为空".into());
        }
        if s.base_qty <= Decimal::ZERO {
            errors.push(format!("{} base_qty 必须 > 0", s.symbol));
        }
        if s.max_segments == Some(0) {
            errors.push(format!("{} max_segments 必须 >= 1", s.symbol));
        }
        if s.target_bp.is_some_and(|v| v <= Decimal::ZERO) {
            errors.push(format!("{} target_bp 必须 > 0", s.symbol));
        }
    }

    ValidationResult { ok: errors.is_empty(), errors }
}

#[derive(Debug, Clone)]
pub struct ArbitrageControl {
    pub enabled: bool,
    /// 点「启动套利」后置位；决策环按当前 `active_venues` 拉市场并匹配后清掉。
    pub rematch: bool,
    pub params: ArbitrageParams,
}

impl ArbitrageControl {
    pub fn new(cfg: &AppConfig) -> Self {
        Self {
            enabled: false,
            rematch: false,
            params: ArbitrageParams::from_config(cfg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::path::Path;

    #[test]
    fn apply_to_overrides_yaml_grid_and_sizing() {
        let mut cfg = AppConfig::load_from(Path::new("config/default.yaml")).unwrap();
        let mut p = ArbitrageParams::from_config(&cfg);
        p.pair_defaults.target_bp = dec!(1);
        p.pair_defaults.max_segments = 5;
        p.paper_trading = true;
        p.pairs = vec![crate::config::PairSetting {
            symbol: "SNDK".into(),
            base_qty: dec!(0.01),
            max_segments: None,
            target_bp: None,
            split_order_size: None,
            overrides: vec![],
        }];
        p.apply_to(&mut cfg);
        assert_eq!(cfg.pairs.defaults.target_bp, dec!(1));
        assert_eq!(cfg.pairs.defaults.max_segments, 5);
        assert!(cfg.execution.paper_trading);
        assert_eq!(cfg.pairs.enabled[0].symbol, "SNDK");
    }

    #[test]
    fn from_config_does_not_preselect_venues() {
        let cfg = AppConfig::load_from(Path::new("config/default.yaml")).unwrap();
        let p = ArbitrageParams::from_config(&cfg);
        assert!(p.active_venues.is_empty());
        assert!(!cfg.venues.is_empty());
    }
}
