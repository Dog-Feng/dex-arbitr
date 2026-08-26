//! 运行时套利开关与可热改参数。
//!
//! [`ArbitrageControl`] 包在 `Arc<Mutex<>>` 里，由 HTTP API 写入、由决策环
//! 同步到 [`AppConfig`] 后再跑策略。`default.yaml` 只做两件事：
//! - HTTP 开着：给页面反显默认值（`GET /api/config`）
//! - HTTP 关着：纯后端测试，整条策略直接读 yaml
//!
//! 状态纯内存，重启后回到 yaml 的初始值。

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::config::{AppConfig, OrderStyle, SizingMode};

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
    pub stable_depeg_bps: u32,

    // ═══ pairs ═══
    #[serde(default)]
    pub whitelist: Vec<String>,

    // ═══ execution ═══
    pub paper_trading: bool,
    pub loop_interval_ms: u64,
    pub hedge_failed_legs: bool,

    // ═══ sizing ═══
    pub sizing_mode: String,            // "fixed" | "margin"
    pub fixed_notional_usdc: Decimal,
    pub max_concurrent_pairs: u32,
    pub leverage_multiplier: Decimal,
    pub min_notional_usdc: Decimal,
    pub max_notional_usdc: Decimal,
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

    // ═══ grid ═══
    pub initial_spread_threshold: Decimal,
    pub t0_ratio: Decimal,
    pub grid_step: Decimal,
    pub max_segments: u32,
    pub split_order_size: Decimal,
    /// 保留字段，热路径不读。页面不再展示。
    #[serde(default)]
    pub close_stop_loss_pct: Decimal,
    pub persistence_ms: u64,
    #[serde(default)]
    pub scalping_enabled: bool,
    #[serde(default)]
    pub scalping_trigger_segment: u32,
    #[serde(default)]
    pub scalping_profit_threshold_pct: Decimal,
    /// 逐币默认数量，如 {"BTC":"0.001","ETH":"0.02"}
    #[serde(default)]
    pub base_qty: HashMap<String, Decimal>,

    // ═══ order ═══
    pub order_style: String,            // "limit_then_market" | "market_taker" | "limit_maker"
    pub limit_timeout_ms: u64,
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

    // ═══ risk — 盘口质量 ═══
    /// 逐币最小一档深度，如 {"BTC":"0.001"}
    #[serde(default)]
    pub min_book_qty: HashMap<String, Decimal>,
    pub max_venue_spread_pct: Decimal,
    pub price_stability_window_secs: Decimal,
    pub price_stability_threshold_pct: Decimal,

    // ═══ risk — reduce-only 探针 ═══
    pub reduce_only_probe_enabled: bool,
    pub reduce_only_probe_second: u32,

    // ═══ risk — 资金费率 ═══
    pub funding_annual_threshold_pct: Decimal,
    pub funding_unfavorable_duration_minutes: u64,
    pub funding_refresh_secs: u64,

    // ═══ risk — 全局限额 ═══
    pub max_daily_opens: u32,
    pub max_position_hours: u64,

    // ═══ risk — 余额下限 ═══
    pub min_balance_warn_usdc: Decimal,
    pub min_balance_close_usdc: Decimal,

    // ═══ risk — 名义敞口 ═══
    pub max_single_token_notional_usdc: Decimal,
    pub max_total_notional_usdc: Decimal,

    // ═══ risk — 错误退避 ═══
    pub backoff_min_secs: u64,
    pub backoff_max_secs: u64,
    pub backoff_multiplier: u32,
    pub backoff_reset_secs: u64,
}

impl ArbitrageParams {
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self {
            active_venues: cfg.venues.clone(),

            monitor_only: cfg.system.monitor_only,
            data_freshness_ms: cfg.system.data_freshness_ms,
            stable_depeg_bps: cfg.system.stable_depeg_bps,

            whitelist: cfg.pairs.whitelist.clone(),

            paper_trading: cfg.execution.paper_trading,
            loop_interval_ms: cfg.execution.loop_interval_ms,
            hedge_failed_legs: cfg.execution.hedge_failed_legs,

            sizing_mode: match cfg.sizing.mode {
                SizingMode::Fixed => "fixed".into(),
                SizingMode::Margin => "margin".into(),
            },
            fixed_notional_usdc: cfg.sizing.fixed_notional_usdc,
            max_concurrent_pairs: cfg.sizing.max_concurrent_pairs,
            leverage_multiplier: cfg.sizing.leverage_multiplier,
            min_notional_usdc: cfg.sizing.min_notional_usdc,
            max_notional_usdc: cfg.sizing.max_notional_usdc,
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

            initial_spread_threshold: cfg.grid.initial_spread_threshold,
            t0_ratio: cfg.grid.t0_ratio,
            grid_step: cfg.grid.grid_step,
            max_segments: cfg.grid.max_segments,
            split_order_size: cfg.grid.split_order_size,
            close_stop_loss_pct: cfg.grid.close_stop_loss_pct,
            persistence_ms: cfg.grid.persistence_ms,
            scalping_enabled: cfg.grid.scalping_enabled,
            scalping_trigger_segment: cfg.grid.scalping_trigger_segment,
            scalping_profit_threshold_pct: cfg.grid.scalping_profit_threshold_pct,
            base_qty: cfg.grid.base_qty.clone(),

            order_style: cfg.order.style.as_str().into(),
            limit_timeout_ms: cfg.order.limit_timeout_ms,
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

            min_book_qty: cfg.risk.min_book_qty.clone(),
            max_venue_spread_pct: cfg.risk.max_venue_spread_pct,
            price_stability_window_secs: cfg.risk.price_stability_window_secs,
            price_stability_threshold_pct: cfg.risk.price_stability_threshold_pct,

            reduce_only_probe_enabled: cfg.risk.reduce_only_probe_enabled,
            reduce_only_probe_second: cfg.risk.reduce_only_probe_second,

            funding_annual_threshold_pct: cfg.risk.funding_annual_threshold_pct,
            funding_unfavorable_duration_minutes: cfg.risk.funding_unfavorable_duration_minutes,
            funding_refresh_secs: cfg.risk.funding_refresh_secs,

            max_daily_opens: cfg.risk.max_daily_opens,
            max_position_hours: cfg.risk.max_position_hours,

            min_balance_warn_usdc: cfg.risk.min_balance_warn_usdc,
            min_balance_close_usdc: cfg.risk.min_balance_close_usdc,

            max_single_token_notional_usdc: cfg.risk.max_single_token_notional_usdc,
            max_total_notional_usdc: cfg.risk.max_total_notional_usdc,

            backoff_min_secs: cfg.risk.backoff_min_secs,
            backoff_max_secs: cfg.risk.backoff_max_secs,
            backoff_multiplier: cfg.risk.backoff_multiplier,
            backoff_reset_secs: cfg.risk.backoff_reset_secs,
        }
    }

    /// 把页面（或 API）当前参数写进 `AppConfig`。费率、密钥、`venues` 列表
    /// 仍来自 yaml：页面改不了私钥，所列表是进程启动时装进适配器的。
    pub fn apply_to(&self, cfg: &mut AppConfig) {
        cfg.system.monitor_only = self.monitor_only;
        cfg.system.data_freshness_ms = self.data_freshness_ms;
        cfg.system.stable_depeg_bps = self.stable_depeg_bps;

        cfg.pairs.whitelist = self.whitelist.clone();

        cfg.execution.paper_trading = self.paper_trading;
        cfg.execution.loop_interval_ms = self.loop_interval_ms;
        cfg.execution.hedge_failed_legs = self.hedge_failed_legs;

        cfg.sizing.mode = match self.sizing_mode.as_str() {
            "margin" => SizingMode::Margin,
            _ => SizingMode::Fixed,
        };
        cfg.sizing.fixed_notional_usdc = self.fixed_notional_usdc;
        cfg.sizing.max_concurrent_pairs = self.max_concurrent_pairs;
        cfg.sizing.leverage_multiplier = self.leverage_multiplier;
        cfg.sizing.min_notional_usdc = self.min_notional_usdc;
        cfg.sizing.max_notional_usdc = self.max_notional_usdc;
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

        cfg.grid.initial_spread_threshold = self.initial_spread_threshold;
        cfg.grid.t0_ratio = self.t0_ratio;
        cfg.grid.grid_step = self.grid_step;
        cfg.grid.max_segments = self.max_segments;
        cfg.grid.split_order_size = self.split_order_size;
        cfg.grid.close_stop_loss_pct = self.close_stop_loss_pct;
        cfg.grid.persistence_ms = self.persistence_ms;
        cfg.grid.scalping_enabled = self.scalping_enabled;
        cfg.grid.scalping_trigger_segment = self.scalping_trigger_segment;
        cfg.grid.scalping_profit_threshold_pct = self.scalping_profit_threshold_pct;
        cfg.grid.base_qty = self.base_qty.clone();

        cfg.order.style = OrderStyle::from_page(&self.order_style);
        cfg.order.limit_timeout_ms = self.limit_timeout_ms;
        cfg.order.maker_inside_ticks = self.maker_inside_ticks.max(1);
        cfg.order.limit_retry_count = self.limit_retry_count.max(1);

        cfg.cost.default_slip_pct = self.default_slip_pct;
        cfg.cost.max_slippage_pct = self.max_slippage_pct;
        cfg.cost.emergency_slippage_multiplier = self.emergency_slippage_multiplier;

        cfg.history.enabled = self.history_enabled;
        cfg.history.window_hours = self.history_window_hours;
        cfg.history.min_points = self.history_min_points as usize;
        cfg.history.sample_interval_secs = self.history_sample_interval_secs;
        cfg.history.refresh_interval_secs = self.history_refresh_interval_secs;

        cfg.risk.min_book_qty = self.min_book_qty.clone();
        cfg.risk.max_venue_spread_pct = self.max_venue_spread_pct;
        cfg.risk.price_stability_window_secs = self.price_stability_window_secs;
        cfg.risk.price_stability_threshold_pct = self.price_stability_threshold_pct;
        cfg.risk.reduce_only_probe_enabled = self.reduce_only_probe_enabled;
        cfg.risk.reduce_only_probe_second = self.reduce_only_probe_second;
        cfg.risk.funding_annual_threshold_pct = self.funding_annual_threshold_pct;
        cfg.risk.funding_unfavorable_duration_minutes = self.funding_unfavorable_duration_minutes;
        cfg.risk.funding_refresh_secs = self.funding_refresh_secs;
        cfg.risk.max_daily_opens = self.max_daily_opens;
        cfg.risk.max_position_hours = self.max_position_hours;
        cfg.risk.min_balance_warn_usdc = self.min_balance_warn_usdc;
        cfg.risk.min_balance_close_usdc = self.min_balance_close_usdc;
        cfg.risk.max_single_token_notional_usdc = self.max_single_token_notional_usdc;
        cfg.risk.max_total_notional_usdc = self.max_total_notional_usdc;
        cfg.risk.backoff_min_secs = self.backoff_min_secs;
        cfg.risk.backoff_max_secs = self.backoff_max_secs;
        cfg.risk.backoff_multiplier = self.backoff_multiplier.max(1);
        cfg.risk.backoff_reset_secs = self.backoff_reset_secs;
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ValidationResult {
    pub ok: bool,
    pub errors: Vec<String>,
}

pub fn validate(p: &ArbitrageParams) -> ValidationResult {
    let mut errors = Vec::new();

    if p.initial_spread_threshold <= Decimal::ZERO {
        errors.push("initial_spread_threshold 必须 > 0".into());
    }
    if p.grid_step < Decimal::ZERO {
        errors.push("grid_step 不能为负".into());
    }
    if p.max_segments == 0 {
        errors.push("max_segments 必须 >= 1".into());
    }
    if p.max_concurrent_pairs == 0 {
        errors.push("max_concurrent_pairs 必须 >= 1".into());
    }
    if p.min_notional_usdc > p.max_notional_usdc && p.max_notional_usdc > Decimal::ZERO {
        errors.push("min_notional_usdc 不能大于 max_notional_usdc".into());
    }
    if p.close_stop_loss_pct < Decimal::ZERO {
        errors.push("close_stop_loss_pct 不能为负".into());
    }
    if p.scalping_enabled && p.scalping_trigger_segment == 0 {
        errors.push("scalping_trigger_segment 必须 >= 1".into());
    }
    if p.scalping_profit_threshold_pct < Decimal::ZERO {
        errors.push("scalping_profit_threshold_pct 不能为负".into());
    }
    if p.funding_annual_threshold_pct < Decimal::ZERO {
        errors.push("funding_annual_threshold_pct 不能为负".into());
    }
    if p.min_balance_close_usdc > Decimal::ZERO
        && p.min_balance_warn_usdc > Decimal::ZERO
        && p.min_balance_close_usdc >= p.min_balance_warn_usdc
    {
        errors.push("min_balance_close_usdc 必须小于 min_balance_warn_usdc".into());
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
        p.fixed_notional_usdc = dec!(80);
        p.sizing_mode = "fixed".into();
        p.initial_spread_threshold = dec!(0.08);
        p.max_segments = 5;
        p.paper_trading = true;
        p.whitelist = vec!["SNDK".into()];
        p.apply_to(&mut cfg);
        assert_eq!(cfg.sizing.fixed_notional_usdc, dec!(80));
        assert_eq!(cfg.sizing.mode, SizingMode::Fixed);
        assert_eq!(cfg.grid.initial_spread_threshold, dec!(0.08));
        assert_eq!(cfg.grid.max_segments, 5);
        assert_eq!(cfg.grid.close_stop_loss_pct, Decimal::ZERO);
        p.scalping_enabled = true;
        p.scalping_trigger_segment = 10;
        p.scalping_profit_threshold_pct = dec!(0.02);
        p.apply_to(&mut cfg);
        assert!(cfg.grid.scalping_enabled);
        assert_eq!(cfg.grid.scalping_trigger_segment, 10);
        assert_eq!(cfg.grid.scalping_profit_threshold_pct, dec!(0.02));
        assert!(cfg.execution.paper_trading);
        assert_eq!(cfg.pairs.whitelist, ["SNDK"]);
    }
}
