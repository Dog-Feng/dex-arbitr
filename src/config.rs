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
    #[serde(default = "default_scan")]
    pub scan: ScanConfig,
    #[serde(default = "default_execution")]
    pub execution: ExecutionConfig,
    #[serde(default = "default_sizing")]
    pub sizing: SizingConfig,
    #[serde(default = "default_live_test")]
    pub live_test: LiveTestConfig,
    #[serde(default = "default_http")]
    pub http: HttpConfig,
    #[serde(skip)]
    pub venue_fees: HashMap<String, VenueFees>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default)]
    pub enabled: bool,
    /// 公网监听地址，例如 `0.0.0.0:8090`。
    #[serde(default = "default_http_bind")]
    pub bind: String,
    #[serde(default = "default_web_root")]
    pub web_root: String,
    /// 非空时 `/api/*` 需 `Authorization: Bearer <token>`（对齐 internal Go API）。
    #[serde(default)]
    pub auth_token: Option<String>,
}

fn default_http_bind() -> String {
    "0.0.0.0:8090".into()
}

fn default_web_root() -> String {
    "web".into()
}

fn default_http() -> HttpConfig {
    HttpConfig {
        enabled: false,
        bind: default_http_bind(),
        web_root: default_web_root(),
        auth_token: None,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LiveTestConfig {
    /// 为 true 时，主流程实盘下单受 max_qty 上限（DEX 单所验证用）。
    /// 价差套利正常运行时应为 false，走 sizing 定仓。
    #[serde(default)]
    pub dex_test_mode: bool,
    /// live-test CLI / dex_test_mode 下单时的单笔最大 base qty。
    #[serde(default = "default_live_test_max_qty")]
    pub max_qty: Decimal,
    #[serde(default = "default_live_test_journal")]
    pub journal_path: Option<String>,
}

fn default_live_test_max_qty() -> Decimal {
    Decimal::new(1, 3) // 0.001
}

fn default_live_test_journal() -> Option<String> {
    Some("data/executions.sqlite".into())
}

fn default_live_test() -> LiveTestConfig {
    LiveTestConfig {
        dex_test_mode: false,
        max_qty: Decimal::new(1, 3),
        journal_path: Some("data/executions.sqlite".into()),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_loop_interval_ms")]
    pub loop_interval_ms: u64,
    /// 无密钥或未接 REST 余额时，用 paper 模拟成交更新内存持仓。
    #[serde(default = "default_true")]
    pub paper_trading: bool,
    /// 本进程第二腿失败后自动在 counterparty 补市价对冲（不对启动前已有仓位操作）。
    #[serde(default = "default_true")]
    pub hedge_failed_legs: bool,
}

fn default_loop_interval_ms() -> u64 {
    100
}

fn default_execution() -> ExecutionConfig {
    ExecutionConfig {
        enabled: false,
        loop_interval_ms: 100,
        paper_trading: true,
        hedge_failed_legs: true,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingMode {
    /// 按保证金短板 × 杠杆定仓（默认）。
    #[default]
    Margin,
    /// 每笔固定 USDC 名义，用于初期小额联调。
    Fixed,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SizingConfig {
    #[serde(default)]
    pub mode: SizingMode,
    /// mode=fixed 时每笔目标名义（USDC）。
    #[serde(default = "default_fixed_notional_usdc")]
    pub fixed_notional_usdc: Decimal,
    #[serde(default = "default_max_concurrent_pairs")]
    pub max_concurrent_pairs: u32,
    #[serde(default = "default_leverage")]
    pub leverage_multiplier: Decimal,
    #[serde(default = "default_min_notional")]
    pub min_notional_usdc: Decimal,
    #[serde(default = "default_max_notional")]
    pub max_notional_usdc: Decimal,
    #[serde(default = "default_depth_pct")]
    pub depth_pct: Decimal,
    #[serde(default = "default_refresh_balance_secs")]
    pub refresh_balance_secs: u64,
    /// REST 余额未接线时的回退可用 USDC（paper / 联调）。
    pub fallback_available_usdc: Option<Decimal>,
    /// 可用保证金使用比例（%），留 buffer 防强平/手续费。
    #[serde(default = "default_margin_utilization_pct")]
    pub margin_utilization_pct: Decimal,
    /// 覆盖单所杠杆；未列出的所用 leverage_multiplier。
    #[serde(default)]
    pub leverage_by_venue: HashMap<String, Decimal>,
}

fn default_margin_utilization_pct() -> Decimal {
    Decimal::from(90)
}

fn default_fixed_notional_usdc() -> Decimal {
    Decimal::from(20)
}

fn default_max_concurrent_pairs() -> u32 {
    5
}

fn default_leverage() -> Decimal {
    Decimal::from(2)
}

fn default_min_notional() -> Decimal {
    Decimal::from(20)
}

fn default_max_notional() -> Decimal {
    Decimal::from(500)
}

fn default_depth_pct() -> Decimal {
    Decimal::new(5, 1)
}

fn default_refresh_balance_secs() -> u64 {
    60
}

fn default_sizing() -> SizingConfig {
    SizingConfig {
        mode: SizingMode::Margin,
        fixed_notional_usdc: Decimal::from(20),
        max_concurrent_pairs: 5,
        leverage_multiplier: Decimal::from(2),
        min_notional_usdc: Decimal::from(20),
        max_notional_usdc: Decimal::from(500),
        depth_pct: Decimal::new(5, 1),
        refresh_balance_secs: 60,
        fallback_available_usdc: None,
        margin_utilization_pct: Decimal::from(90),
        leverage_by_venue: HashMap::new(),
    }
}

/// 对齐参考监控 V2：毛价差门槛 + 定时分析环。不改格子、不下单。
#[derive(Debug, Clone, Deserialize)]
pub struct ScanConfig {
    pub enabled: bool,
    pub min_spread_pct: Decimal,
    pub analysis_interval_ms: u64,
    pub watch_top: usize,
    /// 跨 DEX（如 SoDEX ↔ Lighter）用 24h raw 中位数当天然价差，上榜看 residual。
    #[serde(default = "default_true")]
    pub cross_use_natural: bool,
    /// 仍在榜上的币至少隔这么多秒再打一行。上榜/下榜立刻记。
    #[serde(default = "default_log_interval_secs")]
    pub log_interval_secs: u64,
}

fn default_log_interval_secs() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

fn default_scan() -> ScanConfig {
    ScanConfig {
        enabled: true,
        min_spread_pct: Decimal::new(1, 1),
        analysis_interval_ms: 50,
        watch_top: 10,
        cross_use_natural: true,
        log_interval_secs: 30,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemConfig {
    pub monitor_only: bool,
    pub data_freshness_ms: u64,
    pub stable_depeg_bps: u32,
    /// 文本日志目录。按天滚动 `dex-arbitr.YYYY-MM-DD.log`。空 = 只打 stderr。
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
}

fn default_log_dir() -> String {
    "data/logs".into()
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
    /// 保留字段：参考统一引擎没有往返净利止损，热路径不读。默认 0。
    #[serde(default = "default_close_stop_loss")]
    pub close_stop_loss_pct: Decimal,
    /// T0 = T1 × 该系数，第一格的平仓阈值。对齐参考 `_build_grid_thresholds`
    /// 的 `t0 = initial * 0.4`。
    #[serde(default = "default_t0_ratio")]
    pub t0_ratio: Decimal,
    /// 单笔最大开/平仓量（拆单）。0 = 不拆，一次下完该加或该减的量。
    #[serde(default)]
    pub split_order_size: Decimal,
    /// 对齐参考 `scalping_enabled`。默认关。
    #[serde(default)]
    pub scalping_enabled: bool,
    /// 开仓视角格子 ≥ 该值进入剥头皮。默认 10，对齐参考 yaml。
    #[serde(default = "default_scalping_trigger_segment")]
    pub scalping_trigger_segment: u32,
    /// 剥头皮止盈阈值（%）。建仓均毛价差 − 当前剩余毛价差。
    #[serde(default = "default_scalping_profit_threshold")]
    pub scalping_profit_threshold_pct: Decimal,
}

fn default_close_stop_loss() -> Decimal {
    Decimal::ZERO
}

/// 对齐参考 `t0 = initial * 0.4`。
fn default_t0_ratio() -> Decimal {
    Decimal::new(4, 1) // 0.4
}

fn default_scalping_trigger_segment() -> u32 {
    10
}

fn default_scalping_profit_threshold() -> Decimal {
    Decimal::new(2, 2) // 0.02%
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    pub style: OrderStyle,
    /// 第一腿限价最长等待。超时未成交则撤。
    #[serde(default = "default_limit_timeout_ms")]
    pub limit_timeout_ms: u64,
    /// maker 腿往点差内侧挪几个 tick。0 = 贴自家盘口（队尾，几乎不成交）。
    #[serde(default = "default_maker_inside_ticks")]
    pub maker_inside_ticks: u32,
    /// 第一腿限价最多挂几轮（每轮用最新盘口重算价格，部分成交累积）。
    #[serde(default = "default_limit_retry_count")]
    pub limit_retry_count: u32,
}

fn default_limit_timeout_ms() -> u64 {
    2000
}

fn default_maker_inside_ticks() -> u32 {
    1
}

fn default_limit_retry_count() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStyle {
    LimitMaker,
    MarketTaker,
    /// 吃单费率高的一所先挂限价，成交后再对另一所市价。
    LimitThenMarket,
    /// 激进限价（IOC）：市价腿失败后的兜底，用放大后的滑点当限价挂 IOC。
    ///
    /// 不是配置项，只由 `hedge_second_leg` 内部构造。和 `LimitMaker` 的区别
    /// 是**不驻留**——吃不到就整单撤销，所以成交确认必须走市价腿那套轮询回查，
    /// 不能用「限价单会挂在那里，单次查询就够」的假设，否则重新引入幻影成交。
    AggressiveLimit,
}

impl OrderStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LimitMaker => "limit_maker",
            Self::MarketTaker => "market_taker",
            Self::LimitThenMarket => "limit_then_market",
            Self::AggressiveLimit => "aggressive_limit",
        }
    }

    /// 页面下拉的三种挂单风格。无法识别时回落到先挂后吃。
    pub fn from_page(s: &str) -> Self {
        match s {
            "limit_maker" => Self::LimitMaker,
            "market_taker" => Self::MarketTaker,
            _ => Self::LimitThenMarket,
        }
    }

    /// 是否是「不驻留」的单：下完要么成交要么消失，不会挂在盘口上。
    /// 这类单的成交量只能靠回查/持仓 delta 确认。
    pub fn is_ioc(self) -> bool {
        matches!(self, Self::MarketTaker | Self::AggressiveLimit)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CostConfig {
    pub default_slip_pct: Decimal,
    /// 市价单的滑点保护上限（%）。以**决策信号价**为基准，超出交易所直接拒单。
    /// 对齐参考 `order_execution.max_slippage`（默认 0.001 = 0.1%）。
    ///
    /// 这是唯一的穿档保护：不在本地遍历订单簿估 VWAP，而是把限价交给交易所
    /// 强制执行——参考项目同样不做走档计算。
    #[serde(default = "default_max_slippage_pct")]
    pub max_slippage_pct: Decimal,
    /// 平仓/紧急平仓时把上面的上限放大这么多倍。
    /// 对齐参考的「紧急平仓 50 倍滑点」：平不掉的风险远大于穿档成本，
    /// 宁可吃滑点也要把仓位清掉。
    #[serde(default = "default_emergency_slip_mult")]
    pub emergency_slippage_multiplier: Decimal,
}

fn default_max_slippage_pct() -> Decimal {
    Decimal::new(1, 1) // 0.1%
}

fn default_emergency_slip_mult() -> Decimal {
    Decimal::from(50)
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryConfig {
    pub enabled: bool,
    pub db_path: String,
    pub sample_interval_secs: u64,
    pub window_hours: u64,
    pub min_points: usize,
    pub max_age_secs: u64,
    /// 库里已有 `natural_spreads` 则启动直接用；之后隔这么久用窗口样本重算一次。
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

fn default_refresh_interval_secs() -> u64 {
    1800
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueFees {
    pub maker: Decimal,
    pub taker: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub min_book_qty: HashMap<String, Decimal>,
    /// 单所自身买卖点差上限（%）。超过说明该所报价不可信 / 流动性极差。
    /// 对齐参考 `max_local_orderbook_spread_pct`。0 = 不检查。
    #[serde(default = "default_max_venue_spread_pct")]
    pub max_venue_spread_pct: Decimal,
    /// 价格稳定性观察窗口（秒）。0 = 关闭检查。
    /// 对齐参考 `price_stability_window_seconds`。
    #[serde(default = "default_price_stability_window_secs")]
    pub price_stability_window_secs: Decimal,
    /// 窗口内允许的最大波动（%）：`(max−min)/min×100`。0 = 关闭检查。
    /// 对齐参考 `price_stability_threshold_pct`。
    #[serde(default = "default_price_stability_threshold_pct")]
    pub price_stability_threshold_pct: Decimal,
    /// reduce-only 拉闸后，是否每小时发最小量探针验证开仓能力。
    /// 关掉则拉闸只能靠重启清除（状态纯内存）。
    #[serde(default = "default_true")]
    pub reduce_only_probe_enabled: bool,
    /// 探针在每小时的第几秒触发。对齐参考的 `HH:00:05`。
    #[serde(default = "default_reduce_only_probe_second")]
    pub reduce_only_probe_second: u32,
    /// 资金费率年化阈值（%/年）。对齐参考 `funding_rate_annual_threshold`。
    /// 开仓侧：净支付且年化超此值则不开。持仓侧：净支付超此值触发退出。
    /// 0 = 不检查。
    #[serde(default = "default_funding_annual_threshold_pct")]
    pub funding_annual_threshold_pct: Decimal,
    /// 费率不利需持续多久才平仓（分钟）。对齐参考
    /// `unfavorable_funding_rate_duration_minutes`。
    ///
    /// 这个门是必要的：费率是逐周期结算的，瞬时读数不利不等于要付钱。
    /// 没有持续性要求，费率在阈值边缘抖动就会反复开平，磨掉的手续费
    /// 远超省下的费率。
    #[serde(default = "default_funding_unfavorable_duration_minutes")]
    pub funding_unfavorable_duration_minutes: u64,
    /// 资金费率刷新间隔（秒）。两个所都是小时结算，不需要高频拉。
    #[serde(default = "default_funding_refresh_secs")]
    pub funding_refresh_secs: u64,
    /// 每日最大**开仓**次数。对齐参考 `max_daily_trades`。0 = 不限。
    ///
    /// 只算开仓：平仓占配额会让「开了一半就没配额平」，把仓位锁死。
    #[serde(default = "default_max_daily_opens")]
    pub max_daily_opens: u32,
    /// 单笔持仓最长持有时间（小时），超时自动平仓。
    /// 对齐参考 `max_position_duration` + `auto_close_on_timeout`。0 = 不限。
    #[serde(default = "default_max_position_hours")]
    pub max_position_hours: u64,
    /// 余额告警线（USDC）：低于此值停止该所开仓。0 = 关闭。
    /// 对齐参考 `min_balance_warning`。
    #[serde(default)]
    pub min_balance_warn_usdc: Decimal,
    /// 余额清仓线（USDC）：低于此值主动平掉该所全部仓位。0 = 关闭。
    /// 对齐参考 `min_balance_close_position`。
    ///
    /// 默认 0（关闭）而不是参考的 500：参考假定的是大账户，把这个默认值
    /// 带过来会让小额实盘账户一启动就被全量清仓。要用必须显式配。
    #[serde(default)]
    pub min_balance_close_usdc: Decimal,
    /// 单币最大名义敞口（USDC，建仓名义）。0 = 不限。
    ///
    /// 与参考项目的刻意分歧：参考用币本位（`max_single_token_position`），
    /// 本项目改成名义口径。原因：BTC/ETH/DOGE 的「10 个币」量纲完全不同，
    /// 一个全局默认值不可能同时对大币和小币有意义；名义口径跨币种可比，
    /// 且 `Position.entry_notional_usdc` 本来就有，不需要新数据。
    ///
    /// 用**建仓名义**而非实时市值：盘口拿不到时限额判定仍然有效。
    #[serde(default)]
    pub max_single_token_notional_usdc: Decimal,
    /// 全部持仓名义之和上限（USDC，建仓名义）。0 = 不限。
    ///
    /// 与参考的币本位 `max_total_position` 对应，但口径不同：
    /// 币本位下「所有币数量之和」把不同量纲直接相加（0.05 BTC + 1 ETH = 1.05），
    /// 这个数没有物理意义，名义 USDC 加总才是真实的总风险敞口。
    #[serde(default)]
    pub max_total_notional_usdc: Decimal,
    /// nonce / 限流错误的退避下限（秒）。对齐参考 `min_backoff_seconds`。
    #[serde(default = "default_backoff_min_secs")]
    pub backoff_min_secs: u64,
    /// 退避上限（秒）。对齐参考 `max_backoff_seconds`。
    #[serde(default = "default_backoff_max_secs")]
    pub backoff_max_secs: u64,
    /// 每次连续错误的退避倍数。对齐参考 `backoff_multiplier`。
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: u32,
    /// 多久没再出错就把连续计数清零（秒）。
    #[serde(default = "default_backoff_reset_secs")]
    pub backoff_reset_secs: u64,
}

fn default_backoff_min_secs() -> u64 {
    120 // 同参考
}

fn default_backoff_max_secs() -> u64 {
    3600 // 同参考
}

fn default_backoff_multiplier() -> u32 {
    2
}

fn default_backoff_reset_secs() -> u64 {
    600
}

fn default_max_daily_opens() -> u32 {
    100 // 同参考
}

fn default_max_position_hours() -> u64 {
    168 // 7 天，同参考
}

fn default_funding_annual_threshold_pct() -> Decimal {
    Decimal::new(10, 0) // 10%/年，同参考
}

fn default_funding_unfavorable_duration_minutes() -> u64 {
    60 // 同参考
}

fn default_funding_refresh_secs() -> u64 {
    60
}

fn default_reduce_only_probe_second() -> u32 {
    5
}

fn default_max_venue_spread_pct() -> Decimal {
    Decimal::new(5, 2) // 0.05%，对齐参考 max_local_orderbook_spread_pct
}

fn default_price_stability_window_secs() -> Decimal {
    Decimal::new(1, 0) // 1s，对齐参考 price_stability_window_seconds
}

fn default_price_stability_threshold_pct() -> Decimal {
    Decimal::new(1, 2) // 0.01%，对齐参考 price_stability_threshold_pct
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueFile {
    pub id: String,
    pub rest: String,
    pub ws: String,
    pub chain_id: u64,
    pub quote: String,
    pub fees: VenueFees,
    /// Lighter: `account_index`。SoDEX: `account_id` / 环境变量 `SODEX_ACCOUNT_ID`。
    #[serde(default, alias = "account_id")]
    pub account_index: i64,
    #[serde(default)]
    pub api_key_index: i32,
    /// SoDEX：`X-API-Key` 用的是名称，不是地址。环境变量 `SODEX_API_KEY_NAME`。
    #[serde(default)]
    pub api_key_name: String,
    /// SoDEX / Entropy 主钱包地址（查余额/持仓用，与签名私钥地址可能不同）。
    #[serde(default, alias = "account_address")]
    pub account_address: String,
    /// Lighter / SoDEX / Entropy 私钥。yaml `private_key` 或环境变量 `SODEX_PRIVATE_KEY` / `ENTROPY_PRIVATE_KEY`。
    #[serde(default, alias = "private_key")]
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
            api_key_name: self.api_key_name.trim().to_string(),
            api_key_private_key: key.to_string(),
        })
    }

    pub fn keys_ready(&self) -> bool {
        let Some(auth) = self.auth() else {
            return false;
        };
        if self.id == "sodex" {
            return auth.is_ready() && !auth.api_key_name.is_empty();
        }
        auth.is_ready()
    }
}

#[derive(Clone)]
pub struct VenueAuth {
    pub account_index: i64,
    pub api_key_index: i32,
    pub api_key_name: String,
    pub api_key_private_key: String,
}

impl std::fmt::Debug for VenueAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VenueAuth")
            .field("account_index", &self.account_index)
            .field("api_key_index", &self.api_key_index)
            .field("api_key_name", &self.api_key_name)
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
        let path = venue_config_path(id)?;
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read venue {}", path.display()))?;
        let mut venue: VenueFile = serde_yaml::from_str(&raw).context("parse venue yaml")?;
        overlay_venue_env(&mut venue);
        Ok(venue)
    }

    /// `min_qty`：该所对两腿 min_qty 的较大者（`Pair::min_qty()`）。拆单靠它
    /// 避免切出下不出去的尾巴。传 0 表示不约束（monitor/paper 路径）。
    pub fn grid_for(&self, base: &str, min_qty: Decimal) -> GridParams {
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
            close_stop_loss: self.grid.close_stop_loss_pct,
            t0_ratio: self.grid.t0_ratio,
            split_order_size: self.grid.split_order_size,
            min_qty,
            scalping_enabled: self.grid.scalping_enabled,
            scalping_trigger_segment: self.grid.scalping_trigger_segment,
            scalping_profit_threshold: self.grid.scalping_profit_threshold_pct,
        }
    }

    /// 某个所对上「先挂后吃」一次的手续费。
    pub fn round_leg_fee(&self, a: &VenueId, b: &VenueId) -> Decimal {
        match self.order.style {
            OrderStyle::LimitThenMarket => crate::exec::sequenced_fee(self, a, b),
            _ => self.exec_fee(a) + self.exec_fee(b),
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
            // 激进限价越过盘口吃单，收的是 taker 费率。
            OrderStyle::MarketTaker
            | OrderStyle::LimitThenMarket
            | OrderStyle::AggressiveLimit => self.taker_fee(venue),
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

    pub fn leverage_for(&self, venue: &str) -> Decimal {
        self.sizing
            .leverage_by_venue
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(venue))
            .map(|(_, v)| *v)
            .unwrap_or(self.sizing.leverage_multiplier)
    }
}

fn venue_file_name(id: &str) -> String {
    match id {
        "lighter_rh" => "lighter_robinhood.yaml".into(),
        other => format!("{other}.yaml"),
    }
}

fn venue_example_name(id: &str) -> String {
    match id {
        "lighter_rh" => "lighter_robinhood.example.yaml".into(),
        other => format!("{other}.example.yaml"),
    }
}

fn overlay_venue_env(venue: &mut VenueFile) {
    match venue.id.as_str() {
        "sodex" => {
            if let Ok(v) = std::env::var("SODEX_ACCOUNT_ID") {
                if let Ok(n) = v.trim().parse() {
                    venue.account_index = n;
                }
            }
            if let Ok(v) = std::env::var("SODEX_API_KEY_NAME") {
                if !v.trim().is_empty() {
                    venue.api_key_name = v;
                }
            }
            if let Ok(v) = std::env::var("SODEX_PRIVATE_KEY") {
                if !v.trim().is_empty() {
                    venue.api_key_private_key = v;
                }
            }
            if let Ok(v) = std::env::var("SODEX_ACCOUNT_ADDRESS") {
                if !v.trim().is_empty() {
                    venue.account_address = v;
                }
            }
        }
        "entropy" => {
            if let Ok(v) = std::env::var("ENTROPY_PRIVATE_KEY") {
                if !v.trim().is_empty() {
                    venue.api_key_private_key = v;
                }
            }
            if let Ok(v) = std::env::var("ENTROPY_ACCOUNT_ADDRESS") {
                if !v.trim().is_empty() {
                    venue.account_address = v;
                }
            }
        }
        _ => {}
    }
}

/// 正式 yaml（含密钥）优先；克隆仓库后只有 example 时回退，便于 `cargo test`。
fn venue_config_path(id: &str) -> Result<PathBuf> {
    let dir = PathBuf::from("config/venues");
    let primary = dir.join(venue_file_name(id));
    if primary.exists() {
        return Ok(primary);
    }
    let example = dir.join(venue_example_name(id));
    if example.exists() {
        return Ok(example);
    }
    anyhow::bail!("read venue {} (also missing {})", primary.display(), example.display());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn loads_default_yaml() {
        let cfg = AppConfig::load_from(Path::new("config/default.yaml")).unwrap();
        assert_eq!(cfg.system.log_dir, "data/logs");
        assert_eq!(cfg.venues, ["lighter", "lighter_rh", "sodex", "entropy"]);
        assert_eq!(cfg.grid_for("BTC", Decimal::ZERO).base_qty, dec!(0.001));
        let venue = cfg.load_venue("lighter_rh").unwrap();
        assert_eq!(venue.chain_id, 466324);
        assert_eq!(venue.quote, "USDG");
        assert_eq!(cfg.order.style, OrderStyle::LimitThenMarket);
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter")), dec!(0.005));
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter_rh")), dec!(0.012));
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter")), dec!(0.005));
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter_rh")), dec!(0.035));
        assert!(cfg.maker_fee(&VenueId::from("sodex")) > dec!(0));
        assert_eq!(
            cfg.exec_fee(&VenueId::from("lighter")) + cfg.exec_fee(&VenueId::from("lighter_rh")),
            dec!(0.040)
        );
        assert!(cfg.history.enabled);
        assert_eq!(cfg.history.min_points, 10);
        assert_eq!(cfg.history.refresh_interval_secs, 1800);
        assert_eq!(cfg.scan.min_spread_pct, dec!(0.1));
        assert!(cfg.scan.cross_use_natural);
        assert_eq!(cfg.grid.close_stop_loss_pct, Decimal::ZERO);
        assert_eq!(cfg.grid.scalping_trigger_segment, 10);
        assert_eq!(cfg.grid.scalping_profit_threshold_pct, dec!(0.02));
        assert_eq!(cfg.sizing.leverage_multiplier, dec!(2));
        assert_eq!(cfg.risk.max_venue_spread_pct, dec!(0.05));
        assert_eq!(cfg.risk.price_stability_window_secs, dec!(1));
        assert_eq!(cfg.risk.price_stability_threshold_pct, dec!(0.01));
        let sodex = cfg.load_venue("sodex").unwrap();
        assert_eq!(sodex.chain_id, 623);
        assert_eq!(sodex.id, "sodex");
        // 新增的成本一致性字段
        assert!(cfg.order.maker_inside_ticks >= 1);
        assert!(cfg.order.limit_retry_count >= 1);
        assert!(cfg.risk.max_venue_spread_pct > dec!(0));
    }

    /// 滑点保护必须配上，且紧急倍数 ≥ 1——否则平仓时保护反而更严，平不掉。
    #[test]
    fn slippage_protection_is_configured() {
        let cfg = AppConfig::load_from(Path::new("config/default.yaml")).unwrap();
        assert!(cfg.cost.max_slippage_pct > dec!(0), "市价单必须有滑点保护");
        assert!(
            cfg.cost.emergency_slippage_multiplier >= dec!(1),
            "紧急平仓的滑点上限不能比常规更严"
        );
        // 对齐参考：常规 0.1% 量级，紧急放大到 5%（0.1 × 50）
        let emergency = cfg.cost.max_slippage_pct * cfg.cost.emergency_slippage_multiplier;
        assert!(
            emergency > cfg.cost.max_slippage_pct,
            "紧急平仓必须比常规更宽松，确保平得掉"
        );
    }

}
