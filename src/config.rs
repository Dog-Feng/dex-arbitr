use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
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
    "web/dist".into()
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
        hedge_failed_legs: true,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SizingConfig {
    #[serde(default = "default_leverage")]
    pub leverage_multiplier: Decimal,
    #[serde(default = "default_depth_pct")]
    pub depth_pct: Decimal,
    #[serde(default = "default_refresh_balance_secs")]
    pub refresh_balance_secs: u64,
    /// REST 余额未接线时的回退可用 USDC（联调）。
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

fn default_leverage() -> Decimal {
    Decimal::from(5)
}

fn default_depth_pct() -> Decimal {
    Decimal::from(50)
}

fn default_refresh_balance_secs() -> u64 {
    1
}

fn default_sizing() -> SizingConfig {
    SizingConfig {
        leverage_multiplier: Decimal::from(5),
        depth_pct: Decimal::from(50),
        refresh_balance_secs: 1,
        fallback_available_usdc: None,
        margin_utilization_pct: Decimal::from(90),
        leverage_by_venue: HashMap::new(),
    }
}

/// 扫描环：间隔 + Top N + 样本窗。粗筛门槛仅 yaml，页面第一期不暴露。
#[derive(Debug, Clone, Deserialize)]
pub struct ScanConfig {
    pub enabled: bool,
    pub analysis_interval_ms: u64,
    pub watch_top: usize,
    /// 满这么多个秒级点才打分。与格子 `grid.window_samples` 分开。
    #[serde(default = "default_scan_window_samples")]
    pub window_samples: usize,
    /// 0 = `max(watch_top×4, 40)`。
    #[serde(default)]
    pub candidate_cap: usize,
    #[serde(default = "default_max_own_spread_pct")]
    pub max_own_spread_pct: Decimal,
    #[serde(default = "default_min_level_notional")]
    pub min_level_notional_usdc: Decimal,
    #[serde(default = "default_min_volume_24h")]
    pub min_volume_24h_usdc: Decimal,
    #[serde(default = "default_coarse_refresh_secs")]
    pub coarse_refresh_secs: u64,
}

fn default_scan() -> ScanConfig {
    ScanConfig {
        enabled: false,
        analysis_interval_ms: 50,
        watch_top: 20,
        window_samples: default_scan_window_samples(),
        candidate_cap: 0,
        max_own_spread_pct: default_max_own_spread_pct(),
        min_level_notional_usdc: default_min_level_notional(),
        min_volume_24h_usdc: default_min_volume_24h(),
        coarse_refresh_secs: default_coarse_refresh_secs(),
    }
}

fn default_scan_window_samples() -> usize {
    60
}

fn default_max_own_spread_pct() -> Decimal {
    Decimal::new(15, 2)
}

fn default_min_level_notional() -> Decimal {
    Decimal::from(200)
}

fn default_min_volume_24h() -> Decimal {
    Decimal::from(10_000_000)
}

fn default_coarse_refresh_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemConfig {
    pub data_freshness_ms: u64,
    /// 文本日志目录。按天滚动 `dex-arbitr.YYYY-MM-DD.log`。空 = 只打 stderr。
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
}

fn default_log_dir() -> String {
    "data/logs".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct PairsConfig {
    pub defaults: PairDefaults,
    #[serde(default)]
    pub enabled: Vec<PairSetting>,
}

fn default_target_bp() -> Decimal {
    Decimal::ONE
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairDefaults {
    pub max_segments: u32,
    /// 一格开平目标净利（bp）。1 bp = 0.01%。运行时反推 Δ。
    #[serde(default = "default_target_bp")]
    pub target_bp: Decimal,
    #[serde(default)]
    pub split_order_size: Decimal,
}

impl Default for PairDefaults {
    fn default() -> Self {
        Self {
            max_segments: 3,
            target_bp: default_target_bp(),
            split_order_size: Decimal::ZERO,
        }
    }
}

/// 逐所对覆盖。`venues` 两个 id，顺序无关。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairOverride {
    pub venues: Vec<String>,
    #[serde(default)]
    pub max_segments: Option<u32>,
    #[serde(default)]
    pub target_bp: Option<Decimal>,
    #[serde(default)]
    pub split_order_size: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairSetting {
    pub symbol: String,
    /// 每格币数。必填，无默认。
    pub base_qty: Decimal,
    #[serde(default)]
    pub max_segments: Option<u32>,
    #[serde(default)]
    pub target_bp: Option<Decimal>,
    #[serde(default)]
    pub split_order_size: Option<Decimal>,
    #[serde(default)]
    pub overrides: Vec<PairOverride>,
}

impl PairSetting {
    pub fn override_for(&self, a: &str, b: &str) -> Option<&PairOverride> {
        self.overrides.iter().find(|o| {
            o.venues.len() == 2
                && ((o.venues[0].eq_ignore_ascii_case(a) && o.venues[1].eq_ignore_ascii_case(b))
                    || (o.venues[0].eq_ignore_ascii_case(b) && o.venues[1].eq_ignore_ascii_case(a)))
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GridConfig {
    pub persistence_ms: u64,
    /// 时间窗内累计达标次数。0 = 连续不掉线。
    #[serde(default)]
    pub persistence_min_hits: u32,
    /// 滑动窗口点数。满窗后才有 μ，才允许开仓。
    #[serde(default = "default_window_samples")]
    pub window_samples: usize,
    /// 入窗间隔。1000 = 1Hz，同一间隔内多次盘口覆盖为最后一次。
    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,
    /// STEP 滞后（格）。加仓 raw ≥ k+1−h，减仓 raw ≤ k−1+h。
    #[serde(default = "default_step_hysteresis")]
    pub step_hysteresis: Decimal,
    /// true = 阶段 2 邻档限价；false = 阶段 1 撞线双市价。
    #[serde(default)]
    pub symmetric_limit: bool,
    /// 空仓 |μ_live−μ_quote| ≥ 此比例×Δ 才改挂单价。
    #[serde(default = "default_quote_reprice_ratio")]
    pub quote_reprice_ratio: Decimal,
    /// 加仓档与当前可执行价差至少隔这么多格（×Δ）才挂。减仓档不限制。
    #[serde(default = "default_min_quote_gap_ratio")]
    pub min_quote_gap_ratio: Decimal,
}

fn default_window_samples() -> usize {
    10_000
}

fn default_sample_interval_ms() -> u64 {
    1000
}

fn default_step_hysteresis() -> Decimal {
    Decimal::new(25, 2)
}

fn default_quote_reprice_ratio() -> Decimal {
    Decimal::new(2, 1)
}

fn default_min_quote_gap_ratio() -> Decimal {
    Decimal::new(3, 1)
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    /// 追逐型限价（第二腿 IOC / 非邻档）单轮最长等待。邻档不走这个。
    #[serde(default = "default_limit_timeout_ms")]
    pub limit_timeout_ms: u64,
    /// 邻档第一腿存活。0 = 只按事件撤，不按秒超时。
    #[serde(default)]
    pub adjacent_timeout_ms: u64,
    /// maker 腿往点差内侧挪几个 tick。0 = 贴自家盘口（队尾，几乎不成交）。
    #[serde(default = "default_maker_inside_ticks")]
    pub maker_inside_ticks: u32,
    /// 第一腿限价最多挂几轮（每轮用最新盘口重算价格，部分成交累积）。
    #[serde(default = "default_limit_retry_count")]
    pub limit_retry_count: u32,
    /// 市价 / IOC / 撤单后：等该单私有 WS 这么久，没有再 REST 查一次。
    /// sidecar 与 Rust 撤后确认共用。页面可热改，下次 place 生效。
    #[serde(default = "default_ioc_fill_wait_ms")]
    pub ioc_fill_wait_ms: u64,
}

impl OrderConfig {
    /// 夹在 100ms–30s，避免把 sidecar 请求超时拖垮。
    pub fn ioc_fill_wait(&self) -> Duration {
        Duration::from_millis(self.ioc_fill_wait_ms.clamp(100, 30_000))
    }

    pub fn ioc_fill_wait_ms_clamped(&self) -> u64 {
        self.ioc_fill_wait_ms.clamp(100, 30_000)
    }
}

fn default_limit_timeout_ms() -> u64 {
    2000
}

pub fn default_ioc_fill_wait_ms() -> u64 {
    1000
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

fn default_true() -> bool {
    true
}

fn default_history_sample_interval_secs() -> u64 {
    5
}

fn default_history_window_hours() -> u64 {
    24
}

fn default_refresh_interval_secs() -> u64 {
    1800
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryConfig {
    /// 不再热改。缺省开库；扫描页只留 min_points（样本数）。
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub db_path: String,
    #[serde(default = "default_history_sample_interval_secs")]
    pub sample_interval_secs: u64,
    #[serde(default = "default_history_window_hours")]
    pub window_hours: u64,
    pub min_points: usize,
    pub max_age_secs: u64,
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueFees {
    pub maker: Decimal,
    pub taker: Decimal,
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

    /// `min_qty`：该所对两腿 min_qty 的较大者。拆单靠它避免切出下不出去的尾巴。
    /// 找不到配置返回 `None`，调用方跳过该对。
    pub fn grid_for(
        &self,
        symbol: &str,
        venue_a: &str,
        venue_b: &str,
        min_qty: Decimal,
    ) -> Option<GridParams> {
        let s = self.pair_setting(symbol)?;
        let d = &self.pairs.defaults;
        let ov = s.override_for(venue_a, venue_b);

        macro_rules! pick {
            ($field:ident, $def:expr) => {
                ov.and_then(|o| o.$field).or(s.$field).unwrap_or($def)
            };
        }

        Some(GridParams {
            base_qty: s.base_qty,
            step: self.delta_from_target(symbol, venue_a, venue_b),
            max_segments: pick!(max_segments, d.max_segments),
            split_order_size: pick!(split_order_size, d.split_order_size),
            min_qty,
            persistence: Duration::from_millis(self.grid.persistence_ms),
            persistence_min_hits: self.grid.persistence_min_hits,
        })
    }

    /// 阶段 1 双腿市价：开+平四腿 taker。
    pub fn market_round_trip_taker(&self, a: &VenueId, b: &VenueId) -> Decimal {
        (self.taker_fee(a) + self.taker_fee(b)) * Decimal::from(2)
    }

    pub fn target_bp_for(&self, symbol: &str, venue_a: &str, venue_b: &str) -> Decimal {
        let d = &self.pairs.defaults;
        let Some(s) = self.pair_setting(symbol) else {
            return d.target_bp;
        };
        let ov = s.override_for(venue_a, venue_b);
        ov.and_then(|o| o.target_bp)
            .or(s.target_bp)
            .unwrap_or(d.target_bp)
    }

    /// 静态格距（C=0）。运行时 `process_pair` 用两所 live 点差中枢的平均覆盖。
    fn delta_from_target(&self, symbol: &str, venue_a: &str, venue_b: &str) -> Decimal {
        let target = self.target_bp_for(symbol, venue_a, venue_b);
        let a = VenueId::from(venue_a);
        let b = VenueId::from(venue_b);
        let rt = self.market_round_trip_taker(&a, &b);
        crate::domain::grid_step_from_target_bp(
            target,
            rt,
            Decimal::ZERO,
            self.grid.step_hysteresis,
        )
    }

    pub fn pair_setting(&self, symbol: &str) -> Option<&PairSetting> {
        self.pairs
            .enabled
            .iter()
            .find(|p| p.symbol.eq_ignore_ascii_case(symbol))
    }

    /// 开/平一腿各一次的手续费。策略固定双腿市价，两所都按 taker。
    pub fn round_leg_fee(&self, a: &VenueId, b: &VenueId) -> Decimal {
        self.taker_fee(a) + self.taker_fee(b)
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

    /// 热路径手续费。策略固定双腿市价（含激进限价兜底），一律按 taker。
    pub fn exec_fee(&self, venue: &VenueId) -> Decimal {
        self.taker_fee(venue)
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
        let btc = cfg.grid_for("BTC", "lighter", "sodex", Decimal::ZERO).unwrap();
        assert_eq!(btc.base_qty, dec!(0.001));
        let cheap = cfg.grid_for("BTC", "lighter", "entropy", Decimal::ZERO).unwrap();
        let cheap_rt = cfg.market_round_trip_taker(
            &VenueId::from("lighter"),
            &VenueId::from("entropy"),
        );
        assert_eq!(
            cheap.step,
            crate::domain::grid_step_from_target_bp(
                cfg.target_bp_for("BTC", "lighter", "entropy"),
                cheap_rt,
                Decimal::ZERO,
                cfg.grid.step_hysteresis
            )
        );
        assert_eq!(cheap.max_segments, 6);
        let expensive = cfg.grid_for("BTC", "sodex", "lighter_rh", Decimal::ZERO).unwrap();
        let exp_rt = cfg.market_round_trip_taker(
            &VenueId::from("sodex"),
            &VenueId::from("lighter_rh"),
        );
        assert_eq!(
            expensive.step,
            crate::domain::grid_step_from_target_bp(
                cfg.pairs.defaults.target_bp,
                exp_rt,
                Decimal::ZERO,
                cfg.grid.step_hysteresis
            )
        );
        assert_eq!(
            cfg.grid_for("BTC", "entropy", "lighter", Decimal::ZERO)
                .unwrap()
                .step,
            cheap.step,
            "venue order must not change override match"
        );
        let venue = cfg.load_venue("lighter_rh").unwrap();
        assert_eq!(venue.chain_id, 466324);
        assert_eq!(venue.quote, "USDG");
        let lighter = cfg.load_venue("lighter").unwrap();
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter")), lighter.fees.maker);
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter")), lighter.fees.taker);
        assert_eq!(cfg.maker_fee(&VenueId::from("lighter_rh")), venue.fees.maker);
        assert_eq!(cfg.taker_fee(&VenueId::from("lighter_rh")), venue.fees.taker);
        assert!(cfg.maker_fee(&VenueId::from("sodex")) > dec!(0));
        assert_eq!(
            cfg.exec_fee(&VenueId::from("lighter")) + cfg.exec_fee(&VenueId::from("lighter_rh")),
            cfg.taker_fee(&VenueId::from("lighter")) + cfg.taker_fee(&VenueId::from("lighter_rh"))
        );
        assert!(cfg.history.enabled);
        assert_eq!(cfg.history.min_points, 10);
        assert_eq!(cfg.history.refresh_interval_secs, 1800);
        assert_eq!(cfg.scan.watch_top, 20);
        assert_eq!(cfg.scan.analysis_interval_ms, 50);
        assert_eq!(cfg.scan.window_samples, 120);
        assert_eq!(cfg.grid.persistence_ms, 1000);
        assert_eq!(cfg.grid.persistence_min_hits, 7);
        assert_eq!(cfg.grid.window_samples, 1000);
        assert_eq!(cfg.grid.sample_interval_ms, 1000);
        assert_eq!(cfg.grid.step_hysteresis, Decimal::ZERO);
        assert!(!cfg.grid.symmetric_limit);
        assert_eq!(cfg.grid.quote_reprice_ratio, dec!(0.2));
        assert_eq!(cfg.grid.min_quote_gap_ratio, dec!(0.3));
        assert_eq!(cfg.order.adjacent_timeout_ms, 0);
        assert_eq!(cfg.order.ioc_fill_wait_ms, 1000);
        assert_eq!(cfg.pairs.defaults.target_bp, dec!(1));
        assert_eq!(cfg.sizing.leverage_multiplier, dec!(5));
        let sodex = cfg.load_venue("sodex").unwrap();
        assert_eq!(sodex.chain_id, 623);
        assert_eq!(sodex.id, "sodex");
        // 新增的成本一致性字段
        assert!(cfg.order.maker_inside_ticks >= 1);
        assert!(cfg.order.limit_retry_count >= 1);
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
