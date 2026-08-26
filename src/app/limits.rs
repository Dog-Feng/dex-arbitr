//! 全局风控限额。对齐参考 `GlobalRiskController` 的四个维度：
//! 每日交易次数、持仓时长、余额下限、单币/总持仓数量。
//!
//! 都是**纯状态 + 纯判定**，不碰交易所也不发单——调用方拿到判定后自己决定
//! 是跳过开仓还是下平仓单。这样每条规则都能单测，不用起执行链路。

use std::time::Duration;

use chrono::Local;
use rust_decimal::Decimal;

/// 每日交易次数。对齐参考 `daily_trade_count` + `check_daily_trade_limit`。
///
/// 用**本地日期字符串**而不是 Unix 秒 / 86400：参考按 `%Y-%m-%d` 归日，
/// 那是本地自然日。UTC 日索引在东八区会在早上 8 点跳日，和运维直觉不符。
///
/// 只统计**开仓**：平仓不该被限额挡住（挡了就锁死仓位），所以也不该占用
/// 次数配额——否则一天 100 次里有一半是平仓，实际只能开 50 笔。
#[derive(Debug, Default)]
pub struct DailyTrades {
    date: String,
    count: u32,
}

impl DailyTrades {
    fn today() -> String {
        Local::now().format("%Y-%m-%d").to_string()
    }

    fn roll(&mut self) {
        let today = Self::today();
        if self.date != today {
            self.date = today;
            self.count = 0;
        }
    }

    /// 还能不能再开一笔。`max == 0` 表示不限制。
    pub fn allows(&mut self, max: u32) -> bool {
        if max == 0 {
            return true;
        }
        self.roll();
        self.count < max
    }

    pub fn record(&mut self) {
        self.roll();
        self.count += 1;
    }

    pub fn count(&mut self) -> u32 {
        self.roll();
        self.count
    }
}

/// 余额健康度。对齐参考 `_check_all_balances` 的两级阈值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceHealth {
    Ok,
    /// 低于告警线：还能平仓，但不再开新仓。
    Low,
    /// 低于清仓线：主动平掉该所所有仓位。
    Critical,
}

/// 单所余额分级。两个阈值都可以设 0 关闭。
///
/// 注意 `critical` 必须小于 `warn`，参考在配置校验里也强制这一点
/// （`min_balance_close_position >= min_balance_warning` 直接报错）。
/// 这里不 panic，按「先判 critical」的顺序自然退化。
pub fn balance_health(available: Decimal, warn: Decimal, critical: Decimal) -> BalanceHealth {
    if critical > Decimal::ZERO && available < critical {
        return BalanceHealth::Critical;
    }
    if warn > Decimal::ZERO && available < warn {
        return BalanceHealth::Low;
    }
    BalanceHealth::Ok
}

/// 持仓是否超时。`max_hours == 0` 表示不限制。
/// 对齐参考 `_check_position_duration` + `auto_close_on_timeout`。
pub fn position_expired(held: Duration, max_hours: u64) -> bool {
    max_hours > 0 && held.as_secs() >= max_hours.saturating_mul(3600)
}

/// 单币 / 总持仓的**名义敞口**上限（USDC）。
///
/// ── 与参考的刻意分歧：口径从币本位改成名义 ──
///
/// 参考用的是币本位（`max_single_token_position` 默认 10 = 10 个币），本项目
/// 早期也照搬了，实践下来两个问题：
///
/// 1. **跨币种不可比**。10 BTC 是天量、10 DOGE 是零头，一个全局默认值对
///    任何多币组合都无意义。参考靠 per-symbol 覆盖绕开，等于把「配置正确性」
///    整个甩给运维——漏配一个币就等于该币不设限。
/// 2. **总量上限根本没有物理意义**。币本位下「所有币数量之和」是把不同量纲
///    直接相加：`0.05 BTC + 1 ETH + 10000 DOGE = 10001.05`。这个数没法配——
///    定成 30 时 BTC/ETH 永远撞不到，一笔 DOGE 直接爆掉。
///
/// 名义口径两个问题都不存在：一个数管所有币，币价波动时敞口自动重估。
/// `Position.entry_notional_usdc` 本来就有（`reserved_margin_by_venue` 在用），
/// 不需要新数据。
///
/// **用的是建仓名义，不是实时市值。** 建仓名义是确定的历史事实，实时市值要
/// 依赖当前盘口——盘口拿不到时限额判定会跟着失效，而限额恰恰要在行情异常时
/// 最可靠。代价是币价大涨后实际敞口高于记账值，由止损和 `max_position_hours`
/// 兜底。
#[derive(Debug, Clone, Default)]
pub struct NotionalLimits {
    /// 单币名义上限（USDC）。0 = 不限。
    pub per_symbol: Decimal,
    /// 全部持仓名义之和上限（USDC）。0 = 不限。
    pub total: Decimal,
}

impl NotionalLimits {
    /// 新增 `add_notional`（USDC）是否会破限额。
    ///
    /// `held_symbol` 是该币当前名义之和，`held_total` 是全部持仓名义之和。
    /// 返回 `Err(原因)` 便于直接进日志。
    pub fn check(
        &self,
        base: &str,
        add_notional: Decimal,
        held_symbol: Decimal,
        held_total: Decimal,
    ) -> Result<(), String> {
        let add = add_notional.abs();
        if self.per_symbol > Decimal::ZERO && held_symbol.abs() + add > self.per_symbol {
            return Err(format!(
                "单币名义超限: {} {} + {} > {} USDC",
                base,
                held_symbol.abs().round_dp(2),
                add.round_dp(2),
                self.per_symbol
            ));
        }
        if self.total > Decimal::ZERO && held_total.abs() + add > self.total {
            return Err(format!(
                "总名义超限: {} + {} > {} USDC",
                held_total.abs().round_dp(2),
                add.round_dp(2),
                self.total
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn daily_limit_counts_and_blocks() {
        let mut d = DailyTrades::default();
        assert!(d.allows(2));
        d.record();
        assert!(d.allows(2));
        d.record();
        assert!(!d.allows(2));
        assert_eq!(d.count(), 2);
    }

    #[test]
    fn daily_limit_zero_means_unlimited() {
        let mut d = DailyTrades::default();
        for _ in 0..500 {
            d.record();
        }
        assert!(d.allows(0));
    }

    #[test]
    fn balance_tiers() {
        assert_eq!(balance_health(dec!(2000), dec!(1000), dec!(500)), BalanceHealth::Ok);
        assert_eq!(balance_health(dec!(800), dec!(1000), dec!(500)), BalanceHealth::Low);
        assert_eq!(balance_health(dec!(400), dec!(1000), dec!(500)), BalanceHealth::Critical);
        // 关闭时不管余额多低都是 Ok——默认必须是关，否则小账户一启动就被清仓。
        assert_eq!(balance_health(dec!(1), Decimal::ZERO, Decimal::ZERO), BalanceHealth::Ok);
    }

    #[test]
    fn duration_gate() {
        assert!(!position_expired(Duration::from_secs(3600), 168));
        assert!(position_expired(Duration::from_secs(168 * 3600), 168));
        assert!(!position_expired(Duration::from_secs(u32::MAX as u64), 0));
    }

    #[test]
    fn notional_per_symbol_gate() {
        let l = NotionalLimits {
            per_symbol: dec!(500),
            total: dec!(2000),
        };
        // held 400 + add 200 > 500 → 拒
        assert!(l.check("BTC", dec!(200), dec!(400), dec!(400)).is_err());
        // held 400 + add 50 ≤ 500 → 过
        assert!(l.check("BTC", dec!(50), dec!(400), dec!(400)).is_ok());
    }

    #[test]
    fn notional_total_gate() {
        let l = NotionalLimits {
            per_symbol: Decimal::ZERO,
            total: dec!(2000),
        };
        // 总已有 1900 + 200 > 2000 → 拒
        assert!(l.check("ETH", dec!(200), dec!(1900), dec!(1900)).is_err());
        // 总已有 1900 + 50 ≤ 2000 → 过
        assert!(l.check("ETH", dec!(50), dec!(1900), dec!(1900)).is_ok());
    }

    #[test]
    fn notional_zero_limits_disable_checks() {
        let l = NotionalLimits::default();
        assert!(l
            .check("BTC", dec!(1_000_000), dec!(9_999_999), dec!(99_999_999))
            .is_ok());
    }

    /// 空头名义为负，限额必须按绝对值算，否则做空可以无限加仓。
    #[test]
    fn notional_negative_held_uses_absolute_value() {
        let l = NotionalLimits {
            per_symbol: dec!(500),
            total: Decimal::ZERO,
        };
        assert!(l.check("ETH", dec!(200), dec!(-400), dec!(-400)).is_err());
    }
}
