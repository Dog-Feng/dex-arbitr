//! 资金费率维度：净费率与年化。
//!
//! ── 对齐参考与三处刻意偏离 ──
//!
//! 参考 `_get_funding_rate_data`（spread_pipeline.py:511）算的是
//! `diff = abs(sell − buy)`，年化 `diff × 365 × 3`，判有利靠单独的
//! `is_favorable_for_position = sell > buy`。这里保留它的两条判据
//! （开仓看能不能赚 / 赔多少，持仓看方向反转与幅度），但三处改掉：
//!
//! 1. **年化系数取自结算周期**，不写死 8h。参考硬编 `× 3 × 365`（= 1095），
//!    我们两个所都是**小时结算**，sidecar 按市场返回 `interval_secs`
//!    （SoDEX 由 `/markets/symbols` 的 `fundingInterval` 给出，Lighter 文档
//!    写明按小时）。拿 1095 套小时费率会把年化低报 8 倍。
//!
//! 2. **净费率带符号**。参考的 `abs()` 让它自己的开仓门第二条成了死代码：
//!    ```python
//!    if diff > 0: return True          # 能赚
//!    if diff < 0:                      # abs() 永不为负 → 永远不进
//!        if abs(annual) < threshold: return True
//!    ```
//!    它的 dataclass 注释也写了「资金费率差（百分比，永远≥0）」。结果是
//!    年化阈值在开仓侧从未生效——赔多少都放行。符号必须留在数值里。
//!
//! 3. **止损判不利方向，不判幅度**。参考检查 2 用 `abs(annual) >= threshold`,
//!    费率**强烈有利**时同样触发平仓，把在赚钱的仓砍掉。这里只在净费率为负
//!    （我方净支付）时才比阈值。
//!
//! ── 符号约定 ──
//! 费率为正表示多头付给空头。我们买腿做多、卖腿做空，所以
//! `net = sell_rate − buy_rate`：卖腿收得多、买腿付得少时为正 = 我方净收。

use rust_decimal::Decimal;

use super::VenueId;

/// 一个所在某市场的当期资金费率。
#[derive(Debug, Clone)]
pub struct VenueFunding {
    pub venue: VenueId,
    /// 每个结算周期的费率，**小数**（0.0001 = 0.01%）。正 = 多头支付。
    pub rate: Decimal,
    /// 结算周期（秒）。由交易所返回，不假定 1h/8h。
    pub interval_secs: u32,
}

/// 一对腿的资金费率维度结果。
#[derive(Debug, Clone)]
pub struct FundingView {
    pub buy: VenueId,
    pub sell: VenueId,
    pub buy_rate: Decimal,
    pub sell_rate: Decimal,
    /// 每周期净费率（%）。**带符号**：正 = 我方净收，负 = 我方净付。
    pub net_pct: Decimal,
    /// 年化净费率（%/年）。带符号，系数由 `interval_secs` 推出。
    pub net_annual_pct: Decimal,
}

impl FundingView {
    /// 费率方向是否有利于当前持仓方向。对齐参考 `is_favorable_for_position`。
    pub fn favorable(&self) -> bool {
        self.net_pct >= Decimal::ZERO
    }
}

const SECS_PER_YEAR: i64 = 365 * 24 * 3600;

/// 年化倍数 = 一年的结算次数。参考写死 1095（8h × 365），这里由周期推。
fn periods_per_year(interval_secs: u32) -> Option<Decimal> {
    if interval_secs == 0 {
        return None;
    }
    Some(Decimal::from(SECS_PER_YEAR) / Decimal::from(interval_secs))
}

/// 计算净费率与年化。两腿周期不同则取**较短**的一侧年化——
/// 结算更频繁的那腿决定了我们多快真金白银付出去。
pub fn funding_view(buy: &VenueFunding, sell: &VenueFunding) -> Option<FundingView> {
    let interval = buy.interval_secs.min(sell.interval_secs);
    let periods = periods_per_year(interval)?;
    let hundred = Decimal::from(100);
    // 买腿做多付费率，卖腿做空收费率 → 净收 = sell − buy。
    let net = sell.rate - buy.rate;
    Some(FundingView {
        buy: buy.venue.clone(),
        sell: sell.venue.clone(),
        buy_rate: buy.rate,
        sell_rate: sell.rate,
        net_pct: net * hundred,
        net_annual_pct: net * periods * hundred,
    })
}

/// 开仓侧的资金费率门。对齐参考两条判据，但用带符号净费率，
/// 让「净支付」那条真正生效（参考里是死代码）。
///
/// - 净收（`net >= 0`）→ 放行。
/// - 净付 → 只有年化亏损小于阈值才放行。
///
/// `annual_threshold_pct` 为 0 表示不检查（与其他风控项一致）。
pub fn allows_open(view: &FundingView, annual_threshold_pct: Decimal) -> bool {
    if view.favorable() || annual_threshold_pct <= Decimal::ZERO {
        return true;
    }
    view.net_annual_pct.abs() < annual_threshold_pct
}

/// 持仓侧：当前费率是否已对本仓不利到该退出。
///
/// 只在**净支付**时比阈值。参考用 `abs(annual) >= threshold`,费率强烈
/// 有利时也会触发，等于把在赚钱的仓砍了。
///
/// `opened_favorable` 是开仓时的方向，用于识别参考检查 1 的「方向反转」。
pub fn unfavorable_for_position(
    view: &FundingView,
    opened_favorable: bool,
    annual_threshold_pct: Decimal,
) -> Option<FundingExit> {
    if opened_favorable && !view.favorable() {
        return Some(FundingExit::Reversed);
    }
    if !view.favorable()
        && annual_threshold_pct > Decimal::ZERO
        && view.net_annual_pct.abs() >= annual_threshold_pct
    {
        return Some(FundingExit::TooCostly);
    }
    None
}

/// 资金费率触发退出的原因。对齐参考的检查 1 / 检查 2。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundingExit {
    /// 开仓时有利，现在方向反转成净支付。
    Reversed,
    /// 净支付且年化超阈值。
    TooCostly,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn f(venue: &str, rate: Decimal, interval: u32) -> VenueFunding {
        VenueFunding {
            venue: VenueId::from(venue),
            rate,
            interval_secs: interval,
        }
    }

    /// 小时结算的年化系数是 8760，不是参考硬编的 1095。
    #[test]
    fn hourly_annualizes_by_8760_not_1095() {
        let buy = f("lighter", dec!(0), 3600);
        let sell = f("sodex", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(v.net_pct, dec!(0.01));
        assert_eq!(v.net_annual_pct, dec!(87.60));
    }

    /// 8h 结算时退回参考的 1095——系数由周期推，不是写死小时。
    #[test]
    fn eight_hour_matches_reference_coefficient() {
        let buy = f("a", dec!(0), 8 * 3600);
        let sell = f("b", dec!(0.0001), 8 * 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(v.net_annual_pct, dec!(10.95));
    }

    /// 净费率必须带符号：买腿费率更高 = 我方净付。
    /// 参考的 abs() 在这里会得到 +,把净付伪装成净收。
    #[test]
    fn net_rate_keeps_sign_when_paying() {
        let buy = f("lighter", dec!(0.0002), 3600);
        let sell = f("sodex", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(v.net_pct, dec!(-0.01));
        assert!(!v.favorable());
    }

    /// 参考开仓门的第二条是死代码（abs 永不为负）。这里必须真拦下
    /// 年化超阈值的净支付。
    #[test]
    fn open_gate_blocks_costly_payment() {
        let buy = f("lighter", dec!(0.0002), 3600);
        let sell = f("sodex", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        // 年化 −87.6%，阈值 10% → 拦。
        assert!(!allows_open(&v, dec!(10)));
        // 阈值放到 100% 以上 → 放行。
        assert!(allows_open(&v, dec!(100)));
        // 0 = 不检查。
        assert!(allows_open(&v, dec!(0)));
    }

    #[test]
    fn open_gate_always_allows_when_earning() {
        let buy = f("lighter", dec!(0), 3600);
        let sell = f("sodex", dec!(0.001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert!(v.favorable());
        assert!(allows_open(&v, dec!(1)));
    }

    /// 强烈**有利**的费率不该触发平仓。参考用 abs(annual) >= threshold,
    /// 在这个用例上会把正在赚钱的仓砍掉。
    #[test]
    fn strongly_favorable_never_exits() {
        let buy = f("lighter", dec!(0), 3600);
        let sell = f("sodex", dec!(0.001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert!(v.net_annual_pct > dec!(800));
        assert_eq!(unfavorable_for_position(&v, true, dec!(10)), None);
    }

    #[test]
    fn detects_direction_reversal() {
        let buy = f("lighter", dec!(0.0002), 3600);
        let sell = f("sodex", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(
            unfavorable_for_position(&v, true, dec!(0)),
            Some(FundingExit::Reversed)
        );
    }

    /// 开仓时就是净付（不算反转），靠幅度判。
    #[test]
    fn costly_payment_exits_without_reversal() {
        let buy = f("lighter", dec!(0.0002), 3600);
        let sell = f("sodex", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(
            unfavorable_for_position(&v, false, dec!(10)),
            Some(FundingExit::TooCostly)
        );
        assert_eq!(unfavorable_for_position(&v, false, dec!(100)), None);
    }

    /// 两腿周期不同取较短的一侧：结算更频繁的那腿决定真实付出速度。
    #[test]
    fn mixed_intervals_use_shorter() {
        let buy = f("a", dec!(0), 8 * 3600);
        let sell = f("b", dec!(0.0001), 3600);
        let v = funding_view(&buy, &sell).unwrap();
        assert_eq!(v.net_annual_pct, dec!(87.60));
    }

    #[test]
    fn zero_interval_is_rejected() {
        let buy = f("a", dec!(0), 0);
        let sell = f("b", dec!(0.0001), 3600);
        assert!(funding_view(&buy, &sell).is_none());
    }
}
