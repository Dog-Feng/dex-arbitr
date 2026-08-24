//! 对齐参考项目 `execute_limit_market_mode`：单任务内完成
//! 第一腿限价 → 等成交（order_status + 仓位 delta）→ 第二腿市价 → 失败回滚。

use anyhow::{bail, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::app::reconcile::{first_leg_fill_delta, symbol_matches_symbol};
use crate::config::AppConfig;
use crate::domain::Bbo;
use crate::exec::{HedgeExecutor, HedgePlan};

const POLL_INTERVAL_MS: u64 = 50;
const CANCEL_RACE_MS: u64 = 500;

pub struct LimitMarketRun {
    pub baseline: Decimal,
    pub min_qty: Decimal,
    pub cancel: Arc<AtomicBool>,
}

impl HedgeExecutor {
    /// 参考 OSE.execute_limit_market_mode：post → wait fill → hedge second（同 task）。
    pub async fn execute_limit_market(
        cfg: &AppConfig,
        adapters: &HashMap<String, Arc<dyn crate::exchange::ExchangePort>>,
        plan: &HedgePlan,
        books: &HashMap<(String, String), Bbo>,
        ctx: &LimitMarketRun,
    ) -> Result<crate::exec::ExecResult> {
        let post = Self::post_first_leg(cfg, adapters, plan, books, false).await?;
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            resting = post.resting,
            filled_qty = %post.first.qty,
            order_id = ?post.order_id,
            "limit_market: first leg posted"
        );

        let mut filled_qty = post.first.qty;
        if filled_qty <= Decimal::ZERO {
            filled_qty = Self::wait_first_fill(
                cfg,
                adapters,
                plan,
                post.order_id.as_deref(),
                ctx,
            )
            .await?;
        }

        if filled_qty <= Decimal::ZERO {
            bail!("limit_zero_fill: first leg no fill");
        }

        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            second = %plan.second.venue,
            qty = %filled_qty,
            "limit_market: first leg filled, hedging second immediately"
        );
        Self::hedge_second_leg(cfg, adapters, plan, books, false, filled_qty).await
    }

    async fn wait_first_fill(
        cfg: &AppConfig,
        adapters: &HashMap<String, Arc<dyn crate::exchange::ExchangePort>>,
        plan: &HedgePlan,
        order_id: Option<&str>,
        ctx: &LimitMarketRun,
    ) -> Result<Decimal> {
        let timeout = Duration::from_millis(cfg.order.limit_timeout_ms.max(2000));
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if ctx.cancel.load(Ordering::Relaxed) {
                warn!(pair = %plan.pair_id, "limit_market: cancel requested during wait");
                break;
            }
            if let Some(qty) =
                Self::detect_first_fill(adapters, plan, order_id, ctx.baseline, ctx.min_qty).await?
            {
                return Ok(qty);
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        // 对齐参考：撤单前再查一次；撤单后竞态窗口再查（_wait_fill_after_cancel_failure）
        if let Some(oid) = order_id.filter(|s| !s.is_empty()) {
            if let Some(qty) =
                Self::detect_first_fill(adapters, plan, Some(oid), ctx.baseline, ctx.min_qty)
                    .await?
            {
                return Ok(qty);
            }
            if let Err(err) = Self::cancel_resting(adapters, &plan.first, oid).await {
                warn!(pair = %plan.pair_id, error = %err, "limit_market: cancel resting failed");
            } else {
                info!(pair = %plan.pair_id, order_id = oid, "limit_market: resting limit canceled");
            }
            sleep(Duration::from_millis(CANCEL_RACE_MS)).await;
            if let Some(qty) =
                Self::detect_first_fill(adapters, plan, Some(oid), ctx.baseline, ctx.min_qty).await?
            {
                warn!(
                    pair = %plan.pair_id,
                    qty = %qty,
                    "limit_market: filled around cancel; hedging"
                );
                return Ok(qty);
            }
        }
        Ok(Decimal::ZERO)
    }

    async fn detect_first_fill(
        adapters: &HashMap<String, Arc<dyn crate::exchange::ExchangePort>>,
        plan: &HedgePlan,
        order_id: Option<&str>,
        baseline: Decimal,
        min_qty: Decimal,
    ) -> Result<Option<Decimal>> {
        if let Some(oid) = order_id.filter(|s| !s.is_empty()) {
            match Self::poll_first_leg(adapters, &plan.first, plan.qty, oid).await {
                Ok(Some(f)) if f.qty > Decimal::ZERO => return Ok(Some(f.qty)),
                Ok(Some(_)) | Ok(None) => {}
                Err(err) => {
                    warn!(pair = %plan.pair_id, error = %err, "detect_first_fill: order_status");
                }
            }
        }
        let leg = &plan.first;
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        let positions = adapter.positions().await?;
        let current: Decimal = positions
            .iter()
            .filter(|p| symbol_matches_symbol(&p.symbol, &leg.symbol, &leg.symbol))
            .map(|p| p.qty)
            .sum();
        Ok(first_leg_fill_delta(
            baseline,
            current,
            leg.is_buy,
            plan.qty,
            min_qty,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn min_qty_tolerance() {
        assert_eq!(
            first_leg_fill_delta(dec!(0), dec!(32.43), true, dec!(32.43), dec!(0.1)),
            Some(dec!(32.43))
        );
    }
}
