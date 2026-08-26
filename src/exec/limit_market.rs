//! 对齐参考 `execute_limit_market_mode`：单任务内完成
//! 第一腿限价 → 等成交（order_status + 仓位 delta）→ 不足则撤单重挂 →
//! 第二腿市价 → 失败回滚。
//!
//! 三个关键不变量：
//! 1. 任何退出路径上，第一腿都不能留下状态不明的挂单（撤不掉就上报 orphan 并停止重试）。
//! 2. 第二腿的量严格等于第一腿累计实际成交量。
//! 3. 累计成交量低于第二腿最小下单量时，反向平掉第一腿而不是发一张注定被拒的单。

use anyhow::{bail, Result};
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::app::reconcile::{first_leg_fill_delta, symbol_matches_symbol};
use crate::config::AppConfig;
use crate::domain::Books;
use crate::exec::executor::Adapters;
use crate::exec::{HedgeExecutor, HedgePlan};

const POLL_INTERVAL_MS: u64 = 50;
const CANCEL_RACE_MS: u64 = 500;
/// 刚 post 后 sidecar/交易所可能尚未把挂单列入活跃订单。
const POST_SETTLE_MS: u64 = 400;
/// WS 推送到达后，等这么久让 REST 侧状态跟上再去查。
const WS_SETTLE_MS: u64 = 60;

/// 从 sidecar 的订单推送里判断「这条是不是我等的那张单，且已有成交」。
///
/// 两个所的字段名不同：
/// - SoDEX `AccountOrderUpdate`：`c`=ClOrdID, `i`=OrderID, `z`=累计成交量
/// - Lighter `account_all_orders`：`client_order_index` / `order_index` / `filled_base_amount`
///
/// 只做「是否值得立刻去查一次」的判断，真实成交量仍以 order_status 为准——
/// 推送可能乱序或漏，不能直接拿它记账。
fn push_matches_order(data: &serde_json::Value, order_id: &str) -> bool {
    if order_id.is_empty() {
        return false;
    }
    let hit = |v: Option<&serde_json::Value>| -> bool {
        match v {
            Some(serde_json::Value::String(s)) => s == order_id,
            Some(serde_json::Value::Number(n)) => n.to_string() == order_id,
            _ => false,
        }
    };
    // 顶层字段（SoDEX 直接反序列化后的形状 / Lighter 扁平推送）
    for key in ["c", "i", "client_order_index", "order_index", "client_order_id", "order_id"] {
        if hit(data.get(key)) {
            return true;
        }
    }
    // Lighter 的 orders 数组推送
    if let Some(arr) = data.get("orders").and_then(|v| v.as_array()) {
        return arr.iter().any(|o| push_matches_order(o, order_id));
    }
    false
}

pub struct LimitMarketRun {
    /// 下单前该所该市场的真实持仓。**必须**是查询成功的值，不能用 0 兜底：
    /// 该所已有仓位时 baseline=0 会把既有仓位误判成第一腿成交。
    pub baseline: Decimal,
    /// 判定「有新成交」的最小 delta。
    pub min_qty: Decimal,
    pub cancel: Arc<AtomicBool>,
}

struct AttemptOutcome {
    filled: Decimal,
    /// 该轮成交的实际均价（交易所回报）。拿不到则为 None。
    price: Option<Decimal>,
    /// 撤单失败、状态不明的挂单 id。出现后停止重试并上报。
    orphan: Option<String>,
}

impl HedgeExecutor {
    pub async fn execute_limit_market(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
    ) -> Result<crate::exec::ExecResult> {
        let attempts = cfg.order.limit_retry_count.max(1);
        let hedge_floor = plan.hedgeable_min_qty();
        let mut accumulated = Decimal::ZERO;
        let mut baseline = ctx.baseline;
        let mut orphan: Option<String> = None;
        // 第一腿各轮成交按量加权，得到真实均价。
        let mut first_notional = Decimal::ZERO;
        let mut first_priced = Decimal::ZERO;

        for attempt in 1..=attempts {
            if ctx.cancel.load(Ordering::Relaxed) {
                info!(pair = %plan.pair_id, attempt, "limit_market: cancel requested; stop posting");
                break;
            }
            let remaining = plan.qty - accumulated;
            if remaining < hedge_floor {
                break;
            }
            let mut attempt_plan = plan.clone();
            attempt_plan.qty = remaining;

            let out =
                Self::run_limit_attempt(cfg, adapters, &attempt_plan, books, ctx, baseline, attempt)
                    .await?;
            accumulated += out.filled;
            if let (Some(px), true) = (out.price, out.filled > Decimal::ZERO) {
                first_notional += px * out.filled;
                first_priced += out.filled;
            }
            // 下一轮的 baseline 要把本轮成交算进去，否则会把它当成新成交重复计数。
            baseline += if plan.first.is_buy {
                out.filled
            } else {
                -out.filled
            };

            if out.orphan.is_some() {
                orphan = out.orphan;
                break;
            }
            if accumulated >= plan.qty {
                break;
            }
        }

        if let Some(oid) = &orphan {
            warn!(
                pair = %plan.pair_id,
                venue = %plan.first.venue,
                order_id = %oid,
                "limit_market: resting limit could not be canceled; it may fill later"
            );
        }

        if accumulated <= Decimal::ZERO {
            let note = orphan
                .as_deref()
                .map(|o| format!(" ORPHAN_ORDER={o}"))
                .unwrap_or_default();
            bail!("limit_zero_fill: first leg no fill after {attempts} attempt(s){note}");
        }

        // 对齐参考 `execute_limit_market_mode`：第一腿**没填满就整单放弃**——
        // 平掉已成交部分，绝不带着半仓去对冲。多格网格要求持仓量精确落在
        // 格子边界上（target = n × base_qty），半仓会让按格减仓算错格数。
        if accumulated + Decimal::new(1, 8) < plan.qty {
            let first_bbo =
                crate::exec::executor::book_for(books, &plan.first.venue, &plan.pair_id)?;
            warn!(
                pair = %plan.pair_id,
                filled = %accumulated,
                target = %plan.qty,
                "limit_market: first leg underfilled; closing it and abandoning this round"
            );
            match Self::emergency_close(
                cfg,
                adapters,
                &plan.first,
                accumulated,
                plan.is_open,
                &first_bbo,
                false,
            )
            .await
            {
                Ok(()) => bail!(
                    "EMERGENCY_CLOSED: limit leg underfilled {accumulated}/{}; closed",
                    plan.qty
                ),
                Err(eclose) => bail!(
                    "NAKED_FIRST_LEG: limit leg underfilled {accumulated}/{}; close failed ({eclose})",
                    plan.qty
                ),
            };
        }

        let first_price = if first_priced > Decimal::ZERO {
            Some(first_notional / first_priced)
        } else {
            None
        };
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            second = %plan.second.venue,
            filled = %accumulated,
            target = %plan.qty,
            avg_price = ?first_price,
            "limit_market: first leg fully filled, hedging second leg now"
        );
        // 第二腿量 = 第一腿**实际**成交量（对齐参考的 filled_decimal）。
        let mut result =
            Self::hedge_second_leg(cfg, adapters, plan, books, false, accumulated, first_price)
                .await?;
        result.orphan_order = orphan;
        Ok(result)
    }

    async fn run_limit_attempt(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
        baseline: Decimal,
        attempt: u32,
    ) -> Result<AttemptOutcome> {
        let post = Self::post_first_leg(cfg, adapters, plan, books, false).await?;
        let posted_at = Instant::now();
        let order_id = post.order_id.clone().filter(|s| !s.is_empty());
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            attempt,
            qty = %plan.qty,
            resting = post.resting,
            filled_qty = %post.first.qty,
            order_id = ?order_id,
            "limit_market: first leg posted"
        );

        if post.first.qty > Decimal::ZERO {
            let filled = post.first.qty.min(plan.qty);
            // 立刻部分成交时余量还挂着，必须撤。
            let orphan = if filled < plan.qty {
                Self::cancel_leftover(adapters, plan, order_id.as_deref()).await
            } else {
                None
            };
            return Ok(AttemptOutcome {
                filled,
                price: Some(post.first.price),
                orphan,
            });
        }

        let timeout = Duration::from_millis(cfg.order.limit_timeout_ms.max(200));
        let deadline = posted_at + timeout;
        let mut filled = Decimal::ZERO;
        let mut fill_price: Option<Decimal> = None;

        // 事件驱动优先：私有 WS 推到这张单的更新就立刻去查，不等下一次轮询。
        // 对齐参考 `order_monitor` 的 WS-first + REST 兜底：推送可能乱序或漏，
        // 所以仍保留 50ms 轮询，但正常路径由推送唤醒，延迟从「轮询间隔 + 2 次
        // sidecar 往返」降到「一次推送 + 一次查询」。
        let mut pushes = crate::exchange::subscribe_order_pushes();
        let want_id = order_id.clone().unwrap_or_default();

        while Instant::now() < deadline {
            if ctx.cancel.load(Ordering::Relaxed) {
                break;
            }
            if let Some((qty, px)) = Self::detect_first_fill(
                adapters,
                plan,
                order_id.as_deref(),
                baseline,
                ctx.min_qty,
                posted_at,
            )
            .await?
            {
                filled = qty;
                fill_price = px;
                break;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let nap = remaining.min(Duration::from_millis(POLL_INTERVAL_MS));
            if nap.is_zero() {
                break;
            }
            match pushes.as_mut() {
                Some(rx) => {
                    // 等推送或超时，谁先到算谁。
                    let woke = tokio::time::timeout(nap, async {
                        loop {
                            match rx.recv().await {
                                Ok(p) if push_matches_order(&p.data, &want_id) => return true,
                                Ok(_) => continue,
                                Err(_) => return false,
                            }
                        }
                    })
                    .await;
                    if matches!(woke, Ok(true)) {
                        // 推送说这张单动了：给 REST 一点点时间对齐，然后立刻查。
                        sleep(Duration::from_millis(WS_SETTLE_MS)).await;
                    }
                }
                None => sleep(nap).await,
            }
        }

        if filled >= plan.qty {
            return Ok(AttemptOutcome {
                filled: plan.qty,
                price: fill_price,
                orphan: None,
            });
        }

        // 未成交或部分成交：撤单前再查一次，撤单后再查竞态窗口
        // （对齐参考 `_wait_fill_after_cancel_failure`）。
        let Some(oid) = order_id else {
            return Ok(AttemptOutcome {
                filled,
                price: fill_price,
                orphan: None,
            });
        };
        if let Some((qty, px)) = Self::detect_first_fill(
            adapters,
            plan,
            Some(&oid),
            baseline,
            ctx.min_qty,
            posted_at,
        )
        .await?
        {
            if qty >= filled {
                fill_price = px.or(fill_price);
            }
            filled = filled.max(qty);
            if filled >= plan.qty {
                return Ok(AttemptOutcome {
                    filled: plan.qty,
                    price: fill_price,
                    orphan: None,
                });
            }
        }

        let mut orphan = None;
        match Self::cancel_resting(adapters, &plan.first, &oid).await {
            Ok(()) => {
                info!(pair = %plan.pair_id, order_id = %oid, "limit_market: resting limit canceled")
            }
            Err(err) => {
                warn!(pair = %plan.pair_id, error = %err, "limit_market: cancel resting failed");
                orphan = Some(oid.clone());
            }
        }
        sleep(Duration::from_millis(CANCEL_RACE_MS)).await;
        if let Some((qty, px)) = Self::detect_first_fill(
            adapters,
            plan,
            Some(&oid),
            baseline,
            ctx.min_qty,
            posted_at,
        )
        .await?
        {
            if qty > filled {
                warn!(
                    pair = %plan.pair_id,
                    qty = %qty,
                    "limit_market: filled around cancel; hedging"
                );
                filled = qty;
                fill_price = px.or(fill_price);
            }
        }
        if filled >= plan.qty {
            orphan = None;
        }
        Ok(AttemptOutcome {
            filled,
            price: fill_price,
            orphan,
        })
    }

    /// 部分成交后撤掉余量。撤不掉返回 order_id 作为 orphan。
    async fn cancel_leftover(
        adapters: &Adapters,
        plan: &HedgePlan,
        order_id: Option<&str>,
    ) -> Option<String> {
        let oid = order_id?;
        match Self::cancel_resting(adapters, &plan.first, oid).await {
            Ok(()) => None,
            Err(err) => {
                warn!(
                    pair = %plan.pair_id,
                    error = %err,
                    "limit_market: cancel leftover after partial fill failed"
                );
                Some(oid.to_string())
            }
        }
    }

    async fn detect_first_fill(
        adapters: &Adapters,
        plan: &HedgePlan,
        order_id: Option<&str>,
        baseline: Decimal,
        min_qty: Decimal,
        posted_at: Instant,
    ) -> Result<Option<(Decimal, Option<Decimal>)>> {
        if let Some(oid) = order_id.filter(|s| !s.is_empty()) {
            match Self::fetch_first_leg_ack(adapters, &plan.first, plan.qty, oid).await {
                Ok(ack) if ack.filled_qty > Decimal::ZERO => {
                    return Ok(Some((ack.filled_qty.min(plan.qty), ack.avg_price)));
                }
                Ok(ack) if Self::first_leg_still_resting(&ack) => {
                    return Ok(None);
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(pair = %plan.pair_id, error = %err, "detect_first_fill: order_status");
                }
            }
        }
        if posted_at.elapsed() < Duration::from_millis(POST_SETTLE_MS) {
            return Ok(None);
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
        // 仓位 delta 兜底路径拿不到成交价，只能回 None 让上层退回挂价。
        Ok(
            first_leg_fill_delta(baseline, current, leg.is_buy, plan.qty, min_qty)
                .map(|q| (q, None)),
        )
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

    /// 重试时 baseline 要滚动，否则第二轮会把第一轮的成交再算一遍。
    #[test]
    fn rolling_baseline_does_not_double_count() {
        let first_round = first_leg_fill_delta(dec!(0), dec!(0.5), true, dec!(1), dec!(0.01));
        assert_eq!(first_round, Some(dec!(0.5)));
        let rolled = dec!(0) + dec!(0.5);
        assert_eq!(
            first_leg_fill_delta(rolled, dec!(0.5), true, dec!(0.5), dec!(0.01)),
            None
        );
        assert_eq!(
            first_leg_fill_delta(rolled, dec!(0.9), true, dec!(0.5), dec!(0.01)),
            Some(dec!(0.4))
        );
    }

    /// 多轮重挂时第一腿均价必须按量加权，不能只取最后一轮的价。
    /// 建仓净边只有 0.03%~0.05% 量级，几个 tick 的偏差就会让止盈误判。
    #[test]
    fn multi_attempt_fill_price_is_quantity_weighted() {
        // 第一轮 0.6 @ 100.00，第二轮 0.4 @ 100.50
        let mut notional = Decimal::ZERO;
        let mut priced = Decimal::ZERO;
        for (qty, px) in [(dec!(0.6), dec!(100.00)), (dec!(0.4), dec!(100.50))] {
            notional += px * qty;
            priced += qty;
        }
        let avg = notional / priced;
        // (0.6*100 + 0.4*100.5) / 1.0 = 100.20，不是最后一轮的 100.50
        assert_eq!(avg, dec!(100.20));
        assert_ne!(avg, dec!(100.50));
    }

    /// 仓位 delta 兜底路径没有成交价，聚合后应为 None，
    /// 由上层退回挂价，而不是把 0 当成成交价。
    #[test]
    fn no_price_samples_yields_none() {
        let priced_qty = Decimal::ZERO;
        let avg: Option<Decimal> = if priced_qty > Decimal::ZERO {
            Some(Decimal::ZERO / priced_qty)
        } else {
            None
        };
        assert!(avg.is_none());
    }

    /// 对齐参考：第一腿没填满就整单放弃（平掉已成交部分），不带半仓去对冲。
    /// 多格网格要求持仓量精确落在 n × base_qty 上，半仓会让按格减仓算错格数。
    #[test]
    fn underfill_is_abandoned_not_hedged() {
        let eps = Decimal::new(1, 8);
        let target = dec!(1.0);
        // 部分成交 → 放弃
        let partial = dec!(0.6);
        assert!(partial + eps < target, "0.6/1.0 必须判定为未填满");
        // 全额成交 → 继续第二腿
        let full = dec!(1.0);
        assert!(!(full + eps < target), "足额不应被判成未填满");
        // 浮点尾差不应误判成未填满
        let dust_short = target - Decimal::new(1, 9);
        assert!(!(dust_short + eps < target), "1e-9 的尾差应被 epsilon 吸收");
    }

    /// WS 推送匹配：两个所字段名不同，都要能认出来。
    #[test]
    fn push_matching_handles_both_venues() {
        use serde_json::json;

        // SoDEX AccountOrderUpdate：c=ClOrdID, i=OrderID（数字）
        assert!(push_matches_order(&json!({"c": "arb-123", "z": "0.5"}), "arb-123"));
        assert!(push_matches_order(&json!({"i": 987654, "z": "0.5"}), "987654"));

        // Lighter 扁平推送
        assert!(push_matches_order(
            &json!({"client_order_index": "555", "filled_base_amount": "1.0"}),
            "555"
        ));
        // Lighter orders 数组
        assert!(push_matches_order(
            &json!({"orders": [{"client_order_index": "111"}, {"client_order_index": "222"}]}),
            "222"
        ));

        // 别人的单不能误匹配
        assert!(!push_matches_order(&json!({"c": "arb-999"}), "arb-123"));
        // 空 order_id 永远不匹配，否则会被任意推送唤醒
        assert!(!push_matches_order(&json!({"c": "arb-123"}), ""));
    }
}
