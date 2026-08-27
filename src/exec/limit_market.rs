//! 对齐参考 `execute_limit_market_mode`：单任务内完成
//! 第一腿限价 → **挂单所私有 WS 成交推送**立刻发第二腿市价（REST 兜底）→
//! 不足则撤单重挂 → 第二腿失败回滚。
//!
//! 三个关键不变量：
//! 1. 任何退出路径上，第一腿都不能留下状态不明的挂单（撤不掉就上报 orphan 并停止重试）。
//! 2. 第二腿的量严格等于第一腿累计实际成交量。
//! 3. 累计成交量低于第二腿最小下单量时，反向平掉第一腿而不是发一张注定被拒的单。
//!
//! 先挂后吃两边对称：谁先挂限价，谁的成交推送驱动另一所市价，不绑死 entropy 或 lighter。

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

fn json_id_eq(v: Option<&serde_json::Value>, order_id: &str) -> bool {
    match v {
        Some(serde_json::Value::String(s)) => s == order_id,
        Some(serde_json::Value::Number(n)) => n.to_string() == order_id,
        _ => false,
    }
}

fn json_decimal(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v {
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        Some(serde_json::Value::Number(n)) => n.to_string().parse().ok(),
        _ => None,
    }
}

/// 从 sidecar 的订单推送里判断「这条是不是我等的那张单」。
///
/// 字段名因所而异：
/// - SoDEX `AccountOrderUpdate`：`c`=ClOrdID, `i`=OrderID, `z`=累计成交量
/// - Lighter `account_all_orders`：`client_order_index` / `order_index` / `filled_base_amount`
/// - Entropy/HL `orderUpdates` / `userFills`：嵌套 `order.oid` / `fills[].oid`
fn push_matches_order(data: &serde_json::Value, order_id: &str) -> bool {
    if order_id.is_empty() {
        return false;
    }
    for key in [
        "c",
        "i",
        "oid",
        "client_order_index",
        "order_index",
        "client_order_id",
        "order_id",
    ] {
        if json_id_eq(data.get(key), order_id) {
            return true;
        }
    }
    if let Some(order) = data.get("order") {
        if push_matches_order(order, order_id) {
            return true;
        }
    }
    for key in ["orders", "fills", "data"] {
        match data.get(key) {
            Some(serde_json::Value::Array(arr)) => {
                if arr.iter().any(|o| push_matches_order(o, order_id)) {
                    return true;
                }
            }
            Some(inner) => {
                if push_matches_order(inner, order_id) {
                    return true;
                }
            }
            None => {}
        }
    }
    false
}

fn venue_matches_first(push_venue: &str, first_venue: &str) -> bool {
    push_venue.is_empty() || push_venue.eq_ignore_ascii_case(first_venue)
}

/// 推送数量必须和挂单目标一个量级。Lighter 偶发用 size_decimals 整数上报，
/// 107 vs 0.0107 这种不能当成交量，否则会按错单位去对冲。
fn plausible_ws_fill(qty: Decimal, target: Decimal) -> Option<Decimal> {
    if qty <= Decimal::ZERO {
        return None;
    }
    if target <= Decimal::ZERO {
        return Some(qty);
    }
    let cap = (target * Decimal::from(2)).max(target + Decimal::new(1, 6));
    if qty > cap {
        return None;
    }
    Some(qty)
}

/// 从推送里直接抽出成交量/均价。有量就立刻对冲，不再等 REST。
/// Entropy / Lighter / SoDEX 都走这里——谁先挂，谁的成交推送触发另一腿市价。
///
/// 快照（`userFills.isSnapshot`）不记账——那是历史回放，不是这笔挂单刚成交。
fn fill_from_push(data: &serde_json::Value, order_id: &str) -> Option<(Decimal, Option<Decimal>)> {
    if order_id.is_empty() {
        return None;
    }
    if let Some(channel) = data.get("channel").and_then(|c| c.as_str()) {
        if channel.eq_ignore_ascii_case("orderUpdates") {
            return entropy_order_update_fill(data.get("data").unwrap_or(data), order_id);
        }
        if channel.eq_ignore_ascii_case("userFills") {
            let inner = data.get("data").unwrap_or(data);
            if inner.get("isSnapshot").and_then(|v| v.as_bool()) == Some(true) {
                return None;
            }
            return entropy_user_fills(inner, order_id);
        }
    }
    let mut best: Option<(Decimal, Option<Decimal>)> = None;
    walk_generic_fills(data, order_id, &mut best);
    best
}

fn walk_generic_fills(
    data: &serde_json::Value,
    order_id: &str,
    best: &mut Option<(Decimal, Option<Decimal>)>,
) {
    if node_id_matches(data, order_id) {
        if let Some(qty) = node_filled_qty(data) {
            let px = json_decimal(data.get("ap"))
                .or_else(|| json_decimal(data.get("L")))
                .or_else(|| json_decimal(data.get("p")))
                .or_else(|| json_decimal(data.get("price")))
                .or_else(|| json_decimal(data.get("avg_price")));
            match best {
                Some((q, _)) if *q >= qty => {}
                _ => *best = Some((qty, px)),
            }
        }
    }
    if let Some(order) = data.get("order") {
        walk_generic_fills(order, order_id, best);
    }
    for key in ["orders", "fills", "data"] {
        match data.get(key) {
            Some(serde_json::Value::Array(arr)) => {
                for item in arr {
                    walk_generic_fills(item, order_id, best);
                }
            }
            Some(inner) => walk_generic_fills(inner, order_id, best),
            None => {}
        }
    }
}

fn node_id_matches(data: &serde_json::Value, order_id: &str) -> bool {
    [
        "c",
        "i",
        "oid",
        "client_order_index",
        "order_index",
        "client_order_id",
        "order_id",
    ]
    .iter()
    .any(|k| json_id_eq(data.get(k), order_id))
}

fn node_filled_qty(data: &serde_json::Value) -> Option<Decimal> {
    json_decimal(data.get("z"))
        .or_else(|| json_decimal(data.get("filled_base_amount")))
        .or_else(|| json_decimal(data.get("filled_amount")))
        .or_else(|| json_decimal(data.get("filled_qty")))
        .or_else(|| json_decimal(data.get("filledQty")))
        .filter(|q| *q > Decimal::ZERO)
}

fn entropy_order_update_fill(
    data: &serde_json::Value,
    order_id: &str,
) -> Option<(Decimal, Option<Decimal>)> {
    let items: Box<dyn Iterator<Item = &serde_json::Value>> = match data {
        serde_json::Value::Array(arr) => Box::new(arr.iter()),
        other => Box::new(std::iter::once(other)),
    };
    for item in items {
        let order = item.get("order").unwrap_or(item);
        if !json_id_eq(order.get("oid"), order_id) && !json_id_eq(item.get("oid"), order_id) {
            continue;
        }
        let status = item
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if status == "open" || status == "new" || status == "resting" {
            continue;
        }
        let orig = json_decimal(order.get("origSz")).unwrap_or(Decimal::ZERO);
        let rem = json_decimal(order.get("sz")).unwrap_or(Decimal::ZERO);
        let filled = if orig > Decimal::ZERO {
            (orig - rem).max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        let qty = if status == "filled" && orig > Decimal::ZERO {
            orig
        } else {
            filled
        };
        if qty <= Decimal::ZERO && !status.contains("fill") {
            continue;
        }
        if qty <= Decimal::ZERO {
            continue;
        }
        let px = json_decimal(order.get("limitPx"))
            .or_else(|| json_decimal(item.get("avgPx")))
            .or_else(|| json_decimal(order.get("px")));
        return Some((qty, px));
    }
    None
}

fn entropy_user_fills(
    data: &serde_json::Value,
    order_id: &str,
) -> Option<(Decimal, Option<Decimal>)> {
    let fills = data
        .get("fills")
        .and_then(|v| v.as_array())
        .or_else(|| data.as_array())?;
    let mut qty = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut priced = Decimal::ZERO;
    for f in fills {
        if !json_id_eq(f.get("oid"), order_id) {
            continue;
        }
        let sz = json_decimal(f.get("sz")).unwrap_or(Decimal::ZERO);
        if sz <= Decimal::ZERO {
            continue;
        }
        qty += sz;
        if let Some(px) = json_decimal(f.get("px")) {
            notional += px * sz;
            priced += sz;
        }
    }
    if qty <= Decimal::ZERO {
        return None;
    }
    let avg = if priced > Decimal::ZERO {
        Some(notional / priced)
    } else {
        None
    };
    Some((qty, avg))
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
    order_id: Option<String>,
}

impl HedgeExecutor {
    pub async fn execute_limit_market(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
    ) -> Result<crate::exec::ExecResult> {
        let before = crate::exec::snapshot_realized_before(adapters, plan, false).await;
        let attempts = cfg.order.limit_retry_count.max(1);
        let hedge_floor = plan.hedgeable_min_qty();
        let mut accumulated = Decimal::ZERO;
        let mut baseline = ctx.baseline;
        let mut orphan: Option<String> = None;
        let mut last_first_oid: Option<String> = None;
        // 第一腿各轮成交按量加权，得到真实均价。
        let mut first_notional = Decimal::ZERO;
        let mut first_priced = Decimal::ZERO;

        for attempt in 1..=attempts {
            if ctx.cancel.load(Ordering::Acquire) {
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
            if out.filled > Decimal::ZERO {
                last_first_oid = out.order_id.or(last_first_oid);
            }
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
        if result.first.order_id.is_none() {
            result.first.order_id = last_first_oid;
        }
        result.realized_before = before;
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
        let mut pushes = crate::exchange::subscribe_order_pushes();
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
                order_id: order_id.clone(),
            });
        }

        let timeout = Duration::from_millis(cfg.order.limit_timeout_ms.max(200));
        let deadline = posted_at + timeout;
        let mut filled = Decimal::ZERO;
        let mut fill_price: Option<Decimal> = None;

        // 事件驱动优先：私有 WS 推到这张单的成交就立刻对冲第二腿。
        // 订阅必须在挂单之前，否则下单往返期间的成交推送会丢掉。
        // 推送里没有足额成交量时，下一轮循环开头的 REST detect 兜底。
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
                                Ok(p)
                                    if venue_matches_first(&p.venue, &plan.first.venue)
                                        && push_matches_order(&p.data, &want_id) =>
                                {
                                    return Some(p.data)
                                }
                                Ok(_) => continue,
                                // 慢消费者会丢消息；丢掉的成交靠 REST 轮询补，
                                // 不能把 channel 当成死掉。
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    continue
                                }
                                Err(_) => return None,
                            }
                        }
                    })
                    .await;
                    if let Ok(Some(data)) = woke {
                        // 只有足额才按推送量立刻对冲。部分成交可能只是
                        // userFills 的一截增量，写进 filled 会把累计冲掉。
                        if let Some((qty, px)) = fill_from_push(&data, &want_id) {
                            match plausible_ws_fill(qty, plan.qty) {
                                Some(qty) if qty >= plan.qty => {
                                    info!(
                                        pair = %plan.pair_id,
                                        first = %plan.first.venue,
                                        second = %plan.second.venue,
                                        order_id = %want_id,
                                        filled = %qty,
                                        "limit_market: first leg fill from ws push; hedging now"
                                    );
                                    filled = qty;
                                    fill_price = px.or(fill_price);
                                    break;
                                }
                                Some(_) => {}
                                None => {
                                    warn!(
                                        pair = %plan.pair_id,
                                        first = %plan.first.venue,
                                        filled = %qty,
                                        target = %plan.qty,
                                        "limit_market: ws fill qty implausible; waiting REST"
                                    );
                                }
                            }
                        }
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
                order_id: order_id.clone(),
            });
        }

        // 未成交或部分成交：撤单前再查一次，撤单后再查竞态窗口
        // （对齐参考 `_wait_fill_after_cancel_failure`）。
        let Some(oid) = order_id else {
            return Ok(AttemptOutcome {
                filled,
                price: fill_price,
                orphan: None,
                order_id: None,
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
                    order_id: Some(oid.clone()),
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
            order_id: Some(oid),
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

    #[test]
    fn lighter_and_sodex_ws_fill_triggers_without_rest() {
        use serde_json::json;

        let lighter = json!({
            "type": "account_all_orders",
            "orders": [{
                "client_order_index": "555",
                "order_index": "999",
                "filled_base_amount": "0.0107",
                "price": "1553.19"
            }]
        });
        assert!(push_matches_order(&lighter, "555"));
        let (qty, px) = fill_from_push(&lighter, "555").expect("lighter filled");
        assert_eq!(qty, dec!(0.0107));
        assert_eq!(px, Some(dec!(1553.19)));
        assert_eq!(plausible_ws_fill(qty, dec!(0.0107)), Some(dec!(0.0107)));
        // 缩放整数不能当成交量
        assert!(plausible_ws_fill(dec!(107), dec!(0.0107)).is_none());

        let sodex = json!({"c": "arb-123", "i": 42, "z": "0.5", "L": "100.2"});
        let (qty, px) = fill_from_push(&sodex, "arb-123").expect("sodex filled");
        assert_eq!(qty, dec!(0.5));
        assert_eq!(px, Some(dec!(100.2)));
    }

    #[test]
    fn entropy_ws_fill_triggers_without_rest() {
        use serde_json::json;

        let oid = "527974206188";
        let updates = json!({
            "channel": "orderUpdates",
            "data": [{
                "status": "filled",
                "order": {
                    "oid": 527974206188_u64,
                    "origSz": "0.0107",
                    "sz": "0",
                    "limitPx": "1552.7"
                }
            }]
        });
        assert!(push_matches_order(&updates, oid));
        let (qty, px) = fill_from_push(&updates, oid).expect("filled qty from orderUpdates");
        assert_eq!(qty, dec!(0.0107));
        assert_eq!(px, Some(dec!(1552.7)));

        let fills = json!({
            "channel": "userFills",
            "data": {
                "isSnapshot": false,
                "fills": [{
                    "oid": 527974206188_u64,
                    "sz": "0.0107",
                    "px": "1552.7"
                }]
            }
        });
        assert!(push_matches_order(&fills, oid));
        let (qty, px) = fill_from_push(&fills, oid).expect("filled qty from userFills");
        assert_eq!(qty, dec!(0.0107));
        assert_eq!(px, Some(dec!(1552.7)));

        let snapshot = json!({
            "channel": "userFills",
            "data": {
                "isSnapshot": true,
                "fills": [{"oid": 527974206188_u64, "sz": "0.0107", "px": "1552.7"}]
            }
        });
        assert!(fill_from_push(&snapshot, oid).is_none());

        let open = json!({
            "channel": "orderUpdates",
            "data": [{
                "status": "open",
                "order": {"oid": 527974206188_u64, "origSz": "0.0107", "sz": "0.0107"}
            }]
        });
        assert!(push_matches_order(&open, oid));
        assert!(fill_from_push(&open, oid).is_none());
    }
}
