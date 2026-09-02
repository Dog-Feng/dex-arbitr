//! 第一腿限价 → **该单 WS 成交推送**立刻发第二腿市价。
//! 市价/IOC/撤后：等该单 WS `order.ioc_fill_wait_ms`（默认 1 秒），没有再 REST 查一次。
//! 不用账户缓存仓位差。
//!
//! 邻档（`rest_until_event`）：部分成交立刻对冲该增量，余量继续挂到吃满。
//! 不因未吃满而把已成交部分紧急平掉。改价撤单在已有成交时让余量继续挂。
//! 对面档先成才撤本档并对未对冲量回滚。
//! 撤单后 Lighter `remaining=0` 且 `filled=0` 不是成交（撤单也会把 remaining 打成 0）；
//! 只认 `filled_base_amount` / trades。不要对幻影成交紧急平，否则会叠在赢家第一腿上。
//!
//! 阶段 1 超时重挂：不足则撤余量再挂，第二腿失败回滚。
//!
//! 不变量：
//! 1. 任何退出路径上，第一腿都不能留下状态不明的挂单（撤不掉就上报 orphan）。
//! 2. 第二腿各次对冲量之和等于第一腿累计实际成交量（已对冲部分）。
//! 3. 增量低于第二腿最小下单量时先攒着；退出时仍不够才平掉这一截灰尘。

use anyhow::{bail, Result};
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::domain::Books;
use crate::exchange::OrderPush;
use crate::exec::executor::Adapters;
use crate::exec::{ExecFill, ExecResult, HedgeExecutor, HedgePlan};
use tokio::sync::broadcast;

/// 挂着的限价：每轮最多听 WS 这么久再 REST 兜底。不是市价确认窗口；
/// 市价 / 撤后确认用 `order.ioc_fill_wait_ms`。
const POLL_INTERVAL_MS: u64 = 1000;

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
///   推送是按市场 id 分组的 map，不是数组；成交还走 `account_all_trades`
///   （`ask_client_id` / `bid_client_id` / `size`）。
/// - Entropy/HL `orderUpdates` / `userFills`：嵌套 `order.oid` / `fills[].oid`
fn push_matches_order(data: &serde_json::Value, order_id: &str) -> bool {
    if order_id.is_empty() {
        return false;
    }
    if node_id_matches(data, order_id) {
        return true;
    }
    match data {
        serde_json::Value::Array(arr) => arr.iter().any(|o| push_matches_order(o, order_id)),
        serde_json::Value::Object(map) => map.values().any(|v| push_matches_order(v, order_id)),
        _ => false,
    }
}

fn venue_matches_first(push_venue: &str, first_venue: &str) -> bool {
    push_venue.is_empty() || push_venue.eq_ignore_ascii_case(first_venue)
}

/// 等到该 `order_id` 的一条推送，或超时 / channel 关闭。
async fn recv_matching_push(
    rx: &mut broadcast::Receiver<OrderPush>,
    venue: &str,
    order_id: &str,
    wait: Duration,
) -> Option<serde_json::Value> {
    if order_id.is_empty() {
        return None;
    }
    tokio::time::timeout(wait, async {
        loop {
            match rx.recv().await {
                Ok(p)
                    if venue_matches_first(&p.venue, venue)
                        && push_matches_order(&p.data, order_id) =>
                {
                    return Some(p.data);
                }
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()
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
    fill_from_push_with(
        data,
        order_id,
        FillParseOpts {
            remaining_heuristic: true,
        },
    )
}

#[derive(Clone, Copy)]
struct FillParseOpts {
    /// Lighter 成交推送有时只把 `remaining` 打成 0、`filled_base_amount` 仍是 0。
    /// 挂着时可以用 `initial − remaining`。撤单后 remaining 也会变 0，必须关掉。
    remaining_heuristic: bool,
}

fn fill_from_push_with(
    data: &serde_json::Value,
    order_id: &str,
    opts: FillParseOpts,
) -> Option<(Decimal, Option<Decimal>)> {
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
    walk_generic_fills(data, order_id, opts, &mut best);
    best
}

fn walk_generic_fills(
    data: &serde_json::Value,
    order_id: &str,
    opts: FillParseOpts,
    best: &mut Option<(Decimal, Option<Decimal>)>,
) {
    if node_id_matches(data, order_id) {
        if let Some(qty) = node_filled_qty(data, opts) {
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
    match data {
        serde_json::Value::Array(arr) => {
            for item in arr {
                walk_generic_fills(item, order_id, opts, best);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                walk_generic_fills(v, order_id, opts, best);
            }
        }
        _ => {}
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
        "ask_id",
        "bid_id",
        "ask_id_str",
        "bid_id_str",
        "ask_client_id",
        "bid_client_id",
        "ask_client_id_str",
        "bid_client_id_str",
    ]
    .iter()
    .any(|k| json_id_eq(data.get(k), order_id))
}

fn node_status_canceled(data: &serde_json::Value) -> bool {
    data.get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|s| {
            let s = s.to_ascii_lowercase();
            s.contains("cancel") || s.contains("reject")
        })
}

fn is_trade_node(data: &serde_json::Value) -> bool {
    if data.get("trade_id").is_some() || data.get("trade_id_str").is_some() {
        return true;
    }
    if data.get("initial_base_amount").is_some() {
        return false;
    }
    [
        "ask_id",
        "bid_id",
        "ask_id_str",
        "bid_id_str",
        "ask_client_id",
        "bid_client_id",
        "ask_client_id_str",
        "bid_client_id_str",
    ]
    .iter()
    .any(|k| data.get(*k).is_some())
}

fn node_filled_qty(data: &serde_json::Value, opts: FillParseOpts) -> Option<Decimal> {
    if let Some(q) = json_decimal(data.get("z"))
        .or_else(|| json_decimal(data.get("filled_base_amount")))
        .or_else(|| json_decimal(data.get("filled_amount")))
        .or_else(|| json_decimal(data.get("filled_qty")))
        .or_else(|| json_decimal(data.get("filledQty")))
        .filter(|q| *q > Decimal::ZERO)
    {
        return Some(q);
    }
    if is_trade_node(data) {
        return json_decimal(data.get("size")).filter(|q| *q > Decimal::ZERO);
    }
    // 订单上的 `size` 是挂单量，不是成交量。撤单后 remaining 也会变成 0。
    if node_status_canceled(data) || !opts.remaining_heuristic {
        return None;
    }
    let init = json_decimal(data.get("initial_base_amount"))?;
    let rem = json_decimal(data.get("remaining_base_amount"))?;
    let filled = init - rem;
    (filled > Decimal::ZERO).then_some(filled)
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
    /// 保留字段；第一腿成交只认该单 WS / 查单，不再用持仓 baseline。
    pub baseline: Decimal,
    /// 判定「有新成交」的最小 delta。
    pub min_qty: Decimal,
    pub cancel: Arc<AtomicBool>,
    /// 邻档：等到成交或 cancel，不按 `limit_timeout_ms`。
    pub rest_until_event: bool,
    /// 邻档：本档成交后立刻撤对面。
    pub peer_cancel: Option<Arc<AtomicBool>>,
    /// 邻档：先成的一档抢到，对面若也成了则不平第二腿。
    pub winner: Option<Arc<AtomicBool>>,
    /// 套利开关。停止后邻档已发出的单可以听到成交，但不再市价对冲 / 紧急平。
    pub orders_live: Arc<AtomicBool>,
}

struct AttemptOutcome {
    filled: Decimal,
    /// 该轮成交的实际均价（交易所回报）。拿不到则为 None。
    price: Option<Decimal>,
    /// 撤单失败、状态不明的挂单 id。出现后停止重试并上报。
    orphan: Option<String>,
    order_id: Option<String>,
}

/// 邻档限价还要不要继续挂。
///
/// `claimed`：已经对冲过至少一截（本档赢了）。此后改价撤单忽略，挂到吃满。
/// 还没对冲时改价/对面赢都停，退出时再对冲或回滚未对冲量。
fn should_keep_resting(
    own_cancel: bool,
    peer_lost: bool,
    seen: Decimal,
    target: Decimal,
    claimed: bool,
) -> bool {
    if peer_lost && !claimed {
        return false;
    }
    if target > Decimal::ZERO && seen >= target {
        return false;
    }
    if seen <= Decimal::ZERO {
        return !own_cancel;
    }
    if own_cancel && !claimed {
        return false;
    }
    true
}

fn claim_adjacent_winner(ctx: &LimitMarketRun) -> bool {
    match &ctx.winner {
        Some(w) => w
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok(),
        None => true,
    }
}

fn orders_live(ctx: &LimitMarketRun) -> bool {
    ctx.orders_live.load(Ordering::Acquire)
}

fn add_exec_fill(dst: &mut ExecFill, src: &ExecFill) {
    if src.qty <= Decimal::ZERO {
        return;
    }
    let total = dst.qty + src.qty;
    if total > Decimal::ZERO && dst.price > Decimal::ZERO && src.price > Decimal::ZERO {
        dst.price = (dst.price * dst.qty + src.price * src.qty) / total;
    } else if src.price > Decimal::ZERO {
        dst.price = src.price;
    }
    dst.qty = total;
    if dst.order_id.is_none() {
        dst.order_id = src.order_id.clone();
    }
}

fn merge_hedge_result(acc: &mut Option<ExecResult>, piece: ExecResult) {
    match acc {
        None => *acc = Some(piece),
        Some(a) => {
            add_exec_fill(&mut a.first, &piece.first);
            add_exec_fill(&mut a.second, &piece.second);
            a.unhedged_qty += piece.unhedged_qty;
            if a.orphan_order.is_none() {
                a.orphan_order = piece.orphan_order;
            }
        }
    }
}

impl HedgeExecutor {
    pub async fn execute_limit_market(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
    ) -> Result<crate::exec::ExecResult> {
        if ctx.rest_until_event {
            return Self::execute_resting_adjacent(cfg, adapters, plan, books, ctx).await;
        }
        let attempts = cfg.order.limit_retry_count.max(1);
        let hedge_floor = plan.hedgeable_min_qty();
        let mut accumulated = Decimal::ZERO;
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

            let out = Self::run_limit_attempt(cfg, adapters, &attempt_plan, books, ctx, attempt)
                .await?;
            accumulated += out.filled;
            if out.filled > Decimal::ZERO {
                last_first_oid = out.order_id.or(last_first_oid);
            }
            if let (Some(px), true) = (out.price, out.filled > Decimal::ZERO) {
                first_notional += px * out.filled;
                first_priced += out.filled;
            }

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
        Ok(result)
    }

    /// 邻档：WS/REST 看到增量成交就立刻市价对冲，限价余量继续挂到吃满。
    async fn execute_resting_adjacent(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
    ) -> Result<crate::exec::ExecResult> {
        let hedge_floor = plan.hedgeable_min_qty();
        let mut pushes = crate::exchange::subscribe_order_pushes();
        let post = Self::post_first_leg(cfg, adapters, plan, books, false).await?;
        let posted_at = Instant::now();
        let order_id = post.order_id.clone().filter(|s| !s.is_empty());
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            attempt = 1,
            qty = %plan.qty,
            resting = post.resting,
            filled_qty = %post.first.qty,
            order_id = ?order_id,
            "limit_market: first leg posted"
        );

        let timeout = {
            let ms = cfg.order.adjacent_timeout_ms;
            if ms == 0 {
                Duration::from_secs(24 * 3600)
            } else {
                Duration::from_millis(ms.max(200))
            }
        };
        let deadline = posted_at + timeout;
        let want_id = order_id.clone().unwrap_or_default();
        let mut seen = post.first.qty.min(plan.qty);
        let mut fill_price = (seen > Decimal::ZERO).then_some(post.first.price);
        let mut hedged = Decimal::ZERO;
        let mut written_off = Decimal::ZERO;
        let mut acc: Option<ExecResult> = None;
        let mut claimed = false;
        let mut ignore_cancel_logged = false;
        let mut orphan: Option<String> = None;

        loop {
            let own_cancel = ctx.cancel.load(Ordering::Relaxed);
            let peer_lost = ctx
                .peer_cancel
                .as_ref()
                .is_some_and(|p| p.load(Ordering::Relaxed));
            let timed_out = Instant::now() >= deadline;
            if timed_out
                || !should_keep_resting(
                    own_cancel,
                    peer_lost,
                    seen,
                    plan.qty,
                    claimed,
                )
            {
                break;
            }
            if own_cancel && claimed && !ignore_cancel_logged {
                info!(
                    pair = %plan.pair_id,
                    filled = %seen,
                    target = %plan.qty,
                    "limit_market: reprice cancel ignored; riding limit until filled"
                );
                ignore_cancel_logged = true;
            }

            Self::hedge_seen_increments(
                cfg,
                adapters,
                plan,
                books,
                ctx,
                seen,
                fill_price,
                hedge_floor,
                false,
                &mut hedged,
                &mut written_off,
                &mut acc,
                &mut claimed,
            )
            .await?;
            if seen >= plan.qty {
                break;
            }

            if let Some((qty, px)) = Self::detect_first_fill(
                adapters,
                plan,
                order_id.as_deref(),
            )
            .await?
            {
                let qty = qty.min(plan.qty);
                if qty > seen {
                    seen = qty;
                    fill_price = px.or(fill_price);
                    continue;
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let nap = remaining.min(Duration::from_millis(POLL_INTERVAL_MS));
            if nap.is_zero() {
                break;
            }
            match pushes.as_mut() {
                Some(rx) => {
                    if let Some(data) =
                        recv_matching_push(rx, &plan.first.venue, &want_id, nap).await
                    {
                        let own_cancel = ctx.cancel.load(Ordering::Relaxed);
                        let peer_lost = ctx
                            .peer_cancel
                            .as_ref()
                            .is_some_and(|p| p.load(Ordering::Relaxed));
                        let opts = FillParseOpts {
                            remaining_heuristic: !(own_cancel || peer_lost),
                        };
                        if let Some((qty, px)) = fill_from_push_with(&data, &want_id, opts) {
                            match plausible_ws_fill(qty, plan.qty) {
                                Some(qty) => {
                                    let qty = qty.min(plan.qty);
                                    if qty > seen {
                                        info!(
                                            pair = %plan.pair_id,
                                            first = %plan.first.venue,
                                            order_id = %want_id,
                                            filled = %qty,
                                            target = %plan.qty,
                                            "limit_market: first-leg fill from ws push"
                                        );
                                        seen = qty;
                                        fill_price = px.or(fill_price);
                                    }
                                }
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

        if seen < plan.qty || order_id.is_some() {
            orphan = Self::cancel_leftover(adapters, plan, order_id.as_deref()).await;
            if seen < plan.qty {
                let (qty, px) = Self::confirm_fill_after_action(
                    adapters,
                    plan,
                    order_id.as_deref(),
                    &mut pushes,
                    seen,
                    fill_price,
                    cfg.order.ioc_fill_wait(),
                )
                .await?;
                seen = qty;
                fill_price = px;
            }
        }

        let unhedged = (seen - hedged - written_off).max(Decimal::ZERO);
        if !orders_live(ctx) && unhedged > Decimal::ZERO {
            warn!(
                pair = %plan.pair_id,
                qty = %unhedged,
                "limit_market: arbitrage stopped; not hedging first-leg fill"
            );
            bail!("ARB_STOPPED: first-leg fill after stop; not hedging");
        }
        if !claimed && unhedged > Decimal::ZERO && !claim_adjacent_winner(ctx) {
            let first_bbo =
                crate::exec::executor::book_for(books, &plan.first.venue, &plan.pair_id)?;
            warn!(
                pair = %plan.pair_id,
                side = plan.quote_side.map(|s| s.as_str()).unwrap_or("?"),
                qty = %unhedged,
                "limit_market: lost adjacent race; closing unhedged first-leg fill"
            );
            match Self::emergency_close(
                cfg,
                adapters,
                &plan.first,
                unhedged,
                &first_bbo,
                false,
            )
            .await
            {
                Ok(()) => bail!("QUOTE_LOST_RACE: opposite quote filled first; closed"),
                Err(e) => bail!("NAKED_FIRST_LEG: QUOTE_LOST_RACE close failed ({e})"),
            }
        }
        if !claimed && unhedged > Decimal::ZERO {
            claimed = true;
            if let Some(peer) = &ctx.peer_cancel {
                peer.store(true, Ordering::Release);
            }
        }

        Self::hedge_seen_increments(
            cfg,
            adapters,
            plan,
            books,
            ctx,
            seen,
            fill_price,
            hedge_floor,
            true,
            &mut hedged,
            &mut written_off,
            &mut acc,
            &mut claimed,
        )
        .await?;

        if let Some(oid) = &orphan {
            warn!(
                pair = %plan.pair_id,
                venue = %plan.first.venue,
                order_id = %oid,
                "limit_market: resting limit could not be canceled; it may fill later"
            );
        }

        let Some(mut result) = acc else {
            let note = orphan
                .as_deref()
                .map(|o| format!(" ORPHAN_ORDER={o}"))
                .unwrap_or_default();
            bail!("limit_zero_fill: first leg no fill after wait{note}");
        };
        result.orphan_order = orphan.or(result.orphan_order);
        if result.first.order_id.is_none() {
            result.first.order_id = order_id;
        }
        info!(
            pair = %plan.pair_id,
            first = %plan.first.venue,
            second = %plan.second.venue,
            filled = %seen,
            hedged = %result.hedged_qty(),
            target = %plan.qty,
            "limit_market: resting limit done; second leg hedged incrementally"
        );
        Ok(result)
    }

    async fn hedge_seen_increments(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
        seen: Decimal,
        fill_price: Option<Decimal>,
        hedge_floor: Decimal,
        flushing: bool,
        hedged: &mut Decimal,
        written_off: &mut Decimal,
        acc: &mut Option<ExecResult>,
        claimed: &mut bool,
    ) -> Result<()> {
        let pending = (seen - *hedged - *written_off).max(Decimal::ZERO);
        if pending <= Decimal::ZERO {
            return Ok(());
        }
        let ready = pending >= hedge_floor || seen >= plan.qty || flushing;
        if !ready {
            return Ok(());
        }
        if pending < hedge_floor {
            if !flushing {
                return Ok(());
            }
            if !orders_live(ctx) {
                warn!(
                    pair = %plan.pair_id,
                    filled = %pending,
                    "limit_market: arbitrage stopped; not closing dust"
                );
                bail!("ARB_STOPPED: first-leg fill after stop; not hedging");
            }
            let first_bbo =
                crate::exec::executor::book_for(books, &plan.first.venue, &plan.pair_id)?;
            warn!(
                pair = %plan.pair_id,
                filled = %pending,
                second_min_qty = %hedge_floor,
                "limit_market: leftover dust below min qty; closing it"
            );
            match Self::emergency_close(
                cfg,
                adapters,
                &plan.first,
                pending,
                &first_bbo,
                false,
            )
            .await
            {
                Ok(()) => {
                    *written_off += pending;
                    Ok(())
                }
                Err(e) => Err(anyhow::anyhow!(
                    "NAKED_FIRST_LEG: dust {pending} close failed ({e})"
                )),
            }
        } else {
            if !orders_live(ctx) {
                warn!(
                    pair = %plan.pair_id,
                    increment = %pending,
                    "limit_market: arbitrage stopped; not hedging second leg"
                );
                bail!("ARB_STOPPED: first-leg fill after stop; not hedging");
            }
            if !*claimed {
                if !claim_adjacent_winner(ctx) {
                    return Ok(());
                }
                *claimed = true;
                if let Some(peer) = &ctx.peer_cancel {
                    peer.store(true, Ordering::Release);
                }
            }
            info!(
                pair = %plan.pair_id,
                first = %plan.first.venue,
                second = %plan.second.venue,
                increment = %pending,
                cumulative = %seen,
                target = %plan.qty,
                "limit_market: partial fill; hedging increment on second leg now"
            );
            match Self::hedge_second_leg(cfg, adapters, plan, books, false, pending, fill_price)
                .await
            {
                Ok(piece) => {
                    *hedged += piece.second.qty.min(pending);
                    if piece.unhedged_qty > Decimal::ZERO {
                        *written_off += piece.unhedged_qty;
                    }
                    merge_hedge_result(acc, piece);
                    Ok(())
                }
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("EMERGENCY_CLOSED") {
                        warn!(
                            pair = %plan.pair_id,
                            increment = %pending,
                            error = %msg,
                            "limit_market: increment hedge rolled back; not retrying this slice"
                        );
                        *written_off += pending;
                        Ok(())
                    } else {
                        Err(err)
                    }
                }
            }
        }
    }

    async fn run_limit_attempt(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        ctx: &LimitMarketRun,
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
            let mut filled = post.first.qty.min(plan.qty);
            let mut price = Some(post.first.price);
            // 立刻部分成交时余量还挂着，必须撤；撤后再听 WS + 查一次。
            let orphan = if filled < plan.qty {
                let leftover =
                    Self::cancel_leftover(adapters, plan, order_id.as_deref()).await;
                let (qty, px) = Self::confirm_fill_after_action(
                    adapters,
                    plan,
                    order_id.as_deref(),
                    &mut pushes,
                    filled,
                    price,
                    cfg.order.ioc_fill_wait(),
                )
                .await?;
                filled = qty;
                price = px;
                leftover
            } else {
                None
            };
            return Ok(AttemptOutcome {
                filled,
                price,
                orphan,
                order_id: order_id.clone(),
            });
        }

        let timeout = if ctx.rest_until_event {
            let ms = cfg.order.adjacent_timeout_ms;
            if ms == 0 {
                Duration::from_secs(24 * 3600)
            } else {
                Duration::from_millis(ms.max(200))
            }
        } else {
            Duration::from_millis(cfg.order.limit_timeout_ms.max(200))
        };
        let deadline = posted_at + timeout;
        let mut filled = Decimal::ZERO;
        let mut fill_price: Option<Decimal> = None;

        // 事件驱动优先：私有 WS 推到这张单的成交就立刻对冲第二腿。
        // 订阅必须在挂单之前，否则下单往返期间的成交推送会丢掉。
        // 推送里没有足额成交量时，下一轮循环开头的 REST detect 兜底。
        let want_id = order_id.clone().unwrap_or_default();

        while Instant::now() < deadline {
            if ctx.cancel.load(Ordering::Relaxed)
                || ctx
                    .peer_cancel
                    .as_ref()
                    .is_some_and(|p| p.load(Ordering::Relaxed))
            {
                break;
            }
            if let Some((qty, px)) = Self::detect_first_fill(
                adapters,
                plan,
                order_id.as_deref(),
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
                    if let Some(data) =
                        recv_matching_push(rx, &plan.first.venue, &want_id, nap).await
                    {
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

        // 未成交或部分成交：撤单前再查一次；撤单后听该单 WS，没有再查一次。
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
        let before = filled;
        let (qty, px) = Self::confirm_fill_after_action(
            adapters,
            plan,
            Some(&oid),
            &mut pushes,
            filled,
            fill_price,
            cfg.order.ioc_fill_wait(),
        )
        .await?;
        filled = qty;
        fill_price = px;
        if filled > before {
            warn!(
                pair = %plan.pair_id,
                qty = %filled,
                "limit_market: filled around cancel; hedging"
            );
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

    /// 撤单（或任何已发到 DEX 的动作）之后：等该单 WS `wait`，没有再 REST 查一次。
    /// 有推送也再查一次，避免 WS 只带了部分累计。
    async fn confirm_fill_after_action(
        adapters: &Adapters,
        plan: &HedgePlan,
        order_id: Option<&str>,
        pushes: &mut Option<broadcast::Receiver<OrderPush>>,
        mut seen: Decimal,
        mut fill_price: Option<Decimal>,
        wait: Duration,
    ) -> Result<(Decimal, Option<Decimal>)> {
        let oid = order_id.filter(|s| !s.is_empty()).unwrap_or("");
        if let Some(rx) = pushes.as_mut() {
            let deadline = Instant::now() + wait;
            while Instant::now() < deadline {
                let remain = deadline.saturating_duration_since(Instant::now());
                let Some(data) =
                    recv_matching_push(rx, &plan.first.venue, oid, remain).await
                else {
                    break;
                };
                if let Some((qty, px)) = fill_from_push_with(
                    &data,
                    oid,
                    FillParseOpts {
                        remaining_heuristic: false,
                    },
                ) {
                    match plausible_ws_fill(qty, plan.qty) {
                        Some(qty) => {
                            let qty = qty.min(plan.qty);
                            if qty > seen {
                                info!(
                                    pair = %plan.pair_id,
                                    first = %plan.first.venue,
                                    order_id = %oid,
                                    filled = %qty,
                                    "limit_market: fill from ws after cancel"
                                );
                                seen = qty;
                                fill_price = px.or(fill_price);
                                if seen >= plan.qty {
                                    break;
                                }
                            }
                        }
                        None => {
                            warn!(
                                pair = %plan.pair_id,
                                first = %plan.first.venue,
                                filled = %qty,
                                target = %plan.qty,
                                "limit_market: ws fill after cancel implausible; querying REST"
                            );
                        }
                    }
                }
            }
        } else {
            sleep(wait).await;
        }
        if let Some((qty, px)) = Self::detect_first_fill(adapters, plan, order_id).await? {
            if qty > seen {
                seen = qty.min(plan.qty);
                fill_price = px.or(fill_price);
            }
        }
        Ok((seen, fill_price))
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

    /// 只认该 `order_id` 的查单结果。不用账户缓存仓位差。
    async fn detect_first_fill(
        adapters: &Adapters,
        plan: &HedgePlan,
        order_id: Option<&str>,
    ) -> Result<Option<(Decimal, Option<Decimal>)>> {
        let Some(oid) = order_id.filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        match Self::fetch_first_leg_ack(adapters, &plan.first, plan.qty, oid).await {
            Ok(ack) if ack.filled_qty > Decimal::ZERO => {
                Ok(Some((ack.filled_qty.min(plan.qty), ack.avg_price)))
            }
            Ok(_) => Ok(None),
            Err(err) => {
                warn!(pair = %plan.pair_id, error = %err, "detect_first_fill: order_status");
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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

    /// 阶段 1 超时重挂：第一腿没填满就整单放弃（平掉已成交部分）。
    /// 邻档不走这条，见 `partial_fill_increment_is_hedged_not_abandoned`。
    #[test]
    fn stage1_underfill_threshold() {
        let eps = Decimal::new(1, 8);
        let target = dec!(1.0);
        let partial = dec!(0.6);
        assert!(partial + eps < target);
        let full = dec!(1.0);
        assert!(!(full + eps < target));
        let dust_short = target - Decimal::new(1, 9);
        assert!(!(dust_short + eps < target));
    }

    /// 邻档：部分成交立刻对冲该增量，不把整单当失败去紧急平。
    #[test]
    fn partial_fill_increment_is_hedged_not_abandoned() {
        let target = dec!(0.0005);
        let seen = dec!(0.00029);
        let hedged = Decimal::ZERO;
        let increment = (seen - hedged).max(Decimal::ZERO);
        assert_eq!(increment, dec!(0.00029));
        assert!(increment < target);
        let min_qty = dec!(0.0001);
        assert!(increment >= min_qty, "增量够第二腿最小量就应对冲，而不是平掉");
        let remaining = target - seen;
        assert_eq!(remaining, dec!(0.00021));
    }

    #[test]
    fn keep_resting_until_full_after_claim() {
        let target = dec!(0.0005);
        let partial = dec!(0.00029);
        // 空单改价：撤
        assert!(!should_keep_resting(true, false, Decimal::ZERO, target, false));
        // 有成交但还没对冲：停，退出时对冲这一截
        assert!(!should_keep_resting(true, false, partial, target, false));
        // 已对冲：忽略改价，挂到吃满
        assert!(should_keep_resting(true, false, partial, target, true));
        // 对面先成且自己还没对冲：停
        assert!(!should_keep_resting(false, true, partial, target, false));
        // 吃满：停
        assert!(!should_keep_resting(false, false, target, target, true));
        // 正常挂着
        assert!(should_keep_resting(false, false, Decimal::ZERO, target, false));
    }

    #[test]
    fn merge_partial_hedges_sums_qty() {
        let a = ExecResult::finished(
            ExecFill {
                venue: "rh".into(),
                qty: dec!(0.00029),
                price: dec!(100),
                is_buy: true,
                order_id: None,
            },
            ExecFill {
                venue: "l".into(),
                qty: dec!(0.00029),
                price: dec!(100),
                is_buy: false,
                order_id: None,
            },
            None,
        );
        let b = ExecResult::finished(
            ExecFill {
                venue: "rh".into(),
                qty: dec!(0.00021),
                price: dec!(101),
                is_buy: true,
                order_id: None,
            },
            ExecFill {
                venue: "l".into(),
                qty: dec!(0.00021),
                price: dec!(101),
                is_buy: false,
                order_id: None,
            },
            None,
        );
        let mut acc = Some(a);
        merge_hedge_result(&mut acc, b);
        let r = acc.unwrap();
        assert_eq!(r.first.qty, dec!(0.0005));
        assert_eq!(r.second.qty, dec!(0.0005));
        assert_eq!(r.hedged_qty(), dec!(0.0005));
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

        // Lighter 按市场 id 分组的 map（真实 WS 格式）
        assert!(push_matches_order(
            &json!({"type": "update/account_all_orders", "orders": {"1": [{"client_order_index": 555}]}}),
            "555"
        ));
        let mapped = json!({
            "type": "update/account_all_orders",
            "orders": {"1": [{
                "client_order_index": 555,
                "initial_base_amount": "0.0005",
                "remaining_base_amount": "0",
                "filled_base_amount": "0"
            }]}
        });
        let (qty, _) = fill_from_push(&mapped, "555").expect("map-keyed remaining fill");
        assert_eq!(qty, dec!(0.0005));
        assert!(fill_from_push_with(
            &mapped,
            "555",
            FillParseOpts {
                remaining_heuristic: false
            }
        )
        .is_none());

        // Lighter 成交在 account_all_trades
        let trade = json!({
            "type": "update/account_all_trades",
            "trades": {"1": [{
                "ask_client_id": 555,
                "bid_client_id": 999,
                "size": "0.0005",
                "price": "78500"
            }]}
        });
        assert!(push_matches_order(&trade, "555"));
        let (qty, px) = fill_from_push(&trade, "555").expect("trade fill");
        assert_eq!(qty, dec!(0.0005));
        assert_eq!(px, Some(dec!(78500)));
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

    #[test]
    fn lighter_cancel_update_is_not_a_fill() {
        use serde_json::json;

        let canceled = json!({
            "type": "update/account_all_orders",
            "orders": {"1": [{
                "client_order_index": 178828420185202_u64,
                "initial_base_amount": "0.015",
                "remaining_base_amount": "0",
                "filled_base_amount": "0",
                "size": "0.015",
                "status": "canceled"
            }]}
        });
        assert!(push_matches_order(&canceled, "178828420185202"));
        assert!(fill_from_push(&canceled, "178828420185202").is_none());

        let cancelled_spelling = json!({
            "client_order_index": "555",
            "initial_base_amount": "0.015",
            "remaining_base_amount": "0",
            "filled_base_amount": "0",
            "status": "cancelled"
        });
        assert!(fill_from_push(&cancelled_spelling, "555").is_none());

        let cancel_with_real_fill = json!({
            "client_order_index": "555",
            "initial_base_amount": "0.015",
            "remaining_base_amount": "0",
            "filled_base_amount": "0.015",
            "status": "canceled"
        });
        let (qty, _) = fill_from_push(&cancel_with_real_fill, "555").expect("explicit filled");
        assert_eq!(qty, dec!(0.015));

        let order_size_only = json!({
            "client_order_index": "555",
            "size": "0.015",
            "filled_base_amount": "0"
        });
        assert!(fill_from_push(&order_size_only, "555").is_none());
    }
}
