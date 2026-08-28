use anyhow::{bail, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::app::reconcile::symbol_matches_symbol;
use crate::config::{AppConfig, OrderStyle};
use crate::domain::spread::realized_slip_pct;
use crate::domain::{read_book, Bbo, Books};
use crate::exec::{fill_slip_overrun, HedgeLeg, HedgePlan};
use crate::exchange::{CancelReq, ExchangePort, FillPnl, OrderAck, OrderReq, OrderStatus};

pub type Adapters = HashMap<String, Arc<dyn ExchangePort>>;

/// 激进限价兜底的滑点放大倍数。对齐参考 `_place_aggressive_limit_retry_order`
/// 里写死的 `multiplier = Decimal("2")`。
const AGGRESSIVE_SLIPPAGE_MULT: Decimal = Decimal::TWO;

#[derive(Debug, Clone)]
pub struct ExecFill {
    pub venue: String,
    pub qty: Decimal,
    pub price: Decimal,
    pub is_buy: bool,
    pub order_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecResult {
    pub first: ExecFill,
    pub second: ExecFill,
    /// 撤不掉、可能稍后成交的第一腿挂单。非 None 时必须上报人工介入。
    pub orphan_order: Option<String>,
    /// 第一腿比第二腿多成交的量（> 0 表示存在未对冲的单边敞口）。
    pub unhedged_qty: Decimal,
    /// 平仓前各所仓位上的累计 realized（仅 Lighter/SoDEX 这种 per_fill=false 的所）。
    /// 平仓后用 after − before 得到本笔。Entropy 用成交 closedPnl，不进这张表。
    pub realized_before: HashMap<String, Decimal>,
    /// 开仓下单前两所账户权益。key = venue id。
    pub equity_before: Option<HashMap<String, Decimal>>,
    /// 平仓成交后两所账户权益（稍等结算再拉）。
    pub equity_after: Option<HashMap<String, Decimal>>,
}

impl ExecResult {
    /// 真正对冲上的量：两腿的较小者。持仓回写必须用它，不能用计划量。
    pub fn hedged_qty(&self) -> Decimal {
        self.first.qty.min(self.second.qty)
    }

    pub fn finished(first: ExecFill, second: ExecFill, orphan_order: Option<String>) -> Self {
        let unhedged_qty = (first.qty - second.qty).max(Decimal::ZERO);
        if unhedged_qty > Decimal::ZERO {
            warn!(
                first_qty = %first.qty,
                second_qty = %second.qty,
                unhedged = %unhedged_qty,
                "second leg partially filled; leaving one-sided exposure"
            );
        }
        Self {
            first,
            second,
            orphan_order,
            unhedged_qty,
            realized_before: HashMap::new(),
            equity_before: None,
            equity_after: None,
        }
    }

    /// 该所这一腿的成交均价。对不上所或价为 0 则 None。
    pub fn price_on(&self, venue: &str) -> Option<Decimal> {
        let px = if self.first.venue == venue {
            self.first.price
        } else if self.second.venue == venue {
            self.second.price
        } else {
            return None;
        };
        (px > Decimal::ZERO).then_some(px)
    }
}

#[derive(Debug, Clone)]
pub struct PostFirstResult {
    pub first: ExecFill,
    pub order_id: Option<String>,
    pub resting: bool,
}

pub struct HedgeExecutor;

impl HedgeExecutor {
    pub async fn run_plan(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        paper: bool,
    ) -> Result<ExecResult> {
        let before = snapshot_realized_before(adapters, plan, paper).await;
        let mut result = match plan.style {
            OrderStyle::LimitThenMarket => {
                let post = Self::post_first_leg(cfg, adapters, plan, books, paper).await?;
                if post.resting && !paper {
                    bail!("first leg resting (limit not filled yet)");
                }
                let mut result = Self::hedge_second_leg(
                    cfg,
                    adapters,
                    plan,
                    books,
                    paper,
                    post.first.qty,
                    Some(post.first.price),
                )
                .await?;
                if result.first.order_id.is_none() {
                    result.first.order_id = post.order_id;
                }
                result
            }
            // AggressiveLimit 只由 `hedge_second_leg` 内部构造，不会出现在
            // 配置里；真配上了就按双吃处理。
            OrderStyle::LimitMaker
            | OrderStyle::MarketTaker
            | OrderStyle::AggressiveLimit => {
                Self::dual_taker(cfg, adapters, plan, books, paper).await?
            }
        };
        result.realized_before = before;
        Ok(result)
    }

    pub async fn post_first_leg(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        paper: bool,
    ) -> Result<PostFirstResult> {
        let first_bbo = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let first_price = maker_limit_price(cfg, &plan.first, &first_bbo);
        if paper {
            return Ok(PostFirstResult {
                first: ExecFill {
                    venue: plan.first.venue.clone(),
                    qty: Decimal::ZERO,
                    price: first_price,
                    is_buy: plan.first.is_buy,
                    order_id: None,
                },
                order_id: None,
                resting: true,
            });
        }
        let adapter = adapters
            .get(&plan.first.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", plan.first.venue))?;
        let ack = adapter
            .place(OrderReq {
                symbol: plan.first.symbol.clone(),
                market_index: plan.first.market_index,
                is_buy: plan.first.is_buy,
                qty: plan.qty,
                reduce_only: !plan.is_open,
                style: OrderStyle::LimitMaker,
                limit_price: Some(first_price),
                client_order_id: None,
                // maker 挂单本身就是限价，不需要额外的滑点保护。
                target_price: None,
                slippage_pct: None,
            })
            .await?;
        let filled_qty = effective_filled_qty(&ack);
        let first = ExecFill {
            venue: plan.first.venue.clone(),
            qty: filled_qty,
            price: ack.avg_price.unwrap_or(first_price),
            is_buy: plan.first.is_buy,
            order_id: ack_order_id(&ack),
        };
        // sidecar 对限价单一律回 accepted，所以「未成交」不等于「还挂着」；
        // 只有明确处于 Accepted/Partial 才当作活跃挂单。
        let resting = filled_qty <= Decimal::ZERO
            && matches!(ack.status, OrderStatus::Accepted | OrderStatus::Partial);
        let order_id = if ack.order_id.is_empty() {
            None
        } else {
            Some(ack.order_id)
        };
        Ok(PostFirstResult {
            first,
            order_id,
            resting,
        })
    }

    /// 查第一腿订单：区分活跃挂单 vs 不在列表。
    pub async fn fetch_first_leg_ack(
        adapters: &Adapters,
        leg: &HedgeLeg,
        plan_qty: Decimal,
        order_id: &str,
    ) -> Result<OrderAck> {
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        adapter
            .order_status(&CancelReq {
                order_id: order_id.to_string(),
                symbol: leg.symbol.clone(),
                market_index: leg.market_index,
                qty: Some(plan_qty),
            })
            .await
    }

    pub fn first_leg_still_resting(ack: &OrderAck) -> bool {
        matches!(ack.status, OrderStatus::Accepted | OrderStatus::Partial)
            && ack.filled_qty <= Decimal::ZERO
    }

    pub async fn cancel_resting(
        adapters: &Adapters,
        leg: &HedgeLeg,
        order_id: &str,
    ) -> Result<()> {
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        adapter
            .cancel(&CancelReq {
                order_id: order_id.to_string(),
                symbol: leg.symbol.clone(),
                market_index: leg.market_index,
                qty: None,
            })
            .await
    }

    /// `first_fill_price`：第一腿的真实成交均价（交易所回报）。
    /// None 时退回按当前盘口重算的挂价。
    pub async fn hedge_second_leg(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        paper: bool,
        hedge_qty: Decimal,
        first_fill_price: Option<Decimal>,
    ) -> Result<ExecResult> {
        if hedge_qty <= Decimal::ZERO && !paper {
            bail!("limit_zero_fill: first leg filled zero");
        }
        let qty = if paper { plan.qty } else { hedge_qty };
        let first_bbo = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let first_price = maker_limit_price(cfg, &plan.first, &first_bbo);

        // 第一腿成交量低于第二腿最小下单量 → 第二腿必然被拒。对齐参考
        // `limit_min_qty_unmet`：直接反向平掉第一腿，不去发一张注定失败的单。
        if !paper && qty < plan.hedgeable_min_qty() {
            warn!(
                pair = %plan.pair_id,
                filled = %qty,
                second_min_qty = %plan.hedgeable_min_qty(),
                "first leg fill below second leg min qty; closing first leg"
            );
            return match Self::emergency_close(
                cfg,
                adapters,
                &plan.first,
                qty,
                plan.is_open,
                &first_bbo,
                paper,
            )
            .await
            {
                Ok(()) => Err(anyhow::anyhow!(
                    "EMERGENCY_CLOSED: limit_min_qty_unmet filled={qty} < {}",
                    plan.hedgeable_min_qty()
                )),
                Err(err) => Err(anyhow::anyhow!(
                    "NAKED_FIRST_LEG: limit_min_qty_unmet filled={qty}; close failed ({err})"
                )),
            };
        }

        let first = if paper {
            ExecFill {
                venue: plan.first.venue.clone(),
                qty,
                price: market_price(&plan.first, &first_bbo),
                is_buy: plan.first.is_buy,
                order_id: None,
            }
        } else {
            ExecFill {
                venue: plan.first.venue.clone(),
                qty,
                // 真实成交价优先。`first_price` 是用**当前**盘口重算的挂价，
                // 第一腿可能是几轮前、在别的价位成交的，用它算建仓净边会偏。
                price: first_fill_price.unwrap_or(first_price),
                is_buy: plan.first.is_buy,
                order_id: None,
            }
        };
        let second_bbo = book_for(books, &plan.second.venue, &plan.pair_id)?;
        let second_price = market_price(&plan.second, &second_bbo);
        let second = Self::fill_second_leg(
            cfg,
            adapters,
            plan,
            &first,
            &first_bbo,
            &second_bbo,
            paper,
            second_price,
        )
        .await?;
        Self::log_overrun(cfg, &plan.first, first_price, &first);
        Self::log_overrun(cfg, &plan.second, second_price, &second);
        Ok(ExecResult::finished(first, second, None))
    }

    /// 激进限价兜底：市价腿失败后，用**放大后的**滑点当限价挂一张 IOC。
    ///
    /// 对齐参考 `_place_aggressive_limit_retry_order`：价格取
    /// `信号价 × (1 ± 2×slippage)`，倍数写死 2 和参考一致。
    ///
    /// 相比再发一张市价单的好处是滑点有硬上限——IOC 最差成交在这个价，
    /// 吃不到就整单撤销，不会像市价那样在薄盘口吃穿好几档。代价是不保证
    /// 成交，所以调用方失败后仍要走 emergency_close。
    async fn aggressive_limit_retry(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        qty: Decimal,
        second_bbo: &Bbo,
    ) -> Result<ExecFill> {
        let mut leg = plan.second.clone();
        leg.style = OrderStyle::AggressiveLimit;

        // 基准用决策信号价（市价腿同款），不用当前盘口——盘口正是刚才没吃到
        // 的那个，拿它当基准等于自我实现。
        let base = market_price(&plan.second, second_bbo);
        let slip = cfg.cost.max_slippage_pct * AGGRESSIVE_SLIPPAGE_MULT;
        let ratio = slip / Decimal::from(100);
        let price = if leg.is_buy {
            base * (Decimal::ONE + ratio)
        } else {
            base * (Decimal::ONE - ratio)
        };
        if price <= Decimal::ZERO {
            bail!("aggressive limit price non-positive");
        }

        info!(
            pair = %plan.pair_id,
            venue = %leg.venue,
            qty = %qty,
            price = %price,
            slippage_pct = %slip,
            "aggressive limit retry"
        );
        Self::send_leg(
            cfg,
            adapters,
            &leg,
            qty,
            price,
            !plan.is_open,
            false,
            second_bbo,
            false,
        )
        .await
    }

    async fn dual_taker(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        books: &Books,
        paper: bool,
    ) -> Result<ExecResult> {
        let b0 = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let b1 = book_for(books, &plan.second.venue, &plan.pair_id)?;
        let p0 = market_price(&plan.first, &b0);
        let p1 = market_price(&plan.second, &b1);
        let first = Self::send_leg(
            cfg,
            adapters,
            &plan.first,
            plan.qty,
            p0,
            !plan.is_open,
            paper,
            &b0,
            false,
        )
        .await?;
        let second = Self::fill_second_leg(
            cfg,
            adapters,
            plan,
            &first,
            &b0,
            &b1,
            paper,
            p1,
        )
        .await?;
        Self::log_overrun(cfg, &plan.first, p0, &first);
        Self::log_overrun(cfg, &plan.second, p1, &second);
        Ok(ExecResult::finished(first, second, None))
    }

    /// 第二腿：市价一次 → 明确失败再激进限价 → 仍不确定则等 `second_leg_verify_ms`
    /// 查该所实仓。没有仓就市价平第一腿；查到仓当第二腿已成交。
    async fn fill_second_leg(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        first: &ExecFill,
        first_bbo: &Bbo,
        second_bbo: &Bbo,
        paper: bool,
        second_price: Decimal,
    ) -> Result<ExecFill> {
        let qty = first.qty;
        match Self::send_leg(
            cfg,
            adapters,
            &plan.second,
            qty,
            second_price,
            !plan.is_open,
            paper,
            second_bbo,
            false,
        )
        .await
        {
            Ok(f) => Ok(f),
            Err(err) if is_unverifiable(&err) => {
                warn!(
                    pair = %plan.pair_id,
                    error = %err,
                    "second leg market unverifiable; skipping aggressive limit, verifying position"
                );
                Self::verify_second_or_close_first(
                    cfg, adapters, plan, first, first_bbo, paper, &err,
                )
                .await
            }
            Err(err) => {
                warn!(
                    pair = %plan.pair_id,
                    error = %err,
                    "second leg market failed; trying aggressive limit"
                );
                match Self::aggressive_limit_retry(cfg, adapters, plan, qty, second_bbo).await {
                    Ok(f) => Ok(f),
                    Err(retry_err) => {
                        warn!(
                            pair = %plan.pair_id,
                            market_error = %err,
                            retry_error = %retry_err,
                            "aggressive limit failed; waiting to verify second-leg position"
                        );
                        Self::verify_second_or_close_first(
                            cfg, adapters, plan, first, first_bbo, paper, &retry_err,
                        )
                        .await
                    }
                }
            }
        }
    }

    /// 等超时后拉第二所持仓。有同向实仓 → 记成交；没有 → 市价平第一腿。
    /// 持仓接口失败仍不明，不平第一腿，交人工。
    async fn verify_second_or_close_first(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        first: &ExecFill,
        first_bbo: &Bbo,
        paper: bool,
        prior: &anyhow::Error,
    ) -> Result<ExecFill> {
        let wait_ms = cfg.order.second_leg_verify_ms;
        if wait_ms > 0 {
            info!(
                pair = %plan.pair_id,
                venue = %plan.second.venue,
                wait_ms,
                "waiting to verify second-leg position"
            );
            sleep(Duration::from_millis(wait_ms)).await;
        }
        if paper {
            return Self::close_first_after_verify(
                cfg, adapters, plan, first, first_bbo, paper, prior,
            )
            .await;
        }
        match Self::fetch_second_leg_qty(adapters, plan).await {
            Ok(qty)
                if second_qty_confirms_fill(
                    qty,
                    plan.second.is_buy,
                    first.qty,
                    plan.second.min_qty,
                ) =>
            {
                let filled = qty.abs().min(first.qty);
                info!(
                    pair = %plan.pair_id,
                    venue = %plan.second.venue,
                    qty = %filled,
                    "second leg confirmed by exchange position"
                );
                Ok(ExecFill {
                    venue: plan.second.venue.clone(),
                    qty: filled,
                    price: Decimal::ZERO,
                    is_buy: plan.second.is_buy,
                    order_id: None,
                })
            }
            Ok(qty) => {
                info!(
                    pair = %plan.pair_id,
                    venue = %plan.second.venue,
                    qty = %qty,
                    "second venue has no matching position; market-closing first leg"
                );
                Self::close_first_after_verify(
                    cfg, adapters, plan, first, first_bbo, paper, prior,
                )
                .await
            }
            Err(query) => {
                warn!(
                    pair = %plan.pair_id,
                    venue = %plan.second.venue,
                    prior = %prior,
                    error = %query,
                    "position query failed after wait; not closing first leg"
                );
                Err(anyhow::anyhow!(
                    "SECOND_LEG_UNKNOWN: leg {} fill unverifiable ({prior}); \
                     position query failed ({query})",
                    plan.second.venue
                ))
            }
        }
    }

    async fn close_first_after_verify(
        cfg: &AppConfig,
        adapters: &Adapters,
        plan: &HedgePlan,
        first: &ExecFill,
        first_bbo: &Bbo,
        paper: bool,
        prior: &anyhow::Error,
    ) -> Result<ExecFill> {
        match Self::emergency_close(
            cfg,
            adapters,
            &plan.first,
            first.qty,
            plan.is_open,
            first_bbo,
            paper,
        )
        .await
        {
            Ok(()) => Err(anyhow::anyhow!(
                "EMERGENCY_CLOSED: second leg unconfirmed ({prior}); \
                 no position on {}; first leg market-closed",
                plan.second.venue
            )),
            Err(eclose) => Err(anyhow::anyhow!(
                "NAKED_FIRST_LEG: second leg unconfirmed ({prior}); \
                 no position on {}; emergency close failed ({eclose})",
                plan.second.venue
            )),
        }
    }

    async fn fetch_second_leg_qty(adapters: &Adapters, plan: &HedgePlan) -> Result<Decimal> {
        let adapter = adapters
            .get(&plan.second.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", plan.second.venue))?;
        let positions = adapter.positions().await?;
        let base = plan
            .pair_id
            .split('-')
            .next()
            .unwrap_or(plan.pair_id.as_str());
        Ok(positions
            .iter()
            .filter(|p| symbol_matches_symbol(&p.symbol, &plan.second.symbol, base))
            .map(|p| p.qty)
            .sum())
    }

    pub(crate) async fn emergency_close(
        cfg: &AppConfig,
        adapters: &Adapters,
        leg: &HedgeLeg,
        qty: Decimal,
        reduce_only: bool,
        bbo: &Bbo,
        paper: bool,
    ) -> Result<()> {
        if qty <= Decimal::ZERO {
            return Ok(());
        }
        let mut reverse = leg.clone();
        reverse.is_buy = !leg.is_buy;
        reverse.style = OrderStyle::MarketTaker;
        let price = market_price(&reverse, bbo);
        // emergency=true：放宽滑点上限。平不掉才是真风险，穿档只是成本。
        Self::send_leg(
            cfg,
            adapters,
            &reverse,
            qty,
            price,
            reduce_only,
            paper,
            bbo,
            true,
        )
        .await?;
        info!(venue = %leg.venue, qty = %qty, "emergency close first leg");
        Ok(())
    }

    /// `price`：市价腿的**决策信号价**（滑点保护基准）；maker 腿的挂单价。
    /// `emergency`：紧急平仓/回滚时放宽滑点上限，确保平得掉。
    #[allow(clippy::too_many_arguments)]
    async fn send_leg(
        cfg: &AppConfig,
        adapters: &Adapters,
        leg: &HedgeLeg,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        paper: bool,
        bbo: &Bbo,
        emergency: bool,
    ) -> Result<ExecFill> {
        if qty <= Decimal::ZERO {
            bail!("qty must be positive");
        }
        if paper {
            return Ok(ExecFill {
                venue: leg.venue.clone(),
                qty,
                price: market_price(leg, bbo),
                is_buy: leg.is_buy,
                order_id: None,
            });
        }
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        let is_maker = matches!(leg.style, OrderStyle::LimitMaker);
        // 激进限价要带 limit_price（那是它的价格上限），但保护参数走市价那套。
        let limit_price = if is_maker || matches!(leg.style, OrderStyle::AggressiveLimit) {
            Some(price)
        } else {
            None
        };
        // 市价腿：以决策信号价为基准做滑点保护，超出交易所拒单。
        // 对齐参考——不在本地遍历订单簿估 VWAP，保护交给交易所强制执行。
        let (target_price, slippage_pct) = if is_maker {
            (None, None)
        } else {
            let mut slip = cfg.cost.max_slippage_pct;
            if emergency {
                // 参考项目的「紧急平仓 50 倍滑点」：平不掉的风险 > 穿档成本。
                slip *= cfg.cost.emergency_slippage_multiplier.max(Decimal::ONE);
            }
            (Some(price), Some(slip))
        };
        let ack = adapter
            .place(OrderReq {
                symbol: leg.symbol.clone(),
                market_index: leg.market_index,
                is_buy: leg.is_buy,
                qty,
                reduce_only,
                style: leg.style,
                limit_price,
                client_order_id: None,
                target_price,
                slippage_pct,
            })
            .await?;
        if matches!(leg.style, OrderStyle::LimitMaker) && ack.filled_qty <= Decimal::ZERO {
            return Ok(ExecFill {
                venue: leg.venue.clone(),
                qty: Decimal::ZERO,
                price: ack.avg_price.unwrap_or(price),
                is_buy: leg.is_buy,
                order_id: ack_order_id(&ack),
            });
        }
        // 只认 sidecar 回查到的真实成交量。
        //
        // 参考项目对限价单允许「status=FILLED 但缺 filled 字段 → 推断为全额成交」
        // （`_infer_fill_from_status`），但对市价单**显式关掉**这个兜底
        // （`allow_fallback=not is_market_order`）。原因是市价腿的
        // status 来自下单响应而非成交确认，拿它推断数量就是幻影成交：
        // 上层按请求量记账，平仓时把实际还在的仓位清零成裸仓。
        // 这里同样不推断——查不到数量就当没确认，往下走 Unknown 分支。
        let filled = effective_filled_qty(&ack);
        if filled <= Decimal::ZERO {
            // 「确定没成交」和「不知道有没有成交」必须分开：
            // Canceled/Rejected 是交易所的终态回复，反手平第一腿是安全的；
            // Unknown 意味着连 sidecar 也没能确认，此时紧急平仓可能在
            // 第二腿其实已成交的情况下造出一条无人记账的反向裸仓。
            //
            // Filled/Partial 却查不到数量属于同一类：交易所说成交了，
            // 数量没确认。这种情况**更**不能反手平第一腿——第二腿很可能
            // 真的成交了。一律交人工介入。
            if matches!(
                ack.status,
                OrderStatus::Unknown | OrderStatus::Filled | OrderStatus::Partial
            ) {
                bail!(
                    "SECOND_LEG_UNKNOWN: leg {} fill unverifiable (status {:?})",
                    leg.venue,
                    ack.status
                );
            }
            bail!("leg {} not filled (status {:?})", leg.venue, ack.status);
        }
        // 紧急平仓把滑点上限放得很宽，保护限价离盘口很远，不要误判成真成交。
        let fill_px = if emergency {
            ack.avg_price.unwrap_or(price)
        } else {
            fill_price_for_pnl(ack.avg_price, price, leg.is_buy, cfg.cost.max_slippage_pct)
        };
        Ok(ExecFill {
            venue: leg.venue.clone(),
            qty: filled.min(qty),
            price: fill_px,
            is_buy: leg.is_buy,
            order_id: ack_order_id(&ack),
        })
    }

    /// 补对冲：在 counterparty 所发市价单，使 delta 回归中性。
    /// 这是修复裸敞口的路径，按紧急处理放宽滑点——补不上才是真风险。
    #[allow(clippy::too_many_arguments)]
    pub async fn market_leg(
        cfg: &AppConfig,
        adapters: &Adapters,
        pair_id: &str,
        leg: &HedgeLeg,
        qty: Decimal,
        is_buy: bool,
        reduce_only: bool,
        books: &Books,
        paper: bool,
    ) -> Result<ExecFill> {
        let mut mleg = leg.clone();
        mleg.is_buy = is_buy;
        mleg.style = OrderStyle::MarketTaker;
        let bbo = book_for(books, &mleg.venue, pair_id)?;
        let price = market_price(&mleg, &bbo);
        Self::send_leg(
            cfg, adapters, &mleg, qty, price, reduce_only, paper, &bbo, true,
        )
        .await
    }

    fn log_overrun(cfg: &AppConfig, leg: &HedgeLeg, expected: Decimal, fill: &ExecFill) {
        if let Some(slip) = fill_slip_overrun(cfg, leg.is_buy, expected, fill.price) {
            warn!(
                venue = %leg.venue,
                slip_pct = %slip,
                "fill slip overrun"
            );
        }
    }
}

fn ack_order_id(ack: &OrderAck) -> Option<String> {
    if ack.order_id.is_empty() {
        None
    } else {
        Some(ack.order_id.clone())
    }
}

/// 平仓下单前记下各所累计 realized。Entropy 是逐笔 closedPnl，不必记。
pub async fn snapshot_realized_before(
    adapters: &Adapters,
    plan: &HedgePlan,
    paper: bool,
) -> HashMap<String, Decimal> {
    let mut out = HashMap::new();
    if paper || plan.is_open {
        return out;
    }
    for leg in [&plan.first, &plan.second] {
        let Some(adapter) = adapters.get(&leg.venue) else {
            continue;
        };
        match adapter.fill_realized_pnl(&leg.symbol, None).await {
            Ok(p) if p.found && !p.per_fill => {
                out.insert(leg.venue.clone(), p.realized_pnl);
            }
            Ok(_) => {}
            Err(err) => {
                warn!(venue = %leg.venue, error = %err, "realized_pnl snapshot failed");
            }
        }
    }
    out
}

async fn query_leg_close_pnl(
    adapters: &Adapters,
    leg: &HedgeLeg,
    order_id: Option<&str>,
    before: Option<Decimal>,
) -> Option<Decimal> {
    let adapter = adapters.get(&leg.venue)?;
    for attempt in 0..4u32 {
        let after = match adapter.fill_realized_pnl(&leg.symbol, order_id).await {
            Ok(v) => v,
            Err(err) => {
                warn!(venue = %leg.venue, error = %err, "fill_pnl query failed");
                FillPnl::missing()
            }
        };
        if let Some(pnl) = after.this_close_pnl(before) {
            return Some(pnl);
        }
        if after.found {
            return None;
        }
        if attempt + 1 < 4 {
            sleep(Duration::from_millis(250)).await;
        }
    }
    None
}

/// 平仓后向两所取已实现盈亏再相加。缺任一腿则整笔 None（执行带显示 —）。
pub async fn dex_close_pnl_usdc(
    adapters: &Adapters,
    plan: &HedgePlan,
    result: &ExecResult,
) -> Option<Decimal> {
    let (a, b) = tokio::join!(
        query_leg_close_pnl(
            adapters,
            &plan.first,
            result.first.order_id.as_deref(),
            result.realized_before.get(&plan.first.venue).copied(),
        ),
        query_leg_close_pnl(
            adapters,
            &plan.second,
            result.second.order_id.as_deref(),
            result.realized_before.get(&plan.second.venue).copied(),
        ),
    );
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        _ => {
            warn!(
                pair = %plan.pair_id,
                first_ok = a.is_some(),
                second_ok = b.is_some(),
                "venue realized pnl missing on at least one leg; tape shows —"
            );
            None
        }
    }
}

pub fn book_for(books: &Books, venue: &str, pair_id: &str) -> Result<Bbo> {
    read_book(books, venue, pair_id)
        .ok_or_else(|| anyhow::anyhow!("missing book {venue}/{pair_id}"))
}

/// Maker 腿挂进点差内侧 `order.maker_inside_ticks` 个 tick，并保证不穿到对手价。
///
/// 旧实现是买挂 Bid1、卖挂 Ask1，也就是队尾最被动的位置，配上 2 秒超时几乎
/// 不可能成交。参考项目的 tick 模式是买 `Ask1 − 1 tick`、卖 `Bid1 + 1 tick`。
pub fn maker_limit_price(cfg: &AppConfig, leg: &HedgeLeg, bbo: &Bbo) -> Decimal {
    let ticks = Decimal::from(cfg.order.maker_inside_ticks);
    if ticks <= Decimal::ZERO {
        return if leg.is_buy { bbo.bid } else { bbo.ask };
    }
    let step = bbo.price_tick() * ticks;
    if leg.is_buy {
        let inside = bbo.ask - step;
        if inside > bbo.bid {
            inside
        } else {
            bbo.bid
        }
    } else {
        let inside = bbo.bid + step;
        if inside < bbo.ask {
            inside
        } else {
            bbo.ask
        }
    }
}

fn market_price(leg: &HedgeLeg, bbo: &Bbo) -> Decimal {
    if leg.is_buy {
        bbo.ask
    } else {
        bbo.bid
    }
}

/// 第二腿是否「结果不明」。不明时禁止紧急平仓——见 `Cause::SecondLegUnknown`。
///
/// 除 sidecar 显式返回 SECOND_LEG_UNKNOWN 外，网络层超时和连接失败同样不可知：
/// 交易所可能已收到并成交，此时 emergency_close 反而造出裸仓。
pub(crate) fn is_unverifiable(err: &anyhow::Error) -> bool {
    let s = err.to_string();
    s.contains("SECOND_LEG_UNKNOWN")
        || s.contains("sidecar place timed out")
        || s.contains("sidecar process exited")
        || s.contains("connection reset")
        || s.contains("connection refused")
        || s.contains("broken pipe")
        || s.contains("timed out")
}

/// 第二所仓位是否足以认定第二腿已成交。方向要和本腿一致。
fn second_qty_confirms_fill(
    qty: Decimal,
    is_buy: bool,
    planned: Decimal,
    min_qty: Decimal,
) -> bool {
    let need = planned.abs().min(min_qty.max(Decimal::new(1, 8)));
    if is_buy {
        qty >= need
    } else {
        qty <= -need
    }
}

fn effective_filled_qty(ack: &OrderAck) -> Decimal {
    if ack.filled_qty > Decimal::ZERO {
        ack.filled_qty
    } else {
        Decimal::ZERO
    }
}

/// sidecar 市价单经常把滑点保护限价当成 `avg_price`（Entropy 订单的
/// `limitPx`、Lighter IOC 的 protect price）。那不是成交均价。
///
/// 特征：相对决策 BBO 的滑点贴着 `max_slippage`；HL 买向上取整后会略高
/// （例如 0.1% → 0.1036%）。真成交均价几乎不可能每笔都贴死这条线。
/// 认不出来时退回决策 BBO，比把保护限价记进往返 bp 更接近所方已实现。
fn fill_price_for_pnl(
    reported: Option<Decimal>,
    expected: Decimal,
    is_buy: bool,
    max_slip_pct: Decimal,
) -> Decimal {
    let Some(fill) = reported.filter(|p| *p > Decimal::ZERO) else {
        return expected;
    };
    if expected <= Decimal::ZERO {
        return fill;
    }
    if looks_like_protect_limit(is_buy, expected, fill, max_slip_pct) {
        return expected;
    }
    fill
}

fn looks_like_protect_limit(
    is_buy: bool,
    expected: Decimal,
    fill: Decimal,
    max_slip_pct: Decimal,
) -> bool {
    if max_slip_pct <= Decimal::ZERO {
        return false;
    }
    let Some(slip) = realized_slip_pct(is_buy, expected, fill) else {
        return false;
    };
    // 比保护限价更好 → 真成交；比保护限价差太多 → 不像取整残差。
    if slip < max_slip_pct || slip > max_slip_pct * Decimal::new(125, 2) {
        return false;
    }
    let ratio = max_slip_pct / Decimal::from(100);
    let protect = if is_buy {
        expected * (Decimal::ONE + ratio)
    } else {
        expected * (Decimal::ONE - ratio)
    };
    (fill - protect).abs() <= expected * ratio * Decimal::new(25, 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::OrderAck;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    /// 激进限价必须被当成「不驻留」的单。
    ///
    /// 这条不是形式主义：sidecar 用 style 决定成交确认走轮询还是单次查询。
    /// 如果 AggressiveLimit 被并进 limit 那一类，单次查询会在 IOC 撤销后
    /// 看到 0，分不清「没成交」和「索引没跟上」——幻影成交原样回来。
    #[test]
    fn aggressive_limit_is_ioc_not_resting() {
        assert!(OrderStyle::AggressiveLimit.is_ioc());
        assert!(OrderStyle::MarketTaker.is_ioc());
        // 这两个会驻留，单次查询才是对的。
        assert!(!OrderStyle::LimitMaker.is_ioc());
        assert!(!OrderStyle::LimitThenMarket.is_ioc());
    }

    /// 激进限价越过盘口吃单，收 taker 费率，绝不能按 maker 算。
    #[test]
    fn aggressive_limit_pays_taker_fee() {
        assert_ne!(
            OrderStyle::AggressiveLimit.as_str(),
            OrderStyle::LimitMaker.as_str()
        );
    }

    fn ack(filled: Decimal, status: OrderStatus) -> OrderAck {
        OrderAck {
            order_id: "1".into(),
            client_order_id: None,
            filled_qty: filled,
            avg_price: None,
            status,
        }
    }

    fn leg(is_buy: bool) -> HedgeLeg {
        HedgeLeg {
            venue: "lighter".into(),
            symbol: "BTC".into(),
            market_index: 1,
            is_buy,
            style: OrderStyle::LimitMaker,
            min_qty: dec!(0.0001),
        }
    }

    /// Unknown 且零成交时**不能**当成 filled。这是幻影成交的入口：
    /// sidecar 曾对市价单无条件回 filled+请求量，把没成交的第二腿
    /// 记成已对冲，交易所留下裸仓而内存以为平了。
    #[test]
    fn unknown_status_is_not_a_fill() {
        assert_eq!(
            effective_filled_qty(&ack(Decimal::ZERO, OrderStatus::Unknown)),
            Decimal::ZERO
        );
        assert_eq!(
            effective_filled_qty(&ack(Decimal::ZERO, OrderStatus::Rejected)),
            Decimal::ZERO
        );
        // 明确 filled 且带量时照常采信。
        assert_eq!(
            effective_filled_qty(&ack(dec!(0.5), OrderStatus::Filled)),
            dec!(0.5)
        );
    }

    /// 「不确定」必须与「确定没成交」走不同的错误路径：
    /// 只有后者允许紧急平第一腿。前者反手平仓可能在第二腿其实已成交时
    /// 造出一条无人记账的反向裸仓。
    #[test]
    fn unverifiable_is_distinct_from_confirmed_no_fill() {
        let unknown = anyhow::anyhow!("SECOND_LEG_UNKNOWN: leg lighter fill unverifiable");
        let rejected = anyhow::anyhow!("leg lighter not filled (status Rejected)");
        assert!(is_unverifiable(&unknown));
        assert!(!is_unverifiable(&rejected));
    }

    #[test]
    fn second_qty_confirms_fill_needs_matching_sign_and_min() {
        let min = dec!(0.007);
        let planned = dec!(0.015);
        assert!(second_qty_confirms_fill(dec!(0.015), true, planned, min));
        assert!(second_qty_confirms_fill(dec!(0.007), true, planned, min));
        assert!(!second_qty_confirms_fill(dec!(0.001), true, planned, min));
        assert!(!second_qty_confirms_fill(Decimal::ZERO, true, planned, min));
        assert!(!second_qty_confirms_fill(dec!(-0.015), true, planned, min));
        assert!(second_qty_confirms_fill(dec!(-0.015), false, planned, min));
        assert!(!second_qty_confirms_fill(dec!(0.015), false, planned, min));
    }

    fn book(bid: Decimal, ask: Decimal) -> Bbo {
        Bbo {
            bid,
            ask,
            bid_qty: dec!(1),
            ask_qty: dec!(1),
            bids: vec![(bid, dec!(1))],
            asks: vec![(ask, dec!(1))],
            ts: Instant::now(),
        }
    }

    #[test]
    fn first_leg_still_resting_detects_open_order() {
        assert!(HedgeExecutor::first_leg_still_resting(&ack(
            Decimal::ZERO,
            OrderStatus::Accepted
        )));
        assert!(!HedgeExecutor::first_leg_still_resting(&ack(
            Decimal::ZERO,
            OrderStatus::Unknown
        )));
    }

    #[test]
    fn effective_filled_qty_prefers_reported() {
        assert_eq!(effective_filled_qty(&ack(dec!(100), OrderStatus::Partial)), dec!(100));
        assert_eq!(effective_filled_qty(&ack(Decimal::ZERO, OrderStatus::Filled)), Decimal::ZERO);
    }

    /// 第二腿零成交时的分流：确定没成交 → 可以反手平第一腿；
    /// 成交量未确认 → 只能人工介入。
    ///
    /// 关键是 `Filled` 落在「未确认」一侧：交易所说成交了却不给数量，
    /// 按请求量记账就是幻影成交，反手平第一腿更会造出反向裸仓。
    /// 对齐参考对市价单关闭 `_infer_fill_from_status` 兜底。
    #[test]
    fn zero_fill_only_auto_closes_on_terminal_no_fill() {
        let unconfirmed = |s| {
            matches!(
                s,
                OrderStatus::Unknown | OrderStatus::Filled | OrderStatus::Partial
            )
        };
        // 终态「没成交」——反手平第一腿是安全的
        assert!(!unconfirmed(OrderStatus::Canceled));
        assert!(!unconfirmed(OrderStatus::Rejected));
        // 数量未确认——不能自动处置
        assert!(unconfirmed(OrderStatus::Unknown));
        assert!(unconfirmed(OrderStatus::Filled));
        assert!(unconfirmed(OrderStatus::Partial));
    }

    #[test]
    fn maker_price_sits_inside_the_spread() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let b = book(dec!(100.00), dec!(100.05));
        assert_eq!(maker_limit_price(&cfg, &leg(true), &b), dec!(100.04));
        assert_eq!(maker_limit_price(&cfg, &leg(false), &b), dec!(100.01));
    }

    /// 点差只有一个 tick 时不能穿价，退回贴自家盘口。
    #[test]
    fn maker_price_never_crosses_on_one_tick_spread() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let b = book(dec!(100.00), dec!(100.01));
        assert_eq!(maker_limit_price(&cfg, &leg(true), &b), dec!(100.00));
        assert_eq!(maker_limit_price(&cfg, &leg(false), &b), dec!(100.01));
    }

    #[test]
    fn hedged_qty_is_the_smaller_leg() {
        let r = ExecResult::finished(
            ExecFill {
                venue: "a".into(),
                qty: dec!(1.0),
                price: dec!(1),
                is_buy: true,
                order_id: None,
            },
            ExecFill {
                venue: "b".into(),
                qty: dec!(0.6),
                price: dec!(1),
                is_buy: false,
                order_id: None,
            },
            None,
        );
        assert_eq!(r.hedged_qty(), dec!(0.6));
        assert_eq!(r.unhedged_qty, dec!(0.4));
    }

    #[test]
    fn protect_limit_is_not_used_as_fill_price() {
        // 1448 × 1.001 = 1449.448，HL 买向上取整到 0.1 → 1449.5，滑点 0.1036%
        let expected = dec!(1448);
        let protect = dec!(1449.5);
        assert_eq!(
            fill_price_for_pnl(Some(protect), expected, true, dec!(0.1)),
            expected
        );
        // 真成交：买贵了一点，但远不到保护限价
        assert_eq!(
            fill_price_for_pnl(Some(dec!(1448.2)), expected, true, dec!(0.1)),
            dec!(1448.2)
        );
        // 缺成交价 → 决策 BBO
        assert_eq!(fill_price_for_pnl(None, expected, true, dec!(0.1)), expected);
    }

    #[test]
    fn protect_limit_sell_rounds_away_from_market() {
        let expected = dec!(1448);
        let protect = dec!(1446.5); // 1448 × 0.999 = 1446.552，卖向下取整
        assert_eq!(
            fill_price_for_pnl(Some(protect), expected, false, dec!(0.1)),
            expected
        );
    }
}
