use anyhow::{bail, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::{AppConfig, OrderStyle};
use crate::domain::Bbo;
use crate::exec::{fill_slip_overrun, HedgeLeg, HedgePlan};
use crate::exchange::{CancelReq, ExchangePort, OrderAck, OrderReq, OrderStatus};

#[derive(Debug, Clone)]
pub struct ExecFill {
    pub venue: String,
    pub qty: Decimal,
    pub price: Decimal,
    pub is_buy: bool,
}

#[derive(Debug, Clone)]
pub struct ExecResult {
    pub first: ExecFill,
    pub second: ExecFill,
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
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        plan: &HedgePlan,
        books: &HashMap<(String, String), Bbo>,
        paper: bool,
    ) -> Result<ExecResult> {
        match cfg.order.style {
            OrderStyle::LimitThenMarket => {
                let post = Self::post_first_leg(cfg, adapters, plan, books, paper).await?;
                if post.resting && !paper {
                    bail!("first leg resting (limit not filled yet)");
                }
                Self::hedge_second_leg(cfg, adapters, plan, books, paper, post.first.qty).await
            }
            OrderStyle::LimitMaker | OrderStyle::MarketTaker => {
                Self::dual_taker(cfg, adapters, plan, books, paper).await
            }
        }
    }

    pub async fn post_first_leg(
        _cfg: &AppConfig,
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        plan: &HedgePlan,
        books: &HashMap<(String, String), Bbo>,
        paper: bool,
    ) -> Result<PostFirstResult> {
        let first_bbo = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let first_price = limit_price(&plan.first, first_bbo);
        if paper {
            return Ok(PostFirstResult {
                first: ExecFill {
                    venue: plan.first.venue.clone(),
                    qty: Decimal::ZERO,
                    price: first_price,
                    is_buy: plan.first.is_buy,
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
            })
            .await?;
        let first = fill_from_ack(&plan.first, ack.clone(), first_price);
        let resting = ack.filled_qty <= Decimal::ZERO
            && matches!(ack.status, OrderStatus::Accepted | OrderStatus::Partial);
        Ok(PostFirstResult {
            first,
            order_id: if resting {
                Some(ack.order_id)
            } else {
                None
            },
            resting,
        })
    }

    pub async fn poll_first_leg(
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        leg: &HedgeLeg,
        plan_qty: Decimal,
        order_id: &str,
    ) -> Result<Option<ExecFill>> {
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        let ack = adapter
            .order_status(&CancelReq {
                order_id: order_id.to_string(),
                symbol: leg.symbol.clone(),
                market_index: leg.market_index,
                qty: Some(plan_qty),
            })
            .await?;
        if ack.filled_qty > Decimal::ZERO {
            return Ok(Some(fill_from_ack(leg, ack, Decimal::ZERO)));
        }
        if matches!(ack.status, OrderStatus::Filled) {
            return Ok(Some(fill_from_ack(
                leg,
                OrderAck {
                    filled_qty: plan_qty,
                    ..ack
                },
                Decimal::ZERO,
            )));
        }
        Ok(None)
    }

    pub async fn cancel_resting(
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
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

    pub async fn hedge_second_leg(
        cfg: &AppConfig,
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        plan: &HedgePlan,
        books: &HashMap<(String, String), Bbo>,
        paper: bool,
        hedge_qty: Decimal,
    ) -> Result<ExecResult> {
        if hedge_qty <= Decimal::ZERO && !paper {
            bail!("first leg filled zero");
        }
        let qty = if paper { plan.qty } else { hedge_qty };
        let first_bbo = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let first_price = limit_price(&plan.first, first_bbo);
        let first = if paper {
            ExecFill {
                venue: plan.first.venue.clone(),
                qty,
                price: market_price(&plan.first, first_bbo),
                is_buy: plan.first.is_buy,
            }
        } else {
            ExecFill {
                venue: plan.first.venue.clone(),
                qty,
                price: first_price,
                is_buy: plan.first.is_buy,
            }
        };
        let second_bbo = book_for(books, &plan.second.venue, &plan.pair_id)?;
        let second_price = market_price(&plan.second, second_bbo);
        let second = match Self::send_leg(
            adapters,
            &plan.second,
            qty,
            second_price,
            !plan.is_open,
            paper,
            second_bbo,
        )
        .await
        {
            Ok(f) => f,
            Err(err) => {
                warn!(
                    pair = %plan.pair_id,
                    error = %err,
                    "second leg failed; emergency close first"
                );
                Self::emergency_close(
                    adapters,
                    &plan.first,
                    qty,
                    plan.is_open,
                    first_bbo,
                    paper,
                )
                .await?;
                return Err(err);
            }
        };
        Self::log_overrun(cfg, &plan.first, first_price, &first);
        Self::log_overrun(cfg, &plan.second, second_price, &second);
        Ok(ExecResult { first, second })
    }

    async fn dual_taker(
        cfg: &AppConfig,
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        plan: &HedgePlan,
        books: &HashMap<(String, String), Bbo>,
        paper: bool,
    ) -> Result<ExecResult> {
        let b0 = book_for(books, &plan.first.venue, &plan.pair_id)?;
        let b1 = book_for(books, &plan.second.venue, &plan.pair_id)?;
        let p0 = market_price(&plan.first, b0);
        let p1 = market_price(&plan.second, b1);
        let first = Self::send_leg(adapters, &plan.first, plan.qty, p0, !plan.is_open, paper, b0).await?;
        let second = Self::send_leg(adapters, &plan.second, plan.qty, p1, !plan.is_open, paper, b1).await?;
        Self::log_overrun(cfg, &plan.first, p0, &first);
        Self::log_overrun(cfg, &plan.second, p1, &second);
        Ok(ExecResult { first, second })
    }

    async fn emergency_close(
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        leg: &HedgeLeg,
        qty: Decimal,
        reduce_only: bool,
        bbo: &Bbo,
        paper: bool,
    ) -> Result<()> {
        let mut reverse = leg.clone();
        reverse.is_buy = !leg.is_buy;
        reverse.style = OrderStyle::MarketTaker;
        let price = market_price(&reverse, bbo);
        Self::send_leg(adapters, &reverse, qty, price, reduce_only, paper, bbo).await?;
        info!(venue = %leg.venue, qty = %qty, "emergency close first leg");
        Ok(())
    }

    async fn send_leg(
        adapters: &HashMap<String, Arc<dyn ExchangePort>>,
        leg: &HedgeLeg,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        paper: bool,
        bbo: &Bbo,
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
            });
        }
        let adapter = adapters
            .get(&leg.venue)
            .ok_or_else(|| anyhow::anyhow!("unknown venue {}", leg.venue))?;
        let limit_price = if matches!(leg.style, OrderStyle::LimitMaker) {
            Some(price)
        } else {
            None
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
            })
            .await?;
        if !paper && matches!(leg.style, OrderStyle::LimitMaker) {
            let filled = ack.filled_qty;
            if filled <= Decimal::ZERO {
                return Ok(ExecFill {
                    venue: leg.venue.clone(),
                    qty: Decimal::ZERO,
                    price: ack.avg_price.unwrap_or(price),
                    is_buy: leg.is_buy,
                });
            }
        }
        Ok(fill_from_ack(leg, ack, price))
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

fn book_for<'a>(
    books: &'a HashMap<(String, String), Bbo>,
    venue: &str,
    pair_id: &str,
) -> Result<&'a Bbo> {
    books
        .get(&(venue.to_string(), pair_id.to_string()))
        .ok_or_else(|| anyhow::anyhow!("missing book {venue}/{pair_id}"))
}

fn limit_price(leg: &HedgeLeg, bbo: &Bbo) -> Decimal {
    if leg.is_buy {
        bbo.bid
    } else {
        bbo.ask
    }
}

fn market_price(leg: &HedgeLeg, bbo: &Bbo) -> Decimal {
    if leg.is_buy {
        bbo.ask
    } else {
        bbo.bid
    }
}

fn fill_from_ack(leg: &HedgeLeg, ack: OrderAck, fallback_price: Decimal) -> ExecFill {
    ExecFill {
        venue: leg.venue.clone(),
        qty: if ack.filled_qty > Decimal::ZERO {
            ack.filled_qty
        } else {
            Decimal::ZERO
        },
        price: ack.avg_price.unwrap_or(fallback_price),
        is_buy: leg.is_buy,
    }
}
