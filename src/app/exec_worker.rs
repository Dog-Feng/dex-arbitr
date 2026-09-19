use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::app::balance::{refresh_accounts, BalanceCache, VenueAccountCache};
use crate::app::control::ArbitrageControl;
use crate::app::reconcile::exchange_legs_to_flatten;
use rust_decimal::Decimal;

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{Books, Pair};
use crate::exchange::bridge::{request_account_refresh, take_force_account_refresh};
use crate::exchange::ExchangePort;
use crate::exec::{Adapters, ExecFill, ExecResult, HedgeExecutor, HedgeLeg, HedgePlan, LimitMarketRun};
use tracing::{info, warn};

pub enum ExecEvent {
    RunPlan(RunPlanMsg),
    /// 账户快照。余额/持仓刷新走后台 task，不能在 100ms 决策环里 await——
    /// 单次 sidecar 调用最长十几秒，会把整个决策环拖停。
    Accounts(Box<AccountsMsg>),
    /// 裸仓补对冲结果。下单走后台，避免卡死决策环。
    NakedHedge(NakedHedgeMsg),
}

pub struct AccountsMsg {
    pub balance: BalanceCache,
    pub accounts: VenueAccountCache,
}

pub struct NakedHedgeMsg {
    pub key: String,
    pub pair_id: String,
    pub venue: String,
    pub counterparty: String,
    pub result: Result<ExecFill, String>,
}

pub struct RunPlanMsg {
    /// 币 + 所对。
    pub slot: String,
    pub pair_i: usize,
    pub plan: HedgePlan,
    pub result: Result<ExecResult, String>,
}

/// 对齐参考 execute_limit_market_mode：单 task 内 post → wait fill → hedge second。
pub fn spawn_limit_market(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: Adapters,
    books: Books,
    pair_i: usize,
    plan: HedgePlan,
    ctx: LimitMarketRun,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::execute_limit_market(&cfg, &adapters, &plan, &books, &ctx)
            .await
            .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::RunPlan(RunPlanMsg {
            slot: plan.slot.clone(),
            pair_i,
            plan,
            result,
        }));
    });
}

pub fn spawn_run_plan(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: Adapters,
    books: Books,
    pair_i: usize,
    plan: HedgePlan,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::run_plan(&cfg, &adapters, &plan, &books, false)
            .await
            .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::RunPlan(RunPlanMsg {
            slot: plan.slot.clone(),
            pair_i,
            plan,
            result,
        }));
    });
}

fn burst_market_flatten_legs(pair: &Pair, accounts: &VenueAccountCache) -> Vec<(HedgeLeg, Decimal, bool)> {
    let mut legs = Vec::new();
    for (leg, signed_qty) in exchange_legs_to_flatten(pair, accounts) {
        let close_qty = signed_qty.abs();
        if close_qty <= leg.min_qty {
            warn!(
                pair = %pair.pair_id,
                venue = %leg.venue,
                qty = %signed_qty,
                min_qty = %leg.min_qty,
                "exchange residual below min_qty; skip auto flatten"
            );
            continue;
        }
        let is_buy = signed_qty.is_sign_negative();
        legs.push((
            HedgeLeg {
                venue: leg.venue.to_string(),
                symbol: leg.raw_symbol.clone(),
                market_index: leg.market_index,
                is_buy,
                style: OrderStyle::MarketTaker,
                min_qty: leg.min_qty,
                limit_price: None,
            },
            close_qty,
            is_buy,
        ));
    }
    legs
}

async fn run_burst_exchange_flatten(
    cfg: &AppConfig,
    adapters: &Adapters,
    books: &Books,
    pair_id: &str,
    legs: Vec<(HedgeLeg, Decimal, bool)>,
) {
    if legs.is_empty() {
        return;
    }
    let mut tasks = Vec::with_capacity(legs.len());
    for (leg, qty, is_buy) in legs {
        let cfg = cfg.clone();
        let adapters = adapters.clone();
        let books = books.clone();
        let pair_id = pair_id.to_string();
        tasks.push(async move {
            HedgeExecutor::market_leg(
                &cfg,
                &adapters,
                &pair_id,
                &leg,
                qty,
                is_buy,
                true,
                &books,
                false,
            )
            .await
        });
    }
    for result in futures_util::future::join_all(tasks).await {
        match result {
            Ok(fill) => info!(
                pair = %pair_id,
                venue = %fill.venue,
                qty = %fill.qty,
                "burst exchange flatten leg filled"
            ),
            Err(err) => warn!(
                pair = %pair_id,
                error = %err,
                "burst exchange flatten leg failed"
            ),
        }
    }
    request_account_refresh();
}

/// Burst 小循环平到内存 0 后：等待结算 → 拉两所实际持仓 → 仍有尾差则并发市价 reduce-only。
pub fn spawn_burst_post_close_exchange_flatten(
    cfg: AppConfig,
    port_adapters: Vec<Arc<dyn ExchangePort>>,
    hedge_adapters: Adapters,
    books: Books,
    pair: Pair,
    settle_ms: u64,
) {
    tokio::spawn(async move {
        info!(
            pair = %pair.pair_id,
            settle_ms,
            "burst close rep done; waiting before exchange position check"
        );
        tokio::time::sleep(Duration::from_millis(settle_ms)).await;

        let (_balance, accounts) = refresh_accounts(&port_adapters, &cfg.sizing).await;
        if !accounts.all_fresh() {
            warn!(
                pair = %pair.pair_id,
                "burst flatten skipped: account refresh not fresh after settle wait"
            );
            request_account_refresh();
            return;
        }
        let legs = burst_market_flatten_legs(&pair, &accounts);
        if legs.is_empty() {
            return;
        }
        info!(
            pair = %pair.pair_id,
            n = legs.len(),
            "exchange still has position after settle — spawning market flatten"
        );
        run_burst_exchange_flatten(&cfg, &hedge_adapters, &books, &pair.pair_id, legs).await;
    });
}

pub fn spawn_naked_hedge(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: Adapters,
    books: Books,
    key: String,
    pair_id: String,
    venue: String,
    counterparty: String,
    leg: crate::exec::HedgeLeg,
    qty: rust_decimal::Decimal,
    is_buy: bool,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::market_leg(
            &cfg,
            &adapters,
            &pair_id,
            &leg,
            qty,
            is_buy,
            false,
            &books,
            false,
        )
        .await
        .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::NakedHedge(NakedHedgeMsg {
            key,
            pair_id,
            venue,
            counterparty,
            result,
        }));
    });
}

fn overlay_page(cfg: &mut AppConfig, control: &Option<Arc<Mutex<ArbitrageControl>>>) {
    let Some(ctrl) = control else {
        return;
    };
    if let Ok(g) = ctrl.lock() {
        g.params.apply_to(cfg);
    }
}

/// 后台周期性拉账户快照，通过 channel 回灌决策环。
pub fn spawn_account_refresher(
    tx: mpsc::UnboundedSender<ExecEvent>,
    adapters: Vec<Arc<dyn ExchangePort>>,
    mut cfg: AppConfig,
    control: Option<Arc<Mutex<ArbitrageControl>>>,
) {
    tokio::spawn(async move {
        loop {
            overlay_page(&mut cfg, &control);
            let period = Duration::from_secs(cfg.sizing.refresh_balance_secs.max(1));
            let started = tokio::time::Instant::now();
            let (balance, accounts) = refresh_accounts(&adapters, &cfg.sizing).await;
            let msg = Box::new(AccountsMsg { balance, accounts });
            if tx.send(ExecEvent::Accounts(msg)).is_err() {
                break;
            }
            // 以 refresh_balance_secs 为周期。睡眠拆成短片，页面改间隔后最多 ~200ms 生效。
            loop {
                overlay_page(&mut cfg, &control);
                let want = Duration::from_secs(cfg.sizing.refresh_balance_secs.max(1));
                if want != period {
                    break;
                }
                let elapsed = started.elapsed();
                if elapsed >= period {
                    break;
                }
                if take_force_account_refresh() {
                    break;
                }
                tokio::time::sleep((period - elapsed).min(Duration::from_millis(200))).await;
            }
        }
    });
}
