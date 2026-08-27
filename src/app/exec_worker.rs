use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::app::balance::{refresh_accounts, BalanceCache, VenueAccountCache};
use crate::app::control::ArbitrageControl;
use crate::app::funding::{refresh_funding, FundingCache};
use crate::config::AppConfig;
use crate::domain::Books;
use crate::exchange::ExchangePort;
use crate::exec::{Adapters, ExecResult, HedgeExecutor, HedgePlan, LimitMarketRun};

pub enum ExecEvent {
    RunPlan(RunPlanMsg),
    /// 账户快照。余额/持仓刷新走后台 task，不能在 100ms 决策环里 await——
    /// 单次 sidecar 调用最长十几秒，会把整个决策环拖停。
    Accounts(Box<AccountsMsg>),
    /// 资金费率快照。同样走后台：两个所各一次 REST，不能挡决策环。
    Funding(Box<FundingCache>),
}

pub struct AccountsMsg {
    pub balance: BalanceCache,
    pub accounts: VenueAccountCache,
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
    paper: bool,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::run_plan(&cfg, &adapters, &plan, &books, paper)
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
            tokio::time::sleep_until(started + period).await;
        }
    });
}

/// 后台周期性拉资金费率。
///
/// 首轮立即拉：进入循环先 refresh 再 sleep，启动后就有数据，
/// 不必等一个完整周期——否则启动初期所有 pair 都因缺费率被开仓门拦住。
pub fn spawn_funding_refresher(
    tx: mpsc::UnboundedSender<ExecEvent>,
    adapters: Vec<Arc<dyn ExchangePort>>,
    mut cfg: AppConfig,
    control: Option<Arc<Mutex<ArbitrageControl>>>,
) {
    tokio::spawn(async move {
        loop {
            overlay_page(&mut cfg, &control);
            let cache = refresh_funding(&adapters).await;
            if tx.send(ExecEvent::Funding(Box::new(cache))).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(cfg.risk.funding_refresh_secs.max(5))).await;
        }
    });
}
