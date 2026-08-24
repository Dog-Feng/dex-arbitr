use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::domain::Bbo;
use crate::exec::{ExecResult, HedgeExecutor, HedgePlan, LimitMarketRun};
use crate::exchange::ExchangePort;

pub enum ExecEvent {
    RunPlan(RunPlanMsg),
}

pub struct RunPlanMsg {
    pub pair_id: String,
    pub pair_i: usize,
    pub plan: HedgePlan,
    pub result: Result<ExecResult, String>,
}

/// 对齐参考 execute_limit_market_mode：单 task 内 post → wait fill → hedge second。
pub fn spawn_limit_market(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: HashMap<String, Arc<dyn ExchangePort>>,
    books: HashMap<(String, String), Bbo>,
    pair_i: usize,
    plan: HedgePlan,
    ctx: LimitMarketRun,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::execute_limit_market(&cfg, &adapters, &plan, &books, &ctx)
            .await
            .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::RunPlan(RunPlanMsg {
            pair_id: plan.pair_id.clone(),
            pair_i,
            plan,
            result,
        }));
    });
}

pub fn spawn_run_plan(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: HashMap<String, Arc<dyn ExchangePort>>,
    books: HashMap<(String, String), Bbo>,
    pair_i: usize,
    plan: HedgePlan,
    paper: bool,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::run_plan(&cfg, &adapters, &plan, &books, paper)
            .await
            .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::RunPlan(RunPlanMsg {
            pair_id: plan.pair_id.clone(),
            pair_i,
            plan,
            result,
        }));
    });
}
