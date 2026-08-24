use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::domain::Bbo;
use crate::exec::{ExecResult, HedgeExecutor, HedgePlan, PostFirstResult};
use crate::exchange::ExchangePort;

pub enum ExecEvent {
    PostFirst(PostFirstMsg),
    HedgeSecond(HedgeSecondMsg),
    RunPlan(RunPlanMsg),
}

pub struct PostFirstMsg {
    pub pair_id: String,
    pub pair_i: usize,
    pub plan: HedgePlan,
    pub result: Result<PostFirstResult, String>,
}

pub struct HedgeSecondMsg {
    pub pair_id: String,
    pub pair_i: usize,
    pub plan: HedgePlan,
    pub hedge_qty: rust_decimal::Decimal,
    pub result: Result<ExecResult, String>,
}

pub struct RunPlanMsg {
    pub pair_id: String,
    pub pair_i: usize,
    pub plan: HedgePlan,
    pub result: Result<ExecResult, String>,
}

pub fn spawn_post_first_leg(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: HashMap<String, Arc<dyn ExchangePort>>,
    books: HashMap<(String, String), Bbo>,
    pair_i: usize,
    plan: HedgePlan,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::post_first_leg(&cfg, &adapters, &plan, &books, false)
            .await
            .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::PostFirst(PostFirstMsg {
            pair_id: plan.pair_id.clone(),
            pair_i,
            plan,
            result,
        }));
    });
}

pub fn spawn_hedge_second_leg(
    tx: mpsc::UnboundedSender<ExecEvent>,
    cfg: AppConfig,
    adapters: HashMap<String, Arc<dyn ExchangePort>>,
    books: HashMap<(String, String), Bbo>,
    pair_i: usize,
    plan: HedgePlan,
    hedge_qty: rust_decimal::Decimal,
) {
    tokio::spawn(async move {
        let result = HedgeExecutor::hedge_second_leg(
            &cfg,
            &adapters,
            &plan,
            &books,
            false,
            hedge_qty,
        )
        .await
        .map_err(|e| e.to_string());
        let _ = tx.send(ExecEvent::HedgeSecond(HedgeSecondMsg {
            pair_id: plan.pair_id.clone(),
            pair_i,
            plan,
            hedge_qty,
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
