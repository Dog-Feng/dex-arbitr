use anyhow::Result;
use dex_arbitr::app::Controller;
use dex_arbitr::config::AppConfig;
use dex_arbitr::infra;

#[tokio::main]
async fn main() -> Result<()> {
    infra::init_log();
    let cfg = AppConfig::load()?;
    tracing::info!(
        monitor_only = cfg.system.monitor_only,
        order_style = cfg.order.style.as_str(),
        venues = ?cfg.venues,
        scan = cfg.scan.enabled,
        min_raw = %cfg.scan.min_spread_pct,
        whitelist = ?cfg.pairs.whitelist,
        "dex-arbitr P1 start"
    );
    Controller::run(cfg).await
}
