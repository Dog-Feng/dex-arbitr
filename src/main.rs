use anyhow::Result;
use dex_arbitr::app::Controller;
use dex_arbitr::config::AppConfig;
use dex_arbitr::infra;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = match AppConfig::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("config: {err:#}");
            return Err(err);
        }
    };
    let _log_guard = infra::init_log(&cfg.system.log_dir);
    tracing::info!(
        monitor_only = cfg.system.monitor_only,
        venues = ?cfg.venues,
        scan = cfg.scan.enabled,
        watch_top = cfg.scan.watch_top,
        enabled = ?cfg.pairs.enabled.iter().map(|p| p.symbol.as_str()).collect::<Vec<_>>(),
        log_dir = %cfg.system.log_dir,
        "dex-arbitr P1 start"
    );
    Controller::run(cfg).await
}
