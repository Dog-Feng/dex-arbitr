use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::fmt::time::FormatTime;

struct LocalStamp;

impl FormatTime for LocalStamp {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"))
    }
}

/// 同时打 stderr 和 `log_dir` 下按天滚动的文件。`DEX_LOG_DIR` 可覆盖配置。
/// 返回的 Guard 必须在进程寿命内持有，否则文件日志可能丢尾。
pub fn init_log(log_dir: &str) -> Option<WorkerGuard> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let dir = std::env::var("DEX_LOG_DIR").unwrap_or_else(|_| log_dir.to_string());
    let dir = dir.trim();

    if dir.is_empty() {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .with_target(false)
            .with_timer(LocalStamp)
            .compact()
            .init();
        return None;
    }

    if let Err(err) = std::fs::create_dir_all(dir) {
        eprintln!("create log dir {dir}: {err}");
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .with_target(false)
            .with_timer(LocalStamp)
            .compact()
            .init();
        return None;
    }

    let appender = tracing_appender::rolling::daily(dir, "dex-arbitr.log");
    let (file_writer, guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr.and(file_writer))
        .with_target(false)
        .with_timer(LocalStamp)
        .compact()
        .init();
    Some(guard)
}
