//! sidecar 自动重启的端到端验证。
//!
//! 用**真实的** Go sidecar：假脚本模拟不了进程退出时 stdout 关闭的时序，
//! 而那正是这个机制依赖的信号。
//!
//! 覆盖 `#16`：旧实现用 `OnceLock`，进程死了就永久失效——持仓期间遇上
//! 意味着平仓指令再也发不出去，仓位一直裸着没人管。
//!
//! 没构建 sidecar 时自动跳过（CI 上不强制要求 Go 工具链）。

use std::path::Path;

fn sidecar_path() -> Option<std::path::PathBuf> {
    for name in ["exchange_sidecar.exe", "exchange_sidecar"] {
        let p = Path::new("scripts/exchange_sidecar").join(name);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

/// 杀掉 sidecar 后，下一次调用必须能重建连接并继续服务。
///
/// 必须用 multi_thread：读循环是独立 task，单线程 runtime 下
/// 它拿不到执行机会，请求会永远等不到响应。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sidecar_restarts_after_being_killed() {
    let Some(bin) = sidecar_path() else {
        eprintln!("skip: sidecar 未构建");
        return;
    };
    let venue = Path::new("config/venues/sodex.example.yaml");
    if !venue.exists() {
        eprintln!("skip: 缺少 venue 示例配置");
        return;
    }
    std::env::set_var("DEX_EXCHANGE_SIDECAR", &bin);

    // 首次调用建连。example 配置没有私钥，sidecar 会回一个业务错误——
    // 这正合适：说明**协议链路通了**，而且不会真的连交易所。
    let first = dex_arbitr::exchange::bridge_call(venue, "account", serde_json::json!({})).await;
    assert!(
        first.is_err(),
        "example 配置应回业务错误（无私钥），实际: {first:?}"
    );
    let msg = format!("{:#}", first.unwrap_err());
    assert!(
        msg.contains("private_key") || msg.contains("sodex"),
        "应是 sidecar 的业务错误而非传输错误: {msg}"
    );

    // 杀掉子进程，模拟崩溃。
    let killed = kill_sidecar();
    assert!(killed, "应能杀掉 sidecar 进程");

    // 给读循环时间发现 stdout 关闭并置 dead 标志。
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    // 关键断言：下一次调用应自动重建，而不是永久失败。
    let second = dex_arbitr::exchange::bridge_call(venue, "account", serde_json::json!({})).await;
    let msg2 = format!("{:#}", second.unwrap_err());
    assert!(
        msg2.contains("private_key") || msg2.contains("sodex"),
        "重启后应恢复到同样的业务错误，而不是传输层失败: {msg2}"
    );
    assert!(
        !msg2.contains("process exited") && !msg2.contains("timed out"),
        "不应再报进程退出/超时: {msg2}"
    );

    // 收尾：sidecar 是常驻进程，测试结束后不会自己退出，
    // 留着会让 cargo test 一直等它的 stdout EOF。
    kill_sidecar();
}

fn kill_sidecar() -> bool {
    #[cfg(windows)]
    {
        std::process::Command::new("taskkill")
            .args(["/F", "/IM", "exchange_sidecar.exe"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
    #[cfg(not(windows))]
    {
        std::process::Command::new("pkill")
            .args(["-9", "-f", "exchange_sidecar"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}
