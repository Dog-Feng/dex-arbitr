//! 行情 HTTP / WSS 共用一套内存根证书和连接池。
//!
//! Linux 上 `rustls-native-certs` 会打开 `/etc/ssl/certs` 里每一份 PEM。
//! 每次建连都扫一遍的话，Lighter 约 2 分钟重连一次就能把默认 1024 FD 打满。
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream};
use tracing::warn;

/// JSON 字符串或 number → Decimal。缺字段 / 非法 → None。
pub fn json_decimal(v: &Value) -> Option<Decimal> {
    if let Some(s) = v.as_str() {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        return Decimal::from_str(s).ok();
    }
    if let Some(n) = v.as_number() {
        return Decimal::from_str(&n.to_string()).ok();
    }
    None
}

pub fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent("dex-arbitr/0.1")
            .timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(8)
            .tcp_nodelay(true)
            .use_preconfigured_tls((*rustls_config()).clone())
            .build()
            .expect("http client")
    })
}

fn rustls_config() -> Arc<rustls::ClientConfig> {
    static CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    })
    .clone()
}

pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn connect_ws(url: &str) -> Result<WsStream> {
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async_tls_with_config(
            url,
            None,
            true,
            Some(Connector::Rustls(rustls_config())),
        ),
    )
    .await
    .with_context(|| format!("ws connect timeout {url}"))?
    .with_context(|| format!("ws connect {url}"))?;
    Ok(ws)
}

fn is_emfile(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return io.raw_os_error() == Some(24);
        }
        let s = e.to_string();
        s.contains("Too many open files") || s.contains("os error 24")
    })
}

/// 同一适配器上新的订阅会 bump，旧循环在下一次 select 退出并丢掉旧连接。
#[derive(Clone)]
pub struct FeedGuard {
    tx: watch::Sender<u64>,
    fingerprint: Arc<Mutex<String>>,
}

impl FeedGuard {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self {
            tx,
            fingerprint: Arc::new(Mutex::new(String::new())),
        }
    }

    /// 市场集合没变则 `None`（不必重连）；变了则作废旧循环。
    pub fn begin(&self, fingerprint: &str) -> Option<(u64, watch::Receiver<u64>)> {
        let mut prev = self.fingerprint.lock().unwrap_or_else(|e| e.into_inner());
        if prev.as_str() == fingerprint {
            return None;
        }
        *prev = fingerprint.to_string();
        // `send` 在没有 Receiver 时直接丢值（`FeedGuard::new` 会丢掉初始 rx）。
        // 不更新的话 spawn 里 `*rx.borrow() != gen` 立刻成立，行情循环连 WS 都不连。
        let gen = *self.tx.borrow() + 1;
        self.tx.send_replace(gen);
        Some((gen, self.tx.subscribe()))
    }

    /// 空订必须 bump。第二次 `begin("off")` 指纹相同不会杀掉旧循环。
    pub fn interrupt(&self) {
        let mut prev = self.fingerprint.lock().unwrap_or_else(|e| e.into_inner());
        *prev = String::new();
        let gen = *self.tx.borrow() + 1;
        self.tx.send_replace(gen);
    }
}

pub fn spawn_feed_loop<F, Fut>(mut rx: watch::Receiver<u64>, gen: u64, task: &'static str, run: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    tokio::spawn(async move {
        loop {
            if *rx.borrow() != gen {
                return;
            }
            let mut retry = Duration::from_secs(3);
            tokio::select! {
                _ = rx.changed() => {
                    if *rx.borrow() != gen {
                        return;
                    }
                    continue;
                }
                result = run() => {
                    if let Err(err) = result {
                        if is_emfile(&err) {
                            retry = Duration::from_secs(30);
                        }
                        warn!(task, error = %format!("{err:#}"), retry_secs = retry.as_secs(), "feed stopped");
                    }
                }
            }
            if *rx.borrow() != gen {
                return;
            }
            tokio::select! {
                _ = rx.changed() => {
                    if *rx.borrow() != gen {
                        return;
                    }
                }
                _ = tokio::time::sleep(retry) => {}
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_guard_skips_same_fingerprint() {
        let g = FeedGuard::new();
        assert!(g.begin("a,b").is_some());
        assert!(g.begin("a,b").is_none());
        assert!(g.begin("a,b,c").is_some());
        assert!(g.begin("a,b,c").is_none());
    }

    #[test]
    fn interrupt_kills_even_when_already_off() {
        let g = FeedGuard::new();
        let (gen, rx) = g.begin("off").expect("first off");
        g.interrupt();
        assert_ne!(*rx.borrow(), gen);
        let gen2 = *rx.borrow();
        g.interrupt();
        assert_ne!(*rx.borrow(), gen2);
    }

    #[test]
    fn begin_receiver_sees_generation() {
        let g = FeedGuard::new();
        let (gen, rx) = g.begin("m1").expect("first subscribe");
        assert_ne!(gen, 0);
        assert_eq!(*rx.borrow(), gen);
        let (gen2, rx2) = g.begin("m2").expect("second subscribe");
        assert_eq!(*rx2.borrow(), gen2);
        assert_ne!(*rx.borrow(), gen);
    }

    #[test]
    fn emfile_detected_through_anyhow_context() {
        let err = anyhow::Error::from(std::io::Error::from_raw_os_error(24))
            .context("ws connect wss://example");
        assert!(is_emfile(&err));
        assert!(!is_emfile(&anyhow::anyhow!("connection reset")));
    }
}
