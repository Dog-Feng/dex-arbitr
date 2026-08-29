//! 行情 HTTP / WSS 共用一套 TLS 根证书和连接池。
//!
//! `rustls-native-certs` 在 Linux 上会打开 `/etc/ssl/certs` 里每一份 PEM。
//! 每次 `reqwest::Client::builder()` 或 `connect_async` 都扫一遍的话，
//! 行情重连一密，文件描述符会先被证书读光，再报
//! `failed to read PEM from file ... Too many open files`。
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream};
use tracing::warn;

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
        let native = rustls_native_certs::load_native_certs();
        for err in &native.errors {
            warn!(error = %err, "native TLS cert skipped");
        }
        for cert in native.certs {
            let _ = roots.add(cert);
        }
        if roots.is_empty() {
            warn!("native TLS root store is empty; HTTPS/WSS will fail");
        }
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
    let (ws, _) = connect_async_tls_with_config(
        url,
        None,
        true,
        Some(Connector::Rustls(rustls_config())),
    )
    .await
    .with_context(|| format!("ws connect {url}"))?;
    Ok(ws)
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
            tokio::select! {
                _ = rx.changed() => {
                    if *rx.borrow() != gen {
                        return;
                    }
                }
                result = run() => {
                    if let Err(err) = result {
                        warn!(task, error = %err, "feed stopped");
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
                _ = tokio::time::sleep(Duration::from_secs(3)) => {}
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
    fn begin_receiver_sees_generation() {
        let g = FeedGuard::new();
        let (gen, rx) = g.begin("m1").expect("first subscribe");
        assert_ne!(gen, 0);
        assert_eq!(*rx.borrow(), gen);
        let (gen2, rx2) = g.begin("m2").expect("second subscribe");
        assert_eq!(*rx2.borrow(), gen2);
        assert_ne!(*rx.borrow(), gen);
    }
}
