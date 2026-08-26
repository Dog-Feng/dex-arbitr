//! 小额实盘验证 CLI（单所账户 / 下单 / 撤单）。
//!
//! ```text
//! cargo run --bin live-test -- account lighter
//! cargo run --bin live-test -- balance sodex
//! cargo run --bin live-test -- market entropy SNDK buy 0.007
//! cargo run --bin live-test -- aggressive entropy SNDK buy 0.007 1480
//! cargo run --bin live-test -- limit entropy SNDK buy 0.007 1000
//! cargo run --bin live-test -- cancel entropy SNDK <order_id>
//! cargo run --bin live-test -- recap
//! ```

use anyhow::{Context, Result};
use dex_arbitr::config::{AppConfig, OrderStyle};
use dex_arbitr::exchange::{
    make_adapter, venue_yaml_path, CancelReq, ExchangePort, OrderReq,
};
use dex_arbitr::infra::journal::{ExecJournal, ExecRecord, now_ts};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: live-test <account|balance|positions|market|limit|aggressive|cancel|recap> ...");
        std::process::exit(1);
    }
    let cfg = AppConfig::load()?;
    let cmd = args[1].as_str();

    if cmd == "recap" {
        let path = cfg
            .live_test
            .journal_path
            .clone()
            .unwrap_or_else(|| "data/executions.sqlite".into());
        let j = ExecJournal::open(&path)?;
        for r in j.recent(30)? {
            println!(
                "{} {} {} {}→{} qty={} result={} {}",
                r.ts, r.action, r.pair_id, r.buy_venue, r.sell_venue, r.qty, r.result, r.detail
            );
        }
        return Ok(());
    }

    if args.len() < 3 {
        anyhow::bail!("need venue id");
    }
    let venue_id = &args[2];
    let adapter = load_adapter(&cfg, venue_id).await?;
    if !cfg.load_venue(venue_id)?.keys_ready() {
        anyhow::bail!("venue {venue_id} keys not ready");
    }

    match cmd {
        "account" | "balance" | "positions" => {
            let snap = adapter.account().await?;
            if cmd == "balance" || cmd == "account" {
                for b in &snap.balances {
                    println!("{} available={} total={:?}", b.asset, b.available, b.total);
                }
            }
            if cmd == "positions" || cmd == "account" {
                for p in &snap.positions {
                    println!("{} qty={} entry={:?}", p.symbol, p.qty, p.entry_price);
                }
            }
        }
        "market" | "limit" | "aggressive" => {
            let symbol = args.get(3).context("symbol")?;
            let side = args.get(4).context("buy|sell")?;
            let qty = Decimal::from_str(args.get(5).context("qty")?)?;
            cap_qty(&cfg, qty)?;
            let markets = adapter.list_perps().await?;
            let m = markets
                .iter()
                .find(|x| x.base.eq_ignore_ascii_case(symbol) || x.raw_symbol.eq_ignore_ascii_case(symbol))
                .with_context(|| format!("symbol {symbol} not found"))?;
            let is_buy = side.eq_ignore_ascii_case("buy");
            let style = match cmd {
                "limit" => OrderStyle::LimitMaker,
                "aggressive" => OrderStyle::AggressiveLimit,
                _ => OrderStyle::MarketTaker,
            };
            let limit_price = if cmd == "limit" || cmd == "aggressive" {
                Some(Decimal::from_str(
                    args.get(6).context("limit price")?,
                )?)
            } else {
                None
            };
            let ack = adapter
            .place(OrderReq {
                symbol: m.raw_symbol.clone(),
                market_index: m.market_index,
                is_buy,
                qty,
                reduce_only: args.iter().any(|a| a == "--reduce-only"),
                style,
                limit_price,
                client_order_id: Some(format!("lt-{}", now_ts())),
                // 手工验证工具：不设保护价，交给 sidecar 的默认保护。
                target_price: None,
                slippage_pct: None,
            })
            .await?;
            println!(
                "placed order_id={} filled_qty={} status={:?} avg={:?}",
                ack.order_id, ack.filled_qty, ack.status, ack.avg_price
            );
            log_journal(&cfg, symbol, "test_order", venue_id, "", qty, "ok", &ack.order_id)?;
        }
        "cancel" => {
            let symbol = args.get(3).context("symbol")?;
            let order_id = args.get(4).context("order_id")?;
            let markets = adapter.list_perps().await?;
            let m = markets
                .iter()
                .find(|x| x.base.eq_ignore_ascii_case(symbol))
                .with_context(|| format!("symbol {symbol} not found"))?;
            adapter
                .cancel(&CancelReq {
                    order_id: order_id.clone(),
                    symbol: m.raw_symbol.clone(),
                    market_index: m.market_index,
                    qty: None,
                })
                .await?;
            println!("canceled {order_id}");
            log_journal(&cfg, symbol, "test_cancel", venue_id, "", Decimal::ZERO, "ok", order_id)?;
        }
        other => anyhow::bail!("unknown cmd {other}"),
    }
    Ok(())
}

async fn load_adapter(cfg: &AppConfig, venue_id: &str) -> Result<Arc<dyn ExchangePort>> {
    if !cfg.venues.iter().any(|v| v == venue_id) {
        anyhow::bail!("venue {venue_id} not in config");
    }
    let path = venue_yaml_path(venue_id);
    if !path.exists() {
        anyhow::bail!("missing {}", path.display());
    }
    let venue = cfg.load_venue(venue_id)?;
    Ok(make_adapter(venue, cfg.pairs.whitelist.clone()))
}

fn cap_qty(cfg: &AppConfig, qty: Decimal) -> Result<()> {
    let cap = std::env::var("DEX_LIVE_TEST_MAX_QTY")
        .ok()
        .and_then(|s| Decimal::from_str(s.trim()).ok())
        .unwrap_or(cfg.live_test.max_qty);
    if qty > cap {
        anyhow::bail!("qty {qty} exceeds live_test.max_qty {cap} (override with DEX_LIVE_TEST_MAX_QTY)");
    }
    Ok(())
}

fn log_journal(
    cfg: &AppConfig,
    pair: &str,
    action: &str,
    buy: &str,
    sell: &str,
    qty: Decimal,
    result: &str,
    detail: &str,
) -> Result<()> {
    let path = cfg
        .live_test
        .journal_path
        .clone()
        .unwrap_or_else(|| "data/executions.sqlite".into());
    let j = ExecJournal::open(&path)?;
    j.append(&ExecRecord {
        ts: now_ts(),
        pair_id: format!("{pair}-USD-PERP"),
        action: action.to_string(),
        buy_venue: buy.to_string(),
        sell_venue: sell.to_string(),
        qty,
        net_pct: None,
        result: result.to_string(),
        detail: detail.to_string(),
    })
}
