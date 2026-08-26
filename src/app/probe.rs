//! reduce-only 恢复探针。对齐参考 `_probe_reduce_only_recovery`。
//!
//! 拉闸是纯内存状态，没有探针就只能靠重启清除。每小时 `HH:00:05` 对拉闸中的
//! pair 发一笔**最小量市价开仓**：能开就说明交易所解除了限制 → 解闸；
//! 随后立刻 reduce-only 平掉，不留仓。
//!
//! 判据是「第一步能不能开」，与第二步平仓是否成功无关——我们测的是开仓能力。
//!
//! 参考实现的漏洞：第一步开成了、第二步平失败时，账上留一笔最小量裸仓，
//! 而它只打日志就算了。这里补上：把残留量返回给调用方登记成 `BotFailure`
//! 裸敞口，交给既有的自动补对冲路径收拾。探针本身是风控设施，
//! 不能反过来成为敞口来源。

use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config::{AppConfig, OrderStyle};
use crate::domain::Books;
use crate::exec::{Adapters, HedgeExecutor, HedgeLeg};

/// 平仓重试次数。开仓已经成功，此时平不掉就要留裸仓，值得多试两次。
const CLOSE_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct ProbeOutcome {
    /// 开仓是否成功 = reduce-only 限制是否已解除。解闸只看这个。
    pub success: bool,
    pub venue: String,
    pub detail: String,
    /// `Some(qty)` 表示开成功但没平掉，账上留了这么多**多头**裸量。
    /// 调用方必须登记成裸敞口。
    pub naked_qty: Option<Decimal>,
}

/// 对单个 pair 的单条腿跑一轮探针。
///
/// 方向固定「先买后卖」：最小量下双向风险等价，固定方向让日志和残留处理可预期。
/// `min_qty` 拿不到（<=0）时直接放弃——不能瞎猜一个量去下真单。
pub async fn run_probe(
    cfg: &AppConfig,
    adapters: &Adapters,
    pair_id: &str,
    venue: &str,
    symbol: &str,
    market_index: i32,
    min_qty: Decimal,
    books: &Books,
) -> ProbeOutcome {
    let mut out = ProbeOutcome {
        success: false,
        venue: venue.to_string(),
        detail: String::new(),
        naked_qty: None,
    };
    if min_qty <= Decimal::ZERO {
        out.detail = "min_qty unknown; cannot probe".into();
        return out;
    }
    let leg = HedgeLeg {
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        market_index,
        is_buy: true,
        style: OrderStyle::MarketTaker,
        min_qty,
    };

    info!(
        pair = pair_id,
        venue, qty = %min_qty,
        "reduce_only probe: opening min-qty position"
    );
    let opened = match HedgeExecutor::market_leg(
        cfg, adapters, pair_id, &leg, min_qty, true, false, books, false,
    )
    .await
    {
        Ok(fill) => fill.qty,
        Err(err) => {
            // 开不了：限制还在（或别的原因），保持拉闸。不留仓，无需善后。
            out.detail = format!("probe open failed: {err}");
            info!(pair = pair_id, venue, error = %err, "reduce_only probe: still blocked");
            return out;
        }
    };

    // 开成了 → 限制已解除。无论后面平不平掉，都要解闸。
    out.success = true;
    out.detail = format!("probe opened {opened}");

    for attempt in 1..=CLOSE_ATTEMPTS {
        match HedgeExecutor::market_leg(
            cfg, adapters, pair_id, &leg, opened, false, true, books, false,
        )
        .await
        {
            Ok(fill) => {
                info!(
                    pair = pair_id,
                    venue,
                    qty = %fill.qty,
                    attempt,
                    "reduce_only probe: recovered, probe position closed"
                );
                out.detail = format!("probe ok; opened {opened} closed {}", fill.qty);
                return out;
            }
            Err(err) => warn!(
                pair = pair_id,
                venue,
                attempt,
                error = %err,
                "reduce_only probe: closing probe position failed"
            ),
        }
    }

    // 三次都没平掉：留了裸仓。必须上报，不能只打日志。
    warn!(
        pair = pair_id,
        venue,
        qty = %opened,
        "reduce_only probe: LEFT NAKED min-qty position; registering exposure"
    );
    out.naked_qty = Some(opened);
    out.detail = format!("probe opened {opened} but close failed {CLOSE_ATTEMPTS}x; NAKED");
    out
}
