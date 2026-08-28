use rust_decimal::Decimal;

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{slot_key, Intent, Pair, Position, VenueId};

#[derive(Debug, Clone)]
pub struct HedgeLeg {
    pub venue: String,
    pub symbol: String,
    pub market_index: i32,
    pub is_buy: bool,
    pub style: OrderStyle,
    /// 该所该市场的最小下单量。第二腿用它做前置校验。
    pub min_qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct HedgePlan {
    pub pair_id: String,
    /// 币 + 所对。持仓 / 挂单 / 计时都按它索引。
    pub slot: String,
    pub qty: Decimal,
    pub is_open: bool,
    pub style: OrderStyle,
    pub buy_market_index: i32,
    pub sell_market_index: i32,
    pub buy_symbol: String,
    pub sell_symbol: String,
    pub buy_venue: String,
    pub sell_venue: String,
    /// 决策那一刻的净边（raw − 开仓手续费，**未扣 nat**）。
    /// 成交价拿不到时用它当建仓净边，平仓算往返净利要用。
    pub decision_net_pct: Decimal,
    /// 决策那一刻的毛价差。成交价拿不到时当建仓 raw。
    pub decision_raw_pct: Decimal,
    /// 开仓时定仓所得单格数量，固化到 Position.base_qty。
    /// 平仓计划此字段为 ZERO（平仓不需要重新定仓）。
    pub base_qty: Decimal,
    /// 成交前 / 后有符号 STEP。
    pub grid_from: i32,
    pub grid_to: i32,
    /// 仅平仓：决策时的往返净利 %。
    pub pnl_pct: Option<Decimal>,
    pub first: HedgeLeg,
    pub second: HedgeLeg,
}

impl HedgePlan {
    /// 第一腿至少要成交这么多，第二腿才下得出去。低于它只能撤/平第一腿。
    pub fn hedgeable_min_qty(&self) -> Decimal {
        self.second.min_qty.max(Decimal::new(1, 8))
    }
}

pub fn plan_hedge(
    pair: &Pair,
    intent: &Intent,
    pos: Option<&Position>,
    cfg: &AppConfig,
) -> Option<HedgePlan> {
    match intent {
        Intent::Hold => None,
        Intent::Open {
            qty, buy, sell, ..
        } => build(pair, cfg, *qty, true, buy, sell),
        Intent::Close { qty, .. } => {
            let pos = pos?;
            // 平仓方向 = 持仓方向反过来：在原 sell 所买回、在原 buy 所卖出。
            build(pair, cfg, *qty, false, &pos.sell, &pos.buy)
        }
    }
}

fn build(
    pair: &Pair,
    _cfg: &AppConfig,
    qty: Decimal,
    is_open: bool,
    buy: &VenueId,
    sell: &VenueId,
) -> Option<HedgePlan> {
    let buy_leg = pair.leg(buy.as_str())?;
    let sell_leg = pair.leg(sell.as_str())?;
    let (first, second) = sequenced_legs(buy, sell, buy_leg, sell_leg);
    Some(HedgePlan {
        pair_id: pair.pair_id.clone(),
        slot: slot_key(&pair.pair_id, buy.as_str(), sell.as_str()),
        qty,
        is_open,
        style: OrderStyle::MarketTaker,
        buy_market_index: buy_leg.market_index,
        sell_market_index: sell_leg.market_index,
        buy_symbol: buy_leg.raw_symbol.clone(),
        sell_symbol: sell_leg.raw_symbol.clone(),
        buy_venue: buy.as_str().to_string(),
        sell_venue: sell.as_str().to_string(),
        decision_net_pct: Decimal::ZERO,
        decision_raw_pct: Decimal::ZERO,
        base_qty: Decimal::ZERO, // 由 controller 从配置填入
        grid_from: 0,
        grid_to: 0,
        pnl_pct: None,
        first,
        second,
    })
}

fn sequenced_legs(
    buy: &VenueId,
    sell: &VenueId,
    buy_leg: &crate::domain::VenueMarket,
    sell_leg: &crate::domain::VenueMarket,
) -> (HedgeLeg, HedgeLeg) {
    let buy_h = HedgeLeg {
        venue: buy.as_str().to_string(),
        symbol: buy_leg.raw_symbol.clone(),
        market_index: buy_leg.market_index,
        is_buy: true,
        style: OrderStyle::MarketTaker,
        min_qty: buy_leg.min_qty,
    };
    let sell_h = HedgeLeg {
        venue: sell.as_str().to_string(),
        symbol: sell_leg.raw_symbol.clone(),
        market_index: sell_leg.market_index,
        is_buy: false,
        style: OrderStyle::MarketTaker,
        min_qty: sell_leg.min_qty,
    };
    (buy_h, sell_h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{CloseReason, VenueId, VenueMarket};
    use rust_decimal_macros::dec;

    fn pair() -> Pair {
        Pair {
            pair_id: "BTC-USD-PERP".into(),
            legs: [
                VenueMarket {
                    venue: VenueId::from("lighter"),
                    raw_symbol: "BTC".into(),
                    pair_id: "BTC-USD-PERP".into(),
                    base: "BTC".into(),
                    market_index: 1,
                    qty_precision: 5,
                    min_qty: dec!(0.0002),
                },
                VenueMarket {
                    venue: VenueId::from("lighter_rh"),
                    raw_symbol: "BTC".into(),
                    pair_id: "BTC-USD-PERP".into(),
                    base: "BTC".into(),
                    market_index: 1,
                    qty_precision: 5,
                    min_qty: dec!(0.0003),
                },
            ],
        }
    }

    #[test]
    fn close_reverses_open_legs() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let pos = Position {
            pair_id: "BTC-USD-PERP".into(),
            buy: VenueId::from("lighter"),
            sell: VenueId::from("lighter_rh"),
            qty: dec!(0.001),
            grid: 1,
            entry_notional_usdc: dec!(100),
            entry_net_pct: dec!(0.05),
            entry_raw_pct: dec!(0.05),
            entry_buy_px: Decimal::ZERO,
            entry_sell_px: Decimal::ZERO,
            base_qty: dec!(0.001),
            opened_at: std::time::Instant::now(),
        };
        let plan = plan_hedge(
            &pair(),
            &Intent::Close {
                qty: dec!(0.001),
                grid: 0,
                reason: CloseReason::GridReduce,
                round_trip_pct: dec!(0.01),
            },
            Some(&pos),
            &cfg,
        )
        .unwrap();
        assert!(!plan.is_open);
        assert_eq!(plan.buy_venue, "lighter_rh");
        assert_eq!(plan.sell_venue, "lighter");
        assert_eq!(plan.first.venue, "lighter_rh");
        assert!(plan.first.is_buy);
        assert_eq!(plan.first.style, OrderStyle::MarketTaker);
        assert_eq!(plan.second.venue, "lighter");
        assert!(!plan.second.is_buy);
        assert_eq!(plan.second.style, OrderStyle::MarketTaker);
        // 开仓和平仓落在同一个槽位
        assert_eq!(plan.slot, pos.slot_key());
    }

    #[test]
    fn open_both_legs_market_taker() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let plan = plan_hedge(
            &pair(),
            &Intent::Open {
                qty: dec!(0.001),
                buy: VenueId::from("lighter"),
                sell: VenueId::from("lighter_rh"),
                grid: 1,
            },
            None,
            &cfg,
        )
        .unwrap();
        assert_eq!(plan.style, OrderStyle::MarketTaker);
        assert_eq!(plan.first.venue, "lighter");
        assert!(plan.first.is_buy);
        assert_eq!(plan.first.style, OrderStyle::MarketTaker);
        assert_eq!(plan.second.venue, "lighter_rh");
        assert!(!plan.second.is_buy);
        assert_eq!(plan.second.style, OrderStyle::MarketTaker);
        assert_eq!(plan.hedgeable_min_qty(), dec!(0.0003));
    }
}
