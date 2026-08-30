use rust_decimal::Decimal;

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{slot_key, AdjacentQuote, Intent, Pair, Position, VenueId};
use crate::exec::sequence::first_limit_venue_all_in_or_left;

#[derive(Debug, Clone)]
pub struct HedgeLeg {
    pub venue: String,
    pub symbol: String,
    pub market_index: i32,
    pub is_buy: bool,
    pub style: OrderStyle,
    /// 该所该市场的最小下单量。第二腿用它做前置校验。
    pub min_qty: Decimal,
    /// 阶段 2 邻档：第一腿绝对限价。None 则用盘口 maker_inside。
    pub limit_price: Option<Decimal>,
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
    pub first: HedgeLeg,
    pub second: HedgeLeg,
    /// 阶段 2 邻档侧。None = 阶段 1 市价计划。
    pub quote_side: Option<crate::domain::QuoteSide>,
    /// 邻档挂着等事件，不按 2s 超时。
    pub rest_quote: bool,
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

/// 阶段 2：邻档第一腿限价 + 第二腿市价。
pub fn plan_adjacent(
    pair: &Pair,
    q: &AdjacentQuote,
    cfg: &AppConfig,
    left: &VenueId,
    first_limit: Decimal,
    k: i32,
    base_qty: Decimal,
    buy_spread_pct: Option<Decimal>,
    sell_spread_pct: Option<Decimal>,
) -> Option<HedgePlan> {
    let mut plan = build(pair, cfg, q.qty, q.is_open, &q.buy, &q.sell)?;
    let (first_v, second_v) =
        first_limit_venue_all_in_or_left(cfg, &q.buy, &q.sell, left, buy_spread_pct, sell_spread_pct);
    let first_is_buy = first_v.as_str() == q.buy.as_str();
    let mut first = if first_is_buy {
        plan.first.clone()
    } else {
        plan.second.clone()
    };
    let mut second = if first_is_buy {
        plan.second.clone()
    } else {
        plan.first.clone()
    };
    first.is_buy = first_is_buy;
    first.style = OrderStyle::LimitMaker;
    first.limit_price = Some(first_limit);
    second.is_buy = !first_is_buy;
    second.style = OrderStyle::MarketTaker;
    second.limit_price = None;
    // sequenced_legs 按 buy/sell 填了 first=buy, second=sell。邻档挂在「maker+对面 taker+对面点差」更便宜的所。
    let buy_leg = pair.leg(q.buy.as_str())?;
    let sell_leg = pair.leg(q.sell.as_str())?;
    first.venue = first_v.as_str().to_string();
    second.venue = second_v.as_str().to_string();
    if first_is_buy {
        first.symbol = buy_leg.raw_symbol.clone();
        first.market_index = buy_leg.market_index;
        first.min_qty = buy_leg.min_qty;
        second.symbol = sell_leg.raw_symbol.clone();
        second.market_index = sell_leg.market_index;
        second.min_qty = sell_leg.min_qty;
    } else {
        first.symbol = sell_leg.raw_symbol.clone();
        first.market_index = sell_leg.market_index;
        first.min_qty = sell_leg.min_qty;
        second.symbol = buy_leg.raw_symbol.clone();
        second.market_index = buy_leg.market_index;
        second.min_qty = buy_leg.min_qty;
    }
    plan.slot = pair.slot_key();
    plan.style = OrderStyle::LimitThenMarket;
    plan.first = first;
    plan.second = second;
    plan.grid_from = k;
    plan.grid_to = q.grid_to;
    plan.base_qty = if q.is_open { base_qty } else { Decimal::ZERO };
    plan.quote_side = Some(q.side);
    plan.rest_quote = true;
    Some(plan)
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
        first,
        second,
        quote_side: None,
        rest_quote: false,
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
        limit_price: None,
    };
    let sell_h = HedgeLeg {
        venue: sell.as_str().to_string(),
        symbol: sell_leg.raw_symbol.clone(),
        market_index: sell_leg.market_index,
        is_buy: false,
        style: OrderStyle::MarketTaker,
        min_qty: sell_leg.min_qty,
        limit_price: None,
    };
    (buy_h, sell_h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{adjacent_quotes, CloseReason, VenueId, VenueMarket};
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
                    volume_24h_usdc: None,
                },
                VenueMarket {
                    venue: VenueId::from("lighter_rh"),
                    raw_symbol: "BTC".into(),
                    pair_id: "BTC-USD-PERP".into(),
                    base: "BTC".into(),
                    market_index: 1,
                    qty_precision: 5,
                    min_qty: dec!(0.0003),
                    volume_24h_usdc: None,
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

    #[test]
    fn adjacent_plan_is_limit_then_market_on_pair_slot() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let p = pair();
        let left = p.legs[0].venue.clone();
        let right = p.legs[1].venue.clone();
        let q = adjacent_quotes(
            0,
            dec!(0),
            dec!(0.02),
            3,
            Decimal::ZERO,
            &left,
            &right,
            dec!(0.001),
            Decimal::ZERO,
        );
        let plus = q.iter().find(|x| x.side == crate::domain::QuoteSide::Plus).unwrap();
        let plan = plan_adjacent(
            &p,
            plus,
            &cfg,
            &left,
            dec!(100.05),
            0,
            dec!(0.001),
            None,
            None,
        )
        .unwrap();
        assert!(plan.rest_quote);
        assert_eq!(plan.style, OrderStyle::LimitThenMarket);
        assert_eq!(plan.slot, p.slot_key());
        assert_eq!(plan.quote_side, Some(crate::domain::QuoteSide::Plus));
        assert_eq!(plan.first.style, OrderStyle::LimitMaker);
        assert_eq!(plan.first.limit_price, Some(dec!(100.05)));
        assert_eq!(plan.second.style, OrderStyle::MarketTaker);
        assert!(plan.second.limit_price.is_none());
        assert!(plan.is_open);
        assert_eq!(plan.grid_from, 0);
        assert_eq!(plan.grid_to, 1);
        assert_eq!(plan.base_qty, dec!(0.001));
    }

    #[test]
    fn adjacent_first_leg_is_wider_spread_venue() {
        let cfg = AppConfig::load_from(std::path::Path::new("config/default.yaml")).unwrap();
        let p = pair();
        let left = p.legs[0].venue.clone();
        let right = p.legs[1].venue.clone();
        let q = adjacent_quotes(
            0,
            dec!(0),
            dec!(0.02),
            3,
            Decimal::ZERO,
            &left,
            &right,
            dec!(0.001),
            Decimal::ZERO,
        );
        let plus = q.iter().find(|x| x.side == crate::domain::QuoteSide::Plus).unwrap();
        // plus: 卖 L 买 R。R 点差更宽 → 第一腿挂 R（买）。
        let plan = plan_adjacent(
            &p,
            plus,
            &cfg,
            &left,
            dec!(100.05),
            0,
            dec!(0.001),
            Some(dec!(0.05)),
            Some(dec!(0.01)),
        )
        .unwrap();
        assert_eq!(plan.first.venue, right.as_str());
        assert!(plan.first.is_buy);
        assert_eq!(plan.second.venue, left.as_str());
        assert!(!plan.second.is_buy);
    }
}
