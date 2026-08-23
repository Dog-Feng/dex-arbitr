use rust_decimal::Decimal;

use crate::config::{AppConfig, OrderStyle};
use crate::domain::{Intent, Pair, Position, VenueId};

use super::sequence::first_limit_venue;

#[derive(Debug, Clone)]
pub struct HedgeLeg {
    pub venue: String,
    pub symbol: String,
    pub market_index: i32,
    pub is_buy: bool,
    pub style: OrderStyle,
}

#[derive(Debug, Clone)]
pub struct HedgePlan {
    pub pair_id: String,
    pub qty: Decimal,
    pub is_open: bool,
    pub style: OrderStyle,
    pub buy_market_index: i32,
    pub sell_market_index: i32,
    pub buy_symbol: String,
    pub sell_symbol: String,
    pub buy_venue: String,
    pub sell_venue: String,
    pub first: HedgeLeg,
    pub second: HedgeLeg,
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
        } => {
            let buy_leg = pair.legs.iter().find(|l| &l.venue == buy)?;
            let sell_leg = pair.legs.iter().find(|l| &l.venue == sell)?;
            let (first, second) = sequenced_legs(cfg, buy, sell, buy_leg, sell_leg);
            Some(HedgePlan {
                pair_id: pair.pair_id.clone(),
                qty: *qty,
                is_open: true,
                style: cfg.order.style,
                buy_market_index: buy_leg.market_index,
                sell_market_index: sell_leg.market_index,
                buy_symbol: buy_leg.raw_symbol.clone(),
                sell_symbol: sell_leg.raw_symbol.clone(),
                buy_venue: buy.as_str().to_string(),
                sell_venue: sell.as_str().to_string(),
                first,
                second,
            })
        }
        Intent::Close { qty, .. } => {
            let pos = pos?;
            let buy_leg = pair.legs.iter().find(|l| l.venue == pos.sell)?;
            let sell_leg = pair.legs.iter().find(|l| l.venue == pos.buy)?;
            let buy = pos.sell.clone();
            let sell = pos.buy.clone();
            let (first, second) = sequenced_legs(cfg, &buy, &sell, buy_leg, sell_leg);
            Some(HedgePlan {
                pair_id: pair.pair_id.clone(),
                qty: *qty,
                is_open: false,
                style: cfg.order.style,
                buy_market_index: buy_leg.market_index,
                sell_market_index: sell_leg.market_index,
                buy_symbol: buy_leg.raw_symbol.clone(),
                sell_symbol: sell_leg.raw_symbol.clone(),
                buy_venue: buy.as_str().to_string(),
                sell_venue: sell.as_str().to_string(),
                first,
                second,
            })
        }
    }
}

fn sequenced_legs(
    cfg: &AppConfig,
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
    };
    let sell_h = HedgeLeg {
        venue: sell.as_str().to_string(),
        symbol: sell_leg.raw_symbol.clone(),
        market_index: sell_leg.market_index,
        is_buy: false,
        style: OrderStyle::MarketTaker,
    };
    if !matches!(cfg.order.style, OrderStyle::LimitThenMarket) {
        return (buy_h, sell_h);
    }
    match first_limit_venue(cfg, buy, sell) {
        Some((first_v, _)) if first_v == buy => {
            let mut first = buy_h;
            first.style = OrderStyle::LimitMaker;
            (first, sell_h)
        }
        Some(_) => {
            let mut first = sell_h;
            first.style = OrderStyle::LimitMaker;
            (first, buy_h)
        }
        None => (buy_h, sell_h),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{VenueId, VenueMarket};
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
                    min_qty: dec!(0.0002),
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
        };
        let plan = plan_hedge(
            &pair(),
            &Intent::Close {
                qty: dec!(0.001),
                grid: 0,
            },
            Some(&pos),
            &cfg,
        )
        .unwrap();
        assert!(!plan.is_open);
        assert_eq!(plan.buy_venue, "lighter_rh");
        assert_eq!(plan.sell_venue, "lighter");
        assert_eq!(plan.first.venue, "lighter_rh");
        assert_eq!(plan.first.style, OrderStyle::LimitMaker);
        assert_eq!(plan.second.venue, "lighter");
        assert_eq!(plan.second.style, OrderStyle::MarketTaker);
    }

    #[test]
    fn open_posts_higher_taker_first() {
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
        assert_eq!(plan.first.venue, "lighter_rh");
        assert!(!plan.first.is_buy);
        assert_eq!(plan.first.style, OrderStyle::LimitMaker);
        assert_eq!(plan.second.venue, "lighter");
        assert!(plan.second.is_buy);
        assert_eq!(plan.second.style, OrderStyle::MarketTaker);
    }
}
