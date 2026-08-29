pub mod bridge;
pub mod entropy;
pub mod lighter;
pub mod net;
pub mod port;
pub mod sodex;

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::VenueFile;

pub use bridge::{
    bridge_available, bridge_call, bridge_watch, last_lighter_place_rtt, subscribe_order_pushes,
    LighterPlaceRtt, OrderPush,
};
pub use entropy::EntropyAdapter;
pub use lighter::LighterAdapter;
pub use port::{
    AccountSnapshot, Balance, CancelReq, ExchangePort, FillPnl, FundingRate, OrderAck, OrderReq,
    OrderStatus, VenuePosition,
};
pub use sodex::SodexAdapter;

pub fn make_adapter(venue: VenueFile, whitelist: Vec<String>) -> Arc<dyn ExchangePort> {
    match venue.id.as_str() {
        "sodex" => Arc::new(SodexAdapter::new(venue, whitelist)),
        "entropy" => Arc::new(EntropyAdapter::new(venue, whitelist)),
        _ => Arc::new(LighterAdapter::new(venue, whitelist)),
    }
}

pub fn venue_yaml_path(venue_id: &str) -> PathBuf {
    let name = if venue_id == "lighter_rh" {
        "lighter_robinhood.yaml".to_string()
    } else {
        format!("{venue_id}.yaml")
    };
    PathBuf::from("config/venues").join(name)
}

/// 人读显示名（UI 用）。
pub fn venue_display_label(id: &str) -> String {
    match id {
        "lighter" => "Lighter 主网".to_string(),
        "lighter_rh" => "Lighter RH".to_string(),
        "sodex" => "SoDEX".to_string(),
        "entropy" => "EntropyIO".to_string(),
        other => {
            let mut s = other.to_string();
            s[..1].make_ascii_uppercase();
            s
        }
    }
}
