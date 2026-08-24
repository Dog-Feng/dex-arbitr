pub mod bridge;
pub mod lighter;
pub mod port;
pub mod sodex;

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::VenueFile;

pub use bridge::{bridge_available, bridge_call};
pub use lighter::LighterAdapter;
pub use port::{
    AccountSnapshot, Balance, CancelReq, ExchangePort, OrderAck, OrderReq, OrderStatus,
    VenuePosition,
};
pub use sodex::SodexAdapter;

pub fn make_adapter(venue: VenueFile, whitelist: Vec<String>) -> Arc<dyn ExchangePort> {
    match venue.id.as_str() {
        "sodex" => Arc::new(SodexAdapter::new(venue, whitelist)),
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
