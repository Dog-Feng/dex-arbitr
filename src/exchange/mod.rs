pub mod lighter;
pub mod port;
pub mod sodex;

use std::sync::Arc;

use crate::config::VenueFile;

pub use lighter::LighterAdapter;
pub use port::{Balance, ExchangePort, OrderAck, OrderReq, VenuePosition};
pub use sodex::SodexAdapter;

pub fn make_adapter(venue: VenueFile, whitelist: Vec<String>) -> Arc<dyn ExchangePort> {
    match venue.id.as_str() {
        "sodex" => Arc::new(SodexAdapter::new(venue, whitelist)),
        _ => Arc::new(LighterAdapter::new(venue, whitelist)),
    }
}
