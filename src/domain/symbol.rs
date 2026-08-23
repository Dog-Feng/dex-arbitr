use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VenueId(pub String);

impl VenueId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for VenueId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketKind {
    Perp,
    Spot,
}

#[derive(Debug, Clone)]
pub struct VenueMarket {
    pub venue: VenueId,
    pub raw_symbol: String,
    pub pair_id: String,
    pub base: String,
    /// Lighter `create_order` 的 market_index（口头常叫 maker_index），启动时按交易对拉取。
    pub market_index: i32,
    pub qty_precision: u32,
    pub min_qty: Decimal,
}

#[derive(Debug, Clone)]
pub struct Pair {
    pub pair_id: String,
    pub legs: [VenueMarket; 2],
}

const STABLE_QUOTES: &[&str] = &["USDC", "USDG", "USDT", "USD"];

/// Normalize a venue symbol into `{BASE}-USD-PERP`. Spot markets return None.
pub fn normalize_perp(raw: &str, market_type: &str) -> Option<(String, String)> {
    let kind = market_type.trim().to_ascii_lowercase();
    if kind == "spot" {
        return None;
    }
    let compact = raw.trim().to_ascii_uppercase().replace('/', "-");
    if compact.is_empty() {
        return None;
    }

    let base = extract_base(&compact)?;
    let pair_id = format!("{base}-USD-PERP");
    Some((base, pair_id))
}

fn extract_base(symbol: &str) -> Option<String> {
    let s = symbol
        .trim_end_matches("-PERP")
        .trim_end_matches("PERP")
        .trim_end_matches("-USD")
        .trim_matches('-');

    if let Some((base, quote)) = s.split_once('-') {
        if STABLE_QUOTES.contains(&quote) {
            return Some(base.to_string());
        }
        if quote == "USD" {
            return Some(base.to_string());
        }
    }

    for quote in STABLE_QUOTES {
        if let Some(base) = s.strip_suffix(quote) {
            if !base.is_empty() && base.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Some(base.to_string());
            }
        }
    }

    if s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Some(s.to_string());
    }
    None
}

pub fn whitelist_allows(base: &str, whitelist: &[String]) -> bool {
    if whitelist.is_empty() {
        return true;
    }
    whitelist.iter().any(|item| item.eq_ignore_ascii_case(base))
}

/// Pair markets that share the same pair_id on both venues.
pub fn match_pairs(left: &[VenueMarket], right: &[VenueMarket]) -> Vec<Pair> {
    let mut pairs = Vec::new();
    for l in left {
        if let Some(r) = right.iter().find(|m| m.pair_id == l.pair_id) {
            pairs.push(Pair {
                pair_id: l.pair_id.clone(),
                legs: [l.clone(), r.clone()],
            });
        }
    }
    pairs.sort_by(|a, b| a.pair_id.cmp(&b.pair_id));
    pairs
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn mkt(venue: &str, raw: &str, market_type: &str, id: i32) -> Option<VenueMarket> {
        let (base, pair_id) = normalize_perp(raw, market_type)?;
        Some(VenueMarket {
            venue: VenueId::from(venue),
            raw_symbol: raw.to_string(),
            pair_id,
            base,
            market_index: id,
            qty_precision: 4,
            min_qty: dec!(0.001),
        })
    }

    #[test]
    fn usdc_and_usdg_map_to_same_pair() {
        let a = normalize_perp("BTC-USDC", "perp").unwrap();
        let b = normalize_perp("BTCUSDG", "perp").unwrap();
        let c = normalize_perp("BTC", "perp").unwrap();
        assert_eq!(a.1, "BTC-USD-PERP");
        assert_eq!(b.1, "BTC-USD-PERP");
        assert_eq!(c.1, "BTC-USD-PERP");
    }

    #[test]
    fn drops_spot() {
        assert!(normalize_perp("BTC-USDC", "spot").is_none());
    }

    #[test]
    fn match_requires_both_legs() {
        let left = vec![
            mkt("lighter", "BTC", "perp", 1).unwrap(),
            mkt("lighter", "SOL", "perp", 2).unwrap(),
        ];
        let right = vec![mkt("lighter_rh", "BTCUSDG", "perp", 7).unwrap()];
        let pairs = match_pairs(&left, &right);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].pair_id, "BTC-USD-PERP");
    }
}
