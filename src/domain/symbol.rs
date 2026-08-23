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

const STABLE_QUOTES: &[&str] = &["USDC", "USDG", "USDT", "USD", "VUSDC", "VUSDG", "VUSDT", "VUSD"];
const STABLE_CORE: &[&str] = &["USDC", "USDG", "USDT", "USD"];

/// Normalize a venue symbol into `{BASE}-USD-PERP`. Spot markets return None.
pub fn normalize_perp(raw: &str, market_type: &str) -> Option<(String, String)> {
    let kind = market_type.trim().to_ascii_lowercase();
    if kind == "spot" {
        return None;
    }
    let compact = raw
        .trim()
        .to_ascii_uppercase()
        .replace('/', "-")
        .replace('_', "-");
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
            return Some(canonical_base(base, quote));
        }
    }

    for quote in STABLE_QUOTES {
        if let Some(base) = s.strip_suffix(quote) {
            if !base.is_empty() && base.chars().all(|c| c.is_ascii_alphanumeric()) {
                return Some(canonical_base(base, quote));
            }
        }
    }

    if s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Some(s.to_string());
    }
    None
}

/// SoDEX 虚拟币：`vBTC` + `vUSDC` → `BTC`。`VVV-USD` 的 base 不剥 V。
fn canonical_base(base: &str, quote: &str) -> String {
    let virtual_quote = quote.starts_with('V')
        && STABLE_CORE.contains(&quote.strip_prefix('V').unwrap_or(quote));
    if virtual_quote && base.starts_with('V') && base.len() > 1 {
        return base[1..].to_string();
    }
    base.to_string()
}

/// Lighter 主网 / RH 同一协议家族；SoDEX 等其它所算跨 DEX，价差里常有天然基差。
pub fn venue_family(id: &str) -> &str {
    match id {
        "lighter" | "lighter_rh" => "lighter",
        other => other,
    }
}

pub fn is_cross_dex(buy: &str, sell: &str) -> bool {
    venue_family(buy) != venue_family(sell)
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

/// N 所两两配对：每个 `pair_id` 在每对所上各一条 `Pair`（C(N,2)）。
pub fn match_all_pairs(venues: &[Vec<VenueMarket>]) -> Vec<Pair> {
    let mut pairs = Vec::new();
    for i in 0..venues.len() {
        for j in (i + 1)..venues.len() {
            pairs.extend(match_pairs(&venues[i], &venues[j]));
        }
    }
    pairs.sort_by(|a, b| {
        a.pair_id
            .cmp(&b.pair_id)
            .then_with(|| a.legs[0].venue.as_str().cmp(b.legs[0].venue.as_str()))
            .then_with(|| a.legs[1].venue.as_str().cmp(b.legs[1].venue.as_str()))
    });
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
        let d = normalize_perp("BTC-USD", "perp").unwrap();
        let e = normalize_perp("vBTC_vUSDC", "perp").unwrap();
        assert_eq!(a.1, "BTC-USD-PERP");
        assert_eq!(b.1, "BTC-USD-PERP");
        assert_eq!(c.1, "BTC-USD-PERP");
        assert_eq!(d.1, "BTC-USD-PERP");
        assert_eq!(e, ("BTC".into(), "BTC-USD-PERP".into()));
        assert_eq!(normalize_perp("VVV-USD", "perp").unwrap().0, "VVV");
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

    #[test]
    fn lighter_family_is_not_cross_dex() {
        assert!(!is_cross_dex("lighter", "lighter_rh"));
        assert!(is_cross_dex("lighter", "sodex"));
        assert!(is_cross_dex("lighter_rh", "sodex"));
    }

    #[test]
    fn three_venues_make_three_pairs() {
        let lighter = vec![mkt("lighter", "BTC-USDC", "perp", 1).unwrap()];
        let rh = vec![mkt("lighter_rh", "BTCUSDG", "perp", 7).unwrap()];
        let sodex = vec![mkt("sodex", "BTC-USD", "perp", 24).unwrap()];
        let pairs = match_all_pairs(&[lighter, rh, sodex]);
        assert_eq!(pairs.len(), 3);
        assert!(pairs.iter().all(|p| p.pair_id == "BTC-USD-PERP"));
    }
}
