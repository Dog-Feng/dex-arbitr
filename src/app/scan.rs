//! 扫描环：24h 量门 → 盘口粗筛 → 候选窗满后按 σ−Δ 打分。一行一个 base。

use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;

use crate::app::window_spread::{mid_spread_pct, own_spread_mid_pct};
use crate::domain::{grid_step_from_target_bp, is_cross_dex, Bbo, Pair, VenueMarket};

const MIN_WINDOW_SAMPLES: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPhase {
    Idle,
    Starting,
    Coarse,
    Sampling,
    Live,
    Error,
}

impl ScanPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Coarse => "coarse",
            Self::Sampling => "sampling",
            Self::Live => "live",
            Self::Error => "error",
        }
    }
}

impl Default for ScanPhase {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Debug, Clone)]
pub struct ScanVenueCell {
    pub mid_mean: Option<Decimal>,
    pub own_spread_mean: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct ScoredPair {
    pub pair: Pair,
    pub eligible: bool,
    pub edge: Decimal,
    pub sigma: Decimal,
    pub delta: Decimal,
    pub mu: Decimal,
    pub hub_c: Decimal,
    pub crosses: u32,
    pub n: usize,
    pub cap: usize,
}

#[derive(Debug, Clone)]
pub struct ScanRow {
    pub rank: usize,
    pub base: String,
    pub pair_id: String,
    pub left: String,
    pub right: String,
    pub same_family: bool,
    pub eligible: bool,
    pub edge: Decimal,
    pub sigma: Decimal,
    pub delta: Decimal,
    pub mu: Decimal,
    pub hub_c: Decimal,
    pub crosses: u32,
    pub n: usize,
    pub cap: usize,
    pub venues: HashMap<String, ScanVenueCell>,
}

pub const COARSE_PROBE_BATCH: usize = 20;
pub const COARSE_PROBE_WAIT_SECS: u64 = 3;

#[derive(Debug, Clone)]
pub struct CoarseCfg {
    pub min_volume: Decimal,
    pub freshness_ms: u64,
    /// WS 盘口要查 age；REST 本轮拉成功即可，不要按 WS 新鲜度卡掉慢接口。
    pub require_fresh: bool,
    /// 已不再作硬门；yaml 仍保留，点差之和只用于候选排序。
    #[allow(dead_code)]
    pub max_own_spread_pct: Decimal,
    #[allow(dead_code)]
    pub min_level_notional_usdc: Decimal,
}

#[derive(Debug, Clone)]
struct SlotSample {
    s: Decimal,
    c_l: Decimal,
    c_r: Decimal,
}

struct SlotWin {
    buf: VecDeque<SlotSample>,
    current_bucket: Option<u64>,
}

#[derive(Default)]
struct VenueCoinWin {
    mids: VecDeque<Decimal>,
    spreads: VecDeque<Decimal>,
    current_bucket: Option<u64>,
}

/// 扫描专用窗：容量是 `scan.window_samples`，和格子窗分开。
pub struct ScanEngine {
    slots: HashMap<String, SlotWin>,
    venue_coins: HashMap<String, VenueCoinWin>,
    cap: usize,
    interval_ms: u64,
}

impl Default for ScanEngine {
    fn default() -> Self {
        Self::new(60, 1000)
    }
}

impl ScanEngine {
    pub fn new(cap: usize, interval_ms: u64) -> Self {
        Self {
            slots: HashMap::new(),
            venue_coins: HashMap::new(),
            cap: cap.max(MIN_WINDOW_SAMPLES),
            interval_ms: interval_ms.max(1),
        }
    }

    pub fn configure(&mut self, cap: usize, interval_ms: u64) {
        self.cap = cap.max(MIN_WINDOW_SAMPLES);
        self.interval_ms = interval_ms.max(1);
        for st in self.slots.values_mut() {
            while st.buf.len() > self.cap {
                st.buf.pop_front();
            }
        }
        for st in self.venue_coins.values_mut() {
            while st.mids.len() > self.cap {
                st.mids.pop_front();
                st.spreads.pop_front();
            }
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.venue_coins.clear();
    }

    pub fn drop_except(&mut self, keep_slots: &HashSet<String>, keep_venue_coins: &HashSet<String>) {
        self.slots.retain(|k, _| keep_slots.contains(k));
        self.venue_coins.retain(|k, _| keep_venue_coins.contains(k));
    }

    pub fn is_filled(&self, pair: &Pair) -> bool {
        self.slots
            .get(&pair.slot_key())
            .is_some_and(|s| s.buf.len() >= self.cap)
    }

    pub fn filled_n(&self, pairs: &[Pair]) -> usize {
        pairs.iter().filter(|p| self.is_filled(p)).count()
    }

    pub fn sampling_n(&self, pairs: &[Pair]) -> usize {
        pairs
            .iter()
            .filter(|p| {
                self.slots
                    .get(&p.slot_key())
                    .is_some_and(|s| !s.buf.is_empty() && s.buf.len() < self.cap)
            })
            .count()
    }

    /// 候选里采得最多的窗长，给页面按样本数倒数。
    pub fn max_n(&self, pairs: &[Pair]) -> usize {
        pairs
            .iter()
            .map(|p| {
                self.slots
                    .get(&p.slot_key())
                    .map(|s| s.buf.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0)
    }

    fn bucket(&self, now_ms: u64) -> u64 {
        now_ms / self.interval_ms
    }

    /// 合法盘口才入窗。缺一秒不拿上一秒凑。
    pub fn observe(&mut self, pair: &Pair, left: &Bbo, right: &Bbo, now_ms: u64) {
        let Some(s) = mid_spread_pct(left, right) else {
            return;
        };
        let Some(mid_l) = mid_of(left) else {
            return;
        };
        let Some(mid_r) = mid_of(right) else {
            return;
        };
        let Some(c_l) = own_spread_mid_pct(left) else {
            return;
        };
        let Some(c_r) = own_spread_mid_pct(right) else {
            return;
        };
        let sample = SlotSample { s, c_l, c_r };
        let bucket = self.bucket(now_ms);
        let cap = self.cap;
        let slot = pair.slot_key();
        let st = self.slots.entry(slot).or_insert(SlotWin {
            buf: VecDeque::new(),
            current_bucket: None,
        });
        if st.current_bucket == Some(bucket) {
            if let Some(last) = st.buf.back_mut() {
                *last = sample;
            }
        } else {
            if st.buf.len() == cap {
                st.buf.pop_front();
            }
            st.buf.push_back(sample);
            st.current_bucket = Some(bucket);
        }
        observe_venue_coin(
            &mut self.venue_coins,
            pair.legs[0].venue.as_str(),
            &pair.pair_id,
            mid_l,
            c_l,
            bucket,
            cap,
        );
        observe_venue_coin(
            &mut self.venue_coins,
            pair.legs[1].venue.as_str(),
            &pair.pair_id,
            mid_r,
            c_r,
            bucket,
            cap,
        );
    }

    pub fn score(
        &self,
        pair: &Pair,
        target_bp: Decimal,
        round_trip_taker: Decimal,
        hysteresis: Decimal,
    ) -> Option<ScoredPair> {
        let st = self.slots.get(&pair.slot_key())?;
        if st.buf.len() < self.cap {
            return None;
        }
        let s_vals: Vec<Decimal> = st.buf.iter().map(|x| x.s).collect();
        let mu = mean(&s_vals)?;
        let sigma = sample_std(&s_vals)?;
        let c_l = mean(&st.buf.iter().map(|x| x.c_l).collect::<Vec<_>>())?;
        let c_r = mean(&st.buf.iter().map(|x| x.c_r).collect::<Vec<_>>())?;
        let hub_c = (c_l + c_r) / Decimal::from(2);
        let delta = grid_step_from_target_bp(target_bp, round_trip_taker, hub_c, hysteresis);
        let edge = sigma - delta;
        let crosses = st
            .buf
            .iter()
            .filter(|x| (x.s - mu).abs() >= delta)
            .count() as u32;
        Some(ScoredPair {
            pair: pair.clone(),
            eligible: sigma > delta,
            edge,
            sigma,
            delta,
            mu,
            hub_c,
            crosses,
            n: st.buf.len(),
            cap: self.cap,
        })
    }

    pub fn venue_cell(&self, venue: &str, pair_id: &str) -> ScanVenueCell {
        let key = venue_coin_key(venue, pair_id);
        match self.venue_coins.get(&key) {
            Some(st) if st.mids.len() >= self.cap => ScanVenueCell {
                mid_mean: mean(&st.mids.iter().copied().collect::<Vec<_>>()),
                own_spread_mean: mean(&st.spreads.iter().copied().collect::<Vec<_>>()),
            },
            _ => ScanVenueCell {
                mid_mean: None,
                own_spread_mean: None,
            },
        }
    }

    /// 只采某一所某一币的 mid / 本所点差，不要求它是候选所对的腿。
    pub fn observe_venue(&mut self, venue: &str, pair_id: &str, bbo: &Bbo, now_ms: u64) {
        if !bbo.valid() {
            return;
        }
        let Some(mid) = mid_of(bbo) else {
            return;
        };
        let Some(spread) = own_spread_mid_pct(bbo) else {
            return;
        };
        let bucket = self.bucket(now_ms);
        let cap = self.cap;
        observe_venue_coin(
            &mut self.venue_coins,
            venue,
            pair_id,
            mid,
            spread,
            bucket,
            cap,
        );
    }
}

fn venue_coin_key(venue: &str, pair_id: &str) -> String {
    format!("{venue}|{pair_id}")
}

fn observe_venue_coin(
    map: &mut HashMap<String, VenueCoinWin>,
    venue: &str,
    pair_id: &str,
    mid: Decimal,
    spread: Decimal,
    bucket: u64,
    cap: usize,
) {
    let key = venue_coin_key(venue, pair_id);
    let st = map.entry(key).or_insert(VenueCoinWin {
        mids: VecDeque::new(),
        spreads: VecDeque::new(),
        current_bucket: None,
    });
    if st.current_bucket == Some(bucket) {
        if let Some(last) = st.mids.back_mut() {
            *last = mid;
        }
        if let Some(last) = st.spreads.back_mut() {
            *last = spread;
        }
        return;
    }
    if st.mids.len() == cap {
        st.mids.pop_front();
        st.spreads.pop_front();
    }
    st.mids.push_back(mid);
    st.spreads.push_back(spread);
    st.current_bucket = Some(bucket);
}

fn mid_of(b: &Bbo) -> Option<Decimal> {
    let m = (b.bid + b.ask) / Decimal::from(2);
    (m > Decimal::ZERO).then_some(m)
}

fn mean(xs: &[Decimal]) -> Option<Decimal> {
    if xs.is_empty() {
        return None;
    }
    Some(xs.iter().copied().sum::<Decimal>() / Decimal::from(xs.len() as u64))
}

fn sample_std(xs: &[Decimal]) -> Option<Decimal> {
    let n = xs.len();
    if n < 2 {
        return None;
    }
    let mean = mean(xs)?;
    let mean_f: f64 = mean.to_string().parse().ok()?;
    let mut acc = 0.0_f64;
    for x in xs {
        let v: f64 = x.to_string().parse().ok()?;
        let d = v - mean_f;
        acc += d * d;
    }
    let std = (acc / (n - 1) as f64).sqrt();
    Decimal::from_str(&format!("{std:.10}")).ok()
}

/// 所级：一个字段都解析不到 → 整所本轮剔除。否则丢掉量 ≤ 门槛或缺字段的市场。
pub fn filter_scan_markets(
    listed: Vec<(String, Vec<VenueMarket>)>,
    min_volume: Decimal,
) -> (Vec<Vec<VenueMarket>>, Vec<String>) {
    let mut kept = Vec::new();
    let mut dropped_venues = Vec::new();
    for (id, markets) in listed {
        if !markets.iter().any(|m| m.volume_24h_usdc.is_some()) {
            tracing::warn!(
                venue = %id,
                markets = markets.len(),
                dropped = markets.len(),
                "volume_24h missing, venue excluded from scan match"
            );
            dropped_venues.push(id);
            continue;
        }
        let total = markets.len();
        let missing_field = markets.iter().filter(|m| m.volume_24h_usdc.is_none()).count();
        let below_volume = markets
            .iter()
            .filter(|m| m.volume_24h_usdc.is_some_and(|v| v <= min_volume))
            .count();
        let ok: Vec<VenueMarket> = markets
            .into_iter()
            .filter(|m| m.volume_24h_usdc.is_some_and(|v| v > min_volume))
            .collect();
        tracing::info!(
            venue = %id,
            kept = ok.len(),
            dropped = total.saturating_sub(ok.len()),
            missing_field,
            below_volume,
            "scan volume gate"
        );
        if ok.is_empty() {
            dropped_venues.push(id);
            continue;
        }
        kept.push(ok);
    }
    (kept, dropped_venues)
}

/// 候选所对只长订两腿时，表上其它 DEX 列永远采不到窗。
/// 对候选里出现过的 `pair_id`，把宇宙里同币的其它所对也订上，用来填各所价格/点差均值。
pub fn expand_scan_subscribe(candidates: &[Pair], universe: &[Pair]) -> Vec<Pair> {
    let ids: HashSet<String> = candidates.iter().map(|p| p.pair_id.clone()).collect();
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for p in candidates.iter().chain(universe.iter()) {
        if !ids.contains(&p.pair_id) {
            continue;
        }
        if seen.insert(p.slot_key()) {
            out.push(p.clone());
        }
    }
    out
}

pub fn candidate_cap(watch_top: usize, configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    (watch_top.saturating_mul(4)).max(40)
}

/// 粗筛通过则返回两侧点差之和（越小越优先进入候选 cap）。
pub fn coarse_spread_sum(pair: &Pair, books: &HashMap<(String, String), Bbo>, cfg: &CoarseCfg) -> Option<Decimal> {
    let v0 = pair.legs[0].venue.as_str();
    let v1 = pair.legs[1].venue.as_str();
    if !volume_ok(&pair.legs[0], cfg.min_volume) || !volume_ok(&pair.legs[1], cfg.min_volume) {
        return None;
    }
    let b0 = books.get(&(v0.to_string(), pair.pair_id.clone()))?;
    let b1 = books.get(&(v1.to_string(), pair.pair_id.clone()))?;
    if cfg.require_fresh && (!b0.is_fresh(cfg.freshness_ms) || !b1.is_fresh(cfg.freshness_ms)) {
        return None;
    }
    if !b0.valid() || !b1.valid() {
        return None;
    }
    let c0 = own_spread_mid_pct(b0)?;
    let c1 = own_spread_mid_pct(b1)?;
    Some(c0 + c1)
}

fn volume_ok(m: &VenueMarket, min: Decimal) -> bool {
    m.volume_24h_usdc.is_some_and(|v| v > min)
}

pub fn pair_volume_ok(pair: &Pair, min: Decimal) -> bool {
    volume_ok(&pair.legs[0], min) && volume_ok(&pair.legs[1], min)
}

pub fn pair_has_books(pair: &Pair, books: &HashMap<(String, String), Bbo>) -> bool {
    let v0 = pair.legs[0].venue.as_str();
    let v1 = pair.legs[1].venue.as_str();
    books.contains_key(&(v0.to_string(), pair.pair_id.clone()))
        && books.contains_key(&(v1.to_string(), pair.pair_id.clone()))
}

/// 粗筛刷新：已在 Top N 且仍满窗合格的本周期保留，再按新粗筛结果补到 cap。
pub fn merge_coarse_refresh(
    previous: &[Pair],
    next: Vec<Pair>,
    retain_keys: &HashSet<String>,
    cap: usize,
) -> Vec<Pair> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for p in previous {
        if !retain_keys.contains(&p.slot_key()) {
            continue;
        }
        if cap > 0 && out.len() >= cap {
            break;
        }
        seen.insert(p.slot_key());
        out.push(p.clone());
    }
    for p in next {
        if seen.contains(&p.slot_key()) {
            continue;
        }
        if cap > 0 && out.len() >= cap {
            break;
        }
        seen.insert(p.slot_key());
        out.push(p);
    }
    out
}

pub fn select_candidates(
    universe: &[Pair],
    books: &HashMap<(String, String), Bbo>,
    cfg: &CoarseCfg,
    cap: usize,
) -> Vec<Pair> {
    let mut scored: Vec<(Decimal, Pair)> = universe
        .iter()
        .filter_map(|p| Some((coarse_spread_sum(p, books, cfg)?, p.clone())))
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.slot_key().cmp(&b.1.slot_key())));
    scored.truncate(cap);
    scored.into_iter().map(|(_, p)| p).collect()
}

/// 满窗所对 → 每个 base 留 edge 最大的两个所 → 按 eligible / edge 截 watch_top。
pub fn rank_bases(
    scored: Vec<ScoredPair>,
    engine: &ScanEngine,
    scan_venues: &[String],
    watch_top: usize,
) -> Vec<ScanRow> {
    let mut by_base: HashMap<String, Vec<ScoredPair>> = HashMap::new();
    for row in scored {
        by_base
            .entry(row.pair.legs[0].base.clone())
            .or_default()
            .push(row);
    }
    let mut best: Vec<ScoredPair> = Vec::new();
    for mut rows in by_base.into_values() {
        rows.sort_by(|a, b| {
            b.edge
                .cmp(&a.edge)
                .then_with(|| {
                    let fa = !is_cross_dex(a.pair.legs[0].venue.as_str(), a.pair.legs[1].venue.as_str());
                    let fb = !is_cross_dex(b.pair.legs[0].venue.as_str(), b.pair.legs[1].venue.as_str());
                    fb.cmp(&fa)
                })
                .then_with(|| a.hub_c.cmp(&b.hub_c))
                .then_with(|| a.pair.legs[0].base.cmp(&b.pair.legs[0].base))
        });
        if let Some(top) = rows.into_iter().next() {
            best.push(top);
        }
    }
    best.sort_by(|a, b| {
        b.eligible
            .cmp(&a.eligible)
            .then_with(|| b.edge.cmp(&a.edge))
            .then_with(|| a.pair.legs[0].base.cmp(&b.pair.legs[0].base))
    });
    if watch_top > 0 && best.len() > watch_top {
        best.truncate(watch_top);
    }
    best.into_iter()
        .enumerate()
        .map(|(i, s)| {
            let left = s.pair.legs[0].venue.as_str().to_string();
            let right = s.pair.legs[1].venue.as_str().to_string();
            let mut venues = HashMap::new();
            for v in scan_venues {
                venues.insert(v.clone(), engine.venue_cell(v, &s.pair.pair_id));
            }
            ScanRow {
                rank: i + 1,
                base: s.pair.legs[0].base.clone(),
                pair_id: s.pair.pair_id.clone(),
                same_family: !is_cross_dex(&left, &right),
                left,
                right,
                eligible: s.eligible,
                edge: s.edge,
                sigma: s.sigma,
                delta: s.delta,
                mu: s.mu,
                hub_c: s.hub_c,
                crosses: s.crosses,
                n: s.n,
                cap: s.cap,
                venues,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{VenueId, VenueMarket};
    use rust_decimal_macros::dec;
    use std::collections::{HashMap, HashSet};
    use std::time::Instant;

    fn mkt(venue: &str, pair_id: &str, vol: Option<Decimal>) -> VenueMarket {
        VenueMarket {
            venue: VenueId::from(venue),
            raw_symbol: pair_id.to_string(),
            pair_id: pair_id.to_string(),
            base: "SOL".into(),
            market_index: 1,
            qty_precision: 4,
            min_qty: dec!(0.01),
            volume_24h_usdc: vol,
        }
    }

    fn book(bid: Decimal, ask: Decimal, qty: Decimal) -> Bbo {
        Bbo {
            bid,
            ask,
            bid_qty: qty,
            ask_qty: qty,
            bids: vec![(bid, qty)],
            asks: vec![(ask, qty)],
            ts: Instant::now(),
        }
    }

    #[test]
    fn drops_venue_without_volume_field() {
        let listed = vec![
            (
                "lighter".into(),
                vec![mkt("lighter", "SOL-USD-PERP", None)],
            ),
            (
                "sodex".into(),
                vec![mkt("sodex", "SOL-USD-PERP", Some(dec!(20_000_000)))],
            ),
        ];
        let (kept, dropped) = filter_scan_markets(listed, dec!(10_000_000));
        assert_eq!(dropped, vec!["lighter".to_string()]);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn volume_must_be_strictly_above_ten_million() {
        let listed = vec![(
            "entropy".into(),
            vec![
                mkt("entropy", "A-USD-PERP", Some(dec!(10_000_000))),
                mkt("entropy", "B-USD-PERP", Some(dec!(10_000_001))),
            ],
        )];
        let (kept, _) = filter_scan_markets(listed, dec!(10_000_000));
        assert_eq!(kept[0].len(), 1);
        assert_eq!(kept[0][0].pair_id, "B-USD-PERP");
    }

    #[test]
    fn coarse_keeps_wide_spread() {
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let mut books = HashMap::new();
        books.insert(
            ("lighter".into(), "SOL-USD-PERP".into()),
            book(dec!(100), dec!(100.01), dec!(10)),
        );
        books.insert(
            ("sodex".into(), "SOL-USD-PERP".into()),
            book(dec!(100), dec!(101), dec!(10)),
        );
        let cfg = CoarseCfg {
            min_volume: dec!(10_000_000),
            max_own_spread_pct: dec!(0.15),
            min_level_notional_usdc: dec!(200),
            freshness_ms: 3000,
            require_fresh: true,
        };
        assert!(coarse_spread_sum(&pair, &books, &cfg).is_some());
    }

    #[test]
    fn rest_snapshot_skips_ws_freshness() {
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let mut stale = book(dec!(100), dec!(100.01), dec!(10));
        stale.ts = Instant::now() - std::time::Duration::from_secs(30);
        let mut books = HashMap::new();
        books.insert(("lighter".into(), "SOL-USD-PERP".into()), stale.clone());
        books.insert(("sodex".into(), "SOL-USD-PERP".into()), stale);
        let mut cfg = CoarseCfg {
            min_volume: dec!(10_000_000),
            max_own_spread_pct: dec!(0.15),
            min_level_notional_usdc: dec!(200),
            freshness_ms: 3000,
            require_fresh: false,
        };
        assert!(coarse_spread_sum(&pair, &books, &cfg).is_some());
        cfg.require_fresh = true;
        assert!(coarse_spread_sum(&pair, &books, &cfg).is_none());
    }

    #[test]
    fn coarse_refresh_keeps_topn_keys() {
        let keep = Pair {
            pair_id: "AAA-USD-PERP".into(),
            legs: [
                mkt("lighter", "AAA-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "AAA-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let drop = Pair {
            pair_id: "BBB-USD-PERP".into(),
            legs: [
                mkt("lighter", "BBB-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "BBB-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let fresh = Pair {
            pair_id: "CCC-USD-PERP".into(),
            legs: [
                mkt("lighter", "CCC-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "CCC-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let mut retain = HashSet::new();
        retain.insert(keep.slot_key());
        let out = merge_coarse_refresh(
            &[keep.clone(), drop],
            vec![fresh.clone()],
            &retain,
            40,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].pair_id, "AAA-USD-PERP");
        assert_eq!(out[1].pair_id, "CCC-USD-PERP");
    }

    #[test]
    fn ranks_one_row_per_base_by_edge() {
        let mut engine = ScanEngine::new(10, 1000);
        let a = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("lighter_rh", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        for i in 0..10 {
            let drift = Decimal::from(i) / Decimal::from(1000);
            let left = book(dec!(100) + drift, dec!(100.01) + drift, dec!(5));
            let right = book(dec!(100.20), dec!(100.21), dec!(5));
            engine.observe(&a, &left, &right, 1_000 * (i as u64 + 1));
        }
        let scored = engine
            .score(&a, dec!(1), dec!(0.02), Decimal::ZERO)
            .unwrap();
        assert!(scored.n >= 10);
        let rows = rank_bases(
            vec![scored],
            &engine,
            &["lighter".into(), "lighter_rh".into()],
            20,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].base, "SOL");
        assert!(rows[0].same_family);
    }

    #[test]
    fn unfilled_window_does_not_score() {
        let mut engine = ScanEngine::new(10, 1000);
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let left = book(dec!(100), dec!(100.01), dec!(5));
        let right = book(dec!(100.2), dec!(100.21), dec!(5));
        engine.observe(&pair, &left, &right, 1000);
        assert!(engine.score(&pair, dec!(1), dec!(0.02), Decimal::ZERO).is_none());
        assert_eq!(engine.max_n(&[pair]), 1);
    }

    #[test]
    fn expand_subscribe_adds_same_coin_other_venues() {
        let cand = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("lighter_rh", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let extra = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let other = Pair {
            pair_id: "BTC-USD-PERP".into(),
            legs: [
                mkt("lighter", "BTC-USD-PERP", Some(dec!(20_000_000))),
                mkt("sodex", "BTC-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let out = expand_scan_subscribe(&[cand.clone()], &[cand.clone(), extra.clone(), other]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().any(|p| p.slot_key() == extra.slot_key()));
    }

    #[test]
    fn venue_column_fills_without_being_candidate_leg() {
        let mut engine = ScanEngine::new(10, 1000);
        let pair = Pair {
            pair_id: "SOL-USD-PERP".into(),
            legs: [
                mkt("lighter", "SOL-USD-PERP", Some(dec!(20_000_000))),
                mkt("lighter_rh", "SOL-USD-PERP", Some(dec!(20_000_000))),
            ],
        };
        let sodex = book(dec!(99.5), dec!(99.6), dec!(8));
        for i in 0..10 {
            let left = book(dec!(100), dec!(100.01), dec!(5));
            let right = book(dec!(100.20), dec!(100.21), dec!(5));
            engine.observe(&pair, &left, &right, 1_000 * (i as u64 + 1));
            engine.observe_venue("sodex", "SOL-USD-PERP", &sodex, 1_000 * (i as u64 + 1));
        }
        let scored = engine
            .score(&pair, dec!(1), dec!(0.02), Decimal::ZERO)
            .unwrap();
        let rows = rank_bases(
            vec![scored],
            &engine,
            &[
                "lighter".into(),
                "lighter_rh".into(),
                "sodex".into(),
                "entropy".into(),
            ],
            20,
        );
        assert!(rows[0].venues["sodex"].mid_mean.is_some());
        assert!(rows[0].venues["sodex"].own_spread_mean.is_some());
        assert!(rows[0].venues["entropy"].mid_mean.is_none());
    }
}
