//! 每 slot 一条秒级（可配间隔）滑动窗口：mid 价差入窗、μ_live、冻/解冻。
//!
//! 同一采样桶内多次合法价差覆盖为最后一次；换桶时窗口里已是该桶最后价。
//! 点数未满 `cap` 时 μ 不存在（禁止开仓）。缺盘口的那一拍不调用 `observe`。

use std::collections::{HashMap, HashSet, VecDeque};

use rust_decimal::Decimal;

use crate::domain::Bbo;

/// \(s=(\mathrm{mid}_L-\mathrm{mid}_R)/((\mathrm{mid}_L+\mathrm{mid}_R)/2)\times 100\)
pub fn mid_spread_pct(left: &Bbo, right: &Bbo) -> Option<Decimal> {
    let mid_l = mid_of(left)?;
    let mid_r = mid_of(right)?;
    let avg = (mid_l + mid_r) / Decimal::from(2);
    if avg <= Decimal::ZERO {
        return None;
    }
    Some((mid_l - mid_r) / avg * Decimal::from(100))
}

/// 可执行价差，分母与 mid \(s\) 相同。
///
/// - `plus`：空 L 多 R → \((\mathrm{bid}_L-\mathrm{ask}_R)/\mathrm{avg}\times 100\)
/// - `minus`：多 L 空 R → \((\mathrm{ask}_L-\mathrm{bid}_R)/\mathrm{avg}\times 100\)
pub fn exec_spread_pct(left: &Bbo, right: &Bbo, plus: bool) -> Option<Decimal> {
    let mid_l = mid_of(left)?;
    let mid_r = mid_of(right)?;
    let avg = (mid_l + mid_r) / Decimal::from(2);
    if avg <= Decimal::ZERO {
        return None;
    }
    let num = if plus {
        left.bid - right.ask
    } else {
        left.ask - right.bid
    };
    Some(num / avg * Decimal::from(100))
}

fn mid_of(b: &Bbo) -> Option<Decimal> {
    let m = (b.bid + b.ask) / Decimal::from(2);
    (m > Decimal::ZERO).then_some(m)
}

/// 本所买卖点差（%）：\((\mathrm{ask}-\mathrm{bid})/\mathrm{mid}\times 100\)。
/// 市价开平都要吃这一档才能拿到买一/卖一。分母用 mid，和价差窗口同一套单位。
pub fn own_spread_mid_pct(b: &Bbo) -> Option<Decimal> {
    let mid = mid_of(b)?;
    Some((b.ask - b.bid) / mid * Decimal::from(100))
}

/// 所对 \(C\)：两所点差中枢的算术平均。
pub fn pair_spread_hub_avg(left: Decimal, right: Decimal) -> Decimal {
    (left + right) / Decimal::from(2)
}

#[derive(Debug, Clone)]
struct SlotWindow {
    buf: VecDeque<Decimal>,
    sum: Decimal,
    current_bucket: Option<u64>,
    frozen: Option<Decimal>,
}

impl SlotWindow {
    fn live_mu(&self, cap: usize) -> Option<Decimal> {
        if cap == 0 || self.buf.len() < cap {
            return None;
        }
        Some(self.sum / Decimal::from(self.buf.len() as u64))
    }

    fn quote_mu(&self, cap: usize) -> Option<Decimal> {
        self.frozen.or_else(|| self.live_mu(cap))
    }

    fn trim(&mut self, cap: usize) {
        while self.buf.len() > cap {
            if let Some(old) = self.buf.pop_front() {
                self.sum -= old;
            }
        }
    }
}

#[derive(Debug)]
pub struct WindowBook {
    slots: HashMap<String, SlotWindow>,
    cap: usize,
    interval_ms: u64,
}

impl WindowBook {
    pub fn new(cap: usize, interval_ms: u64) -> Self {
        Self {
            slots: HashMap::new(),
            cap: cap.max(1),
            interval_ms: interval_ms.max(1),
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn configure(&mut self, cap: usize, interval_ms: u64) {
        let cap = cap.max(1);
        let interval_ms = interval_ms.max(1);
        if self.interval_ms != interval_ms {
            for st in self.slots.values_mut() {
                st.current_bucket = None;
            }
        }
        self.interval_ms = interval_ms;
        self.cap = cap;
        for st in self.slots.values_mut() {
            st.trim(cap);
        }
    }

    fn bucket(&self, now_ms: u64) -> u64 {
        now_ms / self.interval_ms
    }

    /// 合法盘口算出的 mid \(s\) 才入窗。同一桶覆盖；新桶 push，满则丢最老。
    pub fn observe(&mut self, slot: &str, now_ms: u64, s: Decimal) {
        let bucket = self.bucket(now_ms);
        let cap = self.cap;
        let st = self.slots.entry(slot.to_string()).or_insert(SlotWindow {
            buf: VecDeque::new(),
            sum: Decimal::ZERO,
            current_bucket: None,
            frozen: None,
        });
        if st.current_bucket == Some(bucket) {
            if let Some(last) = st.buf.back_mut() {
                st.sum -= *last;
                *last = s;
                st.sum += s;
            }
            return;
        }
        if st.buf.len() == cap {
            if let Some(old) = st.buf.pop_front() {
                st.sum -= old;
            }
        }
        st.buf.push_back(s);
        st.sum += s;
        st.current_bucket = Some(bucket);
    }

    pub fn sample_count(&self, slot: &str) -> usize {
        self.slots.get(slot).map(|s| s.buf.len()).unwrap_or(0)
    }

    /// 窗口里最新一个 mid \(s\)（未入窗则为 `None`）。
    pub fn last_s(&self, slot: &str) -> Option<Decimal> {
        self.slots.get(slot).and_then(|s| s.buf.back().copied())
    }

    /// 满窗才有 live μ。
    pub fn live_mu(&self, slot: &str) -> Option<Decimal> {
        self.slots.get(slot).and_then(|s| s.live_mu(self.cap))
    }

    /// 有仓冻 μ；空仓用 live。未满窗且未冻则为 `None`。
    pub fn quote_mu(&self, slot: &str) -> Option<Decimal> {
        self.slots.get(slot).and_then(|s| s.quote_mu(self.cap))
    }

    /// 丢掉空闲槽位的窗口（停止套利后不再算 μ）。
    pub fn drop_except(&mut self, keep: &HashSet<String>) {
        self.slots.retain(|k, _| keep.contains(k));
    }

    pub fn drop_slot(&mut self, slot: &str) {
        self.slots.remove(slot);
    }

    pub fn is_frozen(&self, slot: &str) -> bool {
        self.slots.get(slot).is_some_and(|s| s.frozen.is_some())
    }

    /// `0→±1` 成交后调用。冻当时的 live μ；已冻不覆盖。
    pub fn freeze(&mut self, slot: &str) {
        let cap = self.cap;
        let Some(st) = self.slots.get_mut(slot) else {
            return;
        };
        if st.frozen.is_some() {
            return;
        }
        st.frozen = st.live_mu(cap);
    }

    /// 回到 STEP=0 后解冻。
    pub fn unfreeze(&mut self, slot: &str) {
        if let Some(st) = self.slots.get_mut(slot) {
            st.frozen = None;
        }
    }
}

impl Default for WindowBook {
    fn default() -> Self {
        Self::new(10_000, 1_000)
    }
}

/// 每所一条买卖点差滑动窗口。满窗均值即该所点差中枢。
/// 同一秒桶内多次盘口（多币、多对）取平均，避免被最后一拍盖掉。
/// 不冻（与 μ 不同）：两所 live 中枢的平均 \(C\) 每拍折进 Δ。
#[derive(Debug)]
pub struct VenueSpreadBook {
    slots: HashMap<String, MeanSlot>,
    cap: usize,
    interval_ms: u64,
}

#[derive(Debug, Clone)]
struct MeanSlot {
    buf: VecDeque<Decimal>,
    sum: Decimal,
    current_bucket: Option<u64>,
    bucket_sum: Decimal,
    bucket_n: u32,
}

impl MeanSlot {
    fn live_mu(&self, cap: usize) -> Option<Decimal> {
        if cap == 0 || self.buf.len() < cap {
            return None;
        }
        Some(self.sum / Decimal::from(self.buf.len() as u64))
    }

    fn trim(&mut self, cap: usize) {
        while self.buf.len() > cap {
            if let Some(old) = self.buf.pop_front() {
                self.sum -= old;
            }
        }
    }
}

impl VenueSpreadBook {
    pub fn new(cap: usize, interval_ms: u64) -> Self {
        Self {
            slots: HashMap::new(),
            cap: cap.max(1),
            interval_ms: interval_ms.max(1),
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn configure(&mut self, cap: usize, interval_ms: u64) {
        let cap = cap.max(1);
        let interval_ms = interval_ms.max(1);
        if self.interval_ms != interval_ms {
            for st in self.slots.values_mut() {
                st.current_bucket = None;
                st.bucket_n = 0;
                st.bucket_sum = Decimal::ZERO;
            }
        }
        self.interval_ms = interval_ms;
        self.cap = cap;
        for st in self.slots.values_mut() {
            st.trim(cap);
        }
    }

    fn bucket(&self, now_ms: u64) -> u64 {
        now_ms / self.interval_ms
    }

    pub fn observe(&mut self, venue: &str, now_ms: u64, c: Decimal) {
        let bucket = self.bucket(now_ms);
        let cap = self.cap;
        let st = self.slots.entry(venue.to_string()).or_insert(MeanSlot {
            buf: VecDeque::new(),
            sum: Decimal::ZERO,
            current_bucket: None,
            bucket_sum: Decimal::ZERO,
            bucket_n: 0,
        });
        if st.current_bucket == Some(bucket) {
            st.bucket_sum += c;
            st.bucket_n += 1;
            let mean = st.bucket_sum / Decimal::from(st.bucket_n);
            if let Some(last) = st.buf.back_mut() {
                st.sum -= *last;
                *last = mean;
                st.sum += mean;
            }
            return;
        }
        st.bucket_sum = c;
        st.bucket_n = 1;
        if st.buf.len() == cap {
            if let Some(old) = st.buf.pop_front() {
                st.sum -= old;
            }
        }
        st.buf.push_back(c);
        st.sum += c;
        st.current_bucket = Some(bucket);
    }

    pub fn sample_count(&self, venue: &str) -> usize {
        self.slots.get(venue).map(|s| s.buf.len()).unwrap_or(0)
    }

    pub fn live_mu(&self, venue: &str) -> Option<Decimal> {
        self.slots.get(venue).and_then(|s| s.live_mu(self.cap))
    }

    /// 停止/再启动时丢掉空闲所的点差窗口，下次从空样本重算中枢。
    pub fn drop_except_venues(&mut self, keep: &HashSet<String>) {
        self.slots.retain(|k, _| keep.contains(k));
    }

    pub fn clear(&mut self) {
        self.slots.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Bbo;
    use rust_decimal_macros::dec;
    use std::time::Instant;

    fn bbo(bid: Decimal, ask: Decimal) -> Bbo {
        Bbo {
            bid,
            ask,
            bid_qty: dec!(1),
            ask_qty: dec!(1),
            bids: vec![(bid, dec!(1))],
            asks: vec![(ask, dec!(1))],
            ts: Instant::now(),
        }
    }

    #[test]
    fn mid_spread_matches_spec_example() {
        let l = bbo(dec!(99.5), dec!(100.5)); // mid 100
        let r = bbo(dec!(98.5), dec!(99.5)); // mid 99
        let s = mid_spread_pct(&l, &r).unwrap();
        assert_eq!(s.round_dp(4), dec!(1.0050));
    }

    #[test]
    fn same_bucket_overwrites_last_price() {
        let mut book = WindowBook::new(3, 1000);
        book.observe("s", 1000, dec!(1));
        book.observe("s", 1500, dec!(2));
        assert_eq!(book.sample_count("s"), 1);
        book.observe("s", 2000, dec!(3));
        assert_eq!(book.sample_count("s"), 2);
        book.observe("s", 3000, dec!(4));
        assert_eq!(book.live_mu("s"), Some(dec!(3))); // (2+3+4)/3
    }

    #[test]
    fn mu_none_until_full() {
        let mut book = WindowBook::new(3, 1000);
        book.observe("s", 0, dec!(1));
        book.observe("s", 1000, dec!(2));
        assert!(book.live_mu("s").is_none());
        assert!(book.quote_mu("s").is_none());
        book.observe("s", 2000, dec!(3));
        assert_eq!(book.live_mu("s"), Some(dec!(2)));
    }

    #[test]
    fn last_s_is_latest_mid() {
        let mut book = WindowBook::new(3, 1000);
        assert!(book.last_s("s").is_none());
        book.observe("s", 0, dec!(1));
        assert_eq!(book.last_s("s"), Some(dec!(1)));
        book.observe("s", 500, dec!(9)); // 同桶覆盖
        assert_eq!(book.last_s("s"), Some(dec!(9)));
        book.observe("s", 1000, dec!(4));
        assert_eq!(book.last_s("s"), Some(dec!(4)));
    }

    #[test]
    fn sliding_drops_oldest() {
        let mut book = WindowBook::new(2, 1000);
        book.observe("s", 0, dec!(10));
        book.observe("s", 1000, dec!(20));
        book.observe("s", 2000, dec!(30));
        assert_eq!(book.live_mu("s"), Some(dec!(25)));
    }

    #[test]
    fn freeze_holds_quote_while_live_moves() {
        let mut book = WindowBook::new(2, 1000);
        book.observe("s", 0, dec!(0));
        book.observe("s", 1000, dec!(2));
        assert_eq!(book.quote_mu("s"), Some(dec!(1)));
        book.freeze("s");
        book.observe("s", 2000, dec!(8));
        assert_eq!(book.live_mu("s"), Some(dec!(5)));
        assert_eq!(book.quote_mu("s"), Some(dec!(1)));
        book.freeze("s"); // 已冻不覆盖
        assert_eq!(book.quote_mu("s"), Some(dec!(1)));
        book.unfreeze("s");
        assert_eq!(book.quote_mu("s"), Some(dec!(5)));
    }

    #[test]
    fn drop_except_keeps_live_slots() {
        let mut book = WindowBook::new(2, 1000);
        book.observe("idle", 0, dec!(1));
        book.observe("idle", 1000, dec!(2));
        book.observe("live", 0, dec!(3));
        book.observe("live", 1000, dec!(4));
        let keep = HashSet::from(["live".to_string()]);
        book.drop_except(&keep);
        assert_eq!(book.sample_count("idle"), 0);
        assert_eq!(book.sample_count("live"), 2);
        book.drop_slot("live");
        assert_eq!(book.sample_count("live"), 0);
    }

    #[test]
    fn missing_slot_has_zero_samples() {
        let book = WindowBook::new(8, 1000);
        assert_eq!(book.sample_count("nope"), 0);
        assert!(book.quote_mu("nope").is_none());
        assert!(!book.is_frozen("nope"));
    }

    #[test]
    fn configure_shrinks_buffer() {
        let mut book = WindowBook::new(3, 1000);
        book.observe("s", 0, dec!(1));
        book.observe("s", 1000, dec!(2));
        book.observe("s", 2000, dec!(3));
        book.configure(2, 1000);
        assert_eq!(book.sample_count("s"), 2);
        assert_eq!(book.live_mu("s"), Some(dec!(2.5)));
    }

    #[test]
    fn own_spread_uses_mid() {
        let b = bbo(dec!(99.5), dec!(100.5)); // mid 100, spread 1
        assert_eq!(own_spread_mid_pct(&b).unwrap(), dec!(1));
    }

    #[test]
    fn venue_spread_same_bucket_averages() {
        let mut book = VenueSpreadBook::new(2, 1000);
        book.observe("lighter", 1000, dec!(1));
        book.observe("lighter", 1500, dec!(3)); // 同桶均值 2
        assert_eq!(book.sample_count("lighter"), 1);
        book.observe("lighter", 2000, dec!(4));
        assert_eq!(book.live_mu("lighter"), Some(dec!(3))); // (2+4)/2
    }

    #[test]
    fn venue_spreads_are_independent() {
        let mut book = VenueSpreadBook::new(1, 1000);
        book.observe("lighter", 0, dec!(1));
        book.observe("entropy", 0, dec!(5));
        assert_eq!(book.live_mu("lighter"), Some(dec!(1)));
        assert_eq!(book.live_mu("entropy"), Some(dec!(5)));
    }

    #[test]
    fn venue_spread_drop_except_venues_clears_idle() {
        let mut book = VenueSpreadBook::new(1, 1000);
        book.observe("lighter", 0, dec!(1));
        book.observe("entropy", 0, dec!(5));
        book.drop_except_venues(&HashSet::from(["lighter".to_string()]));
        assert_eq!(book.live_mu("lighter"), Some(dec!(1)));
        assert_eq!(book.sample_count("entropy"), 0);
        assert!(book.live_mu("entropy").is_none());
        book.clear();
        assert_eq!(book.sample_count("lighter"), 0);
        assert!(book.live_mu("lighter").is_none());
    }

    #[test]
    fn exec_plus_is_bid_l_minus_ask_r() {
        let l = bbo(dec!(100), dec!(101));
        let r = bbo(dec!(98), dec!(99));
        // avg mid = (100.5+98.5)/2 = 99.5; plus (100-99)/99.5*100
        let s = exec_spread_pct(&l, &r, true).unwrap();
        assert_eq!(s.round_dp(4), dec!(1.0050));
        let m = exec_spread_pct(&l, &r, false).unwrap();
        assert_eq!(m.round_dp(4), dec!(3.0151));
    }

    #[test]
    fn pair_spread_hub_is_mean() {
        assert_eq!(
            pair_spread_hub_avg(dec!(0.0127), dec!(0.0107)),
            dec!(0.0117)
        );
    }
}
