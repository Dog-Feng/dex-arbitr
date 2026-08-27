import { reactive } from "vue";
import type { ArbitrageParams, AvailableSymbol, PairDraft, PairOverride, PairSetting, VenueMeta } from "./types";
import { pairVenueKey, parseNum, normalizeSymbol } from "./format";

export function emptyDefaults() {
  return {
    max_segments: 3,
    initial_spread_threshold: "0.05",
    grid_step: "0.05",
    t0_ratio: "0.4",
    split_order_size: "0",
    scalping_enabled: false,
    scalping_trigger_segment: 2,
    scalping_profit_threshold_pct: "0.02",
  };
}

export function emptyParams(): ArbitrageParams {
  return {
    active_venues: [],
    monitor_only: false,
    data_freshness_ms: 3000,
    pair_defaults: emptyDefaults(),
    pairs: [],
    paper_trading: true,
    loop_interval_ms: 100,
    hedge_failed_legs: true,
    max_concurrent_pairs: 1,
    leverage_multiplier: "2",
    depth_pct: "50",
    refresh_balance_secs: 1,
    margin_utilization_pct: "90",
    fallback_available_usdc: "500",
    scan_enabled: false,
    min_spread_pct: "0.1",
    analysis_interval_ms: 50,
    watch_top: 20,
    cross_use_natural: true,
    scan_log_interval_secs: 30,
    persistence_ms: 1000,
    persistence_mode: "bucket",
    spread_persistence_seconds: 1,
    strict_persistence_check: true,
    order_style: "limit_then_market",
    limit_timeout_ms: 2000,
    maker_inside_ticks: 1,
    limit_retry_count: 3,
    default_slip_pct: "0.01",
    max_slippage_pct: "0.1",
    emergency_slippage_multiplier: "50",
    history_enabled: true,
    history_window_hours: 24,
    history_min_points: 10,
    history_sample_interval_secs: 5,
    history_refresh_interval_secs: 1800,
    min_book_qty: { BTC: "0.001", ETH: "0.01" },
    max_venue_spread_pct: "0.05",
    price_stability_window_secs: "1",
    price_stability_threshold_pct: "0.01",
    reduce_only_probe_enabled: true,
    reduce_only_probe_second: 5,
    funding_annual_threshold_pct: "10",
    funding_unfavorable_duration_minutes: 60,
    funding_refresh_secs: 60,
    max_daily_opens: 100,
    max_position_hours: 168,
    min_balance_warn_usdc: "40",
    min_balance_close_usdc: "20",
    max_single_token_notional_usdc: "0",
    max_total_notional_usdc: "0",
    backoff_min_secs: 120,
    backoff_max_secs: 3600,
    backoff_multiplier: 2,
    backoff_reset_secs: 600,
  };
}

function blankDraft(symbol = "", qty = ""): PairDraft {
  return {
    symbol,
    pair_id: symbol,
    enabled: true,
    qty,
    segs: "",
    t1: "",
    step: "",
    minQty: 0,
    prec: 8,
    venuePairs: [],
    expanded: false,
    overrides: {},
  };
}

function draftFromSetting(s: PairSetting): PairDraft {
  const overrides: PairDraft["overrides"] = {};
  for (const ov of s.overrides || []) {
    const key = pairVenueKey(ov.venues);
    overrides[key] = {
      t1: ov.initial_spread_threshold != null ? String(ov.initial_spread_threshold) : "",
      step: ov.grid_step != null ? String(ov.grid_step) : "",
      segs: ov.max_segments != null ? String(ov.max_segments) : "",
      scalp: !!ov.scalping_enabled,
      scalpTrig: ov.scalping_trigger_segment != null ? String(ov.scalping_trigger_segment) : "",
    };
  }
  return {
    symbol: s.symbol,
    pair_id: s.symbol,
    enabled: true,
    qty: s.base_qty != null ? String(s.base_qty) : "",
    segs: s.max_segments != null ? String(s.max_segments) : "",
    t1: s.initial_spread_threshold != null ? String(s.initial_spread_threshold) : "",
    step: s.grid_step != null ? String(s.grid_step) : "",
    minQty: 0,
    prec: 8,
    venuePairs: [],
    expanded: false,
    overrides,
  };
}

export const store = reactive({
  params: emptyParams(),
  venues: [] as VenueMeta[],
  drafts: [blankDraft()] as PairDraft[],
  available: [] as AvailableSymbol[],
  filter: "",
});

export function applyParams(p: ArbitrageParams) {
  const next = {
    ...emptyParams(),
    ...p,
    pair_defaults: { ...emptyDefaults(), ...(p.pair_defaults || {}) },
    min_book_qty: { ...(p.min_book_qty || {}) },
    pairs: p.pairs || [],
    active_venues: p.active_venues || [],
  };
  Object.assign(store.params, next);
  Object.assign(store.params.pair_defaults, next.pair_defaults);
  store.drafts = (next.pairs || []).map(draftFromSetting);
  if (!store.drafts.length) store.drafts = [blankDraft()];
}

export function applyAvailable(list: AvailableSymbol[]) {
  store.available = list || [];
}

export function applyVenues(list: VenueMeta[]) {
  store.venues = list || [];
}

export function addDraft() {
  store.drafts.push(blankDraft());
}

export function removeDraft(i: number) {
  store.drafts.splice(i, 1);
  if (!store.drafts.length) store.drafts.push(blankDraft());
}

export function collectEnabledPairs(): PairSetting[] {
  const out: PairSetting[] = [];
  const seen = new Set<string>();
  for (const d of store.drafts) {
    const symbol = normalizeSymbol(d.symbol);
    if (!symbol || seen.has(symbol)) continue;
    const qty = parseNum(d.qty);
    if (qty == null || !(qty > 0)) continue;
    seen.add(symbol);
    const row: PairSetting = { symbol, base_qty: String(qty) };
    const segs = parseNum(d.segs);
    if (segs != null) row.max_segments = segs;
    const t1 = parseNum(d.t1);
    if (t1 != null) row.initial_spread_threshold = t1;
    const step = parseNum(d.step);
    if (step != null) row.grid_step = step;
    const overrides: PairOverride[] = [];
    for (const [key, ov] of Object.entries(d.overrides || {})) {
      const venues = key.split("|").filter(Boolean);
      if (venues.length < 2) continue;
      const item: PairOverride = { venues };
      let has = false;
      const ot1 = parseNum(ov.t1);
      if (ot1 != null) {
        item.initial_spread_threshold = ot1;
        has = true;
      }
      const ostep = parseNum(ov.step);
      if (ostep != null) {
        item.grid_step = ostep;
        has = true;
      }
      const osegs = parseNum(ov.segs);
      if (osegs != null) {
        item.max_segments = osegs;
        has = true;
      }
      if (ov.scalp) {
        item.scalping_enabled = true;
        has = true;
      }
      const trig = parseNum(ov.scalpTrig);
      if (trig != null) {
        item.scalping_trigger_segment = trig;
        has = true;
      }
      if (has) overrides.push(item);
    }
    if (overrides.length) row.overrides = overrides;
    out.push(row);
  }
  return out;
}

export function payload(): ArbitrageParams {
  return {
    ...store.params,
    pairs: collectEnabledPairs(),
    min_book_qty: { ...store.params.min_book_qty },
  };
}
