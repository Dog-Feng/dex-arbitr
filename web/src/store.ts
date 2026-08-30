import { reactive } from "vue";
import type { ArbitrageParams, AvailableSymbol, PairDraft, PairOverride, PairSetting, VenueMeta } from "./types";
import { pairVenueKey, parseNum, normalizeSymbol } from "./format";

export function emptyDefaults() {
  return {
    max_segments: 3,
    target_bp: "1",
    split_order_size: "0",
  };
}

export function emptyParams(): ArbitrageParams {
  return {
    active_venues: [],
    scan_venues: [],
    data_freshness_ms: 3000,
    pair_defaults: emptyDefaults(),
    pairs: [],
    loop_interval_ms: 100,
    hedge_failed_legs: true,
    leverage_multiplier: "5",
    depth_pct: "50",
    refresh_balance_secs: 1,
    margin_utilization_pct: "90",
    fallback_available_usdc: "500",
    scan_enabled: false,
    analysis_interval_ms: 50,
    watch_top: 20,
    scan_window_samples: 120,
    persistence_ms: 1000,
    persistence_min_hits: 7,
    window_samples: 1000,
    sample_interval_ms: 1000,
    step_hysteresis: "0",
    limit_timeout_ms: 2000,
    second_leg_verify_ms: 2000,
    maker_inside_ticks: 1,
    limit_retry_count: 3,
    default_slip_pct: "0.01",
    max_slippage_pct: "1",
    emergency_slippage_multiplier: "50",
    history_min_points: 10,
  };
}

function blankDraft(symbol = "", qty = ""): PairDraft {
  return {
    symbol,
    pair_id: symbol,
    enabled: true,
    qty,
    segs: "",
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
      step: ov.target_bp != null ? String(ov.target_bp) : "",
      segs: ov.max_segments != null ? String(ov.max_segments) : "",
    };
  }
  return {
    symbol: s.symbol,
    pair_id: s.symbol,
    enabled: true,
    qty: s.base_qty != null ? String(s.base_qty) : "",
    segs: s.max_segments != null ? String(s.max_segments) : "",
    step: s.target_bp != null ? String(s.target_bp) : "",
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
  /** 仅扫描页勾选，不进 /api/config。 */
  scan_venues: [] as string[],
  drafts: [blankDraft()] as PairDraft[],
  available: [] as AvailableSymbol[],
  filter: "",
});

export function applyParams(p: ArbitrageParams) {
  const next = {
    ...emptyParams(),
    ...p,
    pair_defaults: { ...emptyDefaults(), ...(p.pair_defaults || {}) },
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
  const ids = store.venues.map((v) => v.id);
  if (!ids.length) return;
  if (!store.scan_venues.length) {
    store.scan_venues = [...ids];
    return;
  }
  const keep = store.scan_venues.filter((id) => ids.includes(id));
  store.scan_venues = keep.length ? keep : [...ids];
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
    const step = parseNum(d.step);
    if (step != null) row.target_bp = step;
    const overrides: PairOverride[] = [];
    for (const [key, ov] of Object.entries(d.overrides || {})) {
      const venues = key.split("|").filter(Boolean);
      if (venues.length < 2) continue;
      const item: PairOverride = { venues };
      let has = false;
      const ostep = parseNum(ov.step);
      if (ostep != null) {
        item.target_bp = ostep;
        has = true;
      }
      const osegs = parseNum(ov.segs);
      if (osegs != null) {
        item.max_segments = osegs;
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
  };
}
