export interface VenueMeta {
  id: string;
  label: string;
  keys_ready: boolean;
  quote: string;
  active?: boolean;
}

export interface PairDefaults {
  max_segments: number;
  target_bp: string | number;
  split_order_size: string | number;
}

export interface PairOverride {
  venues: string[];
  max_segments?: number | null;
  target_bp?: string | number | null;
  split_order_size?: string | number | null;
}

export interface PairSetting {
  symbol: string;
  base_qty: string | number;
  max_segments?: number | null;
  target_bp?: string | number | null;
  split_order_size?: string | number | null;
  overrides?: PairOverride[];
}

export interface ArbitrageParams {
  active_venues: string[];
  monitor_only: boolean;
  data_freshness_ms: number;
  pair_defaults: PairDefaults;
  pairs: PairSetting[];
  paper_trading: boolean;
  loop_interval_ms: number;
  hedge_failed_legs: boolean;
  max_concurrent_pairs: number;
  leverage_multiplier: string | number;
  depth_pct: string | number;
  refresh_balance_secs: number;
  margin_utilization_pct: string | number;
  fallback_available_usdc: string | number;
  scan_enabled: boolean;
  min_spread_pct: string | number;
  analysis_interval_ms: number;
  watch_top: number;
  cross_use_natural: boolean;
  scan_log_interval_secs: number;
  persistence_ms: number;
  persistence_min_hits: number;
  window_samples: number;
  sample_interval_ms: number;
  step_hysteresis: string | number;
  limit_timeout_ms: number;
  second_leg_verify_ms: number;
  maker_inside_ticks: number;
  limit_retry_count: number;
  default_slip_pct: string | number;
  max_slippage_pct: string | number;
  emergency_slippage_multiplier: string | number;
  history_enabled: boolean;
  history_window_hours: number;
  history_min_points: number;
  history_sample_interval_secs: number;
  history_refresh_interval_secs: number;
}

export interface AvailableVenuePair {
  venues: string[];
  min_qty: string;
  qty_precision: number;
  round_trip_fee_pct: string;
  mid?: string | null;
}

export interface AvailableSymbol {
  pair_id: string;
  symbol: string;
  venue_pairs: AvailableVenuePair[];
}

export interface PairRow {
  pair_id: string;
  buy: string;
  sell: string;
  raw_pct: string;
  net_pct: string;
  fee_pct: string;
  nat_pct: string;
  res_pct: string;
  entry_pct: string;
  dev_pct?: string;
  delta_pct?: string;
  grid: string;
  target_qty: string;
  actual_qty: string;
  status: string;
}

export interface PositionRow {
  pair_id: string;
  buy: string;
  sell: string;
  qty: string;
  grid: number;
  entry_notional: string;
}

export interface VenueBalanceRow {
  venue: string;
  available: string;
  total: string;
}

export interface ExchangePositionRow {
  venue: string;
  symbol: string;
  qty: string;
  entry_price?: string | null;
}

export interface VenueLiveRow {
  venue: string;
  spread_mu: string;
  volume: string;
}

export interface NakedExposureRow {
  pair_id: string;
  venue: string;
  qty: string;
  counterparty: string;
  source: string;
}

export interface VenueMatchRow {
  left: string;
  right: string;
  n: number;
}

export interface LiveSnapshot {
  pairs: PairRow[];
  positions: PositionRow[];
  balances: VenueBalanceRow[];
  exchange_positions: ExchangePositionRow[];
  venue_stats?: VenueLiveRow[];
  naked_exposures: NakedExposureRow[];
  venue_matches: VenueMatchRow[];
  stats: { matched_pairs: number; open_positions: number; best_net_pct?: string | null };
  monitor_only: boolean;
  paper_trading: boolean;
  arbitrage_enabled: boolean;
  matching: boolean;
  available: AvailableSymbol[];
  session_pnl_usdc?: string;
  updated_at: number;
}

export interface ExecRow {
  ts: number;
  pair_id: string;
  action: string;
  buy_venue: string;
  sell_venue: string;
  qty: string;
  net_pct?: string | null;
  result: string;
  detail: string;
  grid_from?: number | null;
  grid_to?: number | null;
  pnl_usdc?: string | null;
  pnl_pct?: string | null;
}

export interface PairDraft {
  symbol: string;
  pair_id: string;
  enabled: boolean;
  qty: string;
  segs: string;
  step: string;
  minQty: number;
  prec: number;
  venuePairs: AvailableVenuePair[];
  expanded: boolean;
  overrides: Record<
    string,
    { step: string; segs: string }
  >;
}
