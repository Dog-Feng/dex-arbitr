import type { AvailableVenuePair, PairDraft } from "./types";
import { fmtPctNum, parseNum, pairVenueKey, normalizeSymbol } from "./format";

export function qtyFitsPrecision(qty: number, prec: number): boolean {
  if (!Number.isFinite(qty) || qty <= 0 || prec < 0) return true;
  const scaled = qty * Math.pow(10, prec);
  return Math.abs(scaled - Math.round(scaled)) < 1e-6;
}

/** 与后端 `grid_step_from_target_bp` 同一式：Δ = (目标% + F + C) / (1−2h)，C 为两所中枢平均 */
export function deltaFromTargetBp(
  targetBp: number,
  roundTripFeePct: number,
  h: number,
  spreadPct = 0
): number {
  const cost = Math.max(roundTripFeePct, 0) + Math.max(spreadPct, 0);
  const need = Math.max(targetBp, 0) / 100 + cost;
  const span = 1 - 2 * Math.max(h, 0);
  const step = span > 0 ? need / span : need;
  return Math.max(step, cost);
}

export function feeWarns(step: number, fee: number): string[] {
  const msgs: string[] = [];
  if (!(fee > 0)) return msgs;
  if (step > 0 && step < fee) msgs.push(`Δ ${fmtPctNum(step)}% < 往返费 ${fmtPctNum(fee)}%`);
  return msgs;
}

function pick(ov: number | null, pair: number | null, def: number): number {
  if (ov != null) return ov;
  if (pair != null) return pair;
  return def;
}

export interface PairIssues {
  errors: string[];
  warnings: string[];
}

export function collectPairIssues(
  drafts: PairDraft[],
  activeVenues: string[],
  defTargetBp: number,
  hysteresis: number
): PairIssues {
  const errors: string[] = [];
  const warnings: string[] = [];
  if (!(defTargetBp > 0)) errors.push("target_bp 必须 > 0");
  if (hysteresis >= 0.5) errors.push("step_hysteresis 必须 < 0.5");
  for (const d of drafts) {
    const symbol = normalizeSymbol(d.symbol);
    if (!symbol) continue;
    const qty = parseNum(d.qty);
    if (qty == null || !(qty > 0)) errors.push(`${symbol} 每格数量必填且 > 0`);
    else {
      if (d.minQty > 0 && qty < d.minQty) errors.push(`${symbol} 每格数量 ${qty} 低于最小量 ${d.minQty}`);
      if (d.minQty > 0 && !qtyFitsPrecision(qty, d.prec)) errors.push(`${symbol} 每格数量精度须 ≤ ${d.prec} 位小数`);
    }
    const pairTarget = parseNum(d.step);
    for (const vp of d.venuePairs) {
      if (activeVenues.length && !vp.venues.every((v) => activeVenues.includes(v))) continue;
      const fee = parseFloat(vp.round_trip_fee_pct);
      if (Number.isNaN(fee)) continue;
      const key = pairVenueKey(vp.venues);
      const ov = d.overrides[key];
      const target = pick(parseNum(ov?.step), pairTarget, defTargetBp);
      const step = deltaFromTargetBp(target, fee, hysteresis);
      for (const w of feeWarns(step, fee)) {
        warnings.push(`${d.symbol} ${vp.venues.join("↔")}：${w}`);
      }
    }
  }
  return { errors, warnings };
}

export function qtyError(d: PairDraft): string {
  const symbol = normalizeSymbol(d.symbol);
  if (!symbol && !parseNum(d.qty)) return "";
  const qty = parseNum(d.qty);
  const msgs: string[] = [];
  if (symbol && (qty == null || !(qty > 0))) msgs.push("必填且 > 0");
  else if (qty != null && qty > 0 && d.minQty > 0) {
    if (qty < d.minQty) msgs.push(`低于最小量 ${d.minQty}`);
    if (!qtyFitsPrecision(qty, d.prec)) msgs.push(`精度须 ≤ ${d.prec} 位小数`);
  }
  return msgs.join("；");
}

export function venuePairWarns(
  d: PairDraft,
  vp: AvailableVenuePair,
  activeVenues: string[],
  defTargetBp: number,
  hysteresis: number
): string[] {
  if (activeVenues.length && !vp.venues.every((v) => activeVenues.includes(v))) return [];
  const fee = parseFloat(vp.round_trip_fee_pct);
  if (!(fee > 0)) return [];
  const key = pairVenueKey(vp.venues);
  const ov = d.overrides[key];
  const target = pick(parseNum(ov?.step), parseNum(d.step), defTargetBp);
  return feeWarns(deltaFromTargetBp(target, fee, hysteresis), fee);
}
