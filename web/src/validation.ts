import type { AvailableVenuePair, PairDraft } from "./types";
import { fmtPctNum, parseNum, pairVenueKey, normalizeSymbol } from "./format";

export function qtyFitsPrecision(qty: number, prec: number): boolean {
  if (!Number.isFinite(qty) || qty <= 0 || prec < 0) return true;
  const scaled = qty * Math.pow(10, prec);
  return Math.abs(scaled - Math.round(scaled)) < 1e-6;
}

export function feeWarns(t1: number, step: number, t0ratio: number, fee: number): string[] {
  const msgs: string[] = [];
  if (!(fee > 0)) return msgs;
  const feeS = fmtPctNum(fee);
  if (t1 > 0 && t1 < fee) msgs.push(`T1 ${fmtPctNum(t1)}% < 往返费 ${feeS}%`);
  if (step > 0 && step < fee) msgs.push(`step ${fmtPctNum(step)}% < 往返费 ${feeS}%`);
  if (t1 > 0) {
    const conv = t1 * (1 - (Number.isFinite(t0ratio) ? t0ratio : 0.4));
    if (conv < fee) msgs.push(`T1→T0 收敛 ${fmtPctNum(conv)}% < 往返费 ${feeS}%`);
  }
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
  defT1: number,
  defStep: number,
  t0ratio: number
): PairIssues {
  const errors: string[] = [];
  const warnings: string[] = [];
  for (const d of drafts) {
    const symbol = normalizeSymbol(d.symbol);
    if (!symbol) continue;
    const qty = parseNum(d.qty);
    if (qty == null || !(qty > 0)) errors.push(`${symbol} 每格数量必填且 > 0`);
    else {
      if (d.minQty > 0 && qty < d.minQty) errors.push(`${symbol} 每格数量 ${qty} 低于最小量 ${d.minQty}`);
      if (d.minQty > 0 && !qtyFitsPrecision(qty, d.prec)) errors.push(`${symbol} 每格数量精度须 ≤ ${d.prec} 位小数`);
    }
    const pairT1 = parseNum(d.t1);
    const pairStep = parseNum(d.step);
    for (const vp of d.venuePairs) {
      if (activeVenues.length && !vp.venues.every((v) => activeVenues.includes(v))) continue;
      const fee = parseFloat(vp.round_trip_fee_pct);
      if (Number.isNaN(fee)) continue;
      const key = pairVenueKey(vp.venues);
      const ov = d.overrides[key];
      const t1 = pick(parseNum(ov?.t1), pairT1, defT1);
      const step = pick(parseNum(ov?.step), pairStep, defStep);
      for (const w of feeWarns(t1, step, t0ratio, fee)) {
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
  defT1: number,
  defStep: number,
  t0ratio: number
): string[] {
  if (activeVenues.length && !vp.venues.every((v) => activeVenues.includes(v))) return [];
  const fee = parseFloat(vp.round_trip_fee_pct);
  if (!(fee > 0)) return [];
  const key = pairVenueKey(vp.venues);
  const ov = d.overrides[key];
  const t1 = pick(parseNum(ov?.t1), parseNum(d.t1), defT1);
  const step = pick(parseNum(ov?.step), parseNum(d.step), defStep);
  return feeWarns(t1, step, t0ratio, fee);
}
