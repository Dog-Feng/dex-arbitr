export function venueLabel(id: string, list?: { id: string; label?: string }[]): string {
  const hit = (list || []).find((v) => v.id === id && v.label);
  if (hit?.label) return hit.label;
  if (id === "lighter") return "Lighter";
  if (id === "lighter_rh") return "Lighter RH";
  if (id === "sodex") return "SoDEX";
  if (id === "entropy") return "EntropyIO";
  return id;
}

export function venueQuote(id: string, list?: { id: string; quote?: string }[]): string {
  const hit = (list || []).find((v) => v.id === id && v.quote);
  if (hit?.quote) return hit.quote;
  if (id === "lighter_rh") return "USDG";
  return "USDC";
}

export function pairVenueKey(venues: string[]): string {
  return [...venues].sort().join("|");
}

export function numFmt(v: unknown): string {
  const n = typeof v === "number" ? v : parseFloat(String(v || "").replace(/[^0-9.+\-]/g, ""));
  if (Number.isNaN(n)) return String(v || "—");
  return (n >= 0 ? "+" : "") + n.toFixed(3) + "%";
}

export function netCls(v: unknown): "up" | "dn" | "mu" {
  const n = parseFloat(String(v || "").replace(/[^0-9.+\-]/g, ""));
  if (Number.isNaN(n)) return "mu";
  if (n >= 0.03) return "up";
  if (n < 0) return "dn";
  return "mu";
}

export function moneyFmt(v: unknown): string {
  const n = parseFloat(String(v));
  return Number.isNaN(n) ? "—" : n.toFixed(2);
}

export function qtyFmt(v: unknown): string {
  if (v == null || v === "" || v === "—") return "—";
  const n = parseFloat(String(v));
  return Number.isNaN(n) ? "—" : n.toFixed(6);
}

export function parsePct(v: unknown): number | null {
  if (v == null || v === "" || v === "—") return null;
  const n = parseFloat(String(v).replace(/[^0-9.+\-]/g, ""));
  return Number.isNaN(n) ? null : n;
}

export function parseNum(v: unknown): number | null {
  if (v == null || String(v).trim() === "") return null;
  const n = parseFloat(String(v));
  return Number.isNaN(n) ? null : n;
}

export function normalizeSymbol(raw: string): string {
  let t = raw.trim().toUpperCase();
  t = t.replace(/[-_]?PERP$/, "");
  t = t.replace(/[-_]USD[CG]?$/, "");
  return t;
}

export function fmtPctNum(n: number): string {
  if (!Number.isFinite(n)) return "—";
  return String(Math.round(n * 10000) / 10000);
}

export function statusKind(s: string): "success" | "default" | "error" | "warning" {
  const t = String(s || "");
  if (/开仓|平仓|open|filled|挂单/i.test(t)) return "success";
  if (/持有|hold|已匹配|监控/i.test(t)) return "default";
  if (/error|fail|不足|过期|过宽|非法|波动/i.test(t)) return "error";
  return "warning";
}

/** 监控/执行带状态：兼容旧进程的英文标签。 */
export function statusLabel(s: string): string {
  const t = String(s || "").trim();
  const map: Record<string, string> = {
    open: "开仓中",
    close: "平仓中",
    hold: "持有",
    scalp: "剥头皮",
    scalp_tp: "剥头皮",
    limit: "挂单中",
    canceling: "撤单中",
    filled_open: "已开仓",
    filled_close: "已平仓",
  };
  return map[t] || t || "—";
}
