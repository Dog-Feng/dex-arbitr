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
  if (/开仓|平仓|成交|open|filled|挂单/i.test(t)) return "success";
  if (/持有|hold|已匹配|监控/i.test(t)) return "default";
  if (/error|fail|失败|不足|过期|过宽|非法|波动|未知|敞口|超时/i.test(t)) return "error";
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
    both_filled: "双腿成交",
    cancel_failed: "撤单失败",
    emergency_closed: "第二腿失败，已紧急平第一腿",
    naked: "单边敞口",
    second_leg_unknown: "第二腿结果未知",
    zero_fill: "未成交已撤",
    error: "下单失败",
    watchdog_timeout: "执行超时",
  };
  return map[t] || t || "—";
}

/** 执行带「结果」列：英文码换成中文；失败时带上 detail 摘要。 */
export function tapeResultLabel(r: {
  action?: string;
  result?: string;
  detail?: string;
}): string {
  const code = String(r.result || "").trim();
  const action = String(r.action || "").trim();
  if (code === "both_filled") {
    if (action === "open") return "市价开仓成交";
    if (action === "close") return "市价平仓成交";
    return "双腿成交";
  }
  const label = statusLabel(code);
  const detail = String(r.detail || "").trim();
  if (!detail) return label;
  if (code === "error" || code === "naked" || code === "emergency_closed" || code === "zero_fill") {
    const short = detail.length > 80 ? detail.slice(0, 80) + "…" : detail;
    return `${label}：${short}`;
  }
  return label;
}
