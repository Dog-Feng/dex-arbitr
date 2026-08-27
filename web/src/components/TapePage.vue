<template>
  <div class="wrap">
    <n-card size="small" :title="`执行带 · 本次运行 · ${rows.length} 条`">
      <n-data-table :columns="cols" :data="rows" size="small" :bordered="false" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { h } from "vue";
import { NTag } from "naive-ui";
import type { DataTableColumns } from "naive-ui";
import type { ExecRow } from "../types";
import { statusKind } from "../format";

defineProps<{ rows: ExecRow[] }>();

function gridStep(r: ExecRow): string {
  if (r.grid_from == null && r.grid_to == null) return "—";
  return `${r.grid_from ?? "—"} -> ${r.grid_to ?? "—"}`;
}

function qtyText(v: unknown): string {
  if (v == null || v === "" || v === "—") return "—";
  const s = String(v).trim();
  const m = s.match(/^(-?)(\d+)(?:\.(\d+))?$/);
  if (!m) {
    const n = Number(s);
    return Number.isFinite(n) ? String(n) : "—";
  }
  let frac = (m[3] || "").replace(/0+$/, "");
  if (frac.length > 8) frac = frac.slice(0, 8).replace(/0+$/, "");
  return frac ? `${m[1]}${m[2]}.${frac}` : `${m[1]}${m[2]}`;
}

function pnlText(r: ExecRow): string {
  if (r.action !== "close" || r.pnl_usdc == null || r.pnl_usdc === "") return "—";
  const n = parseFloat(r.pnl_usdc);
  if (Number.isNaN(n)) return "—";
  const body = Math.abs(n).toFixed(2);
  if (n > 0) return `+${body}`;
  if (n < 0) return `-${body}`;
  return "0.00";
}

function pnlCls(r: ExecRow): string {
  if (r.action !== "close" || r.pnl_usdc == null || r.pnl_usdc === "") return "mu";
  const n = parseFloat(r.pnl_usdc);
  if (Number.isNaN(n) || n === 0) return "mu";
  return n > 0 ? "up" : "dn";
}

const cols: DataTableColumns<ExecRow> = [
  {
    title: "时间",
    key: "ts",
    width: 100,
    render: (r) => new Date(r.ts * 1000).toLocaleTimeString("zh-CN", { hour12: false }),
  },
  { title: "标的", key: "pair_id", width: 160, maxWidth: 180, ellipsis: { tooltip: true } },
  {
    title: "路径",
    key: "path",
    width: 180,
    maxWidth: 200,
    ellipsis: { tooltip: true },
    render: (r) => h("span", { class: "mu" }, `${r.buy_venue}→${r.sell_venue}`),
  },
  {
    title: "数量",
    key: "qty",
    width: 110,
    render: (r) => h("span", { class: "tabular" }, qtyText(r.qty)),
  },
  {
    title: "格子步",
    key: "grid",
    width: 90,
    render: (r) => h("span", { class: "tabular mu" }, gridStep(r)),
  },
  {
    title: "盈亏",
    key: "pnl",
    width: 100,
    render: (r) => h("span", { class: `tabular ${pnlCls(r)}` }, pnlText(r)),
  },
  {
    title: "结果",
    key: "result",
    minWidth: 120,
    ellipsis: { tooltip: true },
    render: (r) => h(NTag, { size: "small", type: statusKind(r.result) }, { default: () => r.result || "—" }),
  },
];
</script>

<style scoped>
.wrap { padding: 16px 20px 32px; }
</style>
