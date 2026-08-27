<template>
  <div class="lists">
    <n-card size="small" :title="pairTitle">
      <div class="pills">
        <n-button
          size="tiny"
          :type="filter === '' ? 'primary' : 'default'"
          secondary
          @click="filter = ''"
        >全部所对 <b>{{ (snap?.pairs || []).length }}</b></n-button>
        <n-button
          v-for="g in snap?.venue_matches || []"
          :key="g.left + g.right"
          size="tiny"
          :type="filter === pairKey(g.left, g.right) ? 'primary' : 'default'"
          secondary
          @click="filter = pairKey(g.left, g.right)"
        >{{ venueLabel(g.left, store.venues) }} ↔ {{ venueLabel(g.right, store.venues) }} <b>{{ g.n }}</b></n-button>
      </div>
      <n-data-table
        :columns="pairCols"
        :data="pairRows"
        :row-key="pairRowKey"
        :bordered="false"
        :single-line="false"
        size="small"
        :scroll-x="1100"
        :max-height="420"
      />
    </n-card>

    <n-card size="small" title="交易所持仓">
      <n-data-table
        :columns="exCols"
        :data="snap?.exchange_positions || []"
        size="small"
        :bordered="false"
        :max-height="240"
      />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { computed, h, ref } from "vue";
import { NTag } from "naive-ui";
import type { DataTableColumns } from "naive-ui";
import type { ExchangePositionRow, LiveSnapshot, PairRow } from "../types";
import { netCls, numFmt, parsePct, qtyFmt, statusKind, statusLabel, venueLabel } from "../format";
import { store } from "../store";

const props = defineProps<{ snap: LiveSnapshot | null }>();
const filter = ref("");

function pairKey(a: string, b: string) {
  return a < b ? a + "|" + b : b + "|" + a;
}

function pairRowKey(r: PairRow) {
  return `${r.pair_id}|${r.buy}|${r.sell}`;
}

const pairTitle = computed(() => {
  const rows = props.snap?.pairs || [];
  if (!rows.length) {
    if (props.snap?.matching) return "全部所对 · 正在匹配…";
    return "全部所对";
  }
  return filter.value ? `全部所对 · 筛选 ${rows.length}` : `全部所对 · ${rows.length}`;
});

const pairRows = computed(() => {
  const list = props.snap?.pairs || [];
  if (!filter.value) return list;
  return list.filter((p) => pairKey(p.buy, p.sell) === filter.value);
});

function pctCell(v: unknown) {
  return h("span", { class: netCls(v) }, String(v || "—"));
}

const pairCols: DataTableColumns<PairRow> = [
  { title: "标的", key: "pair_id", width: 120, ellipsis: { tooltip: true } },
  { title: "买", key: "buy", width: 90, render: (r) => venueLabel(r.buy, store.venues) },
  { title: "卖", key: "sell", width: 90, render: (r) => venueLabel(r.sell, store.venues) },
  { title: "毛价差", key: "raw_pct", width: 78, render: (r) => pctCell(r.raw_pct) },
  { title: "净边", key: "net_pct", width: 78, render: (r) => pctCell(r.net_pct) },
  {
    title: "费率",
    key: "fee_pct",
    width: 78,
    render: (r) => {
      const fee = parsePct(r.fee_pct);
      const raw = parsePct(r.raw_pct);
      const net = parsePct(r.net_pct);
      const v = fee == null && raw != null && net != null ? raw - net : fee;
      return h("span", { class: "mu" }, v == null ? "—" : numFmt(v));
    },
  },
  { title: "天然", key: "nat_pct", width: 78, render: (r) => h("span", { class: "mu" }, r.nat_pct || "—") },
  { title: "残差", key: "res_pct", width: 78, render: (r) => pctCell(r.res_pct) },
  { title: "门槛", key: "entry_pct", width: 78, render: (r) => h("span", { class: "mu" }, r.entry_pct || "—") },
  { title: "格子", key: "grid", width: 56, render: (r) => h("span", { class: "mu" }, r.grid || "—") },
  { title: "目标量", key: "target_qty", width: 80, render: (r) => qtyFmt(r.target_qty) },
  { title: "持仓", key: "actual_qty", width: 80, render: (r) => qtyFmt(r.actual_qty || "0") },
  {
    title: "状态",
    key: "status",
    width: 110,
    render: (r) =>
      h(NTag, { size: "small", type: statusKind(r.status) }, { default: () => statusLabel(r.status) }),
  },
];

const exCols: DataTableColumns<ExchangePositionRow> = [
  { title: "交易所", key: "venue", render: (r) => venueLabel(r.venue, store.venues) },
  { title: "合约", key: "symbol" },
  { title: "数量", key: "qty", render: (r) => qtyFmt(r.qty) },
  { title: "开仓价", key: "entry_price", render: (r) => r.entry_price || "—" },
];
</script>

<style scoped>
.lists { display: flex; flex-direction: column; gap: 14px; min-width: 0; }
.pills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
</style>
