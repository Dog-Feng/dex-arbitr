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
        :scroll-x="1000"
        :max-height="420"
      />
    </n-card>

    <n-card size="small" title="交易所持仓">
      <n-data-table
        :columns="exCols"
        :data="venueRows"
        :row-key="venueRowKey"
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
import type { LiveSnapshot, PairRow, VenueLiveRow } from "../types";
import { moneyFmt, netCls, qtyFmt, statusKind, statusLabel, venueLabel } from "../format";
import { store } from "../store";

const props = defineProps<{ snap: LiveSnapshot | null }>();
const filter = ref("");

function pairKey(a: string, b: string) {
  return a < b ? a + "|" + b : b + "|" + a;
}

function pairRowKey(r: PairRow) {
  return `${r.pair_id}|${r.buy}|${r.sell}`;
}

function venueRowKey(r: VenueLiveRow) {
  return r.venue;
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

const venueRows = computed((): VenueLiveRow[] => {
  const stats = props.snap?.venue_stats;
  if (stats && stats.length) return stats;
  return store.venues.map((v) => ({ venue: v.id, spread_mu: "—", volume: "0.00" }));
});

function holdingsOf(venue: string) {
  const rows = (props.snap?.exchange_positions || []).filter((p) => p.venue === venue);
  if (!rows.length) return "—";
  return rows.map((p) => `${p.symbol} ${qtyFmt(p.qty)}`).join(" · ");
}

function pctCell(v: unknown) {
  return h("span", { class: netCls(v) }, String(v || "—"));
}

const pairCols: DataTableColumns<PairRow> = [
  { title: "标的", key: "pair_id", width: 120, ellipsis: { tooltip: true } },
  { title: "买", key: "buy", width: 90, render: (r) => venueLabel(r.buy, store.venues) },
  { title: "卖", key: "sell", width: 90, render: (r) => venueLabel(r.sell, store.venues) },
  { title: "毛价差", key: "raw_pct", width: 78, render: (r) => pctCell(r.raw_pct) },
  { title: "中枢", key: "entry_pct", width: 90, render: (r) => h("span", { class: "mu" }, r.entry_pct || "—") },
  { title: "偏离", key: "dev_pct", width: 78, render: (r) => pctCell(r.dev_pct || "—") },
  { title: "当前格宽", key: "delta_pct", width: 88, render: (r) => h("span", { class: "mu" }, r.delta_pct || "—") },
  { title: "STEP", key: "grid", width: 56, render: (r) => h("span", { class: "mu" }, r.grid || "—") },
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

const exCols: DataTableColumns<VenueLiveRow> = [
  { title: "交易所", key: "venue", width: 110, render: (r) => venueLabel(r.venue, store.venues) },
  {
    title: "点差中枢",
    key: "spread_mu",
    width: 100,
    render: (r) => h("span", { class: "mu" }, r.spread_mu || "—"),
  },
  {
    title: "交易量",
    key: "volume",
    width: 100,
    render: (r) => moneyFmt(r.volume),
  },
  { title: "持仓", key: "holdings", ellipsis: { tooltip: true }, render: (r) => holdingsOf(r.venue) },
];
</script>

<style scoped>
.lists { display: flex; flex-direction: column; gap: 14px; min-width: 0; }
.pills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
</style>
