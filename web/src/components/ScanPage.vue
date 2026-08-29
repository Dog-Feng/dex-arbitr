<template>
  <div class="layout">
    <div class="main">
      <n-card size="small" title="扫描范围" :bordered="true">
        <template #header-extra>
          <div class="hdr-btns">
            <n-button size="small" :loading="busy" @click="save">保存扫描配置</n-button>
            <n-button
              v-if="!enabled"
              size="small"
              type="success"
              :loading="busy"
              @click="startScan"
            >
              启动扫描
            </n-button>
            <n-button v-else size="small" type="error" ghost :loading="busy" @click="stopScan">
              停止扫描
            </n-button>
          </div>
        </template>
        <p class="hint">
          勾选要扫的所（至少两个）。启动走 <code>/api/scan/start</code>，不需要套利配置里的交易对和数量。扫描不下单，与执行互斥。
        </p>
        <div class="venue-grid">
          <div
            v-for="v in venues"
            :key="v.id"
            class="venue-card"
            :class="{ on: scanIds.includes(v.id) }"
            @click="toggle(v.id, !scanIds.includes(v.id))"
          >
            <n-checkbox
              :checked="scanIds.includes(v.id)"
              @click.stop
              @update:checked="(c: boolean) => toggle(v.id, c)"
            />
            <div>
              <div class="vn">{{ v.label }}</div>
              <div class="vm">
                <i class="dot" :class="{ ready: v.keys_ready }" />
                {{ v.keys_ready ? "密钥已配置" : "仅监控" }} · {{ v.quote }}
              </div>
            </div>
          </div>
          <n-empty v-if="!venues.length" description="无法获取交易所列表" size="small" />
        </div>
      </n-card>

      <n-card size="small" title="扫描 / 历史" :bordered="true">
        <p class="hint">保存走现有 /api/config。未展示的扫描/历史项仍按 yaml 与当前内存值提交，本页不再改它们。</p>
        <AdvancedScan />
      </n-card>
    </div>

    <n-card size="small" class="result" :bordered="true">
      <template #header>
        扫描结果 · 按有效振幅 · 前 {{ topN }} 名
        <span class="muted">{{ scanHeadline }}</span>
      </template>
      <p class="legend">
        每个交易对在勾选所里两两打分，取 <strong>价差σ − Δ</strong> 最大的两个所。价差均值是枢纽 μ，只标注不排序。σ &lt; Δ 标为不够一格，排在后面。点差均值进 Δ。勾选变化会重选最优所对。各 DEX 列是该币在该所的 mid / 点差窗口均值；该所没这个币、没订到盘口或未满窗显示 —。
      </p>
      <div class="table-wrap">
        <table class="scan-table">
          <thead>
            <tr>
              <th rowspan="2"><HeadHint k="rank" /></th>
              <th rowspan="2" class="sticky"><HeadHint k="pair" /></th>
              <th rowspan="2"><HeadHint k="best" /></th>
              <th rowspan="2"><HeadHint k="edge" /></th>
              <th rowspan="2"><HeadHint k="sigma" /></th>
              <th rowspan="2"><HeadHint k="delta" /></th>
              <th rowspan="2"><HeadHint k="cross" /></th>
              <th rowspan="2"><HeadHint k="mu" /></th>
              <th rowspan="2"><HeadHint k="hubC" /></th>
              <th v-for="v in scanVenues" :key="'h1-' + v.id" colspan="2" class="dex">
                <HeadHint k="dex" :label="v.label" />
              </th>
            </tr>
            <tr>
              <template v-for="v in scanVenues" :key="'h2-' + v.id">
                <th><HeadHint k="mid" /></th>
                <th><HeadHint k="own" /></th>
              </template>
            </tr>
          </thead>
          <tbody>
            <tr v-if="scanVenues.length < 2 || !rankedRows.length">
              <td :colspan="emptyCols" class="empty">{{ emptyHint }}</td>
            </tr>
            <template v-else>
              <tr
                v-for="(row, i) in rankedRows"
                :key="row.pair"
                :class="{ dead: !row.eligible }"
              >
                <td class="tabular dim">{{ i + 1 }}</td>
                <td class="sticky pair">{{ row.pair }}</td>
                <td>
                  <span class="pair-tags">
                    <span class="ptag l">{{ shortVenue(row.left) }}</span>
                    <span class="x">×</span>
                    <span class="ptag r">{{ shortVenue(row.right) }}</span>
                    <span v-if="row.sameFamily" class="fam">同家族</span>
                  </span>
                </td>
                <td class="tabular" :class="row.eligible ? 'up' : 'dn'">{{ fmtSigned(row.edge) }}</td>
                <td class="tabular">{{ fmtNum(row.sigma) }}</td>
                <td class="tabular mu">{{ fmtNum(row.delta) }}</td>
                <td class="tabular">{{ row.crosses }}</td>
                <td class="tabular mu">{{ fmtSigned(row.mu) }}</td>
                <td class="tabular mu">{{ fmtNum(row.hubC) }}</td>
                <template v-for="v in scanVenues" :key="v.id + '-' + row.pair">
                  <td class="tabular" :class="{ pick: isPick(row, v.id) }">
                    {{ fmtMid(row.mids[v.id]) }}
                  </td>
                  <td class="tabular" :class="isPick(row, v.id) ? 'pick' : 'mu'">
                    {{ fmtNum(row.spreads[v.id]) }}
                  </td>
                </template>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useMessage } from "naive-ui";
import { apiFetch } from "../api";
import { payload, store } from "../store";
import { venueLabel } from "../format";
import type { LiveSnapshot, VenueMeta } from "../types";
import AdvancedScan from "./AdvancedScan.vue";
import HeadHint from "./ScanHeadHint.vue";

const props = defineProps<{ enabled: boolean; snap?: LiveSnapshot | null }>();
const emit = defineEmits<{ started: []; stopped: [] }>();

const msg = useMessage();
const busy = ref(false);

const fallbackVenues: VenueMeta[] = [
  { id: "lighter", label: "Lighter 主网", keys_ready: false, quote: "USDC" },
  { id: "lighter_rh", label: "Lighter RH", keys_ready: false, quote: "USDG" },
  { id: "sodex", label: "SoDEX", keys_ready: false, quote: "USDC" },
  { id: "entropy", label: "EntropyIO", keys_ready: false, quote: "USDC" },
];

type RankedRow = {
  pair: string;
  left: string;
  right: string;
  sigma: number;
  delta: number;
  edge: number;
  crosses: number;
  mu: number;
  hubC: number;
  eligible: boolean;
  sameFamily: boolean;
  mids: Record<string, number>;
  spreads: Record<string, number>;
};

const venues = computed(() => (store.venues.length ? store.venues : fallbackVenues));

const scanIds = computed(() => {
  if (store.scan_venues.length) return store.scan_venues;
  return venues.value.map((v) => v.id);
});

const scanVenues = computed(() => {
  const ids =
    props.enabled && props.snap?.scan?.venues?.length
      ? props.snap.scan.venues
      : scanIds.value;
  return ids
    .map((id) => venues.value.find((v) => v.id === id))
    .filter((v): v is VenueMeta => !!v);
});

const selectionChanged = computed(() => {
  if (!props.enabled) return false;
  const run = [...(props.snap?.scan?.venues || [])].sort().join(",");
  const sel = [...scanIds.value].sort().join(",");
  return !!run && !!sel && run !== sel;
});

const topN = computed(() => Math.max(1, Number(store.params.watch_top) || 20));
const emptyCols = computed(() => 9 + 2 * Math.max(scanVenues.value.length, 0));

const STATUS_ZH: Record<string, string> = {
  idle: "未启动",
  starting: "拉市场",
  coarse: "粗筛",
  sampling: "采样",
  live: "实时",
  error: "失败",
};

const scanHeadline = computed(() => {
  const s = props.snap?.scan;
  if (!props.enabled && (!s || s.status === "idle" || !s.status)) return "未启动";
  if (!s) return "启动中…";
  const bits = [STATUS_ZH[s.status] || s.status];
  if (s.error) bits.push(s.error);
  if (selectionChanged.value) bits.push("勾选已变，重新启动后生效");
  bits.push("宇宙 " + (s.universe ?? 0));
  bits.push("候选 " + (s.candidates ?? 0));
  bits.push("满窗 " + (s.filled_n ?? 0));
  bits.push("展示 " + ((s.rows && s.rows.length) || 0));
  return bits.join(" · ");
});

const emptyHint = computed(() => {
  if (scanVenues.value.length < 2 && !props.enabled) return "请在左侧至少勾选两个交易所";
  const s = props.snap?.scan;
  if (s?.error && (!props.enabled || s.status === "error")) return s.error;
  if (props.enabled && s?.status === "sampling") {
    const cap = Math.max(0, Number(s.window_samples) || 0);
    const n = Math.min(cap, Math.max(0, Number(s.window_n) || 0));
    if (cap > 0) {
      const intervalSec = Math.max(1, (Number(s.sample_interval_ms) || 1000) / 1000);
      const left = Math.max(0, cap - n);
      const sec = Math.ceil(left * intervalSec);
      return `候选已订阅，正在采满样本数… 倒计时 ${fmtCountdown(sec)}（${n}/${cap}）`;
    }
    return "候选已订阅，正在采满样本数…";
  }
  if (props.enabled && s?.status === "coarse") return "正在拉盘口做粗筛…";
  if (props.enabled && s?.status === "starting") return "正在拉市场并匹配…";
  if (props.enabled) return "暂无满窗结果（量门或粗筛后为空）";
  return "启动扫描后显示真实 σ−Δ 排序";
});

function fmtCountdown(sec: number) {
  const s = Math.max(0, Math.floor(sec));
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}:${String(r).padStart(2, "0")}`;
}

function parsePct(v: string | undefined): number | undefined {
  if (v == null || v === '' || v === '—') return undefined;
  const n = parseFloat(String(v).replace('%', '').replace('+', ''));
  return Number.isFinite(n) ? n : undefined;
}

function parseMidStr(v: string | undefined): number | undefined {
  if (v == null || v === '' || v === '—') return undefined;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : undefined;
}

const rankedRows = computed((): RankedRow[] => {
  if (!props.enabled) return [];
  const rows = props.snap?.scan?.rows || [];
  return rows.map((r) => {
    const mids: Record<string, number> = {};
    const spreads: Record<string, number> = {};
    for (const v of scanVenues.value) {
      const cell = r.venues?.[v.id];
      const mid = parseMidStr(cell?.mid_mean);
      const own = parsePct(cell?.own_spread_mean);
      if (mid != null) mids[v.id] = mid;
      if (own != null) spreads[v.id] = own;
    }
    return {
      pair: r.base,
      left: r.left,
      right: r.right,
      sigma: parsePct(r.sigma) ?? 0,
      delta: parsePct(r.delta) ?? 0,
      edge: parsePct(r.edge) ?? 0,
      crosses: r.crosses,
      mu: parsePct(r.mu) ?? 0,
      hubC: parsePct(r.hub_c) ?? 0,
      eligible: r.eligible,
      sameFamily: r.same_family,
      mids,
      spreads,
    };
  });
});

function shortVenue(id: string) {
  const label = venueLabel(id, venues.value);
  if (id === "lighter_rh") return "RH";
  if (id === "lighter") return "Lighter";
  return label;
}

function isPick(row: RankedRow, id: string) {
  return row.left === id || row.right === id;
}

function fmtNum(n: number | undefined, d = 4) {
  if (n == null || !Number.isFinite(n)) return "—";
  return n.toFixed(d);
}

function fmtSigned(n: number, d = 4) {
  if (!Number.isFinite(n)) return "—";
  const body = n.toFixed(d);
  return (n > 0 ? "+" : "") + body;
}

function fmtMid(n: number | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return n >= 100 ? n.toFixed(2) : n >= 1 ? n.toFixed(3) : n.toFixed(5);
}

function toggle(id: string, on: boolean) {
  const cur = scanIds.value;
  store.scan_venues = on ? [...new Set([...cur, id])] : cur.filter((x) => x !== id);
}

function scanPayload() {
  return {
    ...payload(),
    scan_enabled: true,
    scan_venues: [...scanIds.value],
    scan_window_samples: store.params.scan_window_samples,
  };
}

async function save() {
  busy.value = true;
  try {
    await apiFetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload()),
    });
    msg.success("扫描参数已写入运行时（不改 yaml；DEX 勾选仅本页）");
  } catch (e) {
    msg.error("保存失败: " + (e as Error).message);
  } finally {
    busy.value = false;
  }
}

async function startScan() {
  if (scanIds.value.length < 2) {
    msg.error("请至少勾选两个交易所再启动扫描");
    return;
  }
  busy.value = true;
  try {
    store.params.scan_enabled = true;
    store.params.scan_venues = [...scanIds.value];
    await apiFetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(scanPayload()),
    });
    await apiFetch("/api/scan/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ venues: [...scanIds.value] }),
    });
    msg.success("扫描已启动");
    emit("started");
  } catch (e) {
    msg.error("启动扫描失败: " + (e as Error).message);
  } finally {
    busy.value = false;
  }
}

async function stopScan() {
  busy.value = true;
  try {
    await apiFetch("/api/scan/stop", { method: "POST" });
    msg.success("扫描已停止");
    emit("stopped");
  } catch (e) {
    msg.error("停止失败: " + (e as Error).message);
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.layout {
  display: grid;
  grid-template-columns: minmax(320px, 0.9fr) minmax(560px, 1.35fr);
  gap: 16px;
  padding: 16px 20px 32px;
  align-items: start;
}
.main {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
}
.hdr-btns { display: flex; flex-wrap: wrap; gap: 8px; }
.hint { margin: 0 0 10px; font-size: 12px; color: #6b7f99; }
.hint code { color: #a78bfa; }
.legend { margin: 0 0 10px; font-size: 12px; color: #8fa5c0; line-height: 1.5; }
.muted { margin-left: 8px; font-size: 12px; font-weight: 400; color: #6b7f99; }
.venue-grid { display: flex; flex-wrap: wrap; gap: 8px; }
.venue-card {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 10px; border-radius: 8px; min-width: 140px;
  border: 1px solid #1e2d4a; background: #0d1320; cursor: pointer;
}
.venue-card.on { border-color: #8b5cf6; background: #8b5cf614; }
.vn { font-weight: 600; font-size: 13px; }
.vm { font-size: 11px; color: #8fa5c0; display: flex; align-items: center; gap: 4px; }
.dot { width: 7px; height: 7px; border-radius: 50%; background: #344d6e; display: inline-block; }
.dot.ready { background: #22c55e; }
.result { min-width: 0; }
.table-wrap { overflow: auto; max-height: calc(100vh - 220px); }
.scan-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
  white-space: nowrap;
}
.scan-table th,
.scan-table td {
  border: 1px solid #1e2d4a;
  padding: 7px 10px;
  text-align: center;
}
.scan-table thead th {
  background: #0d1320;
  color: #b8cce0;
  font-weight: 600;
  position: sticky;
  z-index: 2;
}
.scan-table thead tr:first-child th { top: 0; }
.scan-table thead tr:nth-child(2) th { top: 32px; }
.scan-table th.dex { color: #a78bfa; }
.scan-table td.pair { font-weight: 600; color: #dde6f0; }
.scan-table .sticky { position: sticky; left: 0; background: #121929; z-index: 1; }
.scan-table thead .sticky { z-index: 3; background: #0d1320; }
.scan-table tbody tr:hover td { background: #8b5cf610; }
.scan-table tbody tr:hover td.sticky { background: #1a2233; }
.scan-table tbody tr.dead td { opacity: 0.55; }
.scan-table tbody tr.dead:hover td { opacity: 0.8; }
.scan-table .empty { color: #6b7f99; text-align: left; }
.scan-table .dim { color: #6b7f99; }
.scan-table td.pick { background: #8b5cf61f; color: #e9d5ff; }
.pair-tags { display: inline-flex; align-items: center; gap: 4px; }
.ptag {
  padding: 1px 6px; border-radius: 4px; font-size: 11px; font-weight: 600;
}
.ptag.l { background: #1d4ed833; color: #93c5fd; }
.ptag.r { background: #7c3aed33; color: #c4b5fd; }
.x { color: #6b7f99; font-size: 11px; }
.fam { font-size: 10px; color: #67e8f9; }
@media (max-width: 1100px) {
  .layout { grid-template-columns: 1fr; }
}
</style>
