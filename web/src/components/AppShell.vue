<template>
  <div class="shell">
    <header class="hdr">
      <div class="logo">DEX <span>套利</span></div>
      <div class="sep" />
      <div class="chips">
        <span v-for="id in venueIds" :key="id" class="chip" :class="{ ok: connected(id) }">
          <i class="dot" />{{ venueLabel(id, venues) }}
        </span>
      </div>
      <div class="right">
        <n-tag :type="enabled ? 'success' : 'default'" round size="small">
          {{ enabled ? "套利运行中" : "未启动" }}
        </n-tag>
        <n-tag v-if="offline" type="error" round size="small">与后端断开</n-tag>
        <span class="clock">{{ clock }}</span>
      </div>
    </header>

    <div class="status">
      <div class="stat" v-for="id in venueIds" :key="'b'+id">
        <div class="lbl">{{ venueLabel(id, venues) }}</div>
        <div class="val tabular">{{ bal(id) }}</div>
        <div class="sub">总额 · {{ venueQuote(id, venues) }}</div>
      </div>
      <div class="stat">
        <div class="lbl">匹配对</div>
        <div class="val tabular">{{ snap?.stats?.matched_pairs ?? "—" }}</div>
        <div class="sub">永续 · 跨所</div>
      </div>
      <div class="stat">
        <div class="lbl">交易所持仓</div>
        <div class="val tabular">{{ (snap?.exchange_positions || []).length }}</div>
        <div class="sub">{{ exDetail }}</div>
      </div>
      <div class="stat">
        <div class="lbl">模式</div>
        <div class="val">{{ modeLabel }}</div>
        <div class="sub">状态</div>
      </div>
    </div>

    <n-tabs v-model:value="tab" type="line" class="tabs">
      <n-tab-pane name="config" tab="套利配置">
        <ConfigPage :enabled="enabled" :snap="snap" @saved="onSaved" @started="onStarted" @stopped="onStopped" />
      </n-tab-pane>
      <n-tab-pane name="tape" tab="执行带">
        <TapePage :rows="execs" />
      </n-tab-pane>
      <n-tab-pane name="scan" tab="套利扫描">
        <ScanPage :enabled="scanOn" :snap="snap" @started="onScanStarted" @stopped="onScanStopped" />
      </n-tab-pane>
    </n-tabs>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { useMessage } from "naive-ui";
import { venueLabel, venueQuote } from "../format";
import { apiFetch } from "../api";
import { applyParams, applyVenues, store } from "../store";
import type { ExecRow, LiveSnapshot, VenueMeta } from "../types";
import ConfigPage from "./ConfigPage.vue";
import TapePage from "./TapePage.vue";
import ScanPage from "./ScanPage.vue";

const tab = ref("config");
const clock = ref("");
const enabled = ref(false);
const snap = ref<LiveSnapshot | null>(null);
const execs = ref<ExecRow[]>([]);
const offline = ref(false);
const msg = useMessage();
let pollFail = 0;
let toldOffline = false;
const venues = computed(() => store.venues);
const venueIds = computed(() => {
  const ids = store.venues.map((v) => v.id);
  if (ids.length) return ids;
  return ["lighter", "lighter_rh", "sodex", "entropy"];
});

const modeLabel = computed(() => {
  if (!snap.value) return "—";
  return "实盘";
});

const scanOn = computed(() => !!snap.value?.scan_running);

const exDetail = computed(() => {
  const rows = snap.value?.exchange_positions || [];
  if (!rows.length) return "各所原始仓";
  return (
    rows
      .slice(0, 3)
      .map((p) => `${venueLabel(p.venue, store.venues)} ${p.symbol} ${p.qty}`)
      .join(" · ") + (rows.length > 3 ? " …" : "")
  );
});

function connected(id: string) {
  return (snap.value?.balances || []).some((b) => b.venue === id);
}
function bal(id: string) {
  const b = (snap.value?.balances || []).find((x) => x.venue === id);
  if (!b) return "—";
  const total = parseFloat(b.total);
  const avail = parseFloat(b.available);
  const n = !Number.isNaN(total) && total > 0 ? total : avail;
  return Number.isNaN(n) ? "—" : n.toFixed(2);
}

let clockT: number;
let snapT: number;
let tapeT: number;
let snapBusy = false;
let tapeBusy = false;
const pollAc = new AbortController();

function tickClock() {
  clock.value = new Date().toLocaleString("zh-CN", { hour12: false });
}

function notePollOk() {
  pollFail = 0;
  if (offline.value) {
    offline.value = false;
    toldOffline = false;
  }
}

function notePollFail() {
  pollFail += 1;
  if (pollFail >= 3) {
    offline.value = true;
    if (!toldOffline) {
      toldOffline = true;
      msg.error("与后端失去连接");
    }
  }
}

async function loadConfig() {
  try {
    const r = await apiFetch<{ enabled: boolean; params: import("../types").ArbitrageParams }>("/api/config");
    enabled.value = r.enabled;
    applyParams(r.params);
  } catch {
    /* 页面仍可显示 */
  }
  try {
    const vr = await apiFetch<{ venues: VenueMeta[] }>("/api/venues");
    applyVenues(vr.venues || []);
  } catch {
    /* ignore */
  }
}

async function pollSnap() {
  if (snapBusy) return;
  snapBusy = true;
  try {
    const s = await apiFetch<LiveSnapshot>("/api/snapshot", { signal: pollAc.signal });
    snap.value = s;
    enabled.value = !!s.arbitrage_enabled;
    notePollOk();
  } catch (e) {
    if ((e as Error).name !== "AbortError") notePollFail();
  } finally {
    snapBusy = false;
  }
}

async function pollTape() {
  if (tapeBusy) return;
  tapeBusy = true;
  try {
    const r = await apiFetch<{ executions: ExecRow[] }>("/api/executions", { signal: pollAc.signal });
    execs.value = (r.executions || []).slice(0, 50);
    notePollOk();
  } catch (e) {
    if ((e as Error).name !== "AbortError") notePollFail();
  } finally {
    tapeBusy = false;
  }
}

function onSaved() {}
function onStarted() {
  enabled.value = true;
}
function onStopped() {
  enabled.value = false;
}
function onScanStarted() {}
function onScanStopped() {}

onMounted(() => {
  tickClock();
  clockT = window.setInterval(tickClock, 1000);
  loadConfig();
  pollSnap();
  pollTape();
  snapT = window.setInterval(pollSnap, 250);
  tapeT = window.setInterval(pollTape, 2000);
});
onUnmounted(() => {
  clearInterval(clockT);
  clearInterval(snapT);
  clearInterval(tapeT);
  pollAc.abort();
});
</script>

<style scoped>
.shell { min-height: 100vh; }
.hdr {
  display: flex; align-items: center; gap: 12px; height: 52px;
  padding: 0 20px; background: #121929; border-bottom: 1px solid #1e2d4a;
  position: sticky; top: 0; z-index: 40;
}
.logo { font-size: 16px; font-weight: 700; }
.logo span { color: #a78bfa; }
.sep { width: 1px; height: 18px; background: #253350; }
.chips { display: flex; gap: 8px; flex-wrap: wrap; }
.chip {
  display: inline-flex; align-items: center; gap: 5px; padding: 3px 8px;
  border-radius: 20px; font-size: 11px; font-weight: 600;
  border: 1px solid #253350; background: #0d1320; color: #8fa5c0;
}
.chip .dot { width: 7px; height: 7px; border-radius: 50%; background: #344d6e; }
.chip.ok .dot { background: #22c55e; box-shadow: 0 0 5px #22c55e; }
.right { margin-left: auto; display: flex; align-items: center; gap: 10px; }
.clock { font-size: 12px; color: #8fa5c0; font-variant-numeric: tabular-nums; }
.status {
  position: sticky; top: 52px; z-index: 30;
  display: flex; flex-wrap: wrap; gap: 10px;
  padding: 10px 20px 12px; background: #0d1320; border-bottom: 1px solid #1e2d4a;
}
.stat {
  background: #121929; border: 1px solid #1e2d4a; border-radius: 8px;
  padding: 10px 14px; flex: 1 1 120px; min-width: 0;
}
.stat .lbl { font-size: 10px; color: #8fa5c0; text-transform: uppercase; letter-spacing: .4px; }
.stat .val { font-size: 18px; font-weight: 700; margin: 2px 0; }
.stat .sub { font-size: 11px; color: #344d6e; }
.tabs { padding: 0 12px; }
:deep(.n-tabs-nav) { padding: 0 8px; background: #121929; }
:deep(.n-tab-pane) { padding: 0; }
</style>
