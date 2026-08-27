<template>
  <div class="bar">
    <n-button size="small" type="primary" :loading="busy" @click="save">保存配置</n-button>
    <n-button v-if="!enabled" size="small" type="success" :loading="busy" @click="start">启动套利</n-button>
    <n-button v-else size="small" type="error" ghost :loading="busy" @click="stop">停止套利</n-button>
    <n-button size="small" quaternary :disabled="busy" @click="reset">重置为 yaml 默认</n-button>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { useMessage } from "naive-ui";
import { apiFetch } from "../api";
import { applyParams, payload, store } from "../store";
import { collectPairIssues } from "../validation";
import { parseNum } from "../format";
import type { ArbitrageParams, LiveSnapshot } from "../types";

defineProps<{ enabled: boolean; snap: LiveSnapshot | null }>();
const emit = defineEmits<{ saved: []; started: []; stopped: [] }>();
const msg = useMessage();
const busy = ref(false);

function issues() {
  const d = store.params.pair_defaults;
  return collectPairIssues(
    store.drafts,
    store.params.active_venues,
    parseNum(d.initial_spread_threshold) ?? 0.05,
    parseNum(d.grid_step) ?? 0.05,
    parseNum(d.t0_ratio) ?? 0.4
  );
}

function formatList(list: string[]) {
  const max = 8;
  if (list.length <= max) return list.join("；");
  return list.slice(0, max).join("；") + `（共 ${list.length} 条）`;
}

function show(text: string, type: "success" | "warning" | "error") {
  if (type === "error") msg.error(text, { duration: 5000 });
  else if (type === "warning") msg.warning(text, { duration: 5000 });
  else msg.success(text);
}

async function save() {
  const iss = issues();
  if (iss.errors.length) {
    show("无法保存：" + formatList(iss.errors), "error");
    return;
  }
  busy.value = true;
  try {
    await apiFetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload()),
    });
    if (iss.warnings.length) show("已写入运行时。注意：" + formatList(iss.warnings), "warning");
    else show("配置已写入运行时（不改 yaml；重启后回到文件默认）", "success");
    emit("saved");
  } catch (e) {
    show("保存失败: " + (e as Error).message, "error");
  } finally {
    busy.value = false;
  }
}

async function start() {
  if (store.params.active_venues.length < 2) {
    show("请至少选择两个交易所再启动", "error");
    return;
  }
  const pairs = payload().pairs;
  if (!pairs.length) {
    show("请至少填写一个交易对和每格数量", "error");
    return;
  }
  const iss = issues();
  if (iss.errors.length) {
    show("无法启动：" + formatList(iss.errors), "error");
    return;
  }
  busy.value = true;
  try {
    await apiFetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload()),
    });
    await apiFetch("/api/arbitrage/start", { method: "POST" });
    if (iss.warnings.length) show("套利已启动。注意：" + formatList(iss.warnings), "warning");
    else show("套利已启动，正在按所选交易所匹配交易对", "success");
    emit("started");
  } catch (e) {
    show("启动失败: " + (e as Error).message, "error");
  } finally {
    busy.value = false;
  }
}

async function stop() {
  busy.value = true;
  try {
    await apiFetch("/api/arbitrage/stop", { method: "POST" });
    show("套利已停止（持有仓位继续平仓）", "success");
    emit("stopped");
  } catch (e) {
    show("停止失败: " + (e as Error).message, "error");
  } finally {
    busy.value = false;
  }
}

async function reset() {
  try {
    const r = await apiFetch<{ params: ArbitrageParams }>("/api/config/defaults");
    applyParams(r.params);
    show("已恢复为启动时的 yaml 默认值（尚未写入运行时，需保存或启动）", "success");
  } catch (e) {
    show("重置失败: " + (e as Error).message, "error");
  }
}
</script>

<style scoped>
.bar { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-end; gap: 8px; }
</style>
