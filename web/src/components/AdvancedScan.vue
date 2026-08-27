<template>
  <div class="g">
    <Field label="scan_enabled" hint="启动纯监控扫描环">
      <n-switch v-model:value="p.scan_enabled" />
    </Field>
    <Field label="cross_use_natural" hint="跨 DEX 用天然基差过滤">
      <n-switch v-model:value="p.cross_use_natural" />
    </Field>
    <Field label="min_spread_pct" hint="上报最低毛价差（%）">
      <n-input :value="String(p.min_spread_pct)" @update:value="p.min_spread_pct = $event" />
    </Field>
    <Field label="watch_top" hint="展示 Top N">
      <n-input-number v-model:value="p.watch_top" :min="1" />
    </Field>
    <Field label="analysis_interval_ms" hint="扫描环间隔">
      <n-input-number v-model:value="p.analysis_interval_ms" :min="10" :step="10" />
    </Field>
    <Field label="scan_log_interval_secs" hint="重复上榜最小间隔">
      <n-input-number v-model:value="p.scan_log_interval_secs" :min="5" :step="5" />
    </Field>
    <Field label="history_enabled" hint="启用天然基差 SQLite">
      <n-switch v-model:value="p.history_enabled" />
    </Field>
    <Field label="history_window_hours" hint="中位数窗口">
      <n-input-number v-model:value="p.history_window_hours" :min="1" />
    </Field>
    <Field label="history_min_points" hint="最少样本点数">
      <n-input-number v-model:value="p.history_min_points" :min="1" />
    </Field>
    <Field label="history_sample_interval_secs" hint="毛价差采样间隔">
      <n-input-number v-model:value="p.history_sample_interval_secs" :min="1" />
    </Field>
    <Field label="history_refresh_interval_secs" hint="重算中位数间隔">
      <n-input-number v-model:value="p.history_refresh_interval_secs" :min="60" :step="60" />
    </Field>
  </div>
</template>

<script setup lang="ts">
import { store } from "../store";
import Field from "./Field.vue";
const p = store.params;
</script>

<style scoped>
.g { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px 16px; padding: 8px 4px 12px; min-width: 0; }
@media (max-width: 900px) { .g { grid-template-columns: 1fr; } }
</style>
