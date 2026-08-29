<template>
  <div class="g">
    <Field label="watch_top" hint="扫描结果最多保留几行。前端表格与扫描环日志都按此截断。">
      <n-input-number v-model:value="p.watch_top" :min="1" />
    </Field>
    <Field label="analysis_interval_ms" hint="扫描环间隔（毫秒）。已对接 loop_scan，保存后下一拍生效，下限 10。">
      <n-input-number v-model:value="p.analysis_interval_ms" :min="10" :step="10" />
    </Field>
    <Field label="样本数" hint="扫描窗满这么多个秒级点才打分（scan.window_samples）。与格子窗分开。保存后热更新。">
      <n-input-number v-model:value="p.scan_window_samples" :min="10" />
    </Field>
  </div>
</template>

<script setup lang="ts">
import { store } from "../store";
import Field from "./Field.vue";
const p = store.params;
</script>

<style scoped>
.g { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 10px 16px; padding: 8px 4px 12px; min-width: 0; }
@media (max-width: 900px) { .g { grid-template-columns: 1fr; } }
</style>
