<template>
  <div class="g">
    <Field label="symmetric_limit" hint="阶段 2：满窗后在 μ±Δ 挂邻档第一腿，成交推进 STEP。关=阶段 1 撞线双市价">
      <n-switch v-model:value="store.params.symmetric_limit" />
    </Field>
    <Field label="quote_reprice_ratio" hint="空仓 |μ_live−μ_quote| ≥ 此×Δ 才撤重挂">
      <n-input :value="String(store.params.quote_reprice_ratio)" @update:value="store.params.quote_reprice_ratio = $event" />
    </Field>
    <Field label="min_quote_gap_ratio" hint="加仓档离当前可执行价差至少这么多格才挂。减仓档不限">
      <n-input :value="String(store.params.min_quote_gap_ratio)" @update:value="store.params.min_quote_gap_ratio = $event" />
    </Field>
    <Field label="target_bp（目标净利）" hint="一格开平扣完费和点差后的目标（bp）。1 bp = 0.01%。阶段 2：F=2×(挂单所 maker + 市价所 taker)，C=市价所点差中枢；阶段 1 仍用四腿 taker + 两所平均">
      <n-input :value="str('target_bp')" @update:value="set('target_bp', $event)" />
    </Field>
    <Field label="max_segments（max_step）" hint="|STEP| 上限。到 ±N 停加不停减。过零必须先回 0">
      <n-input-number v-model:value="store.params.pair_defaults.max_segments" :min="1" :max="20" />
    </Field>
    <Field label="window_samples" hint="满这么多个秒级点才有 μ 和各所点差中枢；点差中枢折进 Δ。代码默认 10000 ≈ 2h47m；yaml 现为 1000 ≈ 17 分钟">
      <n-input-number v-model:value="store.params.window_samples" :min="1" :step="100" />
    </Field>
    <Field label="sample_interval_ms" hint="入窗间隔。1000 = 1Hz；同一间隔内多次盘口覆盖为最后一次">
      <n-input-number v-model:value="store.params.sample_interval_ms" :min="1" :step="100" />
    </Field>
    <Field label="step_hysteresis（格）" hint="加仓 raw ≥ k+1−h，减仓 raw ≤ k−1+h。0.25 时一格只锁 0.5Δ；0 = 满格才开、回到 μ 才平。必须 &lt; 0.5">
      <n-input :value="hystStr" @update:value="setHyst($event)" />
    </Field>
    <Field label="persistence_ms" hint="检查时间窗（毫秒）。1000 + 决策环 100ms ≈ 1 秒查 10 次">
      <n-input-number v-model:value="store.params.persistence_ms" :min="0" :step="100" />
    </Field>
    <Field label="persistence_min_hits" hint="这 1 秒里至少几次达标即通过（5 或 7）。0 = 连续不掉线">
      <n-input-number v-model:value="store.params.persistence_min_hits" :min="0" />
    </Field>
    <Field label="split_order_size（0=不拆）" hint="单笔最大开/平仓量，0 = 一次下完">
      <n-input :value="str('split_order_size')" @update:value="set('split_order_size', $event)" />
    </Field>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { store } from "../store";
import Field from "./Field.vue";
import type { PairDefaults } from "../types";

function str(k: keyof PairDefaults) {
  const v = store.params.pair_defaults[k];
  return v == null ? "" : String(v);
}
function set(k: keyof PairDefaults, v: string | null) {
  (store.params.pair_defaults as Record<string, unknown>)[k] = v ?? "";
}

const hystStr = computed(() => String(store.params.step_hysteresis ?? "0"));
function setHyst(v: string | null) {
  store.params.step_hysteresis = v ?? "0";
}
</script>

<style scoped>
.g { display: grid; gap: 10px; }
</style>
