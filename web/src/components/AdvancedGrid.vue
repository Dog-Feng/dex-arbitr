<template>
  <div class="g">
    <Field label="initial_spread_threshold（%）" hint="T1：毛价差 ≥ T1 并过持续性才开。可被单个交易对或所对覆盖">
      <n-input :value="str('initial_spread_threshold')" @update:value="set('initial_spread_threshold', $event)" />
    </Field>
    <Field label="grid_step（%）" hint="相邻格间距。应大于该所对往返手续费">
      <n-input :value="str('grid_step')" @update:value="set('grid_step', $event)" />
    </Field>
    <Field label="max_segments" hint="默认最多同时持几格">
      <n-input-number v-model:value="store.params.pair_defaults.max_segments" :min="1" :max="20" />
    </Field>
    <Field label="t0_ratio" hint="T0 = T1 × 该系数。跌破 T0 全平">
      <n-input :value="str('t0_ratio')" @update:value="set('t0_ratio', $event)" />
    </Field>
    <Field label="split_order_size（0=不拆）" hint="单笔最大开/平仓量，0 = 一次下完">
      <n-input :value="str('split_order_size')" @update:value="set('split_order_size', $event)" />
    </Field>
    <Field label="persistence_mode" hint="默认秒桶。window 是连续毫秒窗口">
      <n-select v-model:value="store.params.persistence_mode" :options="[
        { label: 'bucket（参考秒桶）', value: 'bucket' },
        { label: 'window（persistence_ms）', value: 'window' },
      ]" />
    </Field>
    <Field label="spread_persistence_seconds" hint="秒桶要连续满足的秒数。≤1 不累计">
      <n-input-number v-model:value="store.params.spread_persistence_seconds" :min="0" />
    </Field>
    <Field label="strict_persistence_check" hint="严格秒桶。关掉则每秒至少一次达标即可">
      <n-switch v-model:value="store.params.strict_persistence_check" />
    </Field>
    <Field label="persistence_ms" hint="仅 window 模式">
      <n-input-number v-model:value="store.params.persistence_ms" :min="0" :step="100" />
    </Field>
    <Field label="scalping_enabled" hint="分段网格剥头皮。默认关">
      <n-switch v-model:value="store.params.pair_defaults.scalping_enabled" />
    </Field>
    <Field label="scalping_trigger_segment" hint="进入剥头皮的开仓视角格子">
      <n-input-number v-model:value="store.params.pair_defaults.scalping_trigger_segment" :min="1" />
    </Field>
    <Field label="scalping_profit_threshold_pct（%）" hint="建仓均毛价差 − 当前剩余毛价差 ≥ 该值才减">
      <n-input :value="str('scalping_profit_threshold_pct')" @update:value="set('scalping_profit_threshold_pct', $event)" />
    </Field>
  </div>
</template>

<script setup lang="ts">
import { store } from "../store";
import Field from "./Field.vue";
import type { PairDefaults } from "../types";

function str(k: keyof PairDefaults) {
  return String(store.params.pair_defaults[k] ?? "");
}
function set(k: keyof PairDefaults, v: string) {
  (store.params.pair_defaults as Record<string, unknown>)[k] = v;
}
</script>

<style scoped>
.g { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px 16px; padding: 8px 4px 12px; min-width: 0; }
@media (max-width: 900px) { .g { grid-template-columns: 1fr; } }
</style>
