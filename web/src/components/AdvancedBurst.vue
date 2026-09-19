<template>
  <div class="g">
    <Field label="burst.enabled" hint="阶段 2：L1 限价重挂 + 第二腿市价。开启后不走 μ/STEP 开仓；且只能配置 1 个交易对">
      <n-switch v-model:value="p.burst.enabled" />
    </Field>
    <Field label="open_repeats" hint="一轮宏观周期内连续开仓次数，之后进入冷却并平到 0">
      <n-input-number v-model:value="p.burst.open_repeats" :min="1" :max="1000" />
    </Field>
    <Field label="total_rounds" hint="大循环总轮次：每轮 = 上述开满 + 平到 0 算 1 次。0 = 不限">
      <n-input-number v-model:value="p.burst.total_rounds" :min="0" :max="100000" />
    </Field>
    <Field label="pause_ms_min / max" hint="每次开/平 rep 完成后的随机暂停（毫秒）">
      <div class="pair">
        <n-input-number v-model:value="p.burst.pause_ms_min" :min="0" :step="500" />
        <n-input-number v-model:value="p.burst.pause_ms_max" :min="0" :step="500" />
      </div>
    </Field>
    <Field label="cooldown_ms_min / max" hint="open_repeats 用尽后的随机冷却（毫秒），之后平到 0 再开">
      <div class="pair">
        <n-input-number v-model:value="p.burst.cooldown_ms_min" :min="0" :step="1000" />
        <n-input-number v-model:value="p.burst.cooldown_ms_max" :min="0" :step="1000" />
      </div>
    </Field>
    <Field label="limit_rehang_timeout_ms" hint="首腿 L1 挂单最长等待，超时撤单重挂（≥200）">
      <n-input-number v-model:value="p.burst.limit_rehang_timeout_ms" :min="200" :step="100" />
    </Field>
    <Field label="hedge_max_attempts" hint="第二腿市价对冲失败重试次数，用尽则停发单并人工介入">
      <n-input-number v-model:value="p.burst.hedge_max_attempts" :min="1" :max="100" />
    </Field>
  </div>
</template>

<script setup lang="ts">
import { store } from "../store";
import Field from "./Field.vue";
const p = store.params;
</script>

<style scoped>
.g { display: grid; gap: 10px; }
.pair { display: flex; gap: 8px; flex-wrap: wrap; }
</style>
