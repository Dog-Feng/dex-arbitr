<template>
  <div class="g">
    <Field label="leverage_multiplier" hint="容量校验杠杆。占用保证金 = 名义 / 杠杆">
      <n-input :value="String(p.leverage_multiplier)" @update:value="p.leverage_multiplier = $event" />
    </Field>
    <Field label="depth_pct" hint="单笔不得超过 L1 数量的这个比例">
      <n-input :value="String(p.depth_pct)" @update:value="p.depth_pct = $event" />
    </Field>
    <Field label="margin_utilization_pct" hint="可用余额最多用到多少">
      <n-input :value="String(p.margin_utilization_pct)" @update:value="p.margin_utilization_pct = $event" />
    </Field>
    <Field label="refresh_balance_secs" hint="余额和交易所持仓刷新间隔（秒）。保存后立即按新间隔拉各所账户">
      <n-input-number v-model:value="p.refresh_balance_secs" :min="1" :step="1" />
    </Field>
    <Field label="fallback_available_usdc" hint="余额接口为空时的回退值">
      <n-input :value="String(p.fallback_available_usdc)" @update:value="p.fallback_available_usdc = $event" />
    </Field>
    <Field label="data_freshness_ms" hint="盘口超过此毫秒未更新视为过期">
      <n-input-number v-model:value="p.data_freshness_ms" :min="100" :step="100" />
    </Field>
    <Field label="hedge_failed_legs" hint="第二腿失败时自动补市价对冲">
      <n-switch v-model:value="p.hedge_failed_legs" />
    </Field>
    <Field label="loop_interval_ms" hint="决策环 tick 间隔">
      <n-input-number v-model:value="p.loop_interval_ms" :min="10" :step="10" />
    </Field>
    <Field label="limit_timeout_ms" hint="非邻档追逐型限价单轮最长等待。邻档第一腿不走这个">
      <n-input-number v-model:value="p.limit_timeout_ms" :min="100" :step="100" />
    </Field>
    <Field label="adjacent_timeout_ms" hint="邻档第一腿存活。0 = 只按事件撤（对面先成 / 边没了 / 改价），不按秒转市价">
      <n-input-number v-model:value="p.adjacent_timeout_ms" :min="0" :step="1000" />
    </Field>
    <Field label="maker_inside_ticks" hint="maker 腿往点差内侧挪几个 tick">
      <n-input-number v-model:value="p.maker_inside_ticks" :min="0" />
    </Field>
    <Field label="limit_retry_count" hint="第一腿最多重试轮数">
      <n-input-number v-model:value="p.limit_retry_count" :min="1" />
    </Field>
    <Field label="default_slip_pct（%）" hint="超过此值记 overrun">
      <n-input :value="String(p.default_slip_pct)" @update:value="p.default_slip_pct = $event" />
    </Field>
    <Field label="max_slippage_pct（%）" hint="市价单滑点保护上限">
      <n-input :value="String(p.max_slippage_pct)" @update:value="p.max_slippage_pct = $event" />
    </Field>
    <Field label="emergency_slippage_multiplier" hint="平仓/补对冲时滑点放大倍数">
      <n-input :value="String(p.emergency_slippage_multiplier)" @update:value="p.emergency_slippage_multiplier = $event" />
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
