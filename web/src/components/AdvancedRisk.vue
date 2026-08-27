<template>
  <div>
    <h5>盘口质量</h5>
    <div class="g">
      <Field label="max_venue_spread_pct（%）" hint="单所买卖点差上限。0=不检查">
        <n-input :value="String(p.max_venue_spread_pct)" @update:value="p.max_venue_spread_pct = $event" />
      </Field>
      <Field label="price_stability_window_secs" hint="稳定性窗口。0=关闭">
        <n-input :value="String(p.price_stability_window_secs)" @update:value="p.price_stability_window_secs = $event" />
      </Field>
      <Field label="price_stability_threshold_pct（%）" hint="窗口内极值波动上限">
        <n-input :value="String(p.price_stability_threshold_pct)" @update:value="p.price_stability_threshold_pct = $event" />
      </Field>
    </div>
    <Field label="min_book_qty（每行 币:数量）" hint="未列出的币退到两腿 min_qty">
      <n-input type="textarea" :rows="3" :value="bqText" @update:value="onBq" placeholder="BTC:0.001&#10;ETH:0.01" />
    </Field>
    <h5>Reduce-only / 费率 / 限额</h5>
    <div class="g">
      <Field label="reduce_only_probe_enabled" hint="拉闸后每小时探针，能开就解闸">
        <n-switch v-model:value="p.reduce_only_probe_enabled" />
      </Field>
      <Field label="reduce_only_probe_second" hint="每小时第几秒触发">
        <n-input-number v-model:value="p.reduce_only_probe_second" :min="0" :max="59" />
      </Field>
      <Field label="funding_annual_threshold_pct" hint="年化净支付超此值拒开仓。0=关">
        <n-input :value="String(p.funding_annual_threshold_pct)" @update:value="p.funding_annual_threshold_pct = $event" />
      </Field>
      <Field label="funding_unfavorable_duration_minutes" hint="费率不利持续多久才平">
        <n-input-number v-model:value="p.funding_unfavorable_duration_minutes" :min="0" :step="5" />
      </Field>
      <Field label="funding_refresh_secs" hint="资金费率刷新间隔">
        <n-input-number v-model:value="p.funding_refresh_secs" :min="10" :step="10" />
      </Field>
      <Field label="max_daily_opens" hint="每日最大开仓次数。0=不限">
        <n-input-number v-model:value="p.max_daily_opens" :min="0" />
      </Field>
      <Field label="max_position_hours" hint="超时全平。0=不限">
        <n-input-number v-model:value="p.max_position_hours" :min="0" />
      </Field>
      <Field label="min_balance_warn_usdc" hint="低于此值停开仓。0=关">
        <n-input :value="String(p.min_balance_warn_usdc)" @update:value="p.min_balance_warn_usdc = $event" />
      </Field>
      <Field label="min_balance_close_usdc" hint="低于此值强制全平，须 < warn">
        <n-input :value="String(p.min_balance_close_usdc)" @update:value="p.min_balance_close_usdc = $event" />
      </Field>
      <Field label="max_single_token_notional_usdc" hint="单币最大名义。0=不限">
        <n-input :value="String(p.max_single_token_notional_usdc)" @update:value="p.max_single_token_notional_usdc = $event" />
      </Field>
      <Field label="max_total_notional_usdc" hint="全部持仓名义之和。0=不限">
        <n-input :value="String(p.max_total_notional_usdc)" @update:value="p.max_total_notional_usdc = $event" />
      </Field>
      <Field label="backoff_min_secs" hint="首次错误退避">
        <n-input-number v-model:value="p.backoff_min_secs" :min="10" :step="10" />
      </Field>
      <Field label="backoff_max_secs" hint="退避上限">
        <n-input-number v-model:value="p.backoff_max_secs" :min="60" :step="60" />
      </Field>
      <Field label="backoff_multiplier" hint="连续出错乘数">
        <n-input-number v-model:value="p.backoff_multiplier" :min="1" />
      </Field>
      <Field label="backoff_reset_secs" hint="多久无新错误清零阶梯">
        <n-input-number v-model:value="p.backoff_reset_secs" :min="30" :step="30" />
      </Field>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { store } from "../store";
import Field from "./Field.vue";

const p = store.params;
const bqText = computed(() =>
  Object.entries(p.min_book_qty || {})
    .map(([k, v]) => `${k}:${v}`)
    .join("\n")
);
function onBq(v: string) {
  const out: Record<string, string> = {};
  v.split("\n").forEach((line) => {
    const parts = line.split(":");
    if (parts.length >= 2) {
      const k = parts[0].trim();
      const val = parts.slice(1).join(":").trim();
      if (k && val) out[k] = val;
    }
  });
  p.min_book_qty = out;
}
</script>

<style scoped>
h5 { margin: 8px 0 6px; font-size: 11px; letter-spacing: .5px; text-transform: uppercase; color: #6b7f99; }
.g { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px 16px; padding: 4px 0 12px; min-width: 0; }
@media (max-width: 900px) { .g { grid-template-columns: 1fr; } }
</style>
