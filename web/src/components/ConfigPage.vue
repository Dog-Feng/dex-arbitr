<template>
  <div class="layout">
    <div class="main">
      <n-card size="small" :bordered="true">
        <template #header>常用</template>
        <template #header-extra>
          <CtrlAside :enabled="enabled" :snap="snap" @saved="emit('saved')" @started="emit('started')" @stopped="emit('stopped')" />
        </template>
        <p class="lead">每天开盘要碰的只有这三项。其余参数在下方「高级」。</p>

        <h4>参与套利的交易所</h4>
        <p class="hint">至少选两个。未选的所不参与开仓，已有持仓会正常平。</p>
        <div class="venue-grid">
          <div
            v-for="v in store.venues"
            :key="v.id"
            class="venue-card"
            :class="{ on: store.params.active_venues.includes(v.id) }"
            @click="toggleVenue(v.id, !store.params.active_venues.includes(v.id))"
          >
            <n-checkbox
              :checked="store.params.active_venues.includes(v.id)"
              @click.stop
              @update:checked="(c: boolean) => toggleVenue(v.id, c)"
            />
            <div>
              <div class="vn">{{ v.label }}</div>
              <div class="vm">
                <i class="dot" :class="{ ready: v.keys_ready }" />
                {{ v.keys_ready ? "密钥已配置" : "仅监控" }} · {{ v.quote }}
              </div>
            </div>
          </div>
          <n-empty v-if="!store.venues.length" description="无法获取交易所列表" size="small" />
        </div>

        <n-divider />

        <h4>交易对</h4>
        <p class="hint">手填交易对（如 BTC）和每格数量。启动后才按所选交易所去各所匹配、订阅。</p>
        <PairList />

        <n-divider />

        <div class="quick">
          <Field label="paper_trading" hint="模拟成交，不向交易所发单">
            <n-switch v-model:value="store.params.paper_trading" />
          </Field>
          <Field label="monitor_only" hint="只监控价差、不发单">
            <n-switch v-model:value="store.params.monitor_only" />
          </Field>
          <Field label="max_concurrent_pairs" hint="同时持仓槽位数">
            <n-input-number v-model:value="store.params.max_concurrent_pairs" :min="1" :step="1" />
          </Field>
          <Field label="leverage_multiplier" hint="容量校验杠杆">
            <n-input :value="String(store.params.leverage_multiplier)" @update:value="store.params.leverage_multiplier = $event" />
          </Field>
        </div>
      </n-card>

      <n-card size="small" title="高级" :bordered="true">
        <n-collapse class="adv" display-directive="show">
          <n-collapse-item title="格子默认值" name="grid">
            <AdvancedGrid />
          </n-collapse-item>
          <n-collapse-item title="容量 / 下单 / 成本" name="exec">
            <AdvancedExec />
          </n-collapse-item>
          <n-collapse-item title="扫描 / 历史" name="scan">
            <AdvancedScan />
          </n-collapse-item>
          <n-collapse-item title="风控" name="risk">
            <AdvancedRisk />
          </n-collapse-item>
        </n-collapse>
      </n-card>
    </div>

    <LiveLists :snap="snap" />
  </div>
</template>

<script setup lang="ts">
import type { LiveSnapshot } from "../types";
import { store } from "../store";
import Field from "./Field.vue";
import PairList from "./PairList.vue";
import CtrlAside from "./CtrlAside.vue";
import LiveLists from "./LiveLists.vue";
import AdvancedGrid from "./AdvancedGrid.vue";
import AdvancedExec from "./AdvancedExec.vue";
import AdvancedScan from "./AdvancedScan.vue";
import AdvancedRisk from "./AdvancedRisk.vue";

defineProps<{ enabled: boolean; snap: LiveSnapshot | null }>();
const emit = defineEmits<{ saved: []; started: []; stopped: [] }>();

function toggleVenue(id: string, on: boolean) {
  const cur = store.params.active_venues;
  store.params.active_venues = on ? [...new Set([...cur, id])] : cur.filter((x) => x !== id);
}
</script>

<style scoped>
.layout {
  display: grid;
  grid-template-columns: minmax(420px, 1fr) minmax(480px, 1.15fr);
  gap: 16px;
  padding: 16px 20px 32px;
  align-items: start;
}
.main {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
  width: 100%;
}
.main :deep(.n-card) {
  width: 100%;
  max-width: 100%;
  min-width: 0;
  box-sizing: border-box;
}
.lead { margin: 0 0 12px; color: #8fa5c0; font-size: 13px; }
h4 { margin: 0 0 6px; font-size: 13px; }
.hint { margin: 0 0 10px; font-size: 12px; color: #6b7f99; }
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
.quick { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px 16px; }
.adv { background: transparent; }
.adv :deep(.n-collapse-item__header-main) { min-width: 0; }
@media (max-width: 1100px) {
  .layout { grid-template-columns: 1fr; }
}
</style>
