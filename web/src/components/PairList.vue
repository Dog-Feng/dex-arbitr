<template>
  <div>
    <div class="bar">
      <n-button size="small" @click="addDraft">+ 添加交易对</n-button>
      <span class="meta">已填 {{ filledCount }} 个</span>
    </div>
    <div class="list">
      <div v-for="(d, i) in store.drafts" :key="i" class="row">
        <n-input
          v-model:value="d.symbol"
          size="small"
          placeholder="交易对，如 BTC"
          style="width:140px"
        />
        <n-input
          v-model:value="d.qty"
          size="small"
          placeholder="每格数量"
          style="width:140px"
          :status="qtyError(d) ? 'error' : undefined"
        />
        <div class="err" v-if="qtyError(d)">{{ qtyError(d) }}</div>
        <n-button text size="small" @click="removeDraft(i)">删除</n-button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { addDraft, removeDraft, store } from "../store";
import { qtyError } from "../validation";

const filledCount = computed(
  () => store.drafts.filter((d) => d.symbol.trim() && parseFloat(d.qty) > 0).length
);
</script>

<style scoped>
.bar { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
.meta { font-size: 12px; color: #8fa5c0; }
.list { display: flex; flex-direction: column; gap: 8px; }
.row {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  border: 1px solid #1e2d4a; border-radius: 8px; background: #0d1320; padding: 8px 10px;
}
.err { font-size: 11px; color: #fca5a5; }
</style>
