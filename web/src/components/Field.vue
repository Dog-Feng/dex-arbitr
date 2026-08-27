<template>
  <div class="field">
    <label v-if="label">{{ label }}</label>
    <slot />
    <p v-if="hint" class="hint">{{ hint }}</p>
  </div>
</template>

<script setup lang="ts">
defineProps<{ label?: string; hint?: string }>();
</script>

<style scoped>
.field { display: flex; flex-direction: column; gap: 6px; min-width: 0; width: 100%; }
label { font-size: 12px; color: #b8cce0; }
.hint { margin: 0; font-size: 11px; color: #6b7f99; line-height: 1.4; }
.field :deep(.n-input),
.field :deep(.n-input-number),
.field :deep(.n-select) { width: 100%; max-width: 100%; }
</style>
