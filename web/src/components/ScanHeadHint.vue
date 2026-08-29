<template>
  <n-tooltip trigger="hover" :delay="200" :width="360">
    <template #trigger>
      <span class="th-tip">{{ label }}</span>
    </template>
    <div class="tip">
      <p v-for="(p, i) in paras" :key="i">{{ p }}</p>
    </div>
  </n-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{ k: string; label?: string }>();

const COPY: Record<string, { label: string; paras: string[] }> = {
  rank: {
    label: "#",
    paras: ["按有效振幅从高到低的名次。价差σ 小于 Δ 的行排在后面。"],
  },
  pair: {
    label: "交易对",
    paras: ["合约标的，如 BTC。一行一个币。"],
  },
  best: {
    label: "最优所对",
    paras: [
      "在勾选的所里两两比较，取有效振幅最大的两个 DEX。",
      "「同家族」表示 Lighter 主网与 Lighter RH，基差通常更小、成交路径更稳。",
    ],
  },
  edge: {
    label: "有效振幅",
    paras: [
      "有效振幅 = 价差σ − Δ。单位 %。",
      "价差绕枢纽晃的幅度，扣掉开一格的门槛。正数：典型波动能盖过一格，本表按此从大到小排序；负数：连一格都不够，行会变淡。",
      "不是收益预测，只说明这对本钱够不够格。",
    ],
  },
  sigma: {
    label: "价差σ",
    paras: [
      "窗口内秒级 mid 价差 s 的标准差，单位 %。",
      "衡量价差怎么晃，不是两所价格均值差了多少。滑动窗口吃的是 s 绕着 μ 来回穿。",
    ],
  },
  delta: {
    label: "Δ",
    paras: [
      "一格有多宽，单位 %。相对枢纽 μ 至少走出这么多才加减 STEP。",
      "Δ = max((target_bp/100 + F + C) / (1−2h), F+C)",
      "target_bp：目标净利，1 表示 1 bp = 0.01%。F：两所 taker 费 × 2（开平共四腿）。C：两所点差均值的平均（本表「点差均值」）。h：step_hysteresis；yaml 为 0 时分母为 1。",
      "点差厚、费率高、滞后大，都会把 Δ 撑宽，同样波动更难开格。",
    ],
  },
  cross: {
    label: "过格示意",
    paras: [
      "不是实测开仓次数。后端尚未逐秒统计 |s−μ|≥Δ 且站住的次数。",
      "页面用 σ 与 Δ 粗算：z = Δ/σ；z ≥ 2.8 记 0；否则 round(window_samples × 0.12 × e^(−z²/2))。",
      "只用来对照宽格/窄格，不能当真实成交次数。",
    ],
  },
  mu: {
    label: "价差均值",
    paras: [
      "最优所对的 mid 价差均值，即枢纽 μ，单位 %。",
      "只标注价差停在哪一档，不参与排序。均值差大不代表波动大。",
    ],
  },
  hubC: {
    label: "点差均值",
    paras: [
      "最优两所各自买卖点差均值的平均 C，单位 %。",
      "C = (点差_A + 点差_B) / 2，折进 Δ。两所都要薄，否则 σ 看起来不小，扣完点差仍够不着一格。",
    ],
  },
  dex: {
    label: "",
    paras: [
      "该所两列：价格均值、点差均值。最优所对以外的所也会采（只要宇宙里有这个币并订到了盘口）。",
      "单元格高亮表示该所是本行最优所对之一。未满窗或没有盘口显示 —。",
    ],
  },
  mid: {
    label: "价格均值",
    paras: ["该所该币在窗口内的 mid 均价。用来核对盘口，不参与排序。"],
  },
  own: {
    label: "点差均值",
    paras: [
      "该所 (ask−bid)/mid 的窗口均值，单位 %。",
      "越厚，配对时的 C 和 Δ 越大。",
    ],
  },
};

const hit = computed(() => COPY[props.k] || { label: props.k, paras: [] });
const label = computed(() => props.label || hit.value.label);
const paras = computed(() => hit.value.paras);
</script>

<style scoped>
.th-tip {
  cursor: help;
  border-bottom: 1px dotted #6b7f99;
}
.tip {
  font-size: 12px;
  line-height: 1.55;
}
.tip p {
  margin: 0 0 8px;
}
.tip p:last-child {
  margin: 0;
}
</style>
