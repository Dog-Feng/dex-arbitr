<template>
  <n-config-provider :theme="darkTheme" :theme-overrides="overrides">
    <n-message-provider>
      <n-dialog-provider>
        <AppShell />
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { darkTheme, type GlobalThemeOverrides } from "naive-ui";
import AppShell from "./components/AppShell.vue";

const overrides: GlobalThemeOverrides = {
  common: {
    primaryColor: "#8b5cf6",
    primaryColorHover: "#a78bfa",
    primaryColorPressed: "#7c3aed",
    bodyColor: "#070b12",
    cardColor: "#121929",
    modalColor: "#121929",
    popoverColor: "#121929",
    tableColor: "#121929",
    inputColor: "#0d1320",
    borderColor: "#1e2d4a",
    dividerColor: "#1e2d4a",
    textColorBase: "#dde6f0",
    textColor1: "#dde6f0",
    textColor2: "#b8cce0",
    textColor3: "#8fa5c0",
    borderRadius: "8px",
    fontFamily:
      '-apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif',
  },
};
</script>
