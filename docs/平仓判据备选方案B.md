# 平仓判据方案 B（往返净利 + 按格减仓）

**状态：已废弃。** 2026-08 整改按决策 **D1-c** 回退纯方案 A：止损和止盈闸都已从热路径删除。`Intent::Close.round_trip_pct` 仍写入 journal / 面板，只作记录，不再拦截发单。

现行判据见 [`本系统套利流程.md`](./本系统套利流程.md) 与 [`整改方案-固定每格数量与方案A.md`](./整改方案-固定每格数量与方案A.md)。下文仅保留当时的设计说明，便于对照历史。

---

# （历史）平仓判据方案 B（往返净利 + 按格减仓）

**原状态**：已实施，当时热路径。实现：`src/domain/grid.rs` 的 `decide_held`。

开仓仍看毛价差 vs T1。平仓：**格子决定减几格，往返净利决定现在能不能减。**

---

## 1. 和方案 A 的差别

| | 方案 A（已替换） | 方案 B（当前） |
|---|---|---|
| 平仓格数 | `relative` vs `T0/T(n−1)` | 同左（滞后减仓仍在） |
| 现在能不能减 | 格数一够就发 | `round_trip ≥ close_take_profit_pct` |
| 公式 | 只看毛价差 | `round_trip = entry_net_pct + exit_net_pct` |
| 手续费 | 比较时不算 | 开、平各扣一次 |
| 止损 | 无 | `round_trip ≤ −close_stop_loss_pct` 全平（`0` = 关） |

默认：`close_take_profit_pct = 0.005`（0.5bp），`close_stop_loss_pct = 0.10`。

---

## 2. 为什么换掉 A

按默认费率往返约 0.034%。T1=0.03% 回落到 T0=0.012% 只收敛 0.018%，方案 A 会在这里平仓，**理论净亏约 0.016%**。

方案 B：格子已经说该减时，若 `round_trip` 还低于止盈线就继续拿着，等到扣费后仍剩 ≥ 0.5bp 再发；穿止损则全平、不加仓。

---

## 3. 热路径

```
stop: close_stop_loss > 0 且 round_trip ≤ −stop
      → 全平（StopLoss），不等格子、不等持续性、不走价格稳定性

add:  开仓 raw 允许的格 > 已持 → 补仓（止损未触发时）

reduce:
      target = 格子滞后目标（T0 / T(n−1)）
      若 target ≥ 已持 → Hold
      若 round_trip < close_take_profit → Hold（清持续性）
      否则过持续性后按格减（GridReduce）
```

下单方式不变，仍走 `order.style`（默认一边限价一边市价）。
