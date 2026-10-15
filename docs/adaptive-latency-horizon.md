# 自适应延迟窗口（σ horizon）实施方案清单

> 分支：`fix/adaptive-latency-horizon-20261016`。**状态：已实现并测试通过。**
> 背景：实测开平仓延迟在 500ms–1s 之间波动，而当前三个波动率估计器（价差 σ、左腿 σ、右腿 σ）共用固定的 `volatility.horizon=500ms`。慢腿让价不足、快腿多付滑点、价差缓冲低估对冲完成时间。

## 0. 目标与判定标准

- 腿价 σ 的窗口跟随**该腿自己的近期成交延迟**；
- 价差 σ 的窗口跟随**两腿对冲完成时间**（取两腿延迟的较大分位）；
- 窗口只随统计缓慢变化，不允许触发 σ 重新预热；σ 未就绪的开平仓准入闸保持不变；
- 延迟统计只纳入"一次性正常成交"，排除补救、超时推断与未知终态。

验收：模拟 L≈300ms / R≈1000ms 时，`horizon_L < horizon_R`，两腿让价 tick 比例与延迟比例一致，价差窗口 = max(两腿分位)；开关关闭时行为与当前 `horizon=500ms` 完全一致。

## 已落地的决策

- `coverage_k` 默认 1.3（mean + 1.3×std，约 p90）；
- 窗口钳制 `window_min=200ms`、`window_max=3s`；
- 更新节奏 `update_interval=1s`（decide tick 内节流，窗口变化才写）；
- 价差窗口 = `max(腿L窗口, 腿R窗口)`；
- 就绪门槛固定在 `horizon/interval`（warm 增量数），与运行时窗口解耦：扩窗渐近使用已有历史、缩窗立即生效，任何窗口变化都不会让已 warm 的估计器失 ready；
- 延迟统计只纳入"一次性正常成交"（`Resolved && Filled && Latency>0`，且该次执行 `RescueOrders==0`）。

## 1. 延迟统计层（新增，engine 内）

1.1 数据来源：`execution.LegResult.Latency`（submit→终态）。仅当满足以下条件才入统计：

- `Resolved == true` 且 `Status == StatusFilled`（一次性正常成交，不区分 partial）；
- 该次执行 `RescueOrders == 0`（补救轮次的延迟不代表主单路径）；
- `Latency > 0`。

排除：`StatusUnknown/Rejected/Canceled`、`Resolved=false` 的 give-up 时间、应急/drain 平仓轮次（可再加 `emergency==false` 过滤）。

1.2 每腿独立维护：

```text
ring[n=32] latency 样本（仅内存）
EWMA mean  = mean + α·(x − mean),   α = 2/(n+1) 或按样本数 1/n
EWMA var   = var  + α·((x−mean)^2 − var)
```

1.3 分位目标（单边覆盖，与让价哲学一致）：

```text
latency_leg = mean + coverage_k × sqrt(var)
coverage_k 默认 1.3（约 p90，正态假设）
```

1.4 冷启动：样本数 < `min_samples`（默认 8）时返回固定 `horizon`（500ms），不参与自适应。

1.5 存放位置（已实现）：`PairActor` 每腿一个 `latencySeries`（`engine/latency.go`，定长环形、O(N) 重算均值/方差，不清零）；原 `latencyStat` 展示版保留不动。

## 2. 估计器层（volatility 改造）

2.1 `Params` 增加 `MaxHorizon`（预分配上限，默认 3s）。构造时 `maxSlots = MaxHorizon/Interval`，环形缓冲按 `maxSlots` 分配。

2.2 引入运行时有效窗口 `k`：

```text
k = clamp(round(horizon_ms / interval_ms), 1, maxSlots)
```

- 求和/ready 只统计最近 k 个增量；
- `SetWindow(k int)`：仅修改 k；k 变化时对最近 k 个增量重算滚动平方和（O(k)，k≤30），**不清空链、不丢增量、不重新预热**；
- 缩窗/扩窗均即时生效，历史增量一直保留在环内，扩窗无需等待补样本。

2.3 `Ready()` 语义保持计数：`cnt ≥ k`。

2.4 三个估计器（价差、左腿、右腿）各自持有独立 k，由引擎写入。

## 3. 引擎接线（pair.go / settlement.go）

3.1 每次 `accountExecution` 成功后，把 `res.L.Latency / res.R.Latency` 按 1.1 的条件喂给对应腿的滚动统计（失败结算、干预、unwind 不喂）。

3.2 更新时机：每个 decide tick（200ms）或每 1s 检查一次：

```text
latency_L = EWMA 分位（冷启动回退 500ms）
latency_R = 同上
window_L  = clamp(latency_L,  min, max) 取整到 interval
window_R  = clamp(latency_R,  min, max) 取整到 interval
window_spread = max(window_L, window_R)
```

分别 `SetWindow` 到左腿 σ、右腿 σ、价差 σ。窗口未变化时不写。
（实现采用 `update_interval=1s` 节流，decide tick 内调用 `applyAdaptiveWindows`。）

3.3 联动（无需额外代码）：

- 开仓门槛 `vol_k×σ_spread` 自动使用新的价差窗口；
- 让价 `tick_k×σ_leg/tick` 自动使用各腿自己的新窗口。

3.4 配置（新增 `execution.adaptive_latency`）：

```yaml
execution:
  adaptive_latency:
    enabled: false          # 关闭 = 固定 volatility.horizon（当前行为）
    min_samples: 8          # 冷启动阈值
    window_min: 200ms
    window_max: 3s
    coverage_k: 1.3         # 单边覆盖系数（≈p90）
    ring_n: 32              # 延迟样本环大小
```

## 4. 边界与风险

- 反馈回路弱：让价不改变交易所延迟本身；慢行情可能同时抬高延迟与 σ，表现为双重保守（可接受，属安全方向）。
- 延迟尖峰由"排除补救/未知 + EWMA + 上限钳制"三重吸收，避免窗口被离群值吹大。
- 采样断链行为不变：σ 仍按计数就绪、断链后大增量抬高 σ（上一提交的语义）。
- 回滚：`enabled: false` 即回到固定 500ms，零行为差异。

## 5. 测试清单

| 编号 | 用例 | 期望 |
|---|---|---|
| T1 | `volatility.TestSetWindowKeepsChain` | 扩/缩 k 后 ready 保持、σ 单调变化、无重置 |
| T2 | `volatility.TestWindowReadyIsCountBased` | k=7 时第 8 个样本 ready，k 无关时间 |
| T3 | `engine.TestLegLatencyStatsFilterOutliers` | 补救/未知/零延迟不入统计 |
| T4 | `engine.TestAdaptiveWindowColdStartFallback` | 样本不足时窗口保持固定值 |
| T5 | `engine.TestAdaptiveWindowFollowsLegLatency` | L=300ms、R=1000ms → window_L < window_R，价差窗口=max |
| T6 | `engine.TestAdaptiveWindowDisabledMatchesFixed` | enabled=false 与固定 horizon 行为一致 |
| T7 | 回归 | 现有 volgate、count-based、admission 测试全部保持通过 |

## 6. 决策记录

- `coverage_k` = 1.3（mean + 1.3×std，约 p90），可用历史延迟分布再校准；
- 边界 `window_min=200ms`、`window_max=3s`；
- 更新节奏 `update_interval=1s`（decide tick 内节流，窗口变化才写）；
- 价差窗口 = `max(腿L窗口, 腿R窗口)`（对冲风险由慢腿决定）；
- 就绪门槛固定为 warm 增量数，窗口缩至 warm 以下不影响 ready。
