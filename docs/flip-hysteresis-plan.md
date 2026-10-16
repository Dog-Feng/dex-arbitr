# 方向反转滞回（flip hysteresis）实施方案

> 分支：`fix/adaptive-latency-horizon-20261016`。**状态：已实现并测试通过。**
> 背景：volume 模式"覆盖成本就开、回本即平"，价差在 μ 附近小幅摆动时会出现"平空 → 立即反手 → 又翻回"的无效往返，每一轮翻仓都付一遍双边价差。

## 1. 目标与验收

- 只在反转信号足够明确时才允许"空仓后反手"；
- 同向继续加仓、平仓路径完全不受影响；
- 仅采用 σ 比例口径（自适应），关闭（0）即回滚到现状；
- 默认 `flip_hysteresis_sigma: 1.0`（反转需比正常多走一个 σ）；
- 方向翻转（flip）计数接入 monitor，`/metrics` 输出全局与每对的 `flips`。
- 验收：回测中反转次数下降、net_bps 不劣化（或小幅改善）；在线无新增异常；重启后不施加缓冲（方向记忆为内存态）。

## 2. 现状与切入点

现状：`strategy.Decide` 中持有仓位时反方向直接返回 `holding opposite steps`（不做反手）；空仓后反手使用普通开仓门槛。缺失的是"上一平仓方向"的记忆——仓位归零后 `Steps` 为空，方向信息丢失，无法区分"同向续开"与"反手"。

改动点：`strategy`（纯逻辑）+ `engine`（方向记忆）+ `config`（一个新配置 `flip_hysteresis_sigma`）+ `monitor`（`flips` 计数）。

## 3. 设计

### 3.1 方向记忆

- `strategy.State` 增加 `LastDir Dir`（0 = 未知）；
- actor 在每次结算后维护：平仓 → `LastDir = 平掉的方向`；开仓 → `LastDir = 新开方向`；归零后保留最后方向；
- 不持久化：重启后为 0（未知），**未知不施加缓冲**，避免冷启动误拦合法反手；
- Decide 保持纯函数：`LastDir` 随 `State` 传入（actor 拥有 `a.state`，结算后同步）。

### 3.2 门槛公式

仅当 `cnt == 0`、`LastDir != 0` 且候选方向与 `LastDir` 相反时：

```text
extra  = flip_hysteresis_sigma × vol
needDev = open_bps + fee + vol_k×σ + extra
```

其余情况（同向续开、未知方向、普通加仓）不加 `extra`。`vol` 用 Decide 已有的价差 σ 入参。`flip_hysteresis_sigma=1.0` 即"反转需要比正常多走一个 σ"；该值相对行情噪声恒定，高波动时缓冲自动变大、平静时自动变小。

### 3.3 配置

```yaml
strategy:
  flip_hysteresis_sigma: 0    # 反转额外缓冲 = 该系数 × σ；0 = 关闭
```

校验：>= 0。volume 模式同样生效（volume 的 open_bps/close_bps 归零不影响本缓冲）。

### 3.4 交互

- REDUCE_ONLY / 深度 / 步数 / σ 准入闸全部不变，只在开仓门槛上叠加；
- 平仓路径不动；
- 执行、账本、持久化不动。

## 4. 成本量化与默认值

- 回测（待执行，可选）：在同一批样本上跑 `0 / 0.5 / 1.0 / 1.5`（σ 倍数）四档，统计反转次数、总往返、net_bps、平均持仓时长；
- 默认值建议：`flip_hysteresis_sigma: 1.0`（反转需比正常多走一个 σ），线上验证后再按回测校准；
- 可选观测：monitor/日志增加方向切换计数（`flips`），便于对比。

## 5. 测试清单

| 编号 | 用例 | 期望 |
|---|---|---|
| H1 | 反手信号落在带内 | 不开仓，返回明确 reason |
| H2 | 反手信号越过带外 | 正常开仓 |
| H3 | 同向续开 | 不受缓冲影响 |
| H4 | LastDir 未知（重启模拟） | 不施加缓冲 |
| H5 | 平仓方向记忆 | 平仓后 LastDir 正确更新（由 `engine.TestDirectionFlipRecordedInMonitor` 覆盖） |
| H6 | 系数生效 | σ 比例缓冲生效、0 关闭（`strategy.TestFlipHysteresisGatesMarginalReversal`）、负值拒绝（`config.TestLoadFlipHysteresisDefaultAndZero`） |
| H7 | 配置校验 | 负值拒绝（`config.TestLoadFlipHysteresisDefaultAndZero`） |

## 6. 风险与回滚

- 真实趋势反转会晚进缓冲大小的距离（0.5 bps 或 1 个 σ），属于"用边缘换稳定"的取舍；
- 关闭（为 0）即回到现状，零行为差异；
- 影响面仅在 Decide 的开仓分支。

## 7. 实施顺序

1. `strategy.Params` / `State.LastDir` / Decide 门槛；
2. actor 方向记忆与同步；
3. `config` 两个配置 + 校验；
4. H1–H7 测试；
5. 回测量化并定默认值。
