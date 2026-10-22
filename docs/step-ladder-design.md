# 阶梯加仓（step_bps）设计方案

日期：2026-10-22
状态：z 单位阶梯（`step_z`，计数口径 |cnt|）已实现于 `codex/zscore-spread` 分支，
可配置开关；bps 阶梯（`step_bps`）未实现，保留为后续选项。

## 1. 目标

当前开仓门槛是固定的，步数会在同一价位附近堆积（回放实测：z-score 50 步只覆盖
3.1 bps，SMA+arbitrage 7 bps）。本方案让"每一步要求的偏离随步数线性抬高"，从而把
步数按固定间距铺满目标价差区间（例如 30~46 USD ≈ 154~236 bps，带宽 82 bps）。

## 2. 语义

已实现形态：z 单位阶梯。开仓门槛在 z 侧叠加一项与步数成正比的要求：

```text
z ≥ z_open + step_z × |cnt|   且   dev ≥ openFeeBps
```

- `step_z`：梯级间距（z 单位，即几个 σ_long）。0 = 关闭，行为与现状完全一致。
- `cnt`：当前步数（本系统单一方向持仓，|cnt| 即已开步数）。
- 覆盖带宽 ≈ `step_z × σ_long × (max_steps − 1)` bps：梯级随市场离散度自适应
  缩放；要覆盖 B bps 的区间需按当前 σ_long 估算 `step_z ≈ B/(σ_long×(max_steps−1))`。
- 平仓规则不变（仍按各步入场毛利 ≥ 入场费摊销 + 出场费）。
- 阶梯只作用于开仓门槛，不产生新状态、不参与恢复路径，天然无持久化负担。

示例：z_open=2、step_z=0.5、σ_long=4 bps、max_steps=5，步数将开在
z ≈ 2、2.5、3、3.5、4，对应 dev ≈ 8、10、12、14、16 bps。

## 3. 与 z-score 的关系

阶梯只作用于 zscore 策略；arbitrage / volume 保持现状不变。`step_z=0` 时
zscore 行为与无阶梯完全一致。

## 4. 配置

```yaml
strategy:
  step_z: 0          # zscore：每一步比前一步多要求 0.5σ（0 = 关闭）
```

## 5. 涉及代码范围

1. `internal/strategy/strategy.go`
   - `Params` 增加 `StepZ float64`；
   - `gate()` 校验 `StepZ >= 0`；
   - `ZScore.Decide` 的 `try()`：`needZ` 叠加 `StepZ × |cnt|`；
   - Arbitrage / Volume 不改动。
2. `internal/config/config.go`
   - `Strategy` 增加 `StepZ float64 \`yaml:"step_z"\``；
   - 校验 `>= 0`；默认 0；
   - `configs/config.yaml` 加注释示例。
3. `internal/engine/run.go`
   - `strategy.Params{...}` 构造处传入 `StepZ: c.Strategy.StepZ`。
4. `cmd/zbacktest`
   - 增加 `-stepz` flag，用现有样本验证覆盖带宽与 PnL。
5. 测试
   - strategy：阶梯门槛随步数递增、step_bps=0 与现状等价、zscore 叠加正确；
   - config：解析与校验；
   - 回放：用 82 bps 目标带验证 `覆盖宽 ≈ step_bps × (max_steps−1)`。
6. 文档：`docs/zscore-spread-redesign.md` 增补阶梯章节。

## 6. 边界与取舍

- 梯级越靠外，成交概率越低，实际覆盖会"头重脚轻"；
- 阶梯是相对当前均值的偏离阶梯，不是钉死的绝对价位网格（绝对网格需另做设计）；
- `step_z` 的间距按当前 σ_long 自动缩放：安静期梯级更密、剧烈期更疏，相邻梯级
  的统计稀有度一致；但覆盖多少 bps 不再可精确预设；
- 与 cap_band / cap_hysteresis 叠加生效：顶部带滞回仍然加在门槛上；
- 反转方向时 cnt 归零，阶梯重新从第一步计。

## 7. 待确认项

1. （已定）阶梯计数用 |cnt|（当前模型下与"同方向连续步数"等价）；
2. （待定）是否后续补 bps 阶梯 `step_bps`（固定带宽场景更直观）；
3. （待定）volume / arbitrage 是否需要同款阶梯。

## 8. 回放证据（2026-10-21 样本，`cmd/zbacktest -stepz`）

| 配置（max_steps=50） | 最终步数 | 入场覆盖带 | 净 PnL(模拟) | 最长冻结 | 末段浮亏 |
|---|---|---|---|---|---|
| 无阶梯 | 50 | 3.1 bps | +1.00 | 7h15m | -10.37 |
| step_z=0.1 | 50 | 3.6 bps | +0.89 | 7h15m | -10.33 |
| step_z=0.25 | 50 | 12.3 bps | +0.91 | 7h05m | -10.05 |
| step_z=0.5 | 33 | 76.4 bps | +2.85 | 55m | -6.17 |
| step_z=1.0 | 17 | 76.4 bps | +3.15 | 1h14m | -3.24 |
| step_z=2.0 | 9 | 76.4 bps | +2.21 | 1h14m | -1.65 |

结论：step_z 在 0.5~1.0 时把入场铺满当天约 76 bps 的可交易区间、冻结时间从
7h+ 降至约 1h、模拟净收益约 3 倍；过小或过大均退化。梯级总高需覆盖目标带宽：
`step_z × (max_steps−1) × σ_long ≥ 目标 bps`。
