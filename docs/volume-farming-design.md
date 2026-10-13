# 刷量方案设计

> 目标：以最小净成本产生最大成交额，用于赚积分/空投。本文档描述两种刷量形态（taker churn 与对冲做市），以及它们共享的地基。积分回报率暂不量化。

## 1. 前提与地基

- Lighter 中心化：所有对以 Lighter 为一腿，Lighter 侧始终 taker（免手续费、滑点低、深度足），spoke 侧费率 >0 且各所不同。
- 已实现的地基（本仓库当前代码）：
  - `strategy.Strategy` 接口 + 注册表（`arbitrage` 已注册，可加 `volume`/`maker`）；
  - 净成本门槛 `FeeBps`（由两所 `TakerFee` 合成往返费，进开平仓判断）；
  - 账户级名义上限 `risk.max_notional`；
  - 保证金风控（floor/trigger 应急平仓）；
  - 100ms 样本落盘 + `volcal` 校准工具。

## 2. 成本模型

单次对冲往返（taker 双腿）的净成本：

```
cost_bps ≈ 2 × (spoke_taker_fee + spoke_half_spread) + 2 × hub_half_spread(≈0)
```

maker 形态下该成本可为负（赚 spread + maker 返佣）。做单笔是否划算，就看 `单笔捕获 − cost_bps`。

## 3. 方案一：taker churn（覆盖成本、不亏就平、快速 churn）

思路：把套利的门槛压到成本线，把“机会驱动”变成“只要不亏就一直做”。

决策规则（`volume` 策略实现）：

```
open  条件：dev ≥ fee_bps + vol_k·σ        # 覆盖住成本即可，不要求正 edge
close 条件：profit ≥ 0                       # 不亏就平，快速 churn
```

配套：

- 库存地板 + 边际 churn：`min_inventory`（如 8 步）保持大仓位，只对第 9 步反复开/平，实现“大仓位 + 量并存”。
- 时间驱动兜底：当价差长期不达标时，按 `churn_interval` 固定节奏做往返（接受全额成本）以保证量目标。
- 量/成本预算闸：`daily_volume_target`、`daily_cost_budget`，达限降频或停。
- 参数放大：`step_qty`/`max_steps` 调大以放量，`interval_min/max` 压到限频允许的最小值。

优点：改动最小、完全复用现有执行器与风控、量最大。缺点：每笔都付 spread，成本随量线性增长。

## 4. 方案二：对冲做市（fair 锚 + σ 宽度 + OBI 偏置）

思路：在 spoke 所持续挂双边报价，成交后用 Lighter taker 立刻对冲，库存只在毫秒级存在。

报价（每本书更新时刷新）：

```
fair_X = Lighter_mid × (1 + μ/1e4)          # μ 为两所 basis，作定价锚
fair_X += α_obi × OBI × σ                    # OBI 短周期方向偏置
bid = fair_X − w
ask = fair_X + w
w = k × σ_hedge + 单边费                     # σ 按对冲延迟(150-300ms)缩放
```

关键动作：

- 成交即对冲：spoke 腿成交瞬间，Lighter taker 对冲，库存风险近零。
- σ 跳升撤单：σ 突然放大时撤掉激进侧，避免被信息优势扫单。
- re-pricing：每次盘口更新，若 fair 偏离挂单价超一个 tick 就撤单重挂；挂单超时未成交也撤。
- OBI 偏置：OBI 为正（买盘重）向上偏报价，负则向下偏，别站在下一跳反面。

数学依据：Avellaneda–Stoikov 在库存 q≈0（对冲做市）时退化为“报价基准 = fair、宽度 ∝ σ”，即上述规则。

优点：量持续、成本最低（吃 spread + 返佣）、库存风险近零。缺点：需要挂单生命周期 + re-pricing + 成交驱动对冲，架构最重；不亏是统计意义（宽度 > adverse + 费），不是逐笔保证。

## 5. 两者对比

| 维度 | taker churn | 对冲做市 |
|---|---|---|
| 量级 | 大（有节奏） | 持续（对手方驱动） |
| 单笔成本 | 付 spread + 费 | 吃 spread + 返佣（可为负） |
| 库存风险 | 有（大仓位） | 近零 |
| 不亏性质 | 门槛 ≥ 成本（近似逐笔） | 统计正 EV |
| 架构改动 | 小（策略 + 记账） | 大（挂单生命周期） |

建议：先上 churn 验证经济账与量目标，对冲做市作为终态在返佣/spread 数据明确后启用。

## 6. 待实现清单

1. `volume` 策略（churn + 库存地板 + 时间兜底）。
2. `maker` 策略 / `Protector` 能力（挂单生命周期 + 成交驱动对冲 + re-pricing）。
3. 量记账器（每日量/成本/PnL）与目标/预算闸。
4. VWAP 执行质量基准（合规友好 + 滑点度量）。

## 7. 配置草案

```yaml
strategy:
  kind: volume            # arbitrage | volume | maker

volume:
  step_qty: 0.1
  max_steps: 20
  min_inventory: 8        # 库存地板（大仓位与量并存）
  churn_interval: 2s      # 时间兜底的固定节奏
  daily_volume_target: 1000000   # USD
  daily_cost_budget: 100         # bps
  loss_tolerance_bps: 0.5  # 每笔允许的亏损(bps)，配合 daily_cost_budget 封顶
  close_at_breakeven: true

maker:
  quote_width_k: 1.28
  obi_skew: 0.5
  hedge_lag: 300ms
  max_inventory_qty: 1
```

## 8. 红线与风险

- 禁止自成交 / 挂撤操纵：违规，且 Lighter Self-Trade Prevention 会拦截；合法量只能来自真实对手方。
- 大仓位放大保证金风险：必须依赖已实现的账户级 `max_notional` 与保证金 floor/trigger。
- 升档后费率上升（RH Standard 0/0 → Premium 有费）：刷量到高频需升档拿 sendTx 限频，但成本随档位变化，经济账要动态评估。
