# 刷量方案设计

> 目标：以最小净成本产生最大成交额，用于赚积分/空投。本文档描述两种刷量形态（taker churn 与对冲做市），以及它们共享的地基。积分回报率暂不量化。

> **当前实现状态**：方案一（taker churn）已实现（`strategy: volume`）。实现细节与本文档草案的差异以 [system-design.md](system-design.md) 为准：volume 的额外开平阈值固定为 `0/0`（`loss_tolerance_bps` 必须为 0）；开平仓受 σ 就绪准入闸约束；草案中的 `daily_volume_target` / `daily_cost_budget` 每日预算闸已移除，量/盈亏记账仅作为 state 文件审计痕迹（`pnl_uncertain` 粘性保留）。方案二（对冲做市）未实现。

> 本文包含早期方案与未实现的草案。当前实现以 [策略与选币完整链路](strategy-selection-flow.md) 为准：volume 也需 2 小时滚动窗口、1 小时预热，额外开平阈值固定为零，手续费与配置的过滤条件仍生效；未实现定时无条件交易或 maker 路径。

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
close 条件：毛利 ≥ 已存入场费 + 预计退出费    # 额外 close_bps=0
```

配套：

- 库存地板 + 边际 churn：`min_inventory`（如 8 步）保持大仓位，只对第 9 步反复开/平，实现“大仓位 + 量并存”。
- 时间驱动兜底：当价差长期不达标时，按 `churn_interval` 固定节奏做往返（接受全额成本）以保证量目标。
- ~~量/成本预算闸：`daily_volume_target`、`daily_cost_budget`~~（已移除：系统不再提供每日量/亏损上限闸门）。
- 参数放大：`step_qty`/`max_steps` 调大以放量，`interval_min/max` 压到限频允许的最小值。

优点：改动最小、完全复用现有执行器与风控、量最大。缺点：每笔都付 spread，成本随量线性增长。

## 4. 方案二：对冲做市（单边挂单 + fair 锚 + hedge 延迟定宽）

思路：在 spoke 所只在**有利一侧**挂 post-only（赚 basis），成交后用 Lighter taker 立刻对冲，库存只在毫秒级存在。

### 4.1 挂哪个方向（单边做市）

- μ>0（X 比 L 贵）：挂 **X 的 ask（卖 X）**，成交后买 L 对冲——卖贵买便宜，赚 μ。
- μ<0（X 比 L 便宜）：挂 **X 的 bid（买 X）**，成交后卖 L 对冲——买便宜卖贵，赚 −μ。

另一侧系统性亏 basis，不挂。方向随 μ 翻转而切换。这就是“单边做市”：只在均值回归那一边（贵了卖、便宜了买）提供流动性。

### 4.2 定价锚与报价宽度

```
fair_X = Lighter_mid × (1 + μ/1e4)          # μ 为两所 basis，作定价锚
ask = fair_X + w      (μ>0 时挂)
bid = fair_X − w      (μ<0 时挂)
w = mean(Δspread) + k × σ(Δspread)
```

其中 `Δspread` 是“X 腿成交 → L 腿对冲完成”这段延迟内**价差**的移动，直接从成交数据统计，k 取 2~3 覆盖尾部。报价宽度不是拍脑袋，而是用实测的 hedge 延迟价差移动来定。

### 4.3 为什么 adverse selection 不大（关键认知）

挂单成交说明价格在往不利方向走；但**跨所对冲做市里，被“走”的不是标的，而是价差在 hedge 延迟内的移动**。标的移动被对冲腿抵消了，真正暴露的只有：

```
亏损来源 = 成交到对冲完成之间，价差(X−L) 又变宽了多少 ≈ σ_spread × √lag
```

这个量远小于标的波动（昨晚价差 σ≈0.36 bps，300ms 内约 0.28 bps）。所以“另一腿市价扩大亏损”是真的，但幅度被价差 σ 约束，用 w 把它预扣掉即可。

### 4.4 成交即对冲 + 自适应 w + 撤单信号

- 成交即对冲：X 腿成交事件触发 → 立即 L 腿 taker 对冲（Lighter 快 + 免手续费），延迟越短 Δspread 越小。
- 自适应 w：每次记录实际 `X成交价 − L对冲价` 的滑点，用 AIMD 反馈调整 w（滑点大加宽、小收窄），与 `tick_k` 的自适应同款。
- 撤单信号：σ 突然跳升或 OBI 强烈单边时，撤激进侧；re-pricing 时 fair 偏离挂单价超一个 tick 就撤单重挂，挂单超时也撤。

数学依据：Avellaneda–Stoikov 在库存 q≈0（对冲做市）时退化为“报价基准 = fair、宽度 ∝ σ”；此处用实测 hedge 延迟价差移动替代抽象的 σ。

优点：量持续、成本最低（吃 spread + basis + 返佣）、库存近零。缺点：需要挂单生命周期 + 成交驱动对冲 + 报价管理，架构最重；不亏是统计意义（宽度 > adverse + 费），不是逐笔保证。

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
2. `maker` 策略 / `Protector` 能力（单边 post-only 挂单 + 成交驱动对冲 + 报价管理 + 自适应 w）。
3. ~~量记账器（每日量/成本/PnL）与目标/预算闸~~（已移除：仅保留审计记账）。
4. VWAP 执行质量基准（合规友好 + 滑点度量）。
5. 上线前数据验证：① 统计样本里“X 成交 → L 对冲”延迟内价差移动的分布（定 w）；② 目标所 maker 返佣/spread 是否 ≥ taker 对冲费 + adverse。

## 7. 配置草案

```yaml
strategy:
  kind: volume            # arbitrage | volume | maker

volume:
  step_qty: 0.1
  max_steps: 20
  min_inventory: 8        # 库存地板（大仓位与量并存）
  churn_interval: 2s      # 时间兜底的固定节奏（草案，当前未实现；实际节奏由 strategy.interval_min/max 控制）
  loss_tolerance_bps: 0    # 当前 volume 必须为 0，不能通过此项降低开仓门槛
  close_at_breakeven: true

maker:
  one_sided: true          # 只在赚 basis 的一侧挂单
  quote_width_k: 2.5       # w = mean(Δspread) + k*σ(Δspread) 的 k
  hedge_lag: 300ms         # 用于 Δspread 统计的对冲延迟
  max_inventory_qty: 1
  # 自适应 w（可选，后续）
  width_adaptive: false
  width_high/low: 0.10/0.02
```

## 8. 红线与风险

- 禁止自成交 / 挂撤操纵：违规，且 Lighter Self-Trade Prevention 会拦截；合法量只能来自真实对手方。
- 大仓位放大保证金风险：必须依赖已实现的账户级 `max_notional` 与保证金 floor/trigger。
- 升档后费率上升（RH Standard 0/0 → Premium 有费）：刷量到高频需升档拿 sendTx 限频，但成本随档位变化，经济账要动态评估。
