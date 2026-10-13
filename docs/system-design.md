# dex-arbitr 系统设计文档（当前实现）

> 本文档描述代码的**实际实现状态**，与 `docs/spread-arb-design.md`（早期设计稿）互补；以本文档为准。
> 分支：`codex/vol-latency-leverage`。

## 1. 系统定位

跨 DEX 永续合约的**价差均值回归步进套利**系统，Go 实现。同一币种在两个交易所之间存在价差时，围绕历史中枢 μ 做对冲开仓，价差回归后平仓赚取价差，一次只动一步，可阶梯加仓到上限。

当前接入：Lighter 主网（chain 304）与 RH Lighter（chain 466324）。

核心原则：**策略只做决定，执行器只管下单，适配器只管对接交易所，三者只通过接口连接。**

## 2. 架构风格

- **六边形（端口-适配器）**：核心只依赖 `exchange.Venue`、`estimator.Estimator` 等接口，不 import 具体交易所。
- **Actor 模型**：每个套利对一个 goroutine，独占自己的步列表、状态、决策节奏，基本无锁（仅盘口缓存用读写锁）。
- **纯函数策略**：`strategy.Decide` 输入（状态 + 盘口 + μ + σ）输出动作，不做 IO，可直接回放回测。
- **接口 + 注册表**：交易所、均值估计器按名字注册，配置选择。

## 3. 目录与依赖

```
cmd/arbd            # 入口：读配置 → engine.Run
cmd/selfcheck       # 自检：instruments/匹配/WS/下单/签名（-live 实盘往返）
cmd/volcal          # 离线校准：签名图 + 500ms 增量分位数
internal/domain     # 纯类型（零依赖）：Instrument/Book/Position/OrderReq…
internal/config     # 配置加载、默认值、校验
internal/exchange   # Venue 端口 + 注册表 + 能力接口（LeverageSetter）
  lighter/          # Lighter 家族适配器（REST + WS + 签名 + 下单）
internal/symbols    # 币种前缀匹配、精度对齐、生成 Pair
internal/spread     # 价差计算（LS/SL/Mid，单位 bps）
  estimator/        # 均值估计器接口 + SMA
internal/volatility # 波动率估计器（实现波动率 RV）
internal/strategy   # 纯函数 Decide + Step/State/Params
internal/execution  # 双腿 IOC 执行器、订单追踪、补救阶梯
internal/engine     # PairActor（每对一个 goroutine）+ 组装 + 保证金风控
internal/store      # 步状态持久化（JSON）+ 样本落盘（JSONL）
```

依赖只能向内：`exchange/store/strategy → domain`；`domain` 不依赖任何内部包；`strategy` 不依赖 `exchange/store`。

## 4. 核心概念与数据模型

| 名词 | 含义 |
|---|---|
| Venue / Instrument | 交易所 / 某所一个永续合约（含精度、最小量、taker/maker 费率） |
| Base / Pair | 币种前缀（如 SNDK）/ 一个 Base 在左右两所的套利对，最小运行单元 |
| Step | 一次双腿开仓：方向、剩余量、开仓价差、开仓成交价、累计已实现盈亏 |
| Counter | 有符号步数：负=左多右空（DirLS），正=左空右多（DirSL） |
| Quote | 盘口价差：`LS/SL` 可成交、`Mid` 中间价（bps） |
| μ / σ | 价差中枢（2h SMA）/ 短周期波动率（500ms 实现波动率） |

关键结构（当前实现）：

```go
// domain.Position
type Position struct {
	MarketID, Symbol, Qty, AvgEntry, LiquidationPrice ...
}

// strategy.Step
type Step struct {
	ID, Dir, Qty, EntryBps, OpenL, OpenR, Realized, OpenedAt ...
}

// strategy.State / Params
type State struct { Mode Mode; Steps []Step }
type Params struct { StepQty, MaxSteps, OpenBps, CloseBps, DepthMult, CloseOrder, VolK ... }
```

## 5. 价差与统计公式

两边直接用交易所原始价格，USDC/USDG 等计价币按 1:1 不换算。单位 bps（1 bps = 0.01%）。

```
mid   = (L.mid + R.mid) / 2
Mid   = (R.mid − L.mid) / mid        // 采样进 μ
LS    = (R.bid1 − L.ask1) / mid      // 买左卖右的可成交价差
SL    = (L.bid1 − R.ask1) / mid      // 卖左买右的可成交价差
```

- **μ**：`Mid` 的滑动均值（SMA），窗口 2h、1s 采样、60m 预热、覆盖率 ≥80%，O(1) 更新。
- **σ**：`Mid` 的实现波动率，100ms 采样、500ms 窗口 `Σ(Δs)²` 滚动和 + EWMA（半衰期 1m），`σ=√(RV)`。
- **开仓门槛**：`dev ≥ open_bps + vol_k·σ`，其中 `devLS = LS−μ`、`devSL = SL+μ`。
- **平仓门槛**：该步往返毛利 `EntryBps + 反向可成交价差 ≥ close_bps`。
- **EntryBps**：开仓成交价差的真实值，分母用成交时 mid `(lPx+rPx)/2`。

## 6. 状态机

```
WARMUP ──(μ 就绪)──> RUNNING ⇄ REDUCE_ONLY
   │                     │
   └──(恢复)── PAUSED <──┘  数据过期 / 单腿失败 / 对冲不变量破坏 / 应急平仓
```

| 状态 | 开仓 | 平仓 | 采样 |
|---|---|---|---|
| WARMUP | ❌ | ✅ | ✅ |
| RUNNING | ✅ | ✅ | ✅ |
| REDUCE_ONLY | ❌ | ✅ | ✅ |
| PAUSED | ❌ | ❌ | ✅ |

状态迁移来源：μ 就绪（WARMUP→RUNNING）；保证金超 floor 线（RUNNING→REDUCE_ONLY）；保证金恢复（REDUCE_ONLY→RUNNING）；保证金超 trigger 线触发应急平仓（→PAUSED）；`checkHedge` 不变量破坏（→PAUSED）；启动 reconcile 失败（→PAUSED）。

## 7. 运行时完整链路

### 7.1 启动

1. `config.Load`：读 YAML、`${ENV}` 展开、填默认值、校验。
2. `engine.Run`：按注册表构建 venues；实盘校验凭证并建 `OrderTracker`（订阅各所订单 WS）。
3. 对每个 pair：拉永续列表 → 前缀匹配 → 精度对齐（`size_decimals` 取短、`min_qty` 取大）。
4. 实盘启动时为每个市场提交 `updateLeverage`（杠杆 `leverage`，默认 10x，即初始保证金比例 1000 bps，全仓）。
5. 每个 pair 起一个 `PairActor` goroutine。

### 7.2 Actor 内部（单 goroutine 事件循环）

select 五类事件：

| 事件 | 频率 | 动作 |
|---|---|---|
| 盘口 WS 推送 | 事件驱动 | `collectBooks` goroutine 更新 bookL/bookR 缓存（latest wins） |
| sample | 1s | 算 `Mid` 喂 SMA（μ）；μ 就绪则 WARMUP→RUNNING |
| volC | 100ms | 新鲜度+两腿偏差 ≤50ms 门控后喂波动率（σ）+ 样本落盘 |
| riskC | 1s | 保证金风控（见 §8） |
| decide | 200ms | 到 `next_action_at` 且非 PAUSED 时 `Decide` → `apply` |
| report | 1min | 状态日志（μ/σ/步数/延迟/价差） |

### 7.3 一次决策（Decide，纯函数）

1. 先平仓：所有满足 `profit ≥ close_bps` 的步按 `close_order`（best/lifo/fifo）排序取最优，检查反向盘口深度后平。
2. 再开仓：仅 RUNNING 且 μ 就绪；`dev ≥ open_bps + vol_k·σ`、同方向、`|counter| < max_steps`、两腿一档深度 ≥ `step_qty×depth_mult`；两方向取偏离更大者。
3. 都不满足则不动。

### 7.4 一次执行（Executor）

1. 双腿并发提交 IOC 限价单（价格 = 决策价 ± 动态让价，见下）。
2. 等 WS 终态 → 超时 REST 查询兜底 → 仍未知从仓位差推断（未知绝不当失败）。
3. settle 后读两边真实仓位；净敞口 ≠ 目标时进入补救阶梯（reduce-only）。
4. 记录每腿延迟（submit→终态）和补救成交（量+均价）。

**动态让价（allowance）**：

```
ticks = clamp( ceil( price × tick_k × σ_bps / 1e4 / tick ), min_ticks, max_ticks )
ticks = max( ticks, 走到 step_qty 实际成交档位所需的 tick 数 )   // 深度感知
```

σ 未就绪或 `dynamic_ticks=false` 时回退到固定 `price_ticks`。

**补救阶梯（rescue）**：严格第一档（3 次）→ 逐 tick 让价（5 tick）→ 保护性市价（mid ±30bps）。`Immediate=true` 时（应急）直接从第一笔就是保护性市价。

### 7.5 平账与持久化

- 开仓：对冲量 `h=min(fillL, fillR)`，`h<半单位` 则放弃；记 `Step`（`EntryBps` 用成交时 mid，`OpenL/OpenR` 存成交价）。
- 平仓：`closed=max(fillL, fillR)`，零成交腿用补救价；`realized = EntryBps + 反向 EntryBps − 往返费` 累计进 `Step.Realized`；Qty 归零则移除该步。
- `checkHedge`：每次 apply 后校验 `posL/posR` 与步账本反推一致，漂移即 PAUSED。
- `persist`：步状态 JSON 原子写（`state_dir/<pair_key>.json`）。

## 8. 保证金风控（risk）

主动监控（不挂单）。每秒评估，每 `position_refresh`（15s）用 REST 刷新两所 `LiquidationPrice`；mark 用 book mid（新鲜才评估）。

```
多腿剩余 = (mark − liq) / (entry − liq)
空腿剩余 = (liq − mark) / (liq − entry)
```

- 任一腿 ≤ `margin_trigger_ratio`（0.1）→ **应急市价平仓**：`EmergencyFlatten` 用 Immediate 市价（保护价 `emergency_market_bps`=100bps）把两腿吃到零，清空步账本，置 PAUSED + 告警。
- 任一腿 ≤ `margin_floor_ratio`（0.2）→ 置 REDUCE_ONLY（停止加仓、允许减仓）。
- 两腿都 > `floor + hysteresis`（0.25）→ 恢复 RUNNING。
- `trigger=0` 或 dry_run 时风控禁用。

## 9. 配置全景

```yaml
dry_run: true            # true=模拟盘（按决策价成交）；false=实盘
state_dir: state
leverage: 10             # 账户杠杆，启动时对每个市场发 updateLeverage（1x..10000x）
samples_dir: state/samples   # 非空则按 pair 落 100ms 样本，供 volcal 校准

venues:
  - name: lighter
    kind: lighter         # 或 rhlighter；opts 传 account_index/api_key_index/private_key/book_depth
    opts: { account_index: ${LIGHTER_ACCOUNT_INDEX}, api_key_index: ${LIGHTER_API_KEY_INDEX}, private_key: ${LIGHTER_PRIVATE_KEY} }

matching:
  strip_suffixes: [PERP, USDC, USDG, USDT, USD]   # 去掉后取币种前缀
  separators: "-_/: ."

estimator:
  kind: sma               # 均值估计器（可替换）
  window: 2h
  min_warmup: 60m
  min_coverage: 0.8
  interval: 1s

volatility:
  interval: 100ms
  horizon: 500ms
  half_life: 1m
  max_age: 50ms

risk:
  margin_floor_ratio: 0.2     # 剩余保证金 ≤20% 停止加仓
  margin_trigger_ratio: 0.1   # ≤10% 应急市价平仓
  margin_hysteresis: 0.05     # 恢复回差
  position_refresh: 15s
  emergency_market_bps: 100

strategy:
  open_bps: 1              # 开仓毛利门槛（未扣费）
  close_bps: 1             # 平仓毛利门槛（未扣费）
  depth_mult: 2
  close_order: best        # best | lifo | fifo
  vol_k: 1.28              # 开仓的 σ 缓冲系数
  interval_min: 7s
  interval_max: 10s
  max_book_age: 10s

execution:
  price_ticks: 30          # 固定让价；σ 未就绪或 dynamic_ticks=false 时兜底
  dynamic_ticks: true
  tick_k: 1.28
  min_ticks: 1
  max_ticks: 0             # 0=不封顶

pairs:
  - base: SNDK
    left: lighter
    right: rhlighter
    step_qty: 0.01
    max_steps: 10
```

## 10. 扩展点

| 需求 | 做法 |
|---|---|
| 接入新交易所 | 实现 `exchange.Venue`（+ 可选 `LeverageSetter`），`init()` 注册 |
| 替换均值算法 | 实现 `estimator.Estimator` 并注册，改 `estimator.kind` |
| 替换策略 | 替换纯函数 `strategy.Decide`（或将其接口化） |
| 新增风控规则 | 在 `engine` 加规则，参考 `margin.go` 的监控模式，置 Mode / 告警 |
| 挂单硬保证（未来） | 新增 `Protector` 能力接口，叠加在现有 risk 之上 |
| 回测 | 回放 `samples_dir` 样本，复用 `strategy.Decide` + 模拟成交 |

## 11. 日志与排查

关键日志行（`msg=`）：

| 消息 | 含义 / 排查 |
|---|---|
| `pair actor started` | 启动参数（dry_run/step_qty/max_steps/size_decimals/recorded_steps） |
| `positions` / `position reconciliation passed` | 启动时本地步账本 vs 交易所仓位核对；失败即 PAUSED，需人工修复后重启 |
| `mean ready -> RUNNING` | μ 采样满 60m 就绪 |
| `status` | 每分钟：mode/steps/mu_bps/samples/span/coverage/LS/SL/mid/vol_500ms_bps/lat_* |
| `action` | 决策：kind/dir/qty/两腿价/expected_bps/reason |
| `executed` | 成交：fill/px/status/rescue_orders/rescue_qty/pos/took/latL_ms/latR_ms |
| `step opened` / `step pnl` / `step closed` | 步账本：entry_bps / gross_fee_net_realized_bps + lpx/rpx / profit_bps + counter |
| `rescue order` / `rescue complete`（WARN） | 补救单与完成 |
| `hedge invariant broken; pausing`（ERROR） | 平账后本地账本与仓位不一致（补救未完成等），已 PAUSED |
| `margin floor reached; reduce only`（WARN） | 剩余保证金 ≤ floor，停止加仓 |
| `ALERT: liquidation risk, emergency unwind` / `emergency unwind done`（ERROR） | 应急市价平仓触发与完成 |
| `order state unresolved; will infer from positions`（WARN） | 订单终态超时，从仓位差推断 |
| `ws read error, reconnecting`（WARN） | WS 断线自动重连 |

常见问题：

- **下单返回 20558**：受限地区，换允许交易地区的服务器；只读行情可加 `?readonly=true`。
- **reconcile 失败 / PAUSED**：本地步账本与交易所仓位不一致，先 `cat state/*.json` 核对、手动平齐后再重启。
- **hedge invariant broken**：补救未把落后腿补平，查 `rescue order` 与 `executed` 的 posL/posR，修复后重启。
- **emergency unwind 触发**：某腿保证金触及 trigger 线，系统已市价清仓；确认账户状态后人工解除 PAUSED。
- **nonce gap / WS 断线**：自动重订阅/重连，无需人工；频繁出现需查网络与限频。

## 12. 当前边界（已知简化）

- **门槛为毛利**：`open_bps/close_bps` 未扣手续费；手续费仅在 `realized_bps` 记账时扣除（往返 taker）。
- **应急平仓不计 step 级盈亏**：应急平仓清空步账本，亏损直接体现在账户余额。
- **mark 用 book mid 近似**：更准可用 WS 的 `market_stats.mark_price`。
- **liq 为 15s REST 缓存**：单币单仓近似稳定；多仓共担保证金时需提高刷新频率。
- **步数上限 ≠ 名义仓位上限**：`max_steps` 限步数，部分平仓可能残留碎步占位。
- **1:1 计价假设**：USDC/USDG 不换算，脱锚会直接表现为价差（不在本次范围）。
