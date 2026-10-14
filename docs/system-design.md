# dex-arbitr 系统设计文档（当前实现）

> 本文档描述代码的实际实现状态，与 `docs/spread-arb-design.md`（早期设计稿）互补；以本文档为准。
> 分支：`codex/vol-latency-leverage`。

## 1. 系统定位

跨 DEX 永续合约的价差均值回归步进套利 + 低成本刷量系统，Go 实现。同一币种在两个交易所之间存在价差时，围绕历史中枢 μ 做对冲开仓，价差回归后平仓赚取价差，一次只动一步，可阶梯加仓到上限；刷量模式则把门槛压到成本线、快速 churn。

当前接入：Lighter 主网（chain 304）与 RH Lighter（chain 466324）。

核心原则：策略只做决定，执行器只管下单，适配器只管对接交易所，三者只通过接口连接；监控/扫描/回放都是只读旁路，绝不下交易指令。

## 2. 架构风格

- 六边形（端口-适配器）：核心只依赖 `exchange.Venue`、`estimator.Estimator` 等接口，不 import 具体交易所。
- Actor 模型：每个套利对一个 goroutine，独占步列表、状态、决策节奏，基本无锁（仅盘口缓存用读写锁）。
- 可插拔纯策略：`strategy.Strategy` 接口 + 注册表（`Arbitrage` / `Volume`），输入（状态 + 盘口 + μ + σ）输出动作，不做 IO，可直接回放回测。
- 接口 + 注册表：交易所、均值估计器、策略按名字注册，配置选择。
- 选对驱动解耦：`engine.Run` 只做公共组装（venues/store/tracker/monitor），按 `strategy.pair_selection` 分发到 `runManual` / `runAuto` 两个平级驱动源；二者共享 `runtime`（newStats / buildActor / applyLeverage），互不侵入。
- 只读旁路模块：monitor / backtest / scan 三个模块只读，不 import 交易引擎，也不被引擎反向依赖（引擎仅单向向 monitor 发事件）。

## 3. 目录与依赖

```
cmd/arbd            交易引擎入口：读配置 -> engine.Run
cmd/selfcheck       自检：instruments/匹配/WS/下单/签名（-live 实盘往返）
cmd/backtest        离线回放标定：读样本 -> 参数扫描 -> 净 EV 报告
cmd/scanner         只读机会扫描：匹配对 -> 净机会排序/告警（无交易密钥）
cmd/volcal          离线校准：签名图 + 500ms 增量分位数
internal/domain     纯类型（零依赖）：Instrument/Book/Position/OrderReq
internal/config     配置加载、默认值、校验
internal/exchange   Venue 端口 + 注册表 + 能力接口（LeverageSetter）
  lighter/          Lighter 家族适配器（REST + WS + 签名 + 下单）
internal/symbols    币种前缀匹配、精度对齐、生成 Pair
internal/spread     价差计算（LS/SL/Mid，单位 bps）
  estimator/        均值估计器接口 + SMA
internal/volatility 波动率估计器（实现波动率 RV，价差/腿价通用）
internal/strategy   Strategy 接口 + Arbitrage/Volume + Step/State/Params
internal/execution  双腿 IOC 执行器、订单追踪、补救阶梯、动态让价
internal/engine     PairActor + 组装(build.go/run.go) + 自动选对(auto.go) + 保证金风控 + 量记账
internal/adaptive   AIMD 自适应系数控制器（tick_k）
internal/store      步状态持久化（JSON）+ 样本落盘（JSONL）
internal/monitor    运行监控（事件聚合 + 指标 + 告警 + HTTP，只读）
internal/backtest   离线回放（复用 strategy/spread/volatility，不碰引擎）
internal/scan       套利检测（Eval/Watch）+ 刷量健康度打分（HealthTracker），只读
```

依赖方向：交易核心各包 -> `domain`；`strategy` 不依赖 `exchange/store`；`monitor/backtest/scan` 复用共享统计包，但不 import `engine`，也不被 `engine` 反向依赖（engine 仅单向调用 monitor 的 Record）。

## 4. 核心概念与数据模型

| 名词 | 含义 |
|---|---|
| Venue / Instrument | 交易所 / 某所一个永续合约（含精度、最小量、taker/maker 费率） |
| Base / Pair | 币种前缀 / 一个 Base 在左右两所的套利对，最小运行单元 |
| Step | 一次双腿开仓：方向、剩余量、开仓价差、开仓成交价、累计已实现盈亏 |
| Counter | 有符号步数：负=左多右空（DirLS），正=左空右多（DirSL） |
| Quote | 盘口价差：LS/SL 可成交、Mid 中间价（bps） |
| μ / σ | 价差中枢（2h SMA）/ 短周期波动率（500ms 实现波动率）；另有腿价 σ（让价用） |
| Candidate | 自动模式下每个匹配对的共享市场视图：盘口缓存 + μ/σ/腿σ + 刷量健康度，实现 `PairView` |
| Churn score | 刷量适宜度：σ_bps × 深度质量 × tick质量 × 稳定性 × 频率因子，乘法合成，任一为 0 即归零 |
| Picker | 自动选对器：按分数排序 + 显著性/驻留滞回选 top-Y，动态晋升/降出交易对 |

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

// strategy.Strategy（接口）+ Arbitrage / Volume 实现
type Strategy interface {
	Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action
}

// strategy.Params
type Params struct { StepQty, MaxSteps, OpenBps, CloseBps, DepthMult, CloseOrder, VolK, FeeBps, MinInventory ... }
```

## 5. 价差与统计公式

两边直接用交易所原始价格，USDC/USDG 等计价币按 1:1 不换算。单位 bps（1 bps = 0.01%）。

```
mid   = (L.mid + R.mid) / 2
Mid   = (R.mid - L.mid) / mid        // 采样进 μ
LS    = (R.bid1 - L.ask1) / mid      // 买左卖右的可成交价差
SL    = (L.bid1 - R.ask1) / mid      // 卖左买右的可成交价差
```

- μ：`Mid` 的滑动均值（SMA），窗口 2h、1s 采样、60m 预热、覆盖率 >=80%，O(1) 更新。
- σ（价差）：`Mid` 的实现波动率，100ms 采样、500ms 窗口 sum(Δs)^2 滚动和 + EWMA（半衰期 1m），σ=sqrt(RV)。
- σ（腿价）：`bookL.Mid()`/`bookR.Mid()` 各自估计，用于单腿让价（腿价波动 = 标的 + 价差，比价差 σ 大）。
- 开仓门槛：`dev >= open_bps + fee_bps + vol_k*σ`，其中 `devLS = LS-μ`、`devSL = SL+μ`，`fee_bps` 为该 pair 的往返 taker 费（由两所 TakerFee 合成，净成本）。
- 平仓门槛：该步往返净利 `EntryBps + 反向可成交价差 - fee_bps >= close_bps`。
- EntryBps：开仓成交价差的真实值，分母用成交时 mid `(lPx+rPx)/2`。

## 6. 状态机

```
WARMUP --(μ 就绪)--> RUNNING <-> REDUCE_ONLY
   |                     |
   +--(恢复)-- PAUSED <--+  数据过期 / 单腿失败 / 对冲不变量破坏 / 应急平仓
```

| 状态 | 开仓 | 平仓 | 采样 |
|---|---|---|---|
| WARMUP | 否 | 是 | 是 |
| RUNNING | 是 | 是 | 是 |
| REDUCE_ONLY | 否 | 是 | 是 |
| PAUSED | 否 | 否 | 是 |

状态迁移：μ 就绪（WARMUP->RUNNING）；保证金超 floor（RUNNING->REDUCE_ONLY）；保证金恢复（REDUCE_ONLY->RUNNING）；保证金超 trigger 应急平仓（->PAUSED）；checkHedge 不变量破坏（->PAUSED）；启动 reconcile 失败（->PAUSED）。

## 7. 运行时完整链路

### 7.0 模式分发

`engine.Run` 完成公共组装后打印 `engine starting`，按 `strategy.pair_selection` 分流：

- `manual`（默认）：`runManual` 只跑配置里的 `pairs`，每个 pair 自建并自采样 μ/σ，预热 1h 后执行策略，行为与历史版本一致。
- `auto`：`runAuto` 全量匹配所有候选对，启动共享数据面持续预热，picker 按 churn score 选出 top-Y 动态晋升/降出，晋升时复用共享统计、免二次预热。

两者共享 `runtime.buildActor`（建 PairActor）、`runtime.newStats`（建估计器）与 `runtime.applyLeverage`（自适应杠杆），只是「活跃对从哪来、统计从哪来」不同。

### 7.1 启动

1. `config.Load`：读 YAML、${ENV} 展开、填默认值、校验。
2. `engine.Run`：构建 monitor（可选 HTTP）、按注册表构建 venues；实盘校验凭证并建 OrderTracker。
3. 对每个候选/配置 pair：拉永续列表 -> 前缀匹配 -> 精度对齐（size_decimals 取短、min_qty 取大）。
4. 实盘时为每个市场的杠杆做自适应解析（见 §8.1）：`min(配置 leverage, 市场最大杠杆)`，失败降级为交易所当前杠杆。
5. manual 模式每个 pair 起一个 PairActor；auto 模式先起数据面收集器（全量预热），picker 后续才起活跃对 actor。

### 7.2 Actor 内部（单 goroutine 事件循环）

| 事件 | 频率 | 动作 |
|---|---|---|
| 盘口 WS 推送 | 事件驱动 | collectBooks goroutine 更新 bookL/bookR 缓存 |
| sample | 1s | 算 Mid 喂 SMA（μ）；μ 就绪则 WARMUP->RUNNING |
| volC | 100ms | 门控后喂价差 σ、腿价 σ（VolL/VolR）+ 样本落盘 |
| riskC | 1s | 保证金风控 |
| decide | 200ms | 到点且非 PAUSED 时 Decide -> apply |
| report | 1min | 状态日志 + 向 monitor 发 status 事件 |

### 7.3 一次决策（Decide，纯函数）

1. 先平仓：满足 `profit >= close_bps + fee_bps` 的步按 close_order 排序取最优；`|counter| > min_inventory` 才平（刷量库存地板）。
2. 再开仓：仅 RUNNING 且 μ 就绪；`dev >= open_bps + fee_bps + vol_k*σ`、同方向、`|counter| < max_steps`、两腿一档深度 >= `step_qty*depth_mult`。
3. 都不满足则不动。

注：`volume` 策略在 build 阶段把 `open_bps/close_bps` 覆盖为 `-loss_tolerance_bps / 0`，即「覆盖成本就开、回本即平」，故 config 里这两项在 volume 模式下不生效，真正调激进程度的是 `vol_k` 与 `loss_tolerance_bps`。

### 7.4 一次执行（Executor）

1. 双腿并发提交 IOC 限价单（价格 = 决策价 ± 动态让价）。
2. 等 WS 终态 -> 超时 REST 兜底 -> 仍未知从仓位差推断（未知绝不当失败）。
3. settle 后读真实仓位；净敞口不等于目标时进入补救阶梯（reduce-only）。
4. 记录每腿延迟和补救成交。

动态让价（用腿价 σ）：

```
ticks = clamp( ceil( price * tick_k * σ_leg / 1e4 / tick ), min_ticks, max_ticks )
ticks = max( ticks, 走到 step_qty 实际成交档位所需的 tick 数 )
```

- tick_k 由 adaptive AIMD 控制器按实测单腿失败率动态调整（失败率高放宽、低收紧，夹在 [min_k,max_k]）；窗口未满用配置初值。
- σ_leg 未就绪或 dynamic_ticks=false 时回退固定 price_ticks。

补救阶梯（rescue）：严格第一档（3 次）-> 逐 tick 让价（5 tick）-> 保护性市价（mid ±30bps）。Immediate=true 时（应急）从第一笔就是保护性市价。

### 7.5 平账、量记账与持久化

- 开仓：对冲量 h=min(fillL,fillR)，h<半单位则放弃；记 Step（EntryBps 用成交时 mid，OpenL/OpenR 存成交价）。
- 平仓：closed=max(fillL,fillR)，零成交腿用补救价；realized = EntryBps + 反向 EntryBps - 往返费 累计进 Step.Realized；Qty 归零则移除。
- 统一量记账：volume_usd += (fillL + fillR + rescue_qty) * mid（失败开仓的单腿成交+补救也计入）；补救滑点 rescue_cost_usd 从 day_pnl_usd 扣除。
- 每日预算闸：刷量模式下 daily_volume_target / daily_cost_budget 达限停止开仓，跨天清零。
- checkHedge：每次 apply 后校验 posL/posR 与步账本一致，漂移即 PAUSED。
- persist：步状态 JSON 原子写（state_dir/<pair_key>.json）。

### 7.6 一次完整往返（启动 -> 订阅 -> 下单时机 -> 平仓时机）

1. 启动与订阅：构建 venue -> 对 pair 的左右市场 `SubscribeBook`（WS 推 `subscribed/order_book` 快照 + `update/order_book` 增量，50ms 批量）；本地维护 bookL/bookR，nonce 连续性校验、gap 自动重订阅。
2. 采样与价差：1s sample 算第一档 `Mid` 喂 SMA（2h 窗口、60m 预热、覆盖率 80% 才 Ready）；100ms vol 门控后喂价差 σ 与腿价 σ，并落样本。
3. 下单时机：decide 循环 200ms 检查，到 next_action_at（7-10s 抖动）且 RUNNING；`dev >= open_bps + fee_bps + vol_k*σ`、同方向、未满 max_steps、两腿一档深度够 -> 双腿并发 IOC 开仓。
4. 持仓：开仓后记 Step（EntryBps、OpenL/OpenR），持有对冲仓位，步数 ±1。
5. 平仓时机：decide 循环里平仓优先；`profit = EntryBps + 反向可成交价差 - fee_bps >= close_bps` 且 `|counter| > min_inventory` -> 平仓。
6. 平账：双腿成交后 realized 累计、移除步、checkHedge 校验、持久化。

注：μ/σ 都用第一档；"10 档价差曲线"未实现，深档风险由让价的深度感知（depthTicks）近似覆盖；如需多档 σ，需另加每档估计器。

### 7.7 手续费处理

- 已纳入成本：`fee_bps = 2 * (L.taker + R.taker)`（bps），由两所 `Instrument.TakerFee` 在 build 阶段自动合成。
- 用途：开仓门槛 `dev >= open_bps + fee_bps + vol_k*σ`；平仓 `profit >= close_bps + fee_bps`；已实现盈亏 `gross - fee_bps`。
- 后续接有手续费的 DEX：`TakerFee` 从该所合约元数据读取，自动抬高门槛，无需改代码（即"手续费作为损耗点、提高阈值"）。
- 注意：当前只算 taker 费（执行是 taker IOC）；未来 maker 模式需把 maker 费/返佣加入成本模型。

### 7.8 自动选对模式（auto）

auto 模式把「选哪些对」从静态配置变成运行时闭环，核心链路分四层。

数据面（全量预热，只读）：

1. `matchAll` 拉取所有 venues 的永续列表，`symbols.Match` 生成候选对（可配 `auto.candidates` 白名单过滤）。
2. 每个候选建一个 `candidate`（实现 `PairView`），起 `collect` goroutine：订阅两腿盘口，1s 喂 SMA μ，100ms 喂价差 σ 与腿价 σ，并维护刷量健康度（`scan.HealthTracker`）。
3. 全量候选一直保持温热（预热 1h + 覆盖率 80%），所以晋升时无需二次预热；可选把 100ms 样本落盘到 samples_dir。

健康度打分（`scan.HealthTracker`，乘法合成）：

```
score = σ_bps × DepthQuality × TickQuality × Stability × min(1, reverts_per_min / 10)
DepthQuality = 两腿四侧 top 深度都 >= notional/mid 的样本占比
TickQuality  = clamp(σ_bps / tick_bps / 5, 0, 1)     // 边际相对最小 tick 的余量
Stability    = σ 均值 / (σ 均值 + 2×σ 标准差)          // 反比 σ 的变异系数
```

任一项为 0 直接归零；`min_churn_score=0` 表示只按分数排序、不设硬门槛。

picker（稳定选对）：

1. 每 2s 计算各候选 score，先保留现任（incumbent），再用备选填满空位。
2. 换人需显著性 + 驻留：备选分数要高于最弱现任 `×(1+promote_margin)`，并持续 `dwell`（默认 1m）才晋升；现任落后且持续 `dwell`、且 `Flat()`（步数为 0）才降出。
3. 硬剔除走快速门限，不参与软排序：`auto.max_fail_rate` 超限的备选永不晋升、已在跑的自动 drain 并平仓后降出。
4. 账户级名义帽：`auto.max_total_notional > 0` 时，晋升前按 `sum(active 的 notional_per_leg × max_steps)` 预占，超出则拒绝晋升。

定容（晋升时）：

```
qty = floor( min(notional_per_leg, depth_use_ratio × min(四侧 top 深度) × mid) / mid )
```

再 floor 到 pair 的 size_decimals，校验 >= min_qty；不过则跳过该对。深盘吃满预算、薄盘自动缩量。

动态生命周期：

- 晋升：`applyLeverage` -> `buildActor`（View=candidate，共享 μ/σ/腿σ 与盘口）-> 起 actor goroutine。
- 降出：cancel actor ctx -> actor 持久化后退出；候选仍留在数据面继续温热，随时可再晋升。
- 晋升失败：退避一个 dwell 再重试，不刷屏。

## 8. 保证金风控（risk）

主动监控（不挂单）。每秒评估，每 position_refresh（15s）用 REST 刷新两所 LiquidationPrice；mark 用 book mid（新鲜才评估）。

```
多腿剩余 = (mark - liq) / (entry - liq)
空腿剩余 = (liq - mark) / (liq - entry)
```

- 任一腿 <= margin_trigger_ratio（0.1）-> 应急市价平仓：EmergencyFlatten 用 Immediate 市价（保护价 emergency_market_bps=100bps）把两腿吃到零，清空步账本，置 PAUSED + 告警。
- 任一腿 <= margin_floor_ratio（0.2）-> 置 REDUCE_ONLY（停止加仓、允许减仓）。
- 两腿都 > floor + hysteresis（0.25）-> 恢复 RUNNING。
- 账户级名义上限：max_notional > 0 时按 venue 汇总该账户所有市场的 |qty|*avgEntry 名义，超限即 REDUCE_ONLY。
- trigger=0 或 dry_run 时风控禁用。

### 8.1 自适应杠杆（resolveLeverage）

不同市场的最大杠杆不同（低流动性币会压低上限，lighter 以 21113 拒绝过高的初始保证金率）。系统不依赖未公开接口，按「交易所是否接受」反推：

1. 先提交配置杠杆；成功即用配置值（绝大多数市场的单次路径）。
2. 被 `domain.ErrLeverageRejected`（lighter 把 21113 映射为它）拒绝时，在 `[1, 配置-1]` 内二分查找最大可接受杠杆，取 `min(配置, 市场最大)`。
3. 非杠杆类错误（网络/签名等）不当作「太高一档」，直接放弃本次设置；`applyLeverage` 尽力而为，失败只打 WARN 并沿用交易所当前杠杆，不阻断该对。

与账户名义两套口径：`risk.max_notional` 是 venue 侧实际持仓名义（被动，超限 REDUCE_ONLY）；`auto.max_total_notional` 是 picker 晋升前的预占预算（主动，超限拒绝晋升）。

## 9. 只读旁路模块

交易核心是唯一会下单的；下面三个模块只读、独立、不互相 import。

### 9.1 monitor（运行监控）

职责：盯系统自己的交易数据（仓位、成交量、成功/失败率、盈亏、延迟、保证金、补救/应急/错误）。

- 引擎在关键点发类型化事件（step_opened/step_closed/executed/rescue/status/emergency/error），picker 另发 pair_promoted/pair_demoted/pair_health/pair_warmup；monitor.Monitor.Record 按 pair 归集指标并告警。
- 双视图：`/metrics` 顶层是全局汇总（跨对合计），新增 `pairs` 字段是每个交易对的明细（状态、步数、成交量、盈亏、μ/σ、tick_k、失败率、churn score 分量、预热覆盖度、是否活跃）。
- 滚动失败率：`RollingFailRate(pair)` 暴露 actor 上报的滚动单腿失败率（AIMD 窗口），供 picker 做硬剔除，线程安全。
- 可选 HTTP：monitor.addr 非空时 arbd 内起服务，GET /metrics 返回 JSON；monitor.token 非空时要求 Authorization: Bearer <token> 或 ?token=（常量时间比较，401 拒绝）。
- 只读、不 import engine。

### 9.2 backtest（回放标定）

- 离线：backtest.Load 读样本（unixNano,midBps,lsBps,slBps），backtest.Replay 复用 strategy.Arbitrage + SMA/波动率在可成交价差上模拟成交，算净 EV。
- 命令：go run ./cmd/backtest -samples <file> 扫 open_bps * close_bps 网格，输出 trips / net_bps / avg_bps。
- 只依赖 strategy/spread/volatility，不碰引擎与交易所。

### 9.3 scan（套利检测）

- scan.Eval 是核心纯函数：net = dev - fee_bps - vol_k*σ - open_bps，>0 即机会（与交易策略口径一致）。
- scan.Watch 订阅两腿盘口（只读 WS），维护 μ/σ，按四层条件（可交易/数据新鲜/统计就绪/净机会>0）发机会。
- scan.HealthTracker 是刷量健康度打分器（σ_bps / 反转频率 / 深度质量 / tick 质量 / 稳定性），供 auto picker 使用；纯统计、不碰引擎。
- 命令：go run ./cmd/scanner -config <file> 用只读 venue 扫描所有匹配对，去重后打日志；不 import engine。

## 10. 配置全景

```yaml
dry_run: true
state_dir: state
leverage: 10
samples_dir: state/samples

venues:
  - name: lighter
    kind: lighter
    opts: { account_index: ${LIGHTER_ACCOUNT_INDEX}, api_key_index: ${LIGHTER_API_KEY_INDEX}, private_key: ${LIGHTER_PRIVATE_KEY} }

matching:
  strip_suffixes: [PERP, USDC, USDG, USDT, USD]
  separators: "-_/: ."

estimator: { kind: sma, window: 2h, min_warmup: 60m, min_coverage: 0.8, interval: 1s }
volatility: { interval: 100ms, horizon: 500ms, half_life: 1m, max_age: 50ms }

risk:
  margin_floor_ratio: 0.2
  margin_trigger_ratio: 0.1
  margin_hysteresis: 0.05
  position_refresh: 15s
  emergency_market_bps: 100
  max_notional: 0

strategy:
  kind: arbitrage
  pair_selection: manual   # manual | auto
  open_bps: 1
  close_bps: 1
  depth_mult: 2
  close_order: best
  vol_k: 1.28
  interval_min: 7s
  interval_max: 10s
  max_book_age: 10s

volume:
  min_inventory: 0
  daily_volume_target: 0
  daily_cost_budget: 0
  loss_tolerance_bps: 0

auto:                      # 仅 pair_selection=auto 生效
  top_y: 5
  max_steps: 3
  notional_per_leg: 200
  depth_use_ratio: 0.25
  min_churn_score: 0
  promote_margin: 0.05
  dwell: 1m
  max_fail_rate: 0.3
  max_total_notional: 0
  candidates: []

execution:
  price_ticks: 30
  dynamic_ticks: true
  tick_k: 2.5
  min_ticks: 1
  max_ticks: 0

adaptive:
  window: 0
  fail_high: 0.10
  fail_low: 0.02
  up_factor: 1.3
  down_factor: 0.95
  min_k: 0.5
  max_k: 10

monitor:
  addr: ":8080"
  token: ""

pairs:
  - { base: SNDK, left: lighter, right: rhlighter, step_qty: 0.01, max_steps: 10 }
```

## 11. 扩展点

| 需求 | 做法 |
|---|---|
| 接入新交易所 | 实现 exchange.Venue（+ 可选 LeverageSetter），init() 注册 |
| 替换均值算法 | 实现 estimator.Estimator 并注册，改 estimator.kind |
| 新策略（刷量/做市） | 实现 strategy.Strategy 并注册，改 strategy.kind |
| 新增风控规则 | 在 engine 加规则，参考 margin.go 的监控模式 |
| 挂单硬保证（未来） | 新增 Protector 能力接口，叠加在现有 risk 之上 |
| 回放标定 | go run ./cmd/backtest -samples <file> |
| 机会扫描 | go run ./cmd/scanner -config <file> |
| 监控接入 | monitor.addr/token 配好，前端带 token 读 /metrics |

## 12. 日志与排查

关键日志行（msg=）：

| 消息 | 含义 / 排查 |
|---|---|
| engine starting | 模式分发前：pair_selection / strategy / dry_run |
| auto: fetching instruments | auto 模式拉取某 venue 永续列表（停在这 = 网络卡住） |
| auto pair selection / auto collectors started | 候选数与预热窗口，确认 auto 已进入预热 |
| picker status（每 30s） | candidates/ready/warming/active 计数，预热进度 |
| pair promoted / pair demoted | 自动选对的晋升/降出 |
| leverage set / leverage resolve failed; using venue default | 自适应杠杆结果（失败则沿用交易所当前杠杆） |
| pair promote failed（WARN） | 晋升失败，退避一个 dwell 后重试 |
| pair actor started | 启动参数 |
| positions / position reconciliation passed | 启动仓位核对；失败即 PAUSED |
| mean ready -> RUNNING | μ 采样满 60m 就绪 |
| status | 每分钟：mode/steps/mu/σ/价差/tick_k/fail_rate/day_volume/day_pnl/latency |
| action | 决策：kind/dir/qty/两腿价/expected_bps/reason |
| executed | 成交：fill/px/status/rescue/rescue_cost_usd/pos/took/latency |
| step opened / step pnl / step closed | 步账本与已实现盈亏 |
| rescue order / rescue complete（WARN） | 补救单与完成 |
| hedge invariant broken; pausing（ERROR） | 账本与仓位不一致，已 PAUSED |
| margin floor reached; reduce only（WARN） | 剩余保证金 <= floor |
| ALERT: liquidation risk, emergency unwind（ERROR） | 应急平仓触发 |
| order state unresolved; will infer from positions（WARN） | 订单终态超时，从仓位差推断 |
| ws read error, reconnecting（WARN） | WS 断线自动重连 |
| opportunity（scanner） | 检测到净机会 |
| monitor server（ERROR） | 监控端口监听失败（检查 monitor.addr 格式） |

常见问题：

- 下单返回 20558：受限地区，换允许交易地区的服务器；只读行情加 ?readonly=true。
- reconcile 失败 / PAUSED：步账本与仓位不一致，cat state/*.json 核对后手动平齐再重启。
- hedge invariant broken：补救未补平落后腿，查 rescue/executed 的 posL/posR。
- emergency unwind 触发：某腿保证金触及 trigger，系统已清仓；确认账户后人工解除 PAUSED。
- monitor server: missing port in address：monitor.addr 少了冒号，写成 ":8080"。
- 监控 401：monitor.token 非空时必须带 Bearer 或 ?token=。
- nonce gap / WS 断线：自动重订阅/重连。

## 13. 当前边界（已知简化）

- 门槛为净成本：open_bps/close_bps 已扣 fee_bps；补救滑点计入 day_pnl_usd，但应急平仓亏损未记 step 级盈亏（直接体现账户余额）。
- mark 用 book mid 近似：更准可用 WS 的 market_stats.mark_price。
- liq 为 15s REST 缓存：单币单仓近似稳定；多仓共担保证金需提高刷新频率。
- 步数上限不等于名义仓位上限：max_steps 限步数，部分平仓可能残留碎步。
- 自适应只管 tick_k：vol_k / min_ticks 也可挂 adaptive.K 模式，暂未做。
- 无 10 档价差曲线：μ/σ 用第一档，深档风险由让价深度感知近似。
- 1:1 计价假设：USDC/USDG 不换算，脱锚会直接表现为价差（不在本次范围）。
- 杠杆无公开查询接口：市场最大杠杆由「交易所是否接受」二分反推（§8.1），无法先验读取 max_leverage。
- churn score 是乘法合成的无量纲值（量级约 0.1~0.5）：`min_churn_score` 默认 0 只排序，要设门槛需先按 `/metrics` 的 pairs 分布校准。
- auto 名义预占按 `notional_per_leg × max_steps` 保守计（实际定容受深度 cap 只会更小）。
- auto 的 `pair_promoted` 等事件与 status 的 per-pair 字段来自同一 monitor 归集，promote/demote 历史只保留当前状态（未做时间序列留存）。
