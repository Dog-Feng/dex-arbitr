# 多交易所价差步进套利系统 · 设计文档

> 版本：v0.5 ｜ 后端：Go ｜ 前端：暂不实现
>
> v0.5 变更：重写第 2A 节——只拉取永续合约；币种前缀匹配即视为同一币种；USDC / USDG 等计价币按 1:1 处理，不做 USD 换算；数量精度以两边中较短的为准。删除汇率换算、脱锚风控、合约乘数相关设计。
>
> v0.4 变更：新增"币种匹配与对齐"（第 2A 节）：统一合约键、名称归一化、计价币（USDC / USDG 等）换算与脱锚风控、合约乘数与数量精度对齐；价差公式改为统一换算后的价格。
>
> v0.3 变更：确定补救单"重试几次后逐步放宽价格"的规则（见 8.3.5）；明确"严格不吃穿第一档"只适用于正常开平仓。
>
> v0.2 变更：平仓顺序改为"收益最高先平"（可配置）；明确采样含义；第一档深度 ≥ 下单量 2 倍；双腿并发下单；支持"提交 + WS 回报"两段式下单；单腿失败立即平掉另一腿并循环直至成功。
> 范围：衍生品交易所（Lighter / RH Lighter / Ondo / Vanta / Arcus 等）之间的同币种永续合约价差套利，兼顾刷交易量。

---

## 0. 本版范围

| 项目 | 本版 | 说明 |
|---|---|---|
| 多交易所、每个交易所多币种 | ✅ | 适配器 + 配置驱动 |
| 步进开平（不分档位） | ✅ | 条件满足就加一步 / 减一步 |
| 每步随机间隔 7～10 秒 | ✅ | 可配置 |
| 中心均值：滑动窗口均值 | ✅ | 窗口 2 小时，至少 60 分钟数据才开始交易 |
| 中心值算法可替换 | ✅ | `Estimator` 接口 + 注册表 |
| 手续费、滑点纳入门槛 | ❌ | 门槛暂按毛利 1 bps，后续改配置即可 |
| 中枢切换检测 / 长期锚点 / 不对称门槛 | ❌ | 本版移除，后续作为新的 `Estimator` 或 `RiskRule` 接入 |
| 前端面板 | ❌ | 只预留 HTTP / WebSocket 接口 |

---

## 1. 设计模型

| 层面 | 采用的模型 | 解决的问题 |
|---|---|---|
| 整体结构 | **六边形架构（端口-适配器）** | 核心逻辑不依赖具体交易所；新增交易所只加适配器 |
| 并发 | **Actor 模型**：每个套利对一个 goroutine，独占自己的状态 | 无锁；套利对之间互不影响 |
| 策略 | **纯函数 + 状态机** | 输入"状态 + 盘口快照 + 时间"，输出"动作"；不做 IO，可直接回测 |
| 数据流 | **事件驱动 + 只保留最新值** | 盘口只存最新一份，避免消息积压后用旧价格决策 |
| 扩展点 | **接口 + 注册表（工厂）** | 交易所、中心值算法、风控规则都按名字注册，配置选择 |

**核心原则：策略只做决定，执行器只管下单，适配器只管对接交易所，三者只通过接口连接。**

---

## 2. 核心概念

| 名词 | 含义 |
|---|---|
| **Venue** | 一个交易所，如 `lighter`、`ondo` |
| **Instrument** | 某交易所上的一个永续合约，含原始代码、币种前缀、计价币、数量精度、价格精度、最小下单量（见 2A） |
| **Symbol（币种）** | 去掉计价币和合约类型后缀后的币种前缀，如 `SNDK`、`XAU`，配置里写作 `SNDK-PERP` |
| **Pair（套利对）** | 一个 Symbol + 左交易所 + 右交易所，如 `XAU-PERP: lighter ↔ ondo`。**系统最小运行单元** |
| **Step（步）** | 一次双腿开仓，记录自己的开仓价差，之后单独平仓 |
| **Position（步数）** | 当前持有步数。**负数 = 左多右空，正数 = 左空右多** |

同一币种在 N 个交易所上，最多可组成 N×(N−1)/2 个套利对，各自独立运行。

---

## 2A. 币种匹配与精度对齐

### 2A.1 规则总览

| 规则 | 说明 |
|---|---|
| **只拉取永续合约** | 适配器只返回永续合约（PERP），现货、交割合约、期权一律忽略 |
| **前缀匹配即同一币种** | 去掉计价币和合约类型后缀后，剩下的币种前缀相同，就视为同一币种 |
| **计价币 1:1** | USDC、USDG、USDT、USD 都按 1:1 处理，**不做汇率换算**，直接用原始价格计算价差 |
| **精度取最短** | 同一币种在不同交易所的数量精度不同时，以小数位数最少的那家为准，两边下单数量都按它对齐 |

### 2A.2 数据模型

```go
// domain/instrument.go
type Instrument struct {
    Venue       string          // lighter / ondo …
    VenueSymbol string          // 交易所原始代码，下单只用它，如 "SNDK-USDC"
    Base        string          // 匹配用的币种前缀，如 "SNDK"
    Quote       string          // 原始计价币，仅记录：USDC / USDG / USDT / USD
    SizeDecimals  int           // 数量小数位数，如 5 表示最小单位 0.00001
    PriceDecimals int           // 价格小数位数（只用于本交易所下单取整）
    MinQty      decimal.Decimal // 最小下单量
}
```

套利对的匹配键就是 `Base`（配置里写作 `SNDK-PERP`）。

### 2A.3 匹配流程

```
① 拉取永续合约列表 → ② 提取币种前缀 → ③ 按前缀分组 → ④ 计算对齐精度 → ⑤ 生成套利对
```

1. **拉取**：每个适配器实现 `PerpInstruments()`，按交易所文档读取合约类型字段，只保留永续合约。
2. **提取前缀**：从交易所代码里依次去掉
   - 合约类型后缀：`-PERP`、`_PERP`、`PERP`、`-SWAP`
   - 计价币后缀：`-USDC`、`USDC`、`-USDG`、`USDG`、`-USDT`、`USDT`、`-USD`、`USD`
   - 分隔符：`-`、`_`、`/`
   - 统一转大写

   剩下的就是币种前缀 `Base`：

   | 交易所代码 | 提取结果 |
   |---|---|
   | `SNDK` | SNDK |
   | `SNDK-USDC` | SNDK |
   | `SNDKUSDG-PERP` | SNDK |
   | `XAU/USD` | XAU |

   如果交易所元数据里直接给了基础资产字段，优先用该字段，不解析代码。
3. **分组**：`Base` 完全相同的合约归为同一币种。同一币种在 N 个交易所上，生成 N×(N−1)/2 个候选套利对。
4. **对齐精度**：见 2A.4。
5. **启用**：配置里 `pairs` 可以写具体套利对，也可以写 `auto: true` 表示所有匹配上的币种都启用（可配合白名单 / 黑名单）。

```go
// symbols/matcher.go
type Matcher interface {
    Base(venueSymbol string) string                       // 提取币种前缀
    Group(all []domain.Instrument) map[string][]domain.Instrument // Base → 各交易所合约
}
```

前缀提取规则（要去掉的后缀列表）放在配置里，接入新交易所时如有特殊命名，加一条后缀即可。

**注意：** 后缀必须完整去掉后再比较，不能用"字符串开头相同"判断，否则 `SOL` 会误配到 `SOLV`。

### 2A.4 精度对齐：以最短精度为准

**规则：** 同一套利对两边的数量精度取较小值。

```
pair.SizeDecimals = min(L.SizeDecimals, R.SizeDecimals)
pair.MinQty       = max(L.MinQty, R.MinQty)
```

**例子：SNDK**

| | 数量精度 | 最小单位 |
|---|---|---|
| A 所 | 小数点后 5 位 | 0.00001 |
| B 所 | 小数点后 3 位 | 0.001 |
| **套利对** | **小数点后 3 位** | **0.001** |

- `step_qty` 配成 0.0025 → 启动时**向下截断**为 0.002，并打印提示
- 两边下单都用 0.002，A 所能接受（0.002 也是 0.00001 的整数倍），B 所也能接受，两腿数量完全一致
- 截断后小于两边最小下单量中的较大者 → 启动报错，提示最小可用数量

**同样适用于：**
- 补救单的剩余数量 `remain`：按套利对精度截断；截断后剩下的零头（只可能出现在精度更高的那一边）用该交易所自己的精度单独平掉，并记录
- 部分成交后保留的对冲数量：按套利对精度向下截断

**价格精度不需要对齐：** 两边价格只用来算价差，各自下单时按各自的价格精度取整即可（买单向下取整、卖单向上取整，保证不吃穿第一档）。

### 2A.5 精度从哪里来

- 优先从交易所 API 的合约元数据读取（如 `size_decimals`、`lot_size`、`step_size` 等，字段名各家不同，由适配器统一转换成 `SizeDecimals`）。
- 每个交易所接入时，**对照官方文档核对一次**，并在适配器测试里固定几个币种的期望精度，防止接口字段理解错误。
- 配置里允许手动覆盖某个币种在某交易所的精度，用于文档和接口不一致的情况。

### 2A.6 运行期维护

- 每天（或交易所推送合约变更时）重新拉取永续合约列表，对比精度、最小下单量、上下架状态；**有变化就把相关套利对转 REDUCE_ONLY 并告警**，确认后再恢复。
- 合约下架或改名时，旧的未平仓步按原 `VenueSymbol` 平仓，不会因改名丢失仓位。

### 2A.7 已知风险（按当前规则接受）

| 风险 | 说明 | 兜底 |
|---|---|---|
| 计价币脱锚 | USDG 与 USDC 若偏离 1:1，会直接表现为价差，可能被误判为套利机会 | 滑动窗口均值会吸收稳定的偏差；突然跳变由"价差偏离过大"风控拦截 |
| 前缀相同但不是同一资产 | 例如两家都叫 `GOLD` 但标的指数不同，或一家是 `1000PEPE` 一家是 `PEPE`（后者前缀不同，不会匹配） | 启动时检查两边中间价偏差，超过 `match_max_diff_bps`（如 200 bps）的套利对不启用并告警 |

---

## 3. 价差定义

只使用盘口**第一档**，单位 bps（1 bps = 0.01%）。两边直接使用交易所原始价格，USDC / USDG 等计价币按 1:1 处理，不做换算。

```
mid       = (L.mid + R.mid) / 2
S_mid     = (R.mid − L.mid) / mid            // 中间价价差：只用于计算中心均值 μ
O_LS      = (R.bid1 − L.ask1) / mid          // 左多右空 可成交价差（买左卖右）
O_SL      = (L.bid1 − R.ask1) / mid          // 左空右多 可成交价差（卖左买右）
```

- 中心均值 μ 用 `S_mid` 计算，避免把盘口买卖价差混进中心。
- 开平仓判断用可成交价差 `O_LS` / `O_SL`，盘口买卖价差已自动包含在内。

---

## 4. 中心均值：滑动窗口均值

### 4.0 "每 1 秒采样一次"是什么意思

- 两边盘口通过 **WS 实时推送**，持续写入内存缓存（MarketData Hub），每次推送都更新为最新值。
- 采样器每 1 秒**读取一次内存里两边的最新盘口**（不发任何请求），算出 `S_mid`，放进滑动窗口。
- 为什么不用每次推送都算一个样本：盘口推送频率不均匀，活跃时每秒几十次、安静时几秒一次。按推送计数会让活跃时段权重过大；按固定 1 秒取样，每个样本代表等长时间，均值更准确。
- **采样只用于计算均值 μ**。开平仓判断用的是下单那一刻的实时盘口，不是 1 秒前的样本。

### 4.1 规则

| 参数 | 值 | 说明 |
|---|---|---|
| 采样周期 | 1 秒 | 固定时间采样，每个样本代表等长时间，等价于时间加权（含义见 4.0） |
| 窗口长度 | 2 小时 | 只保留最近 2 小时的样本，超出的从窗口移除 |
| 最低数据量 | 60 分钟 | 窗口内数据覆盖 ≥ 60 分钟才 `Ready`，之前只采样不交易 |
| 覆盖率要求 | ≥ 80% | 有效样本数 ≥ 时间跨度 × 采样频率 × 80%，防止数据断断续续时误判已就绪 |

```
μ = 窗口内所有有效 S_mid 样本的算术平均
```

### 4.2 样本过滤（进入窗口前）

以下样本丢弃，不进入窗口：
- 任一边盘口超过 `stale_ms`（默认 1000ms）没更新
- 两边盘口时间戳相差超过 `max_skew_ms`（默认 500ms）
- 任一边第一档缺失或买价 ≥ 卖价（盘口异常）

### 4.3 实现要点

- **环形缓冲 + 累加和**：每次 `Update` 加入新样本、按时间戳淘汰过期样本，同时维护 `sum`，`Mean()` 为 O(1)。
- 按**时间**淘汰，而不是按个数，数据中断时窗口不会把很久以前的样本算进来。
- 浮点累加误差：每淘汰 N 次（如 10 万次）用窗口内样本重算一次 `sum`。
- **重启快速预热**：启动时从 `spread_samples` 表加载最近 2 小时的样本灌入窗口；覆盖满 60 分钟即可直接进入运行，不必重新等 1 小时。

```go
// spread/estimator/sma.go
type SMA struct {
    window   time.Duration // 2h
    minSpan  time.Duration // 60m
    period   time.Duration // 1s，用于覆盖率计算
    coverage float64       // 0.8
    buf      []sample      // 环形缓冲
    head, n  int
    sum      float64
}

func (s *SMA) Update(v float64, t time.Time) { /* 入队、按 t-window 淘汰、维护 sum */ }
func (s *SMA) Mean() float64              { return s.sum / float64(s.n) }
func (s *SMA) Ready() bool {
    span := s.newest().Sub(s.oldest())
    need := float64(span/s.period) * s.coverage
    return span >= s.minSpan && float64(s.n) >= need
}
```

### 4.4 可替换设计

```go
// spread/estimator.go
type Estimator interface {
    Update(v float64, t time.Time) // 喂入一个有效样本
    Mean() float64                 // 当前中心值（bps）
    Ready() bool                   // 数据是否足够
    Stats() EstimatorStats         // 样本数、时间跨度等，供监控
}

type Factory func(cfg map[string]any) (Estimator, error)

func Register(name string, f Factory)            // 各算法在 init() 中注册
func New(name string, cfg map[string]any) (Estimator, error)
```

后续替换为 EWMA、滚动中位数、截尾均值、卡尔曼滤波等，只需新增一个实现并注册，然后改配置 `estimator.type`，策略和引擎代码不变。

---

## 5. 交易规则（策略层）

### 5.1 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `open_threshold_bps` | 1.0 | 开仓：可成交价差相对 μ 的偏离 ≥ 该值 |
| `close_profit_bps` | 1.0 | 平仓：这一步的往返毛利 ≥ 该值（暂不扣手续费、滑点） |
| `max_steps` | 100 | 单方向最大步数 |
| `interval_sec` | [7, 10] | 两次操作之间的随机间隔 |
| `step_qty` | 0.002 | 每步固定数量 |
| `min_depth_multiple` | 2 | 两边第一档挂单量都 ≥ `step_qty × 该值` 才允许下单 |
| `close_order` | `best` | 平仓顺序：`best` 收益最高先平（默认）/ `lifo` 后开先平 / `fifo` 先开先平 |

### 5.2 一步的往返毛利

```
左多右空的一步：毛利 = 开仓时 O_LS + 平仓时 O_SL
左空右多的一步：毛利 = 开仓时 O_SL + 平仓时 O_LS
```

### 5.3 决策顺序（每到可操作时间点执行一次）

0. **深度检查**：本次要用到的两边第一档挂单量都 ≥ `step_qty × min_depth_multiple`，否则本轮跳过。
1. **先判断平仓**：按 `close_order` 选出候选步（默认选往返毛利最高的一步），若 `开仓价差 + 当前反向可成交价差 ≥ close_profit_bps` → **减一步**
2. **再判断开仓**：
   - 步数 ≤ 0 且 `O_LS − μ ≥ open_threshold_bps` 且 `|步数| < max_steps` → **加一步左多右空**（步数 −1）
   - 步数 ≥ 0 且 `O_SL + μ ≥ open_threshold_bps` 且 `|步数| < max_steps` → **加一步左空右多**（步数 +1）
3. 都不满足 → 不动

**平仓顺序为什么默认"收益最高先平"：**
- 同方向所有步的平仓价格都一样（都是当前反向可成交价差），所以**开仓价差最好的那一步收益最高、最先达标**。
- 每次检查只需看这一步：它不达标，其他步一定也不达标。
- 分档设计里"后开先平"等价于收益最高先平（越往后开、偏离越大）；本版不分档，后开的步不一定开在更好的价位，所以改为直接按收益排序。
- 实现：每个方向的未平仓步按开仓价差排序（有序切片或堆），取最优者 O(1)。

说明：
- 单一步数计数器，同一时间只持有一个方向；反向条件出现时只能先平，不会同时持有两个方向。
- 一次只操作一步。
- 盘口变化时随时计算，但只有到了可操作时间才真正下单。

### 5.4 时间节奏

```
next_action_at = 上次操作完成时间 + rand(7s, 10s)
```

- 随机数源每个套利对独立（`math/rand/v2` 带种子），便于回测复现。
- 下单失败或跳过（深度不足）也重新计时，避免连续高频重试。

### 5.5 策略接口（纯函数）

```go
type Action struct {
    Kind ActionKind // None / Open / Close
    Side Side       // LongLeftShortRight / ShortLeftLongRight
    Qty  decimal.Decimal
    Ref  float64    // 决策时的可成交价差（bps）
}

type Strategy interface {
    Decide(st *PairState, snap Snapshot, mean float64, now time.Time) Action
}
```

### 5.6 套利对状态机

```
WARMUP ──(Estimator.Ready)──> RUNNING ⇄ REDUCE_ONLY
   ▲                             │
   └──(恢复后重新检查)── PAUSED <──┘  数据过期 / 单腿失败 / 熔断 / 仓位核对不一致
```

| 状态 | 允许开仓 | 允许平仓 | 采样 |
|---|---|---|---|
| WARMUP | ❌ | ✅（重启后有遗留步时） | ✅ |
| RUNNING | ✅ | ✅ | ✅ |
| REDUCE_ONLY | ❌ | ✅ | ✅ |
| PAUSED | ❌ | ❌ | ✅ |

---

## 6. 架构

```
        ┌──────────────── 适配器层（每个交易所一个包）────────────────┐
        │  lighter   rhlighter   ondo   vanta   arcus   ...          │
        └──────┬──────────────────────────────────▲─────────────────┘
      盘口推送 │                                  │ 提交订单 / 撤单 / 查订单 / 查仓位
               ▼                                  │        ▲ 订单回报（WS 推送）
      ┌─────────────────┐               ┌─────────┴────────┴┐
      │ MarketData Hub  │               │ Order Gateway      │ 按账户：限流、nonce、串行提交
      │ 每个合约最新盘口  │               │ OrderTracker       │ clientOrderID ↔ 回报 关联、等待终态
      └────────┬────────┘               └─────────▲─────────┘
               │ 更新通知（chan 容量 1，满即丢）     │
               ▼                                  │
   ┌───────────────────────────────────────────────┴──────────┐
   │ PairActor（每个套利对一个 goroutine）                       │
   │  Sampler(1s) → Estimator(μ)                                │
   │  Ticker(7~10s) → Strategy.Decide → RiskChain → Executor    │
   └───────────────┬──────────────────────────────────────────┘
                   │ 事件：StepOpened / StepClosed / Fill / Alert
                   ▼
              Event Bus ──> Store（数据库）──> API（HTTP / WS，预留）
```

---

## 7. 关键接口

### 7.1 交易所适配器

```go
type Venue interface {
    Name() string
    Instruments(ctx context.Context) ([]domain.Instrument, error)
    SubscribeBook(ctx context.Context, venueSym string, out chan<- domain.Book) error

    // 两段式下单：Submit 只负责提交，返回"已受理"；最终结果通过 SubscribeOrders 推送
    Submit(ctx context.Context, req domain.OrderReq) (domain.OrderAck, error) // 仅 IOC 限价，req 带 ClientID
    SubscribeOrders(ctx context.Context, out chan<- domain.OrderEvent) error  // WS 订单回报
    QueryOrder(ctx context.Context, venueSym, clientID string) (domain.OrderEvent, error) // 回报超时时兜底查询

    CancelOrder(ctx context.Context, venueSym, clientID string) error
    Positions(ctx context.Context) ([]domain.Position, error)
}

// domain.OrderEvent
type OrderEvent struct {
    ClientID  string
    Status    OrderStatus     // Accepted / PartiallyFilled / Filled / Canceled / Rejected / Expired
    FilledQty decimal.Decimal // 累计成交量
    AvgPrice  decimal.Decimal
    Reason    string
    At        time.Time
}
```

- **同步型交易所**（下单接口直接返回成交结果）：适配器在 `Submit` 返回后自己往 `SubscribeOrders` 的通道推一条终态事件。上层统一按"提交 + 等回报"处理，不区分交易所类型。
- `ClientID` 由系统生成（如 `pairID-stepID-leg-attempt`），贯穿提交、回报、查询和数据库，保证幂等和可追踪。

```go
// exchange/registry.go
func Register(name string, f func(cfg VenueConfig) (Venue, error))

// exchange/lighter/lighter.go
func init() { exchange.Register("lighter", New) }
```

`main` 通过空白导入加载适配器包，核心代码中不出现任何交易所名字。

### 7.2 风控规则链

```go
type RiskRule interface {
    Name() string
    Check(st *PairState, a Action, snap Snapshot) error // 返回非 nil 即拒绝
}
```

### 7.3 执行器

```go
type Executor interface {
    Execute(ctx context.Context, p *PairState, a Action, snap Snapshot) (StepResult, error)
}
```

---

## 8. 模块说明

### 8.1 MarketData Hub
- 每个合约存一份最新盘口（`atomic.Pointer[domain.Book]`），读无锁。
- 同一合约被多个套利对使用时只订阅一次，扇出通知。
- 通知 channel 容量 1，满了直接丢弃；Actor 每次读取的都是最新值。
- 维护盘口时间戳，供过期判断。

### 8.2 PairActor
- 独占状态：步列表（栈）、步数、状态机、`next_action_at`、Estimator 实例。
- `select` 三类事件：盘口通知、1 秒采样 tick、可操作时间到。
- 执行一步期间不再处理新决策，防止重复下单。

### 8.3 Executor（双腿执行）

#### 8.3.1 下单前检查
1. **深度**：两边要吃的第一档挂单量都 ≥ `step_qty × min_depth_multiple`（默认 2 倍）。目的是给别人抢先吃单留余量，降低 IOC 落空的概率。不满足 → 本轮跳过。
2. **数量**：固定 `step_qty`，按套利对对齐后的精度（两边较短的小数位，见 2A.4）向下截断，两腿数量完全一致。
3. **价格**：第一档价格，IOC 限价，**严格不吃穿第一档**。
4. **适用范围**：以上三条只约束**正常开仓 / 平仓单**。单腿失败后的补救单不做深度检查，价格按 8.3.5 逐步放宽，因为补救的首要目标是尽快消除裸露仓位。

#### 8.3.2 两腿怎么下：并发提交
- 默认 **两腿同时提交**（两个 goroutine 并发调用 `Submit`），然后并行等待两边回报。
- 理由：两边价格都是同一时刻的盘口，并发能让两腿成交时间最接近；先下一腿再下另一腿会多出一次回报延迟，第二腿的价格更容易变。
- 代价：可能出现一腿成功、一腿失败，由 8.3.4 的补救流程处理。
- 预留 `leg_mode: sequential`（先下盘口更薄的一腿，成交后再下另一腿），以后按交易所特点切换。

#### 8.3.3 等待回报（两段式）
```
Submit(ClientID) ──> 收到受理 ──> OrderTracker.Await(ClientID, order_result_timeout)
                                     │
                     ┌───────────────┼──────────────────┐
                 WS 推送终态      超时未收到回报      Submit 本身报错
                     │               │                    │
                  得到结果     QueryOrder 主动查询      视为未成交（但仍要查询确认，
                                 │                     防止"请求超时但实际已成交"）
                          查到终态 / 仍未知 → 继续查，直到确定
```
- **终态**：Filled、Canceled（IOC 未成交部分被撤）、Rejected、Expired。只有拿到终态（或查询确认）才算这条腿结束。
- **未知状态绝不当成失败处理**：先查清楚，否则可能重复下单或重复平仓。

#### 8.3.4 结果处理

两腿都拿到终态后，按成交量判断（记左腿成交 `qL`，右腿成交 `qR`）：

| 情况 | 处理 |
|---|---|
| `qL = qR = step_qty` | 成功，这一步记为 OPEN（或平仓时 CLOSED） |
| `qL = qR = 0` | 两边都没成交，什么都不用做，本轮结束 |
| `qL ≠ qR`（一腿失败或部分成交） | 进入**补救流程** |

**补救流程（开仓阶段）：把多出来的那部分立即平掉**
- 多出部分 = `|qL − qR|`，在成交多的那一边**反向平仓**（reduce-only），使两边仓位重新相等。
- 例：左腿买入 0.002 成功，右腿卖出失败 → 立即在左边卖出 0.002 平掉。
- 若两边都有部分成交（如左 0.002、右 0.001），保留对冲好的 0.001 作为这一步，平掉左边多出的 0.001。

**补救流程（平仓阶段）：把没平掉的那一腿继续平完**
- 平仓时一腿成功、一腿失败，**不要把成功的那腿重新开回来**，而是继续平掉失败的那一腿，让这一步完整结束。

**循环直至成功：**
```
attempt := 0
for {
    remain := 查询该交易所当前实际仓位 − 应有仓位      // 每次重试前先核对，防止重复平仓
    if remain == 0 { break }
    price  := rescuePrice(attempt, 最新盘口)          // 按 8.3.5 逐步放宽
    提交 reduce-only IOC（数量 remain，ClientID 带 attempt 序号）
    等待回报（同 8.3.3）
    attempt++
    退避等待 rescue_backoff（200ms 起，每次翻倍，上限 1s）
}
```
- **reduce-only**：补救单只能减仓，即使重复提交也不会反向开出新仓位。
- **价格**：重试几次后逐步放宽，规则见 8.3.5；这部分滑点单独记账。
- **补救期间**：该套利对暂停新的开平决策；补救超过 `rescue_alert_after`（如 10 秒）发告警；补救完成后这一步记为 `BROKEN`，计入连续失败熔断。
- **不阻塞其他套利对**：补救在本套利对的 Actor 内执行，其他套利对照常运行。

#### 8.3.5 补救单价格：重试几次后逐步放宽

**分三个阶段，每次重试都基于最新盘口重新计算价格：**

| 阶段 | 第几次（attempt 从 0 开始） | 卖单限价（买单对称） | 目的 |
|---|---|---|---|
| ① 严格第一档 | 0 ～ `rescue_strict_attempts − 1`（默认 0～2） | `bid1` | 不付额外滑点，大多数情况下前几次就能成交 |
| ② 逐次让价 | 之后每次 +1 tick，最多 `rescue_max_ticks` 个（默认 3～7 次，让 1～5 tick） | `bid1 − k × tick` | 能吃到第一档之后的少量挂单 |
| ③ 保护性市价 | 超过 `rescue_max_ticks` 之后的每一次 | `mid × (1 − rescue_max_slippage_bps)` | 确保一定能平掉 |

```go
// execution/rescue.go
func rescuePrice(attempt int, side Side, b Book, inst Instrument, c RescueCfg) decimal.Decimal {
    best := b.Bid1; dir := -1           // 卖单：向下让价
    if side == Buy { best = b.Ask1; dir = 1 } // 买单：向上让价

    switch k := attempt - c.StrictAttempts; {
    case k < 0:                          // ① 严格第一档
        return best
    case k < c.MaxTicks:                 // ② 每次多让 1 tick
        return best.Add(inst.Tick.Mul(decimal.NewFromInt(int64(dir * (k + 1)))))
    default:                             // ③ 保护性市价
        return b.Mid.Mul(decimal.NewFromFloat(1 + float64(dir)*c.MaxSlippageBps/1e4)).Round(inst.TickPlaces)
    }
}
```

**规则说明：**
- **"市价"也带保护价**：很多永续 DEX 的市价单本身需要一个最差成交价，统一用"中间价 ± `rescue_max_slippage_bps`（默认 30 bps）"的 IOC 限价代替纯市价单，防止在极端行情下成交到离谱的价格。
- **阶段 ③ 仍未成交**：说明盘口异常（深度被抽空或交易所故障），继续按阶段 ③ 循环，同时立即升级告警，并把该账户下**所有套利对转为 REDUCE_ONLY**。
- **每个补救流程独立计数**：attempt 从 0 开始，补救结束即清零，不影响下一次补救。
- **价格按最新盘口计算**：每次重试都重新读取盘口，让价是相对"当时的第一档"而言，不会在旧价格上累加。
- **记账**：每笔补救单记录阶段、让价 tick 数、实际成交价相对第一档的滑点（bps），汇总到这一步的 `rescue_cost_bps`，便于统计补救成本、调整参数。

**时间线示例**（默认参数：严格 3 次、最多让 5 tick、退避 200ms 翻倍至 1s；时间只算退避，不含等待回报的耗时）：

| attempt | 退避后约在 | 价格 |
|---|---|---|
| 0 | 0s | 第一档 |
| 1 | 0.2s | 第一档 |
| 2 | 0.6s | 第一档 |
| 3 | 1.4s | 让 1 tick |
| 4～7 | 2.4s～5.4s | 让 2～5 tick |
| 8 起 | 6.4s 起 | 保护性市价（中间价 ± 30 bps） |

正常情况下 1～2 秒内平掉；最坏约 6～7 秒进入保护性市价，10 秒仍未完成触发告警（`rescue_alert_after`）。

步的生命周期：`PENDING → OPEN → CLOSING → CLOSED`，异常为 `BROKEN`。

### 8.4 Order Gateway / OrderTracker

**Order Gateway**
- 按**账户**而非套利对划分，同一账户的所有套利对共享限流和 nonce（Lighter 这类按 API Key 维护 nonce 的必须串行提交）。
- 令牌桶限流，参数来自交易所配置。补救单优先级高于普通开平单。

**OrderTracker**（每个账户一个）
- 订阅该账户的 `SubscribeOrders`，维护 `ClientID → 等待者` 映射。
- `Await(ctx, clientID, timeout)`：阻塞直到收到该订单的终态事件或超时。
- 处理"回报比受理先到"的情况：事件先存入短期缓存，`Await` 注册时先查缓存。
- WS 断线重连后，对所有仍在等待的订单逐个 `QueryOrder` 补查，防止回报丢失。

### 8.5 RiskChain（本版规则）
| 规则 | 动作 |
|---|---|
| 盘口过期 / 两边时间戳偏差过大 | 拒绝本次操作 |
| 步数达到 `max_steps` | 拒绝开仓 |
| 单交易所名义仓位上限（按账户汇总） | 拒绝开仓 |
| 账户保证金率低于阈值 | 转 REDUCE_ONLY |
| 资金费结算前后 N 分钟 | 拒绝开仓 |
| 连续 BROKEN 达到上限（如 25） | 转 PAUSED |
| 当日亏损超过上限 | 转 PAUSED |
| 合约参数变更（精度、最小下单量、下架） | 转 REDUCE_ONLY 并告警 |
| 启动时两边中间价偏差 > `match_max_diff_bps` | 该套利对不启用并告警 |

### 8.6 Store
| 表 | 字段要点 |
|---|---|
| `steps` | pair_id、方向、序号、开仓价差、开仓时 μ、平仓价差、毛利、状态、开/平时间、rescue_cost_bps |
| `orders` / `fills` | 每腿订单与成交，关联 step_id；含 client_id、类型（open / close / rescue）、attempt、补救阶段（strict / tick / market）、让价 tick 数、相对第一档滑点 bps、终态、成交量、均价 |
| `spread_samples` | pair_id、时间、S_mid、O_LS、O_SL（1 秒一条，用于预热和回测） |
| `pair_state` | 步数、状态快照 |

**重启恢复**：从 `steps` 还原未平仓步 → 与交易所实际仓位核对 → 一致则继续，不一致转 PAUSED 等人工处理；同时从 `spread_samples` 加载最近 2 小时样本预热 Estimator。

---

## 9. 目录结构

```
cmd/arbd/main.go                 # 读配置、组装、启动
internal/
  domain/                        # 纯数据类型：Instrument、Book、Step、Action、Side…（无依赖）
  config/                        # 配置加载与校验
  exchange/                      # Venue 接口 + 注册表
    lighter/ rhlighter/ ondo/ vanta/ arcus/
  symbols/                       # 永续合约拉取、前缀提取、分组、精度对齐
  marketdata/                    # Hub、盘口缓存
  spread/                        # 价差计算、样本过滤
    estimator/                   # Estimator 接口 + 注册表
      sma.go                     # 滑动窗口均值（本版默认）
  strategy/                      # 步进策略（纯函数）
  execution/                     # 双腿执行器
    rescue.go                    # 补救流程 + 价格放宽（rescuePrice）
    gateway.go                   # 按账户限流、nonce、串行提交
    tracker.go                   # OrderTracker：WS 回报关联与等待
  risk/                          # 风控规则
  engine/                        # PairActor、套利对生命周期管理
  store/                         # 数据库读写
  api/                           # HTTP / WS（预留）
```

依赖只能向内：`exchange/*`、`store`、`api` → `domain`；`domain` 不依赖任何内部包。`strategy` 不依赖 `exchange` 和 `store`。

---

## 10. 配置示例

```yaml
accounts:
  main: { key_env: LIGHTER_KEY }

venues:
  lighter: { account: main, rate_limit: 20/s }
  ondo:    { account: main, rate_limit: 10/s }

matching:
  perp_only: true                # 只拉取永续合约
  strip_suffixes: [-PERP, _PERP, PERP, -SWAP, -USDC, USDC, -USDG, USDG, -USDT, USDT, -USD, USD]
  quote_parity: 1:1              # USDC / USDG / USDT / USD 一律 1:1，不换算
  match_max_diff_bps: 200        # 启动校验：两边中间价偏差上限
  whitelist: []                  # 为空表示不限制
  blacklist: []

precision_overrides:             # 可选：文档与接口不一致时手动覆盖
  ondo:
    SNDK: { size_decimals: 3 }

defaults:
  estimator:
    type: sma
    window: 2h
    min_span: 60m
    sample_period: 1s
    coverage: 0.8
  open_threshold_bps: 1.0
  close_profit_bps: 1.0
  close_order: best              # best | lifo | fifo
  interval_sec: [7, 10]
  min_depth_multiple: 2          # 第一档挂单量 ≥ 下单量 × 2
  stale_ms: 1000
  max_skew_ms: 500
  execution:
    leg_mode: concurrent         # concurrent | sequential
    order_result_timeout: 2s     # 等 WS 回报的超时，超时后主动查询
    rescue_backoff: [200ms, 1s]  # 补救重试退避（起始，每次翻倍，上限）
    rescue_strict_attempts: 3    # ① 前 N 次严格第一档
    rescue_max_ticks: 5          # ② 之后每次多让 1 tick，最多让 N tick
    rescue_max_slippage_bps: 30  # ③ 保护性市价：中间价 ± 该值
    rescue_alert_after: 10s      # 补救超过该时长告警

pairs:
  - symbol: XAU-PERP
    left: lighter
    right: ondo
    step_qty: 0.002
    max_steps: 100
  - symbol: SNDK-PERP            # A 所 5 位、B 所 3 位 → 按 3 位对齐
    left: lighter
    right: ondo
    step_qty: 0.002
    max_steps: 100
  # 或：所有前缀匹配上的币种自动启用（受 whitelist / blacklist 限制）
  # - auto: true
  #   step_qty_usd: 10           # 自动启用时按名义金额折算数量，再按对齐精度截断
```

每个 pair 可覆盖 `defaults` 中的任意字段。

---

## 11. 扩展方式

| 需求 | 做法 | 核心代码是否改动 |
|---|---|---|
| 接入新交易所 | 新建 `exchange/xxx`，实现 `Venue` 并注册 | 否 |
| 新增币种 / 套利对 | 交易所上架后自动按前缀匹配；`auto: true` 时自动启用，否则写进 `pairs` | 否 |
| 交易所命名特殊 | 在 `strip_suffixes` 加一条后缀 | 否 |
| 替换中心值算法 | 在 `spread/estimator` 新增实现并注册，改 `estimator.type` | 否 |
| 门槛加入手续费、滑点 | 改 `open_threshold_bps` / `close_profit_bps`，或新增成本模型注入策略 | 否 |
| 新增风控规则 | 实现 `RiskRule`，加入规则链配置 | 否 |
| 更换策略（如分档） | 实现 `Strategy` | 否 |
| 回测 | 回放 `spread_samples`，复用同一 `Strategy` + `Estimator`，执行器换模拟成交 | 否 |

---

## 12. 待确认事项

1. 各交易所实际手续费、挂单/吃单费率、API 限频、nonce 机制，以官方文档为准。
2. 门槛 1 bps 为毛利；上线前用 `spread_samples` 回测，确认扣除成本后的平均净收益。
3. 同一账户下多个套利对共享保证金，仓位上限需按账户汇总检查。
4. XAU 等有休市时段的品种，休市期间是否转 REDUCE_ONLY，按品种配置。
5. 各交易所永续合约的数量精度、价格精度、最小下单量、合约类型字段，接入时对照官方文档逐个核对。
