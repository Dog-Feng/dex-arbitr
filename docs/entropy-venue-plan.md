# Entropy DEX 接入方案（Hyperliquid HIP-3 `io`）

> 分支：`feat/entropy-venue-20261019`。状态：对接实现完成（本地未提交），待实盘账户验证。

## 0. 结论

Entropy（ticker `io`）是 Hyperliquid 上的 **HIP-3 builder-deployed perp dex**，
不是独立交易所：撮合、保证金、清算都跑在 Hyperliquid 主网匹配引擎上，**没有
Entropy 专有 API**。对接直接使用 Hyperliquid 标准 API/SDK（REST + WS），通过
`dex` 参数和 `io:` 前缀的 coin 名访问 Entropy 市场。SPY 不在 Entropy universe
中，因此无法直接配对现有 SPY 交易对；可配资产为 `io:*`（SNDK/NBIS/OAI/ANTH 等）。

## 0.1 已确认参数（全部经过实测或官方 SDK 0.24.0 逐字节验证）

### Dex 与资产 ID

- `{"type":"perpDexs"}` 实测：`io` 的 perp dex 索引 = **10**。
- 资产 ID 公式（官方 asset-ids 文档）：builder perp = `100000 + perp_dex_index*10000
  + index_in_meta`。因此 `io:*` 资产 ID = `200000 + index_in_meta`。
- 实测 `io` universe（14 个，3 个已下架）及元数据索引：
  `io:OAI`(200000)、`io:ANTH`(200001)、`io:SNDK`(200002)、`io:IONQ`(200003)、
  `io:NBIS`(200004)、`io:EWY`(200005)、`io:GPRO`(200006)、`io:SBE`(200007, 已下架)、
  `io:TCNT`(200008)、`io:DRAM`(200009)、`io:PRL`(200010)、`io:XIAOMI`(200011)、
  `io:H100`(200012, 已下架)、`io:MOONSHOT`(200013, 已下架)。
- 默认永续 dex（`dex` 缺省）资产 ID = meta 索引本身（BTC=0），**不使用** 100000 偏移。

### 行情 / 账户 / 订单状态（`POST https://api.hyperliquid.xyz/info`）

- `{"type":"meta","dex":"io"}`：universe、`szDecimals`、`maxLeverage`(3~25)、
  `onlyIsolated: true`（全部资产仅支持逐仓）、`marginMode: "strictIsolated"` 或
  `"noCross"`（io:OAI）、`growthMode: "enabled"`、`deployerFeeScale: "1.0"`。
- `{"type":"l2Book","coin":"io:SNDK"}`：实测可用，20 档/侧。
- `{"type":"clearinghouseState","user":addr,"dex":"io"}`：`assetPositions` 含
  `szi/entryPx/liquidationPx/positionValue/leverage.type`，`marginSummary` /
  `crossMarginSummary` 同源返回权益与维持保证金。逐仓仓位用 `liquidationPx`
  直接参与现有 `remainingMargin` 清算距离计算。
- `{"type":"orderStatus","user":addr,"oid":"0x...32hex..."}`：按 oid **或 128 位
  cloid** 查询终态（实测 `dex` 参数可带）。状态值集合见官方文档（filled/canceled/
  rejected/marginCanceled/… 均已映射）。
- `{"type":"userFillsByTime","user":addr,"startTime":ms,"endTime":ms}`：按 oid
  聚合成交量和成交额，用于终态补全成交明细。

### 下单 / 撤单（`POST https://api.hyperliquid.xyz/exchange`）

- 下单 body：`{action:{type:"order",orders:[{a,b,p,s,r,t,c}],grouping:"na"},
  nonce(ms), signature, vaultAddress?, expiresAfter?}`。`t:{limit:{tif:"Ioc"}}`、
  `r=reduceOnly`、`c=cloid`。
- **cloid 是 128 位（0x + 32 hex）**。实测 256 位 cloid 会让服务器直接
  `422 Failed to deserialize`。
- 响应 `statuses`：`resting{oid}` / `filled{totalSz,avgPx,oid}` / `error`。
- 撤单用 `cancelByCloid`（asset + cloid）。
- WS（`wss://api.hyperliquid.xyz/ws`）：`l2Book` 订阅 `{type:"l2Book",coin:"io:SNDK"}`
  （整本快照推送，非增量）；`orderUpdates` / `userFills` 订阅 `{type:...,user:addr}`
  （公开流，无需签名）。`allMids` 订阅带 `dex` 参数。
- **盘口推送节奏（实测）**：`l2Book` 不带参数时被节流到 ~5s/帧；带 `fast:true`
  时 ~0.54s/帧（5 档/侧）；`bbo` 频道 ~0.40s/帧（仅最优买卖一档）。适配器订阅
  `l2Book(fast)+bbo` 并把 bbo 合并进 book state 的顶层，实测有效更新 ~0.27s/帧
  （3.7 Hz）。订单终态事件（orderUpdates/userFills）按块即时推，不受盘口节流
  影响。
- **对应配置**：`volatility.max_age: 600ms`（给 0.4~0.54s 节奏留 1.1~1.5 倍
  余量；400ms 零余量、推送抖动会打碎 σ 采样链），`strategy.max_book_age: 3s`。

### 签名方案（与官方 hyperliquid-python-sdk 0.24.0 逐字节一致）

- API wallet（agent）签名 = EIP-712 主类型 `"Agent"`：
  - domain：`{name:"Exchange", version:"1", chainId:1337, verifyingContract:0x0}`；
  - message：`{source:"a"(主网)/"b"(测试网), connectionId: bytes32}`；
  - `connectionId = keccak(msgpack(action) || nonce_be64 || 0x00)`（0x00 为无
    vault 标记；有 `expiresAfter` 时再追加 `0x00 || expires_be64`）。
- msgpack 字段顺序固定：order 为 `type,orders,grouping`，order wire 为
  `a,b,p,s,r,t[,c]`，`t:{limit:{tif}}`；cancelByCloid 为 `type,cancels`，
  cancel wire 为 `asset,cloid`。数值用最小长度编码，`p/s` 为 `%.8f` 去尾零
  字符串（SDK `float_to_wire`）。
- 签名返回 `{r,s,v}`，v ∈ {27,28}，r/s 为 0x 开头、无前导零填充的 hex。
- **已实测**：Go 实现签名与 SDK 参考向量（同私钥/同 action/同 nonce）逐字节
  一致；主网 `/exchange` 对 Go 签名请求的拒绝为 `User or API Wallet ... does not
  exist`（即签名通过、仅身份未授权），而非 `Invalid signature`。

### 费率与资金

- 所有 `io` 资产 `onlyIsolated: true`：现有引擎按逐仓清算价计算风险，不依赖
  全仓权益；`AccountSnapshot.CrossMargin` 只在存在 cross 仓位时返回。
- growth mode + `deployerFeeScale=1.0`：基础档 taker **9 bps**、maker **3 bps**
  （`0.00045 × 2 × 0.1`）。账户有更高费率档/质押折扣时实际更低，静态取基础档
  是保守估计。
- 抵押品是 **USDC（tokenId 0）**，且按 dex 隔离：`io` 有独立 USDC 余额。交易前
  需把资金转入 io dex（UI 划转，或 `agentSendAsset` sourceDex="spot"/"" →
  destinationDex="io"）。本适配器暂不自动划转。
- 默认永续 dex 基础档 taker 4.5 bps / maker 1.5 bps（不乘 HIP-3 系数）。
- Lighter 合约规格实测包含 ANTHROPIC（5x 杠杆，tick 0.1，min qty 0.00001）；
  与 io 符号完全一致的可配标的还包括 SNDK / NBIS / EWY / DRAM / PRL / XIAOMI。

## 1. 架构映射（已实现）

- 新包 `internal/exchange/hyperliquid`：`venue.go`（工厂/Instruments/账户）、
  `rest.go`（info/exchange 客户端）、`ws.go`（单连接订阅+重连重订阅）、
  `sign.go`（msgpack + EIP-712 + cloid）、`orders.go`（Submit/Cancel/Query/
  离线签名自检）。
- 注册两个 kind：`hyperliquid`（默认永续 dex，dex 为空）与 `entropy`
  （preset dex=`io`）。`cmd/arbd`、`cmd/scanner`、`cmd/selfcheck` 已空白导入。
- 复用现有 `exchange.Venue` 契约：IOC + reduce-only + cloid→ClientID、WS 订单/
  成交流、`AccountSnapshotter`（clearinghouseState 一次请求同源返回仓位与账户
  风险）。`QueryOrder` 走 orderStatus+userFillsByTime；filled 但成交未索引时
  返回非终态，让执行器走仓位推断（fail-safe）。
- 价格收紧链：executor 决策价 → PriceDecimals(=6-szDecimals) 收紧 → 5 位有效
  数字收紧（buy floor / sell ceil），保证不会放宽最差成交价。
- 配置示例（configs/config.yaml，注释形式）：
  `kind: entropy`，opts：`account`(主账户地址)、`private_key`(agent 私钥)、
  `book_depth`。
- 依赖：`go-ethereum v1.15.6` 由 indirect 提升为 direct（EIP-712 哈希与
  secp256k1 签名）。它本就在模块图与 go.sum 内，无新增二进制。

## 2. 实施阶段（完成度）

- **P1 只读接入**：完成。`Instruments`（meta+perpDexs→资产 ID）、`SubscribeBook`
  （WS 整本快照）、`Positions`/`AccountSnapshot`（clearinghouseState）。主网
  实测：io:SNDK→200002 映射正确、WS 盘口 20 档可用。
- **P2 交易接入**：代码完成，签名已经主网验证；**实盘未验证**（需要真实账户：
  在 Hyperliquid 主网授权 agent，并向 io dex 划入 USDC）。
- **P3 风控兼容**：完成（逐仓清算价直通现有 remainingMargin；io 全逐仓因此
  CrossMargin 通常为 nil，属预期降级）。`LeverageSetter` 未实现：`leverage`
  配置不会应用到 hyperliquid 腿，需在 UI 预设逐仓杠杆，或用后续 updateLeverage
  action 补齐。
- **P4 实盘验证**：待办。流程：`dry_run: true` 全链路 → `selfcheck -live` →
  小仓实盘 1~2 天 → 并入主力配置。

## 2.1 实盘前置清单（操作侧）

1. **账户与 agent（已就绪）**：arb.env 的 agent 私钥经 `userRole` 实测归属主
   账户 `0xc013...9197`（arb.env 的 ENTROPY_ACCOUNT 已修正为该地址）。主账户在
   默认永续 dex 有约 98.33 USDC。
2. **余额与入金（已核实，注意账户模式）**：Entropy 前端把用户在所有 perp dex
   的余额加总展示（读了 `hyperliquid-account` 模块源码确认），因此 UI 显示的可
   用余额包含默认永续 dex 的钱，不等于 io dex 余额。标准模式下交易 io 需先把
   USDC 划入 io dex；`agentSendAsset` 可由 agent 签名完成划转，本适配器已实现
   （`Venue.SendAsset` + `cmd/hltransfer`），并已实盘验证（0.0001 与 50 USDC
   两笔成功）。**注意：账户当前处于 unifiedAccount 模式**，该模式下所有永续
   资金集中在现货余额、随交易自动流转，io dex 余额恒为 0 属正常，无需手工入
   金；逐仓杠杆建议在 UI 预设（适配器暂未实现 SetLeverage）。
3. **Lighter 腿凭据（待办）**：ANTH 的另一条腿是 Lighter 的 ANTHROPIC
   （`io` 侧叫 ANTH，经 `matching.aliases: {ANTH: ANTHROPIC}` 归一化配对）。
   本机没有 LIGHTER_* 凭据，且 Lighter API 对本机/备份 VPS 均 403；小额实盘
   需在部署机（AWS，可访问 Lighter）配置 `LIGHTER_ACCOUNT_INDEX` /
   `LIGHTER_API_KEY_INDEX` / `LIGHTER_PRIVATE_KEY` 后运行
   `configs/anth-live.yaml`。
4. **测试配置与结果**：`configs/anth-live.yaml`（rhlighter × entropy、dry_run
   默认 true、step_qty 0.01、max_steps 2、monitor 关闭、state_dir=state-anth）。
   已实盘验证：`selfcheck -live` 22/22 通过——双端开仓 0.01 成交、双端平仓成交、
   终态仓位归零、零补救单。io 腿 3 笔成交净亏 0.004524 USD（平仓 PnL -0.001 +
   手续费 0.003524），与账户余额变化逐分核对一致；io 历史订单恰为本次 4 条。
   注意 selfcheck 的 `-symbol` 与归一化后的 base（ANTHROPIC）一致。

## 2.2 交易对组合（不局限于 Lighter）

- 系统本身以 venue 名为单位任意两两配对（`pairs.left/right` 引用任意两个已配
  置 venue），lighter/rhlighter/hyperliquid/entropy 可自由组合；跨场符号不一
  致时用 `matching.aliases` 归一化（如 `ANTH: ANTHROPIC`）。
- 当前 RH Lighter × Entropy 的符号交集为 3 个（ANTHROPIC/OPENAI/SNDK，
  含内置别名），Lighter × Entropy 为 7 个
  （ANTHROPIC/SNDK/NBIS/EWY/DRAM/PRL/XIAOMI）。

### 符号配对的四层策略

两个 DEX 都只提供 `symbol` 一个身份字段（实测 RH 原始元数据无 ISIN/全名等
标识），同一标的可能以不同缩写出现（Entropy `OAI` = RH `OPENAI`）。配对按
零假阳性原则分层：

1. **精确相等**：`symbols.Match` 的默认行为。
2. **确定性别名表**：内置 `symbols.DefaultAliases()`（当前 `ANTH→ANTHROPIC`、
   `OAI→OPENAI`），配置 `matching.aliases` 可覆盖/扩展。这是唯一影响实盘交易
   的层，条目人工审核，绝不运行时自动推导（配错 = 拿无关资产对冲）。
3. **统计发现/校验（建议下一步，只读）**：计划 `cmd/pairdiscover`——对候选
   跨场组合采样双方 mid，同标的必有收益相关系数≈1、价格比率标准差≈0；用于
   发现未知映射，并持续校验第 2 层（比率异常 ⇒ 别名错误或标的变更，告警）。
4. **人工启用**：工具只输出建议，确认后写入配置才生效。

实测：内置别名后 RH Lighter × Entropy 从 2 对增至 3 对
（ANTHROPIC / OPENAI / SNDK）。

## 2.3 签名实现质量锚点

- 与官方 hyperliquid-python-sdk 0.24.0 的参考向量逐字节一致：下单、撤单
  （cancelByCloid）、agentSendAsset 三套签名全部固化为单测。
- 实测发现并修复了一个 msgpack 编码 bug：uint64 曾只写低 4 字节，导致含 nonce
  的 action（agentSendAsset）哈希错位、签名被服务器拒绝；订单签名因 nonce 不
  进 msgpack 而幸免。修复后 agentSendAsset 已实盘成功。

## 3. 风险与决策点

- **SPY 不在 Entropy**：现有 SPY（Lighter↔RHLighter）无法迁移。新对必须从
  `io:*` 与 Lighter 的交叉集合中选择；Lighter 侧列表需在可访问 Lighter API
  的服务器上确认（本机与备份 VPS 均被其反爬 403）。
- **费率与精度**：Entropy 的 taker 费率与 tick/size 与 Lighter 不同，接入后回测参数（fee_bps、让价 tick 计算）必须重跑校准。
- **逐仓 + 零清算价**：io 资产若返回 0/null `liquidationPx`（如 noCross 模式），
  现有风控按 fail-closed 判定 `position_risk_unknown` 并禁止开仓，属预期保守
  行为，运营需知晓。
- **订单终态**：filled 但成交流未回传时，适配器不下发零成交的终态，执行器按
  仓位增量推断（安全路径，不虚构成交价）。
- **部分成交归一化**：Hyperliquid 的 IOC 部分成交后剩余自动取消，但终态仍报
  `filled`。执行器要求 FILLED 终态等于请求量，因此适配器在所有终态发布点把
  “FILLED 且 0<累计成交<请求量”归一化为 `Canceled`（部分成交），让执行器走
  常规的部分成交+补救路径，避免 `invalid terminal fill ... status FILLED`
  致命错误（2026-10-20 实盘曾因此 PAUSED 并悬置仓位 4.5 小时）。请求量来源：
  Submit 时的取整数量、WS orderUpdates 的 `origSz`、REST orderStatus 的
  `origSz`。注意：FILLED 但成交事实缺失（qty=0）仍保持非终态，不做归一化。
- **部分成交后的三级处理（executor）**：开仓单腿部分成交时，
  1) 若短差 ≥ 两腿最小可交易量且补单价在 `execution.topup_bps`（默认 25 bps）
  硬上限内，短腿同方向补一单把整步补全（只试一次，失败回退）；
  2) 补不了但缩腿量可交易 → 缩长腿（原有行为）；
  3) 缩腿量也低于任一腿的最小值 → 整步强平到零（每一步减仓量=完整仓位，
  必然可交易）；若某条腿的完整仓位本身低于最小值，返回显式错误并告警
  （等待人工处理）。flatten 对低于 venue 最小值的减仓量直接报错，不再无限
  重试。配套：hyperliquid Instruments 的 `MinQuote=10`（HL 最低订单额），
  使数量校验与最小可交易量计算知道该下限。

## 4. 验收标准

- `cmd/scanner -config` 能列出 `io:` 全部市场并正常打分（已完成实测）；
- `cmd/selfcheck` 通过（只读凭证、行情新鲜度、离线签名自检）；
- `dry_run` 下选定交易对完成 开仓→平仓→补救 闭环；
- 风控字段齐全，或降级行为有明确日志与文档说明；
- 全量 `go build` / `go vet` / `go test -race ./...` 通过；
- 签名与官方 SDK 参考向量逐字节一致（已纳入单测，`TestSignReferenceVectors`）。
