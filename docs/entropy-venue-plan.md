# Entropy DEX 接入方案

> 分支：`feat/entropy-venue-20261019`。状态：调研完成（外部探测），待 API 文档补全后实施。

## 0. 调研结论

### 已确认

- **产品形态**：perp 交易平台，官网 `entropy.io`（"Trade the Frontier"），交易域 `entropy.trade`，后端 API 域名 `https://api.entropy.trade`（前端 `api` 模块中硬编码），静态资源 `assets.entropy.trade`。
- **架构线索**（前端 bundle 模块名）：`hyperliquid-ws`、`hl-stream`、`hyperliquid-account`、`cloid-pool`、`signed-execution`、`request-deadline`、`useOrderSigner`——与 Hyperliquid 同构：链下撮合 + 签名执行 + cloid 限价单池，订单带 nonce/deadline 签名。
- **账户体系**：Privy 嵌入式钱包 + 会话 Bearer token；统一后端返回的持仓 schema 为 `coin/size/entryPx/markPrice/liquidationPx/marginUsed/leverage`。
- **结算链**：Arbitrum（前端经 `arb-rpc` 用 `eth_call` 读 USDC 余额，说明链上资产在 Arbitrum）。
- **API 风格**：自定义路由（`GET /` 返回 `Cannot find any route matching ...` 的路由框架），与 Lighter 的 `/api/v1/markets` 不兼容，不能直接复用 lighter 适配器。
- GitHub `entropyxyz` 组织是托管/门限签名项目（"Trustless and secure crypto custody"），可能是交易所的托管钱包基建，但不是交易所协议本身。

### 待确认（实现前必须补齐）

1. **完整端点清单**：行情（盘口快照/增量、成交）、下单、撤单、订单状态、账户/仓位/快照、市场元数据（tick/size/最小量/taker 费率）。可从 `/assets/usePlaceOrder-*.js`、`useOrderBook-*.js`、`hyperliquid-ws-*.js`、`useOrderSigner-*.js` 提取，或向熵方索取 API 文档。
2. **程序化签名方案（最大阻塞点）**：前端走 Privy 嵌入式钱包，服务端不能依赖 Privy。需要确认熵方是否提供 API key / 服务端签名（EIP-712 结构、nonce、deadline）路径。若无，实盘接不了，只能做只读行情。
3. **链上交互面**：Arbitrum 上的结算/充值合约是否需要本系统自己发交易（类比 lighter 的 CreateOrderTx），还是统一后端代签。
4. **WS 流格式与频率**：订阅协议、nonce 连续性、断线重连语义。

## 1. 架构映射

现有扩展点是 `internal/exchange/venue.go` 的 `exchange.Venue` 接口（`Instruments`、`SubscribeBook`、`Submit`、`Positions`），以及可选的 `AccountSnapshotter`（全仓快照，用于零清算价保护）与 `LeverageSetter`。Entropy 接入按同一模式：

- 新包 `internal/exchange/entropy/`：`venue.go`（实现 Venue）、`rest.go`（元数据/行情 REST）、`ws.go`（订阅）、`orders.go`（签名执行）、`account.go`（账户与快照）。
- 通过 `Register("entropy", ...)` 注册，`cmd/arbd` 用 `_ "internal/exchange/entropy"` 空白导入，与 lighter/rhlighter 并列。
- `ResolvePair` 的精度对齐（size_decimals、min_qty）与风控字段（清算价、维持保证金）沿用现有逻辑，只是数据源换成 Entropy 的 schema。

## 2. 实施阶段

- **P0 调研补全（0.5 天）**：从前端 bundle 提取端点和消息结构，或向熵方索取文档；确认签名路径与费率表。
- **P1 只读接入**：`Instruments` + `SubscribeBook` + `Positions` + `AccountSnapshot`；用 `cmd/scanner` 与 `cmd/selfcheck` 验证行情、符号匹配与账户读取。
- **P2 交易接入**：`Submit`（签名执行）、订单终态跟踪、reduce-only 兼容、rescue 兼容、分腿延迟采集。
- **P3 风控兼容**：把 Entropy 的清算价/维持保证金/全仓权益映射进 `margin.go` 的 `CrossMarginRisk`；若没有同源快照，明确降级并在启动日志告警"零清算价保护不可用"。
- **P4 实盘验证**：`dry_run: true` 全链路闭环 → 小仓实盘 1~2 天 → 并入主力配置。

## 3. 风险与决策点

- **签名/托管**：无服务端签名方案则无法实盘（P0 首要结论）。
- **费率与精度**：Entropy 的 taker 费率与 tick/size 与 Lighter 不同，接入后回测参数（fee_bps、让价 tick 计算）必须重跑校准。
- **订单类型**：需确认是否支持 IOC、reduce-only、订单终态推送；不支持 IOC 则需要改造执行器或用限价+撤销近似。
- **跨保证金零清算价**：若 Entropy 返回 0 清算价且无同源账户快照，风控会保守地持续 REDUCE_ONLY（现有 fail-closed 行为），需提前告知运营。

## 4. 验收标准

- `cmd/scanner -config` 能列出 Entropy 全部市场并正常打分；
- `cmd/selfcheck` 通过（只读凭证、行情新鲜度）；
- `dry_run` 下选定交易对完成 开仓→平仓→补救 闭环；
- 风控字段齐全，或降级行为有明确日志与文档说明；
- 全量 `go build` / `go vet` / `go test -race ./...` 通过。
