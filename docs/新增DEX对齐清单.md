# 新增 DEX 对齐清单

接入一个新所要实现的**全部契约**。既有所（Lighter 主网 / Lighter RH / SoDEX）已按本文对齐，新所必须逐项落齐——**漏掉行为契约不会编译报错，只会在实盘静默出错**。

流程背景见 [本系统套利流程.md](本系统套利流程.md)。

---

## 1. Rust 侧：实现 `ExchangePort`

`src/exchange/port.rs:92` 定义 trait。新建 `src/exchange/{新所}.rs`，并在 `src/exchange/mod.rs:19` `make_adapter()` 加分支：

```rust
match venue.id.as_str() {
    "sodex" => Arc::new(SodexAdapter::new(venue, whitelist)),
    "新所"  => Arc::new(新Adapter::new(venue, whitelist)),
    _       => Arc::new(LighterAdapter::new(venue, whitelist)),
}
```

配置路径规则也在 `mod.rs:26`：默认 `config/venues/{id}.yaml`，特例才映射（现有特例：`lighter_rh` → `lighter_robinhood.yaml`）。

### 1.1 必须实现

| 方法 | 返回 | 语义要求 |
|---|---|---|
| `id()` | `VenueId` | — |
| `list_perps()` | `Vec<VenueMarket>` | 只收在交易状态的永续；`whitelist` 非空时按 `base` 过滤 |
| `subscribe_bbo()` | 推 `(VenueId, pair_id, Bbo)` | 长连接，**断线自动重连** |
| `place()` | `OrderAck` | 见 §4，行为契约最密集 |
| `cancel()` | `()` | — |
| `order_status()` | `OrderAck` | 不确认就不下结论 |
| `account()` | `AccountSnapshot` | 余额 + 持仓一并返回 |
| `funding()` | `Vec<FundingRate>` | **缺失返回空表**，不是 0 费率 |
| `fill_realized_pnl(symbol, order_id)` | `FillPnl` | 该所这一笔（或累计）已实现盈亏。盘口订阅没有这个字段 |

`positions()` / `balances()` 可由 `account()` 派生。`order_status()` / `funding()` 在 trait 上有默认实现（分别返回 `Err` / 空表），但新所两个都要真实现——前者是成交确认的基础，后者关系到费率门。

### 1.2 `VenueMarket` 字段

| 字段 | 说明 |
|---|---|
| `venue` | venue id |
| `raw_symbol` | **交易所原始符号**（下单、查持仓都用它） |
| `pair_id` | 归一后的标准 id（`BTC-USD-PERP`） |
| `base` | 基础币，白名单过滤用 |
| `market_index` | 交易所市场编号，下单必用 |
| `qty_precision` | 数量小数位 |
| `min_qty` | 最小下单量 |

`qty_precision` 和 `min_qty` 必须真实：容量校验和配置页精度检查用前者，分批平仓的尾巴判断用后者（`src/domain/grid.rs`）。填错会让仓位平不干净。

**符号归一**在 `src/domain/symbol.rs`。新所若用第三种计价符号（如 `BTC-PERP`），要在这里加映射，否则配不成 Pair。稳定币按 1:1 折算。

### 1.3 `Bbo`

`bid`／`ask`／`bid_qty`／`ask_qty`／`bids`／`asks`／`ts: Instant`。`ts` 是新鲜度门输入，必须是**收到推送的时刻**，不能用交易所时间戳（时钟偏移会让新鲜度门失效）。深度 `bids/asks` 可只给一档，但一档数量必须准——容量校验的深度上限靠它。

---

## 2. Go sidecar 侧

`scripts/exchange_sidecar/`。在 `main.go` `dispatch()` 加 venue 分支，新建 `{新所}.go` 实现 `dispatch新所()`。

协议是 JSON-lines 长连接（`main.go` 顶部注释）：

```
请求  {"id":1,"cmd":"...","venue_yaml":"config/venues/x.yaml","params":{...}}
响应  {"id":1,"ok":true,"data":{...}}   /   {"id":1,"ok":false,"error":"..."}
推送  {"push":"order","venue":"x","data":{...}}      ← 私有 WS，无 id
```

会话必须**长存**（连接、认证、市场精度只建一次）。每条请求独立 goroutine，会话内部自行加锁。

### 2.1 七条 cmd

| cmd | params | data |
|---|---|---|
| `account` | — | `{balances:[{asset,available,total}], positions:[{symbol,qty,entry_price,realized_pnl?}]}` |
| `place` | 见 §2.2 | `{order_id, client_order_id, filled_qty, status, avg_price}` |
| `cancel` | `order_id, symbol, market_index` | `{order_id, status}` |
| `order_status` | `order_id, symbol, market_index, qty` | 同 `place` |
| `funding` | — | `{rates:[{symbol, rate, interval_secs}]}` |
| `watch` | — | `{status:"watching"}`，启私有订单流，**幂等** |
| `fill_pnl` | `symbol, order_id` | `{realized_pnl, per_fill, found}`。盘口没有盈亏。Entropy 用成交 `closedPnl`（`per_fill=true`）；Lighter 用成交 `ask_account_pnl`/`bid_account_pnl`（`per_fill=true`）；SoDEX 用持仓累计 `realizedPnL`（`per_fill=false`，本笔=平后−平前）。`found=false` 时上层显示 `—`，不要当成 0 |

`positions[].symbol` 用**交易所原始符号**，不是 `pair_id`——对账靠它匹配。`qty` 带符号（正多负空）。

### 2.2 `place` 入参

```json
{"symbol":"BTC-USD","market_index":1,"is_buy":true,"qty":"0.1",
 "reduce_only":false,"style":"limit|market|aggressive_limit",
 "limit_price":"42000","client_order_id":"arb-...",
 "target_price":"41900","slippage_pct":"0.1"}
```

### 2.3 `funding` 的结算周期

`interval_secs` **必须真实上报，不能写死**。参考项目把 8 小时结算硬编成 `×3×365`，改周期后费率数值本身看不出变化，只有这个字段会变——上层拿它算年化，写死就静默算错。

现状：Lighter 无对应字段、文档写明按小时结算，取常量 3600（`main.go` `lighterFundingInterval`）；SoDEX 从 `/markets/symbols` 的 `fundingInterval` 逐市场读。`interval_secs = 0` 的行要丢掉，避免年化除零。

---

## 3. 配置

### 3.1 `config/venues/{新所}.yaml`

**共通字段名**（值是密钥的不要提交，`.gitignore` 已忽略；示例见 `*.example.yaml`）：

`id` / `rest` / `ws` / `chain_id` / `quote` / `fees.maker` / `fees.taker`

各所特有字段按需增补。既有例子：Lighter 用 `account_index`、`api_key_index`、`api_key_private_key`；SoDEX 用 `eip712_chain_id`、`account_id`、`account_address`、`api_key_name`、`private_key`。

**地址类字段务必区分「签名地址」和「账户地址」。** SoDEX 踩过：API key 私钥推导出的签名地址与实际持仓账户地址可以不同，拿签名地址去查单会**每次都返回空**，且不报错——只是「查不到这张单」。成交确认建立在回查之上，查不到就一律 `unknown`，整套幻影成交防线被静默架空。现在 `sodex.go` `readAccountAddress()` 优先用配置的 `account_address`，`addr` 显式穿参到 `placeOrder`／`orderStatus`／`findOrder`／`waitOrderFill`，没有一处再自己去问 client 要地址。新所若也有「签名地址 ≠ 账户地址」这种两地址概念，照此处理。

### 3.2 `config/default.yaml`

- `venues:` 列表加上新 id
- `sizing.leverage_by_venue`：新所杠杆
- 手续费在 venue 配置里，不在这

---

## 4. 行为契约（漏掉不会报错，只会亏钱）

### 4.1 成交量必须真查到

**禁止用请求量 `qty` 顶替 `filled_qty`。**

- 限价单会驻留，下单后单次查询可确认
- IOC 市价单**不驻留**，下单响应里的量不可信 → 等该单 WS 最多 1 秒，没有再 REST 查一次该单
- 查不到 → 报 `unknown` / `filled_qty=0`，上层当作失败，立刻市价平另一腿

确认窗口三所相同（`main.go` `iocFillWait`）：

| 常量 | 值 | 作用 |
|---|---|---|
| `iocFillWait` | 1s | 等该单私有 WS |
| 之后 | 查一次该单 | 仍没有量 → 失败 |

Lighter 查单：活跃列表没有则再查 `accountInactiveOrders`（IOC 成交后不在活跃列表）。成交量用 `initial − remaining`。不用持仓 delta。

Lighter 私有 WS：必须订 `account_all_orders` **和** `account_all_trades`。`orders` 按市场 id 分组是 **map**（`{"1":[Order]}`），不是数组；真正成交常在 trades（`ask_client_id` / `size`）。`rawList` 只展开这类 map；**禁止**递归拆账户快照、`orderBookDetails`——会变成 `decimals unknown`（挂单失败、面板邻档但 DEX 无单）和 `account snapshot unavailable`。

Lighter `client_order_id` 是整数，上限 \(2^{48}-1\)。现网 `ms*100 + n%100`。同一毫秒两档不能撞号；`ms*1000+seq` 会超上限被拒。SoDEX / Entropy 用字符串 `arb-{ms}-{seq}`。

**加长窗口要连带改两处超时**：sidecar `requestTimeout`（`iocFillWait + 15s`）、Rust `WRITE_SIDECAR_TIMEOUT`（`src/exchange/bridge.rs`，当前 80s）。任一处先到期都会打断回查。

### 4.2 市价腿关闭 status 推断

参考对限价单允许「`status=FILLED` 但缺 `filled` → 推断为全额成交」（`_infer_fill_from_status`），对市价单**显式关掉**（`allow_fallback = not is_market_order`）。市价腿的 status 来自下单响应而非成交确认，拿它推断数量就是幻影成交。

Rust 侧同样不推断：`src/exec/executor.rs` `effective_filled_qty()` 只认回查到的量。`Filled`／`Partial`／`Unknown` 却查不到数量，一律当失败 → 立刻市价平另一腿（最多 3 次）；3 次仍未确认才人工介入。

### 4.3 拒单必须报错

拒单走 error 返回，**不能报 `accepted`**。IOC 单报 `accepted` 会让上层空等一张永远不会存在的挂单，直到超时。

### 4.4 滑点保护基准是决策信号价

`target_price` = **决策那一刻**的信号价，不是下单时刻盘口。用下单时盘口做基准等于自我实现——价格跑了照样成交。超出 `slippage_pct` 由**交易所拒单**，不是本地估 VWAP。

保护带算法：买 `base × (1 + slippage%)`，卖 `base × (1 − slippage%)`。

### 4.5 取整方向按意图分

| 场景 | 方向 | 原因 |
|---|---|---|
| post-only 限价（maker 腿） | **远离**对手价（买向下、卖向上） | 穿过点差会被拒，post-only 就废了 |
| aggressive IOC 限价（兜底腿） | **靠向**对手价（买向上、卖向下） | 目的是吃到单 |

`aggressive_limit` 的 TimeInForce 必须是 **IOC**。用 post-only（SoDEX 的 `GTX`）会让兜底腿挂着不成交，第一腿继续裸着。

### 4.6 nonce 串行化

Lighter 需要：「取 nonce → 签名 → sendTx」整段原子化（`lighter.go` `submitMu`）。只锁计数器不够——它保护的是取号，不是**提交顺序**；两个 goroutine 各拿 5 和 6 并发发送，6 先到就被判 invalid nonce，5 随后也废。**撤单同样要持锁**（共用一条 nonce）。

锁刻意只覆盖到 `sendTx` 返回：之后的回查是 REST 轮询，圈进来会让并发撤单排队等，而撤单在超时路径上要抢时间。

新所若用链上 nonce／序号，照此处理。

### 4.7 请求报错 ≠ 订单未落地

`sendTx`／HTTP 报错后，必须用 `client_order_id` 回查一次。已落地就返回真实成交量，否则才上报错误。市价腿在这条路径上也要轮询——请求虽报错，单子可能已落地并成交。

### 4.8 `reduce_only`

直接传给交易所，语义是仅平仓。所有所都要支持——`max_position_hours`、余额清仓线、探针解闸都依赖它。若交易所回报 reduce-only 相关错误（SoDEX 是 `code=21740`），错误串要能被 `src/app/reduce_only.rs` `is_reduce_only_error()` 识别，否则拉闸机制失效。

---

## 5. 测试

| 层 | 位置 | 命名惯例 |
|---|---|---|
| Rust adapter | `src/exchange/{新所}.rs` 末尾 `#[cfg(test)]` | 描述行为，如 `snapshot_then_delta_keeps_best`、`parse_channel_slash_or_colon` |
| Go sidecar | `scripts/exchange_sidecar/{新所}_test.go` | `Test` + 场景 |

新所至少要覆盖：市场列表解析、BBO 快照+增量合成、下单应答状态映射、取整方向（post-only vs IOC 各一例）、成交确认返回 `unknown` 的路径。

### 5.1 实盘小额验收（必须，接入才算完成）

单元测试过不了成交确认、拒单、IOC 轮询。每个新所在合入主流程前，必须用 `live-test`（或等价 sidecar 调用）对该所**各做一次**：

| 步骤 | 命令 | 要看到的结果 |
|---|---|---|
| 市价开仓 | `live-test market {venue} {BASE} buy {qty}` | `filled_qty > 0`，随后 `positions` 有仓 |
| 市价平仓 | `live-test market {venue} {BASE} sell {qty} --reduce-only` | 成交后 `positions` 清空或绝对值下降 |
| taker 限价 | `live-test aggressive {venue} {BASE} buy {qty} {穿越盘口的限价}` | IOC：成交或整单撤销，**不得**报 `accepted` 后空等 |
| 挂单 + 撤单 | `live-test limit {venue} {BASE} buy {qty} {远离盘口的价}` → `cancel {venue} {BASE} {order_id}` | 限价单驻留拿到 `order_id`，撤单成功、盘口上消失 |
| 平仓盈亏 | `live-test fill-pnl {venue} {BASE} {order_id}` | `found=true`，`realized_pnl` 为所方字段（不要用开平仓价本地算） |

注意：

- 数量受 `live_test.max_qty` 限制；交易所若有最小名义（Hyperliquid / Entropy 约 **$10**），临时设 `DEX_LIVE_TEST_MAX_QTY` 放到能下出去。
- 两地址所（SoDEX、Entropy）：`account_address` 必须是**持仓主账户**，`private_key` 是签名/API 钱包。查余额用错地址会看到 0 却不报错。
- 统一账户（Hyperliquid / Entropy 推荐项）：可用保证金在 API 的 `spotClearinghouseState`，不要只读 perp `withdrawable`。
- 测完确认无残留仓位、无驻留挂单。记录 `order_id` / `filled_qty` / 持仓变化。
- 平仓后用 `live-test fill-pnl {venue} {BASE} {order_id}` 核对所方已实现盈亏。Entropy 看 `closedPnl`；Lighter 看成交 `ask_account_pnl` / `bid_account_pnl`（持仓 `realized_pnl` 全平后是 0，不要用）。

```bash
# Entropy 示例（SNDK，名义约 10 USDC；数量按当时价格改）
$env:DEX_LIVE_TEST_MAX_QTY="0.01"   # Windows
cargo run --bin live-test -- account entropy
cargo run --bin live-test -- market entropy SNDK buy 0.007
cargo run --bin live-test -- positions entropy
cargo run --bin live-test -- market entropy SNDK sell 0.007 --reduce-only
cargo run --bin live-test -- fill-pnl entropy SNDK <close_order_id>
cargo run --bin live-test -- aggressive entropy SNDK buy 0.007 1480
cargo run --bin live-test -- limit entropy SNDK buy 0.007 1000
cargo run --bin live-test -- cancel entropy SNDK <order_id>
```

---

## 6. 落地清单

- [ ] `src/exchange/{新所}.rs` 实现 `ExchangePort`（含 `fill_realized_pnl`）
- [ ] `src/exchange/mod.rs:19` 加 `make_adapter` 分支
- [ ] `src/domain/symbol.rs` 补符号归一（若计价符号是新形态）
- [ ] `scripts/exchange_sidecar/{新所}.go` 实现七条 cmd（含 `fill_pnl`）
- [ ] `main.go` `dispatch()` 加 venue 分支，确定该所的 fill wait 档位
- [ ] `config/venues/{新所}.example.yaml` 字段占位；真配置不提交
- [ ] `config/default.yaml` 的 `venues` 与 `leverage_by_venue`
- [ ] §4 八条行为契约逐条对照
- [ ] Rust + Go 测试
- [ ] 按 [实盘验证.md](实盘验证.md) 小额验收：**市价开仓 / 市价平仓 / taker 限价 / 挂单撤单** 各一次（见 §5.1）
