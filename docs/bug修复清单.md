# DEX 套利系统问题清单（复核版）

> 对照当前代码复核原稿的**诊断是否成立**，不是实施记录。编号沿用原稿，便于对照。
>
> 阶段 1 热路径是**双腿市价**（`force_market_taker`）。第二腿失败后：明确失败才试激进限价 IOC；`Unknown` 不试；然后等 `second_leg_verify_ms` 查第二所实仓——无仓则市价平第一腿，查仓失败仍 `SECOND_LEG_UNKNOWN`、不平第一腿。
>
> 相对原稿主要改动：拆掉写错的因果链；把夸大的 P0 降级；策略项不再写成「代码写错了」。

---

## 怎么读

| 判定 | 含义 |
|------|------|
| **成立** | 机制和代码对得上，按缺陷排期合理 |
| **成立但过重** | 现象对，后果/优先级按热路径写过了 |
| **不成立** | 调用链或机制写错了，不要按原稿去修 |
| **策略缺口** | 设计上没做某层保护，不是实现写错 |

---

## 总表

| 编号 | 判定 | 一句话 |
|------|------|--------|
| **#1** | 成立 | sidecar goroutine panic 打死进程 |
| **#2** | 成立 | `GetNextNonce` 不吃 ctx，占着 `submitMu` 会堵住撤单 |
| **#3** | 成立（收窄） | REST 持仓 delta 并发会串单；WS 按 order_index 命中时不是这条 |
| **#4** | 成立 | `priceUnits` 截成 `uint32` 无上限检查 |
| **#5** | **不成立** | 主路径用 numeric oid，不是「原始 cloid 永远 miss」 |
| **#6** | 成立但过重 | 激进限价在 Rust 侧不轮询；「立刻反手平第一腿造反向裸仓」对不上现码 |
| **#7** | 成立（收窄） | `audit_position_qty` 用绝对值 min；裸仓检测按净额仍会报同向 |
| **#8** | 成立但过重 | 限价看门狗是死代码；阶段 1 不挂那类限价单 |
| **#9** | 成立但过重 | `qty<=0` 放行；热路径不用 `l1_covers` |
| **#10** | 成立但过重 | `this_close_pnl` 不检出场价；实盘 tape 不走这个函数 |
| **#11** | 成立 | Lighter WS 重连泄漏 goroutine |
| **#12** | 成立 | Lighter WS 无读超时/ping，半开连接会挂死 |
| **#13** | 成立（收窄） | 全局锁只罩**建连**，建成后不一直占着 |
| **#14** | 成立 | Entropy `postAction` 锁罩住整段 HTTP |
| **#15** | 成立但过重 | 签名测试是自洽，测不到错误 hash；不是线上必错签 |
| **#16–20** | 成立 | 缓存无界、单位启发式、Log10、stdin 扇出、忽略 `scanner.Err()` |
| **#21** | 成立但过重 | 2× 硬编码是真的；失败后不是必然 50× 紧急平仓 |
| **#22** | **不成立** | Rust 没有 5×200ms 市价轮询；确认在 sidecar |
| **#29–35** | 成立 | 锁中毒、空 order_id、JSON 精度、WATCHED、重复刷新、min_qty=0 |
| **#40、#43–45** | 成立 | 磁带轮询竞态、`+0.000%`、失败静默、无 AbortSignal |
| **#41/#42** | 产品选择 | 无二次确认，不是「代码与设计不符」 |
| **S1–S8** | 策略缺口 | 缺保护层或未调参；S1/S2 是否「必需」是判断不是事实 |

原稿无 #23–28、#36–39。行号会过时，下面用符号和职责定位。

---

## 一、成立：按缺陷修

### Sidecar：进程、锁、成交确认

#### #1 sidecar panic 打死进程

`scripts/exchange_sidecar/main.go` 每条 stdin 请求一个 goroutine，无 `recover()`。`hl_sign.go` 的 `hlActionHash` 编码失败直接 `panic`。

Go 里未捕获的 panic 会退出整个进程。Rust 能重启 sidecar，崩溃当下飞着的单会短暂无人管。不是「永远失控」，但窗口真实存在。

**修**：handler 里 `recover` 并 `respond` 失败；`hlActionHash` 改返回 `error`，不要 panic。

#### #2 Lighter `reserveNonce` 不吃 ctx，撤单可能被堵住

`reserveNonce` 在 `nonceMu` 里同步调 `lighterhttp.NewClient(...).GetNextNonce`，**新客户端、不用请求 ctx**。`place` 在 `submitMu` 里调它；`cancel` 也拿同一把 `submitMu`（撤单必须和 nonce 串行）。

REST 挂起时，正在 `place` 的 goroutine 占着锁，撤单进不去。是否「永久」取决于 SDK HTTP 有没有自己的超时；「平仓单被堵住」成立。

**修**：网络调用移出 `submitMu` / `nonceMu`；用带超时的 ctx。不要把「已预留的 nextNonce」随手清成 nil 却不消费——原稿示例修法本身有计数漏洞。

#### #3 Lighter IOC 持仓 delta 竞态（仅 REST 兜底）

`waitMarketFill` **先按 client_order_index / order_index 看 WS**，REST 持仓 `cur - baseline` 只是兜底。baseline 在 `submitMu` 外读，确认在锁外。

同市场两条 IOC 并发（主路径 + 裸仓对冲可打同一账户）且 WS 未命中时，两边都会把对方的仓变算进自己的 delta：未成交的一侧可能报出幻影成交。`want` 有封顶，所以「两边都 +1」时各自上报 1 不一定错；典型坏例是 **A 成交、B 未成交，B 仍看到 delta**。

**修**：REST 路径用 per-market 锁罩住 baseline → 下单 → 确认；或 WS 未确认时不要用未加锁的 delta 当真成交。不要理解成「整段确认都没按单去重」。

#### #4 `uint32` 截断价格

`CreateOrderTxReq.Price` 是 `uint32(priceUnits)`，只检查了 `<= 0`。`price × 10^priceDec` 超过 `MaxUint32` 会截成无关小数再提交。

Lighter 缺省 `priceDec` 多为 2，常见永续价溢不出 4.29e9；`120000 × 10^6` 的例子偏极端。类型截断仍应拒单。`marketIndex → int16` 同理可一并卡住。

#### #11 Lighter WS 重连泄漏 goroutine

`orderStreamOnce` 起一个 goroutine 只等 **session** `ctx.Done()` 再 `conn.Close()`。读失败返回、`defer conn.Close()` 之后，这个 goroutine 仍卡在 `<-ctx.Done()`。每 2 秒重连就永久多一个。

对照 Entropy：`done` chan，连接结束就退出。照抄即可。

#### #12 Lighter WS 无读超时 / ping

Entropy 有约 40s ping、90s `SetReadDeadline`。Lighter `ReadJSON` 无 deadline、无 ping。NAT/LB 半开连接会让读永久阻塞、不报错、不重连，成交流静默死掉，只剩 REST 轮询且无告警。

#### #13 registry 全局锁罩住建连

`lighterSession` / `entropySession` / `sodexSession` 在持 `registry.mu` 时做文件 I/O + 网络。一个所建连挂起，**所有**还没拿到 session 的请求（含别的所的撤单）都堵在这把锁上。

会话**已经建好**之后，后续 `place`/`cancel` 不再为了查 map 而占着锁跑 HTTP。问题在冷启动、进程内第一次连某所、重建会话。

**修**：per-venue 初始化（`Once` 或等价）把网络放锁外。

#### #14 Entropy `postAction` 锁罩住 HTTP

`s.mu` 本应只护 nonce，实际罩住签名 + `http.Do`（客户端 20s 超时）。撤单也走 `postAction`，慢下单会堵住撤单。

**修**：取出 nonce 立即解锁，网络在锁外。注意并发下 nonce 仍必须单调、失败要作废，不能只「解锁」不管序号。

---

### Sidecar：活性 / 精度（P2）

#### #16 WS fill 缓存无界

Lighter / SoDEX / Entropy 的 `fills` map 按 oid+cloid 写入，无过期。长驻会涨。按时间戳定期删过期条目即可。

#### #17 Lighter 成交量启发式

`normalizeWsFilled`：整数且 `raw > want×50` 才按 `sizeDec` 缩放。请求 0.01、回报人类可读整数 1 时会误除，数量被低报。`marketDecimals` 失败时静默 `sizeDec=4` 同样该失败而不是猜。

#### #18 `hlSigFigStep` 走 float `Log10`

`math.Log10` 在 10 的整数次幂可能得到 `n-epsilon`，`Floor` 少一档，限价过细被拒。用 Decimal 位数/指数，不要经 f64。

#### #19 stdin 无界 goroutine

每行一个 goroutine，无信号量。有 `requestTimeout`（约 Lighter 确认窗口 + 15s），不是无限跑，但 Rust 侧重试仍可堆很多 in-flight。加并发上限。

#### #20 忽略 `scanner.Err()`

循环结束后不检查 scanner 错误。stdin 损坏或超缓冲时进程以 0 退出，和正常 EOF 分不清。查 `scanner.Err()`，失败写 stderr / 非零退出。

---

### Rust：对账与基础设施

#### #7 `audit_position_qty` 把同向仓当对冲量（函数级）

```rust
let a = venue_position_qty(...).abs();
let b = venue_position_qty(...).abs();
let hedged = a.min(b);
```

两所都是多（或都是空）时，`hedged` 仍是较小绝对值。若内存量碰巧等于这个数，审计认为对齐，**不会**收缩内存仓。

**不要写成「系统完全看不见同向双多」**：`detect_naked_exposures` 按 pair 各所数量**加总净额**，+10/+5 净额 +15 会报裸仓。真缺口是审计与裸仓两套口径不一致：审计可能不响、裸仓仍响。

**修**：两腿同号且都非零时，审计应视为对冲量 0（或直接告警），不要用 `min(abs)`。**现码已按此处理。** 反向重叠时内存按实盘对冲量校正：少则缩、多则在上限内抬，避免漏记成交后告警刷屏、全平留残仓。

#### #29 API 锁中毒静默丢更新

`publish` / `push_execution` 在 `if let Ok` 失败时丢掉写入。Mutex poison 少见（持锁时 panic），一旦发生前端一直看旧数据且无日志。`into_inner()` + warn。

`ExecJournal` 只在单线程 `live_test` 里用，**不是**跨线程缺 Mutex 的线上事故（原稿已排除，维持）。

#### #30 空 `order_id`

sidecar 缺字段时 `bridge` 解析成 `""` 往下传，后续撤单/查询注定失败。缺字段应报错。

#### #31 JSON 数字经 f64

`serde_json` 未开 `arbitrary_precision`。sidecar 不少字段已是字符串；仍走 JSON number 的价格/数量会丢有效位。开 feature，或约定数值一律字符串。

#### #32 / #33 WATCHED

中毒时 `unwrap_or_default()` 得到空列表，重启不补订阅。`bridge_watch` 在 `bridge_call` **成功前**就把路径推进 `WATCHED`，失败的 watch 会每次重启重试。中毒用 `into_inner`；入表放到调用成功之后。

#### #34 `refresh_natural` 并发重复算

多 pair 同时到期会重复读库、排序、写回。**结果一致**，浪费 I/O。in-flight set 去重即可，不是算错。

#### #35 `min_qty` 解析失败变 0

Lighter / SoDEX 畸形 `min_base_amount` 时 `unwrap_or(ZERO)`，最小量校验失效。该市场应跳过或报错，不要当 0。

---

### 前端

#### #40 `pollTape` 无 in-flight 锁

`pollSnap` 有 `snapBusy`，`pollTape` 没有。慢请求返回后可能盖掉更新的磁带。同样加 busy 或按请求世代丢弃旧响应。

#### #43 零值显示 `+0.000%`

`format.ts`：`(n >= 0 ? "+" : "")`。改成 `n > 0` 才加号。

#### #44 / #45 轮询失败静默、无 abort

失败空 `catch`；`apiFetch` 无 `AbortSignal`。连续失败应提示断线；卸载或下次请求前取消上一次。

#### #41 / #42 启动、停止、重置无确认

这是产品体验，不是实现与设计不符。要防误触再加弹窗。

---

## 二、成立但不要按原稿 P0 / 亏钱故事排期

#### #6 激进限价在 Rust 侧不额外轮询

`aggressive_limit_retry` 只走一次 `send_leg`（内部 `place`）。Rust 没有清单里的 `poll_ioc_fill`（5 次 × 200ms）。`OrderStyle::AggressiveLimit` 注释要求成交确认走「市价那套轮询」——那套在 **sidecar**（IOC + 持仓/WS），不在 `executor.rs` 再轮一轮。

**原稿后半段不成立**：`Filled + qty=0` 被当成未成交 → `emergency_close` 第一腿 → 第二腿其实已成交 → 反向裸仓翻倍。

现码：`send_leg` 把 `Filled`/`Unknown`/`Partial` 且数量为 0 打成 `SECOND_LEG_UNKNOWN`；`fill_second_leg` 在不可核实或激进限价失败后走 `verify_second_or_close_first`（等 `second_leg_verify_ms` 查实仓），不是立刻反手平第一腿。

若要补，应与 sidecar IOC 确认对齐，不要按「1 秒 Rust 轮询」去改；也不要假设失败必 50× 平仓。

#### #8 限价超时看门狗从未写入

全仓库无 `pending.insert` / `PendingLimit { ... }` 构造；`spawn_limit_market` 无调用点。`watch_pending_slot` 是死代码。

阶段 1 **不走** `LimitThenMarket`，热路径不挂那种驻留限价，不存在「现网限价单挂死无人撤」。阶段 2 若恢复对称限价，必须先接上登记再派发，否则看门狗仍是空的。

#### #9 `l1_covers` 对 `qty<=0` 返回 true

函数在 `spread.rs`。`qty == 0` 当「不需要盘口」说得通；**负数量**也会放行，那才是坑。`executable_spread` / `l1_covers` **不在**滑动窗口决策热路径（现用 `exec_spread_pct`）。改成 `qty > 0 && ...` 作为防御可以，不是现网开仓开关。

#### #10 `Position::this_close_pnl` 不检出场价

入场价检了，出场价没检。`raw_spread_pct(sell_venue_px, buy_venue_px)` 对第一个参数 `<=0` 已返回 `None`，**不是除零**。买所出场价为 0 时可能算出约 -100% 的假亏。

实盘执行记录盈亏走 `this_close_from_balances`（开平权益差）。决策环不再调 `Position::this_close_pnl`。补校验作为函数健全性即可。

#### #15 `TestSignL1RecoversSigner` 测不到错误 hash

用被测函数自己的 hash 再恢复地址，字段顺序/vault 写错也会过。应加已知向量（SDK 或线上抓包）做字节比对。这是测试质量，不能单独证明线上 hash 算错。

#### #21 激进限价滑点 2× 硬编码

`AGGRESSIVE_SLIPPAGE_MULT = 2`，紧急平仓倍数在配置里（默认 50）。2× 吃不到就失败，现码接着查仓，不是必然再发 50×。做成配置项可以，默认不必改成 5–10 除非实盘证明 2× 不够。

---

## 三、诊断不成立（不要按原稿修）

#### #5 Entropy `fillPnl`「用未哈希 cloid 查哈希缓存，永远 miss」

不成立。

- `place` 返回给 Rust 的 `order_id` 是 **numeric oid**，`client_order_id` 才是原始 cloid。
- WS `addFill` 会写入 numeric oid，以及交易所回报的 `Cloid`。
- `fillPnl` 用 Rust 传来的 `order_id` 查缓存；REST `userFills` 按 `f.Oid` 也能对上。

主路径不是「原始 cloid vs 哈希 key → 永远 miss → 退化成 30 秒窗口乱猜」。若将来有人拿**原始 cloid** 当 `order_id` 去查，哈希不一致才会 miss。不必按原稿做「双 key + 先哈希」当 P0。

`dex_close_pnl_usdc` / `query_leg_close_pnl` 仍在执行器里，决策环记账已改走权益差；即便 fill 缓存偶发 miss，也不等于现网 PnL 静默瞎编。

#### #22「`executor.rs` 市价轮询 5 次 × 200ms，1 秒不够」

不成立。原稿指向的行现在是 `fetch_second_leg_qty`（拉第二所持仓）。仓库里没有 `max_attempts=5` / `retry_gap_ms=200`。

市价/IOC 确认在 **Go sidecar**（Lighter 窗口约 60s，SoDEX/Entropy 约 3s，轮询间隔是 sidecar 常量）。「给 Rust `OrderConfig` 加 5/200」解决的是一个不存在的函数。若要可配，改 sidecar 常量或 yaml 映射到 Go，不是 `executor.rs:611`。

---

## 四、策略缺口（S1–S8）

代码按当前设计在跑：中枢冻结、持续性、`Δ` 折入两所点差中枢**算术平均**。下面是**没做的保护或未用数据调的参数**，不要当成 #1 那种实现缺陷。S1/S2 在长期无人值守时是否「必需」是策略判断，不是对错。

`Position.opened_at` 和 `CloseReason::HoldTimeout` **已经有字段/枚举**，决策环从未用来强平。`src/domain/funding.rs` 有费率视图，controller **未**用来闸开仓。`src/app/funding.rs` 若未编进 `mod.rs` 则是死文件，不是「缺模块」的证据。

### S1 结构破裂熔断

假设价差回归 μ。所故障、冻结、流动性枯竭时可能加到 `±max_segments` 后一直扛。可在窗口上估半衰期 / 平稳性，连续失败则禁止新开（已有仓怎么处理要单独立规，避免误杀正常回归）。

原稿用 $15 量级的数字当例子，和「黑天鹅必亏」不是同一量级；是否做、阈值多严，要用窗口数据看误触发率。

### S2 时间止损

没有「持有超过 N 秒强制减/平」。接上已有 `opened_at`（补仓是否刷新时钟：现注释写明补仓不刷新）。`max_hold_secs` 建议跟 S1 半衰期一起定，不要拍一个 30 分钟砍正常回归。

### S3 `Δ` 随波动放大

当前 `Δ` 由目标 bp、双边 taker、点差中枢平均、滞后 `h` 反推，**不含**滚动 σ。剧烈市容易顶格。可用 `max(成本下界, k·σ)`，σ 小时仍要盖住 F（手续费是绝对 bps）。**先有顶格频率数据再决定要不要做。**

### S4 窗口长度

yaml `window_samples: 1000`（约 17 分钟，1s 一点）；代码缺省曾偏向 10000。1000 vs 10000、简单均值 vs EWMA 是调参，不是写错。冷启动快慢、趋势市滞后是权衡。

### S5 `step_hysteresis`

yaml `"0"`：满格才开、回到 μ 才平。震荡多再考虑 0.1–0.15。用实盘「锁到的 bp / 开平白打占比」选，不要先改。

### S6 资金费方向闸

价差方向与资金费拥挤同向时跳过开仓。域逻辑已有，决策环未接。默认关、可配。

### S7 MAD vs σ

仅当 S3 上线后，噪声大再考虑 MAD。依赖 S3。

### S8 前端半衰期 / ADF

依赖 S1 的估计值，给人工关 pair 用。无 S1 就没有这两列。

---

## 五、建议顺序

先执行层（锁、panic、确认竞态、价格溢出），再 Sidecar WS，再防御性校验和前端。策略增强放在执行层稳定之后，S3–S5 等数据。

| 次序 | 内容 |
|------|------|
| 1 | #1 recover / 去掉 hash panic；#2 nonce+ctx+锁范围；#4 uint32；#3 REST 确认窗口（WS 已优先时优先级低于纯 REST 场景） |
| 2 | #11 #12 Lighter WS；#13 建连锁；#14 Entropy HTTP 锁 |
| 3 | #7 审计同向；#16–20、#29–35 按打扰程度穿插 |
| 4 | #40 #43–45；#41 若要防误触 |
| 5 | #6/#8/#9/#10/#21 按是否进入阶段 2 限价、是否碰那些函数再排 |
| — | **不要做**：按 #5 改双 key 当 P0；按 #22 给 Rust 加 5×200ms |
| 之后 | S1/S2 若要无人值守再立项；S3–S5 看顶格率、震荡占比、μ 漂移 |

验证：sidecar `go test` / `go build`；Rust 对 #3/#4/#7 补单测（mock 延迟、溢出、同向仓）。S1 若做，AR(1)/平稳性必须有已知 OU vs 随机游走的单测，避免误熔断。
