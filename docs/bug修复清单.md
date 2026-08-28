# DEX 套利系统 Bug 修复清单

> 生成方式：7 个并行子代理逐文件通读 Rust / Go / Vue 全部源码后交叉产出，
> 关键结论已用 grep/read 回查代码验证。按严重程度排序，每条给出文件:行号、
> 触发场景、修复代码。
>
> 本文分两部分：
> - **第一部分（#1-45）实现缺陷**：代码写错了，与设计意图不符，修了就对。
> - **第二部分（S1-S8）策略改进**：代码按设计跑，但设计本身缺一层保护
>   或参数没经实盘验证。属于增强，需要实盘数据支撑决策。
>
> 两部分优先级不同：实现缺陷里的 P0 会直接亏钱，必须先修；策略改进
> 里只有 S1/S2（结构破裂熔断、时间止损）在长期无人值守时是必需的。

---

## 🔴 P0：立即修复（可能导致资金损失或系统失控）

### Go Sidecar

#### 1. 任何 panic 杀死整个进程，所有仓位失控
**位置**: `scripts/exchange_sidecar/main.go:184-195`, `hl_sign.go:79`

handler goroutine 无 `recover()`；`hlActionHash` 编码失败直接 `panic`。
一次 nil 解引用/编码错误就让 sidecar 整体退出，正在开仓的订单变成无人管理。

```go
go func(req request) {
    defer inflight.Done()
    defer func() {
        if r := recover(); r != nil {
            log.Printf("panic in request %s: %v\nstack: %s", req.ID, r, debug.Stack())
            respond(req.ID, false, nil, fmt.Sprintf("internal panic: %v", r))
        }
    }()
    dispatch(reg, req)
}(r)
```

`hlActionHash` 改签名为 `([]byte, error)`，调用方检查错误而不是让它 panic。

---

#### 2. Lighter `reserveNonce` 忽略 ctx，撤单可能永久阻塞
**位置**: `scripts/exchange_sidecar/lighter.go:877-892`

`submitMu` 锁内同步调 `GetNextNonce` 且无超时。Lighter REST 挂起时，
持锁的 place 卡住，导致等待同一把锁的 cancel 也发不出去——正是平仓单
发不出去的场景。

修复：把网络调用移出 `submitMu`，改用带超时的 ctx：

```go
func (s *lighterSession) reserveNonce(ctx context.Context) (int64, error) {
    s.nonceMu.Lock()
    if s.nextNonce != nil {
        n := *s.nextNonce
        s.nextNonce = nil
        s.nonceMu.Unlock()
        return n, nil
    }
    s.nonceMu.Unlock()

    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    api := lighterhttp.NewClientWithContext(ctx, s.baseURL)
    nonce, err := api.GetNextNonce(s.venue.AccountIndex, s.venue.APIKeyIndex)
    if err != nil {
        return 0, fmt.Errorf("fetch nonce: %w", err)
    }
    return nonce, nil
}
```

---

#### 3. Lighter 持仓增量确认竞态 → 幻影成交
**位置**: `lighter.go:604-615`, `716`, `301-312`

baseline 在 `submitMu` 外读取，`waitMarketFill` 在锁释放后运行，整个
确认窗口未同步。裸仓对冲任务和主执行任务同时交易同一市场时，两边都会
把对方的成交计入自己的 delta，造成持仓虚高、触发误报的反向对冲。

修复方向（二选一）：
- 优先信任 WS fill（已带 order_index 去重），REST 持仓差值仅作降级路径；
- 或给每个 market 建独立锁，覆盖 baseline→confirm 整个窗口：

```go
type lighterSession struct {
    // ...
    marketLocks sync.Map // market -> *sync.Mutex
}

func (s *lighterSession) getMarketLock(market string) *sync.Mutex {
    v, _ := s.marketLocks.LoadOrStore(market, &sync.Mutex{})
    return v.(*sync.Mutex)
}
```

place 时 `getMarketLock(market).Lock()`，覆盖 baseline 读取 → 下单 → 确认，
`defer Unlock()`。

---

#### 4. `uint32` 截断限价价格
**位置**: `lighter.go:543,560,591,630`

高价市场 `price × 10^priceDec` 超过 4.29e9（例如 120000 × 10^6）会溢出成
无关小数，被当作真实限价提交。

```go
priceUnits := lp.Mul(pScale).IntPart()
if priceUnits <= 0 {
    return OrderAck{Status: OrderStatus_Rejected}, fmt.Errorf("non-positive price units: %d", priceUnits)
}
if priceUnits > math.MaxUint32 {
    return OrderAck{Status: OrderStatus_Rejected}, fmt.Errorf("price %v overflow uint32 after scale 10^%d", lp, priceDec)
}
if marketIndex < math.MinInt16 || marketIndex > math.MaxInt16 {
    return OrderAck{Status: OrderStatus_Rejected}, fmt.Errorf("market index %d out of int16 range", marketIndex)
}
msg.Price = uint32(priceUnits)
msg.Index = int16(marketIndex)
```

---

#### 5. Entropy `fillPnl` 用未哈希 cloid 查哈希缓存
**位置**: `entropy.go:1298,1336` vs `1066`, `509-513`

`place` 对外返回原始 cloid，但 WS `addFill` 用 hlCloid 哈希后的 cloid 做
key，导致 `fillPnl` 永远查不到，PnL 归因静默退化成 30 秒窗口猜测。

修复：让调用方同时传 numeric order_id 和原始 cloid，Go 侧分别用两个 key
查缓存，其中 cloid 先按 `orderStatus` 里已验证正确的方式哈希：

```go
func (s *entropySession) fillPnl(ctx context.Context, numericOid, rawCloid string) (decimal.Decimal, bool, error) {
    hashedCloid := hlCloid(rawCloid)
    s.mu.Lock()
    if p, ok := s.fills[numericOid]; ok {
        s.mu.Unlock()
        return p, true, nil
    }
    if p, ok := s.fills[hashedCloid]; ok {
        s.mu.Unlock()
        return p, true, nil
    }
    s.mu.Unlock()
    // ... 降级 REST /info，用 numericOid 或 hashedCloid 比对
}
```

---

### Rust 执行层

#### 6. AggressiveLimit 缺少轮询确认（幻影成交风险）
**位置**: `src/exec/executor.rs:410-457`

`config.rs:353` 注释明确要求 AggressiveLimit 必须走轮询回查，但
`aggressive_limit_retry` 只 `place` 一次就采信返回值。若交易所回包
`status=Filled, filled_qty=0`（更新延迟），会被判定为未成交 →
触发 `emergency_close` 平掉第一腿 → 而第二腿实际已成交 → 反向裸仓翻倍。

修复：抽出与市价单共用的 IOC 轮询函数，AggressiveLimit 复用：

```rust
async fn aggressive_limit_retry(cfg: &AppConfig, adapters: &Adapters, plan: &HedgePlan, qty: Decimal, bbo: &Bbo) -> Result<ExecFill> {
    let leg = &plan.second;
    let slip = cfg.cost.max_slippage_pct * cfg.cost.aggressive_slippage_multiplier;
    let limit_price = compute_aggressive_limit_price(leg.is_buy, bbo, slip);

    let ack = adapters.get(leg.venue.as_str())?.place(OrderReq {
        symbol: leg.symbol.clone(), is_buy: leg.is_buy, qty,
        style: OrderStyle::AggressiveLimit, limit_price: Some(limit_price), slippage_pct: Some(slip),
    }).await?;

    let filled_qty = if ack.filled_qty <= Decimal::ZERO
        && matches!(ack.status, OrderStatus::Filled | OrderStatus::Unknown | OrderStatus::Partial)
    {
        poll_ioc_fill(adapters, leg, &ack.order_id, qty, 5, 200).await?
    } else {
        effective_filled_qty(&ack)
    };

    if filled_qty <= Decimal::ZERO {
        bail!("aggressive limit leg {} not filled after polling", leg.venue);
    }
    Ok(ExecFill { qty: filled_qty, avg_price: ack.avg_price, order_id: ack.order_id, venue: leg.venue.clone() })
}

async fn poll_ioc_fill(adapters: &Adapters, leg: &Leg, order_id: &str, _target_qty: Decimal, max_attempts: u32, retry_gap_ms: u64) -> Result<Decimal> {
    for _ in 0..max_attempts {
        tokio::time::sleep(Duration::from_millis(retry_gap_ms)).await;
        let status = adapters.get(leg.venue.as_str())?.order_status(&leg.symbol, order_id).await?;
        if status.filled_qty > Decimal::ZERO { return Ok(status.filled_qty); }
        if matches!(status.status, OrderStatus::Canceled | OrderStatus::Rejected) { return Ok(Decimal::ZERO); }
    }
    Ok(Decimal::ZERO)
}
```

同步在 `config.rs` 的 `CostConfig` 加 `aggressive_slippage_multiplier`
字段（默认 5~10，见 P1 #21），把当前硬编码的 2× 换掉。

---

#### 7. 同向持仓被误判为对冲
**位置**: `src/app/reconcile.rs:96-99`

```rust
let a = venue_position_qty(accounts, &pair.legs[0]).abs();
let b = venue_position_qty(accounts, &pair.legs[1]).abs();
let hedged = a.min(b); // 两腿同向时应为 0，而不是较小值
```

两所都是 +10 / +5（同向双多）时，当前逻辑算出 `hedged=5`，会让内存持仓
和"实盘对冲量"匹配通过，掩盖双边都开多这个严重错误。

```rust
pub fn audit_position_qty(pair: &Pair, accounts: &VenueAccountCache, memory_qty: Decimal) -> Option<(Decimal, Decimal)> {
    if !accounts.all_fresh() || memory_qty <= Decimal::ZERO { return None; }
    let a = venue_position_qty(accounts, &pair.legs[0]);
    let b = venue_position_qty(accounts, &pair.legs[1]);

    if !a.is_zero() && !b.is_zero() && a.is_sign_positive() == b.is_sign_positive() {
        warn!(pair_id = %pair.pair_id, qty_a = %a, qty_b = %b, "CRITICAL: both legs same direction, zero hedge");
        return Some((memory_qty, Decimal::ZERO));
    }
    let hedged = a.abs().min(b.abs());
    let tol = pair.min_qty();
    if (memory_qty - hedged).abs() <= tol { return None; }
    Some((memory_qty, hedged))
}
```

---

#### 8. 限价单超时看门狗从未生效（死代码）
**位置**: `src/app/controller.rs:76, 1663-1680`；已核查全仓库无
`self.pending.insert` / `PendingLimit { ... }` 字面构造，`exec_worker.rs:44`
的 `spawn_limit_market` 也未被任何调用点使用。

`pending` 映射表定义了、看门狗读了，但没人写。限价单一旦挂死，超时
撤单机制完全不会触发。

修复：在决策环选中 `LimitThenMarket` 时补上登记与派发（大致位置在
`controller.rs` 的执行分支）：

```rust
if let OrderStyle::LimitThenMarket = plan.style {
    let cancel = Arc::new(AtomicBool::new(false));
    self.pending.insert(slot.to_string(), PendingLimit {
        plan: plan.clone(), since: Instant::now(), cancel: Arc::clone(&cancel),
    });
    spawn_limit_market(
        self.exec_tx.clone(), self.cfg.clone(), Arc::clone(&self.adapters),
        Arc::clone(&self.books), pair_i, plan.clone(), LimitMarketRun { cancel },
    );
    return;
}
```

`process_exec_event` 里已有 `self.pending.remove(&msg.slot)`（2101 行），
补上登记后清理路径自动生效，不需要额外改动。

---

### Rust 数据验证

#### 9. 零/负数量订单通过流动性检查
**位置**: `src/domain/spread.rs:58-60`

```rust
// 现状：qty <= 0 时直接放行
pub fn l1_covers(buy_book: &Bbo, sell_book: &Bbo, qty: Decimal) -> bool {
    qty <= Decimal::ZERO || (buy_book.ask_qty >= qty && sell_book.bid_qty >= qty)
}
```

```rust
pub fn l1_covers(buy_book: &Bbo, sell_book: &Bbo, qty: Decimal) -> bool {
    qty > Decimal::ZERO && buy_book.ask_qty >= qty && sell_book.bid_qty >= qty
}
```

---

#### 10. 出场价格未校验，可能除零/错误 PnL
**位置**: `src/domain/position.rs:95-100`

现状只检查入场价非零，未检查出场价：

```rust
if qty <= Decimal::ZERO
    || self.entry_buy_px <= Decimal::ZERO
    || self.entry_sell_px <= Decimal::ZERO
    || exit_px_buy_venue <= Decimal::ZERO   // 新增
    || exit_px_sell_venue <= Decimal::ZERO  // 新增
{
    return None;
}
```

---

## 🟡 P1：高优先级（两周内）

### Go Sidecar 活性问题

#### 11. Lighter WS 每次重连泄漏一个 goroutine
**位置**: `lighter.go:466-469`

watchdog 只在 session ctx 取消时退出，但 `orderStreamOnce` 因读错误
返回、连接被 `defer` 关闭后，这个 goroutine 仍卡在 `<-ctx.Done()` 上，
每 2 秒重连一次就永久泄漏一个。参照 `entropy.go:1188-1203` 的正确模式：

```go
done := make(chan struct{})
defer close(done)
go func() {
    select {
    case <-done:
    case <-ctx.Done():
    }
    _ = conn.Close()
}()
```

---

#### 12. Lighter WS 无读超时/keepalive，半开连接永久挂起
**位置**: `lighter.go:471-484`

没有 `SetReadDeadline` / ping ticker / pong handler，对比
`entropy.go:1190`（40s ping）和 `entropy.go:1210`（90s 读超时）。半开
连接（NAT/LB 空闲超时）会让 `ReadJSON` 永久阻塞、不报错、不重连，
成交流悄悄死掉，降级为 60 秒 REST 轮询而无任何告警。

修复：复制 entropy 的 ping/pong/超时模式到 lighter 的 WS 循环。

---

#### 13. registry 全局锁跨网络初始化，一个死交易所卡住所有请求
**位置**: `main.go:220`（连同 `lighter.go:157-178`, `entropy.go:97-109`,
`sodex.go:64-81`）

`registry.mu` 在文件 I/O + 网络请求期间一直持有。Entropy 不可达时，
所有请求都排队在这把全局锁后面，包括本该立刻发出的撤单。

修复：per-venue `sync.Once` + 失败结果缓存，把网络调用移出全局锁：

```go
type registry struct {
    mu       sync.RWMutex
    sessions map[string]interface{} // *xxxSession 或 error
    initOnce map[string]*sync.Once
}
```

获取 session 时：读锁查缓存 → 未命中则针对该 venue 的 `sync.Once` 做
初始化（网络调用在锁外）→ 写回结果（无论成功还是错误）。

---

#### 14. Entropy 锁覆盖整个 HTTP 往返
**位置**: `entropy.go:928-963`

`s.mu` 本该只保护 `nextNonce()`，却覆盖了整个受 20s 超时限制的
`s.http.Do(req)`。慢下单会连带阻塞走同一 `postAction` 路径的撤单。

```go
s.mu.Lock()
nonce := s.nextNonce()
s.mu.Unlock() // 立即释放，网络请求在锁外
h, err := hlActionHash(action, nonce)
// ... 签名 + s.http.Do(req) 都不持锁
```

---

#### 15. `TestSignL1RecoversSigner` 无法检测错误的 action hash
**位置**: `entropy_test.go:126-177`

测试用被测函数自己生成的 hash 去验证签名恢复，属于自我一致性检查，
即使 msgpack 字段顺序或 vault marker 写错也会通过。

修复：补一条用已知向量（从 Python SDK 或真实线上请求抓取的
action+nonce+hash）做字节级比对的测试，专门覆盖 hash 计算本身。

---

### Rust 执行层

#### 21. 激进限价滑点倍数硬编码为 2×，可能仍失败
**位置**: `src/exec/executor.rs:17-19,431`

紧急平仓用配置的 50× 滑点，但激进限价兜底只有硬编码 2×，波动大时
容易失败，进而触发更贵的 50× 紧急平仓。

```rust
// config.rs CostConfig 新增
#[serde(default = "default_aggressive_slip_mult")]
pub aggressive_slippage_multiplier: Decimal,

fn default_aggressive_slip_mult() -> Decimal { Decimal::from(5) }
```

`executor.rs` 里把 `AGGRESSIVE_SLIPPAGE_MULT` 换成
`cfg.cost.aggressive_slippage_multiplier`。

---

#### 22. 市价单轮询次数/间隔硬编码
**位置**: `src/exec/executor.rs:611-614`（`max_attempts=5, retry_gap_ms=200`）

高延迟环境下 1 秒总窗口可能不够，误判为 `SECOND_LEG_UNKNOWN`。提到
`OrderConfig` 做成可配置项（`market_poll_attempts` / `market_poll_gap_ms`），
默认值保持 5 / 200 不变。

---

## 🟢 P2：计划内修复

### Go Sidecar

#### 16. WS fill 缓存无界增长
**位置**: `lighter.go:70`, `sodex.go:50`, `entropy.go:63`

`fills` map 每单增加两条（oid + cloid）且从不清理，长驻进程下会持续
增长。加时间戳字段，起个 ticker（如 10 分钟一次）清理超过 1 小时的
旧条目。

#### 17. Lighter fill 单位靠"整数且 >50×"启发式判断
**位置**: `lighter.go:377-385`

请求 0.01、实际整数成交 1 时会被误判为要除以 `10^sizeDec`，报告
0.0001，相当于 1 万倍低报，读起来像"没成交"。改为按字段名区分
scaled/unscaled，而不是按数值大小猜。`waitMarketFill:289` 在
`marketDecimals` 出错时静默回退 `sizeDec=4` 同样应改为返回错误。

#### 18. `hlSigFigStep` 用 float64 Log10 在十的整数次幂处出错
**位置**: `entropy.go:1254-1261`

`math.Log10(1000)` 可能返回 2.9999999999999996，`Floor` 后变 2 而非 3，
导致价格精度过细被交易所拒单。改用 Decimal 自身的位数/指数计算，
不经过浮点。

#### 19. 无界 goroutine 扇出
**位置**: `main.go:184-195`

每行 stdin 起一个 goroutine，无并发上限。Rust 侧重试风暴会级联成上百
并发连接。加一个带缓冲 channel 的并发信号量。

#### 20. `scanner.Err()` 被忽略
**位置**: `main.go:172-197`

stdin 出错或超 8MB 缓冲时循环静默退出、进程返回码 0，和正常关闭无法
区分。循环结束后检查 `scanner.Err()` 并写 stderr、考虑非零退出码。

---

### Rust 基础设施

#### 29. 状态发布/执行记录写入失败静默丢弃
**位置**: `src/infra/api.rs:186-189, 192-199`

`RwLock`/`Mutex` 中毒（poisoned）时 `if let Ok(...)` 静默放弃更新，
前端会一直看到旧数据且无感知。改为 `unwrap_or_else(|e| e.into_inner())`
恢复锁内数据并记录一条 warn 日志。

> 注：审查中曾提出 `journal.rs` 的 `Connection` 缺 `Mutex` 保护是并发
> 隐患，经代码回查确认 `ExecJournal` 目前只在 `live_test.rs`（单线程
> CLI 工具）里打开使用，不存在跨线程共享，**此前的"严重"判定已核实为
> 误报，不需要修复**；如未来把 journal 挂到主进程多线程写入路径上，
> 再补 `Mutex<Connection>` 包装。

#### 30. 空 `order_id` 导致后续撤单/查询必然失败
**位置**: `src/exchange/bridge.rs:486-490`

sidecar 返回缺 `order_id` 字段时被解析成空字符串，静默传给下游。改为
`ok_or_else` 直接返回错误，让调用方走告警路径而不是发出注定失败的
撤单请求。

#### 31. 价格/数量可能因 f64 中转丢精度
**位置**: `src/exchange/bridge.rs:504-520`

`serde_json` 未开 `arbitrary_precision` 时数值先过一遍 f64
（15~17 位有效数字），高精度价格/极小数量会失真。在 `Cargo.toml` 给
`serde_json` 加 `features = ["arbitrary_precision"]`，或要求 sidecar
始终以字符串形式返回数值字段。

#### 32/33. sidecar 重启后订单流可能丢订阅 / 失败 watch 被持久化重试
**位置**: `bridge.rs:147`, `bridge.rs:316-324`

`WATCHED` 的 `Mutex` 中毒时 `unwrap_or_default()` 返回空 Vec，重启后
不会补订阅；另外 `bridge_watch` 在调用 `bridge_call` **之前**就把
venue 加入 `WATCHED`，失败的订阅会被持久化并在每次重启后重试。

- 前者同样改 `unwrap_or_else(|e| e.into_inner())`；
- 后者把 `w.push(...)` 移到 `bridge_call` 成功之后。

#### 34. 并发刷新自然价差重复计算
**位置**: `src/infra/history.rs:277-299`

多个 pair 同时触发 `refresh_interval_secs` 到期时，可能并发读同一份
DB、排序、算中位数、写回。结果一致但浪费 I/O。加一个
`Mutex<HashSet<String>>` 记录"正在刷新"的 key，第二个请求直接跳过。

#### 35. `min_qty` 解析失败静默退化为 0
**位置**: `src/exchange/lighter.rs:174`, `src/exchange/sodex.rs:137`

交易所返回畸形 `min_base_amount` 时 `unwrap_or(Decimal::ZERO)`，后续
下单会因为"最小量为 0"而失去校验意义。改为 `.context(...)?`
直接把这个市场标记为不可用，而不是悄悄放行。

---

### 前端

#### 40. `pollTape` 轮询竞态，旧响应可能覆盖新数据
**位置**: `web/src/components/AppShell.vue:176-183`

```typescript
let pollBusy = false;
async function pollTape() {
  if (pollBusy) return;
  pollBusy = true;
  try {
    const r = await apiFetch<{ executions: ExecRow[] }>("/api/executions");
    execs.value = (r.executions || []).slice(0, 50);
  } catch {
    // 连续失败次数计数，达到阈值时提示断线
  } finally {
    pollBusy = false;
  }
}
```

#### 41. 启动/停止套利无二次确认
**位置**: `web/src/components/CtrlAside.vue:69-100, 102-113`

```typescript
import { useDialog } from 'naive-ui';
const dialog = useDialog();

function start() {
  dialog.warning({
    title: '确认启动套利',
    content: `将在所选交易所开始${store.params.execution?.paper_trading ? '模拟' : '实盘'}套利，是否继续？`,
    positiveText: '启动',
    negativeText: '取消',
    onPositiveClick: () => { /* 原启动逻辑 */ },
  });
}
```
停止套利和"重置为 yaml 默认"（115-123 行）同样加确认弹窗。

#### 43. 零值百分比显示为 `+0.000%`
**位置**: `web/src/format.ts:22-26`

```typescript
// 现状: (n >= 0 ? "+" : "")
return (n > 0 ? "+" : "") + n.toFixed(3) + "%";
```

#### 44/45. 轮询失败静默、无请求取消机制
**位置**: `AppShell.vue:162-174`, `api.ts:21-32`

- 轮询失败计数达到阈值（如连续 3 次）时用 `n-message` 或 header
  状态点提示"与后端失去连接"；
- `apiFetch` 支持传入 `AbortSignal`，组件里用 `AbortController` 在
  `onUnmounted` 或下一次请求发起前取消上一次未完成的请求。

---

## 🔵 策略设计改进（S1-S8）

> 以下不是实现 bug，而是策略设计本身可以增强的点。核心逻辑（中枢冻结、滞后、持续性）
> 是对的，但缺少**结构破裂检测**和**自适应机制**。S1/S2 在长期无人值守时必需，
> 其余视实盘数据决定优先级。

### 🔴 S1：缺少结构破裂熔断（高优先级）

**现状**: 策略假设价差会回归到 μ。如果某次偏离是永久性的（某所故障、监管冻结、流动性枯竭），
策略会一路加仓到 `±max_segments` 然后扛单，直到人工介入。

**风险场景**:
- Lighter 主网出现技术故障、提现冻结 3 天，价差偏离 0.5% 且不回归
- 策略打到 STEP=+3 停止加仓，持有 `3 × base_qty` 库存等回归
- 若 BTC 此时从 100k 跌到 95k，两所虽然对冲但价差仍在 0.5%，浮亏 = `3 × 0.001 × 5000 = $15` 每个 BTC pair

**修复**: 在 `src/app/window_spread.rs` 的 `WindowBook` 加半衰期监控和协整检验。

```rust
pub struct WindowBook {
    // ... 现有字段
    last_structure_check: Option<Instant>,
    estimated_halflife_secs: Option<f64>,  // T_half = ln(2) / κ
    adf_failures: u8,  // 连续 ADF 检验失败次数
}

impl WindowBook {
    /// 每 30 分钟或满窗后每 1800 个点触发一次
    pub fn check_mean_reversion(&mut self) -> StructureStatus {
        // 1. AR(1) 回归估半衰期: s_t - s_{t-1} = α + β·s_{t-1} + ε
        //    κ = -β, T_half = ln(2) / κ
        let kappa = self.estimate_ar1_kappa();
        
        // 2. 如果 κ < 0.01 (半衰期 > 69 秒 → 几乎不回归)
        if kappa < 0.01 {
            self.adf_failures += 1;
        } else {
            self.adf_failures = 0;
        }
        
        // 3. 连续 3 次失败 → 暂停开新仓
        if self.adf_failures >= 3 {
            warn!("mean reversion weakened, halflife={:.0}s", 
                  0.693 / kappa.max(0.001));
            return StructureStatus::Broken;
        }
        
        StructureStatus::Healthy
    }
}
```

决策环 `process_pair` 里在判断 Open 之前检查：

```rust
if intent.is_open() && window_book.check_mean_reversion() == StructureStatus::Broken {
    warn!("structure check failed, skip open");
    return Intent::Hold;
}
```

参考文献：Avellaneda & Lee (2010) 统计套利框架；Ernest Chan《算法交易》第 7 章。

---

### 🔴 S2：缺少时间止损（高优先级）

**现状**: 没有"持仓超时强平"机制。如果开仓后价差不回归，会一直扛着。

**修复**: `Position` 里记录 `opened_at: Option<Instant>`，决策环检查：

```rust
if let Some(opened) = position.opened_at {
    let max_hold = Duration::from_secs(cfg.grid.max_hold_secs);  // 配置项，默认 7200 (2h)
    if opened.elapsed() > max_hold && position.grid != 0 {
        warn!(pair=%pair.pair_id, step=%position.grid, 
              held_secs=%opened.elapsed().as_secs(), 
              "position held too long, force close");
        return Intent::Close(position.qty());
    }
}
```

`max_hold_secs` 应设为 **2~3 倍预期半衰期**。如果 μ 的半衰期是 30 分钟（从 S1 算出），
那么超过 1.5~2 小时不回归就强平。不要设太短（如 30 分钟），否则正常回归也会被止损。

---

### 🟡 S3：固定 Δ 在剧烈市会顶满格子（中优先级）

**现状**: Δ 固定在 `(target_bp + F + C) / (1−2h)`。波动剧烈时价差容易偏离 2~3 个 Δ，
打到 `±max_segments` 上限停止加仓，策略退化成"持有顶格库存、等回归"。

**对比**: z-score 方案用 `k·σ` 做格距，波动大时 σ 自动撑大、阈值升高，不容易顶格。
但问题是手续费 F 是绝对 bps，σ 小时 `k·σ` 可能低于 F，开仓不赚钱。

**修复** (杂交方案，文档《统计套利与配对交易.md》 §8.3 已提到):

```rust
// controller.rs 算 Δ 的地方
let delta_min = /* 当前公式: (target + F + C) / (1-2h) */;
let sigma_rolling = window_book.rolling_std_dev();  // 窗口内最近 1000 点标准差
let k_vol = Decimal::from_str("1.5").unwrap();  // 配置项 grid.volatility_multiplier
let delta_adaptive = (k_vol * sigma_rolling).max(delta_min);
```

`rolling_std_dev()` 在 `WindowBook` 里用 Welford 在线算法维护，不用每次重算全窗口。

配置新增：
```yaml
grid:
  volatility_multiplier: "1.5"  # Δ = max(Δ_min, k·σ)；0 = 禁用自适应
```

安静市用固定 Δ_min 保证覆盖成本；剧烈市用 `k·σ` 自动放宽格距。

---

### 🟡 S4：窗口长度 1000 vs 10000 未经实盘验证（中优先级）

**现状**: yaml 设 1000 点（17 分钟），代码默认 10000（2h47m）。没有说明为什么选这个数。

**问题**:
- 1000 点：冷启动快，但对低频震荡（8 小时资金费率周期、日内欧美盘切换）不够稳健
- 10000 点：更稳健但冷启动慢，且趋势市中反应滞后（BTC 95k→105k 时两所价差分布可能已变）

**方案 A**（保守）: 改回 10000，第一次启动先跑纯监控 `monitor_only: true` 攒 3 小时窗口再开交易。

**方案 B**（激进）: 改用 EWMA 替代简单均值：

```rust
// WindowBook::observe
let alpha = Decimal::from_str("0.001").unwrap();  // 半衰期约 693 秒 ≈ 11.5 分钟
if self.mu_live.is_none() {
    self.mu_live = Some(s);
} else {
    let old = self.mu_live.unwrap();
    self.mu_live = Some(alpha * s + (Decimal::ONE - alpha) * old);
}
```

EWMA 好处是冷启动快、对结构变化响应快；缺点是第一个点就有 μ（不稳），需另加
"至少 100 个点才允许开仓"门槛。

**建议**: 先用方案 A（10000 简单均值）跑 1 周，看 μ 的波动幅度和 S1 算出的半衰期，
再决定是否换 EWMA。

---

### 🟡 S5：step_hysteresis=0 可能导致震荡过多（中优先级）

**现状**: yaml `h="0"` 意味着满格才开（raw ≥ k+1）、回到 μ 才平（raw ≤ k），
锁整格利润但成交频率低。

**观察指标** (实盘数据积累后):
- 平均一格锁多少 bp 利润（扣完 F+C）？
- 开了又平、没赚到的震荡比例？

**调整方向**:
- 如果震荡多（贴线反复开平），改成 `h=0.1~0.15`
- 如果锁利润稳定但成交太少，保持 `h=0`

---

### 🟢 S6：增加反向资金费率闸（低优先级）

**原理**: 如果价差和资金费率同向（L 贵且 L 的 funding 是正），说明套利者已经大量
空 L 多 R，价差不是"错误定价"而是"拥挤交易的代价"，不该盲目做多价差回归。

**修复**: `process_pair` 判断 Open 之前：

```rust
if intent.is_open() {
    let l_funding = adapters.get(&pair.legs[0].venue)?.funding(&pair.legs[0].symbol).await?;
    let r_funding = adapters.get(&pair.legs[1].venue)?.funding(&pair.legs[1].symbol).await?;
    let spread_dir = if next_step > current_step { 1 } else { -1 };  // +1 = 要空 L 多 R
    let funding_dir = (l_funding.rate - r_funding.rate).signum();    // +1 = L funding 更高
    
    if spread_dir == funding_dir {
        warn!("funding aligns with spread direction, skip open");
        return Intent::Hold;
    }
}
```

配置新增：
```yaml
grid:
  funding_filter: true  # 默认 false，开启后资金费率同向时不开仓
```

---

### 🟢 S7：分位数替代标准差（低优先级，锦上添花）

**现状**: S3 里的 `rolling_std_dev()` 用标准差。加密盘口噪声大时，MAD（中位绝对偏差）
更稳健——对插针不敏感。

**修复**: `WindowBook` 同时维护 σ 和 MAD，配置项选用哪个：

```rust
pub fn rolling_mad(&self) -> Decimal {
    let median = self.median();
    let deviations: Vec<Decimal> = self.buf.iter()
        .map(|&s| (s - median).abs())
        .collect();
    median_of(&deviations) * Decimal::from_str("1.4826").unwrap()  // 常数使 MAD ≈ σ (正态分布下)
}
```

配置：
```yaml
grid:
  volatility_metric: "std"  # "std" | "mad"
```

---

### 🟢 S8：前端增加半衰期和协整监控（低优先级）

**修复**: 配置页"交易所持仓"表格里，给每个 pair 增加两列：
- **半衰期**（秒）: 窗口估计的 κ 对应的回归速度，绿色 < 1800s（快），黄色 1800~3600，红色 > 3600（慢/不回归）
- **ADF p-value**: < 0.05 绿色（平稳），> 0.05 红色（非平稳，不该开仓）

让用户直观看到哪些币对的均值回归性在减弱，手动关闭或缩小 `max_segments`。

---

## 策略改进总结

| 编号 | 项目 | 优先级 | 实盘前必需？ | 需要配置字段 |
|------|------|--------|------------|-------------|
| S1 | 结构破裂熔断（半衰期 + ADF） | 🔴 高 | **是**（长期无人值守） | - |
| S2 | 时间止损 | 🔴 高 | **是**（长期无人值守） | `grid.max_hold_secs` |
| S3 | Δ 加滚动波动率适应 | 🟡 中 | 否（但剧烈市更稳） | `grid.volatility_multiplier` |
| S4 | 窗口长度调参 / EWMA | 🟡 中 | 否（先跑 10000 点） | `grid.window_samples` |
| S5 | step_hysteresis 调参 | 🟡 中 | 否（先跑 h=0） | `grid.step_hysteresis` |
| S6 | 资金费率方向闸 | 🟢 低 | 否（锦上添花） | `grid.funding_filter` |
| S7 | MAD 替代标准差 | 🟢 低 | 否 | `grid.volatility_metric` |
| S8 | 前端半衰期展示 | 🟢 低 | 否（运维辅助） | - |

**核心结论**: 滑动窗口策略的**设计思路是对的**（中枢冻结、滞后、持续性机制合理），
但缺少两层保护：
1. **结构破裂检测**（S1）：价差不回归时的熔断
2. **时间止损**（S2）：持仓超时强平

这两项在长期无人值守时是**必需**的，否则遇到黑天鹅（某所被 hack / 监管冻结 / 
流动性枯竭）会扛单到顶格。其余 S3-S8 视实盘数据决定优先级。

---

## 修复验证清单

1. **编译**：`cargo build` / `go build`（sidecar 目录下）
2. **单测**：为每个修复场景补充针对性用例（尤其是 #6/#7/#9/#10 这类
   数值边界，以及 #2/#3/#4 这类需要 mock 网络延迟才能触发的竞态）
3. **集成**：在 testnet / example venue 配置下跑一次完整
   下单→撤单→成交确认链路
4. **回归**：staging 环境跑 48 小时观察日志里的 warn/error 计数，
   确认新增的告警路径没有被高频触发（说明修复引入了新问题）
5. **文档**：涉及新增配置字段时同步更新 `docs/配置参考.md`。
   本文涉及的新字段汇总：
   - `cost.aggressive_slippage_multiplier`（#21）
   - `order.market_poll_attempts` / `order.market_poll_gap_ms`（#22）
   - `grid.max_hold_secs`（S2）
   - `grid.volatility_multiplier`（S3）
   - `grid.funding_filter`（S6）
   - `grid.volatility_metric`（S7）

策略改进（S1-S8）的验证方式与实现缺陷不同——它们没有"修对了"的二元判据，
要靠对照数据：

- **S1/S2**：在 paper 或小仓位下人为构造不回归场景（把某个 slot 的 μ 手动
  偏置，或用历史上价差长期偏离的时段回放），确认熔断和超时强平真的触发，
  且**没有**在正常回归的行情里误触发。
- **S3/S4/S5**：不写单测判对错，而是并行跑两组参数（如 h=0 vs h=0.15）在
  同一段行情上对比每格实际锁利和震荡占比，用数据选参数。
- **S1 的 AR(1)/ADF 实现本身**：必须有单测。用已知平稳序列（人工生成的
  OU 路径）和已知非平稳序列（随机游走）各喂一次，确认前者判 Healthy、
  后者判 Broken，否则熔断会在错误的时候开火。

## 修复顺序建议

实现缺陷（#）和策略改进（S）合并成一条时间线。原则：**先让执行层不出错，
再让策略层更聪明**——在幻影成交和方向反转还没修掉之前，加自适应格距或
熔断只是在错误的库存上做更精细的决策。

| 阶段 | 内容 | 目的 |
|---|---|---|
| 第 1 周 | 实现缺陷 P0 全部（#1-10） | 唯一可能直接亏钱/失控的项，必须先清零 |
| 第 2 周 | P1（#11-15, #21-22） | WS 稳定性、滑点与轮询配置化 |
| 第 3 周 | **S1 + S2**（结构破裂熔断、时间止损） | 补上策略唯一的两个必需保护 |
| 第 4 周 | paper trading 跑满 7 天 | 收集 μ 稳定性、半衰期、成交频率、每格实际锁利 |
| 第 5 周 | P2 批量（#16-20, #29-35）+ 前端（#40-45） | 资源清理、防御性校验、UI 与测试 |
| 第 6 周起 | 小仓位实盘（单 pair、`max_segments=1`） | 用真实数据回答 S3/S4/S5 的调参问题 |

关于 S3/S4/S5（Δ 自适应、窗口长度、滞后系数）：**不要在实盘数据之前拍板**。
这三项都是参数选择而非缺陷，第 4 周的 paper 数据和第 6 周的小仓位实盘才能
给出依据。具体要看的指标：

- 每格实际锁到多少 bp（扣完 F+C 后是否还有 `target_bp`）
- 开了又平、没赚到的震荡占比（决定 S5 的 h 该不该从 0 抬起来）
- STEP 打到 `±max_segments` 的频率（决定 S3 的自适应格距是否必要）
- S1 报出的半衰期分布（决定 S2 的 `max_hold_secs` 和 S4 的窗口长度）

S6/S7/S8 属于增强，随时可做，不占关键路径。
