# Bug 修复文档

审查日期：2026-09-02  
范围：当前仓库代码（Rust 决策/执行、Go sidecar、配置与文档对照）。只记**对照源码核实过**的问题；文档过时但实现正确的不列为 bug。  
**2026-03 注**：原 μ±Δ 邻档（`symmetric_limit`）已删除，阶段 2 改为 **Burst**（`burst.enabled`）。下文大量「邻档」条目为历史记录，代码路径已移除。

默认配置：`burst.enabled: false`（阶段 1 双市价）、`step_hysteresis: "0"`、`split_order_size: "0"`。标了「默认配置可触发」的项即使不改 yaml 也会踩。

严重程度：

| 级 | 含义 |
|---|---|
| P0 | 资金风险：错边/重复开仓、假平仓、漏记账导致敞口 |
| P1 | 逻辑错误：状态机、方向、数量、确认口径错，实盘会放大或拖延敞口 |
| P2 | 场景缺口：特定路径才出，或产品选择导致的长敞口 |
| P3 | 不一致 / 低危：日志、默认值、文档与代码偏离 |

建议修复顺序见文末。

---

## P0 — 资金风险

### P0-1 阶段 1 空仓用 sticky μ，不跟 live

**默认配置可触发。**

- **位置**：`src/app/window_spread.rs` `SlotWindow::quote_mu`；`src/app/controller.rs` `process_pair`（约 2092–2117 行）
- **规格**：`docs/滑动窗口-阶段1-追STEP.md` §3.3：`STEP=0` 必须用 \(\mu_{\text{live}}\) 每秒更新；sticky / `maybe_advance_quote` 只给阶段 2 改价
- **场景**：`symmetric_limit=false`，窗已满，长时间空仓
- **现状**：`quote_mu` = `frozen.or(sticky).or(live)`。`observe` 在首次满窗写下 `sticky` 后不再改。`maybe_advance_quote` 只在邻档路径（约 3832、4011 行）调用。阶段 1 决策一直拿第一次满窗的中枢
- **后果**：μ 漂移后该开不开，或相对过时中枢错边开仓。`0→±1` 时 `freeze()` 还会把这份过时 sticky 冻进持仓期

**修复：**

1. `process_pair` 阶段 1 决策改读 live：

```rust
let mu = if has_pos {
    self.windows.quote_mu(&slot) // 有仓：frozen
} else {
    self.windows.live_mu(&slot)  // 空仓：必须跟窗
};
```

2. `WindowBook::freeze` 改为冻 **当时的 live μ**，不要 `sticky.or(live)`：

```rust
st.frozen = st.live_mu(cap);
```

3. `quote_mu` 可保留给阶段 2；不要让阶段 1 再走 sticky。
4. 单测：满窗后继续 `observe` 不同 s，阶段 1 空仓 `decide` 用的 μ 必须等于 `live_mu`，不能停在第一份均值。

---

### P0-2 紧急平仓把部分成交当成整笔成功

**默认配置可触发（双市价一腿失败走回滚）。**

- **位置**：`src/exec/executor.rs` `emergency_close`（约 524–544 行）→ `send_leg` 在 `filled > 0` 时 `Ok(ExecFill { qty: filled.min(qty) })`（约 668–674 行）
- **场景**：双市价一腿成、一腿确定没成；或阶段 2 第二腿失败平第一腿；交易所只平掉一部分
- **现状**：`match Ok(_) => return Ok(())`，不看 `fill.qty`
- **后果**：上层记 `EMERGENCY_CLOSED`、不挂介入、不登记裸仓，策略当中性继续开。所上残留单边

**修复：**

```rust
Ok(fill) => {
    if fill.qty + eps < qty {
        // 对剩余量继续 attempt；最后一拍仍不足则 NAKED
        remaining = qty - fill.qty;
        continue;
    }
    return Ok(());
}
```

约定：`Ok(())` **只**表示请求量已全部确认平掉。部分成交必须累加剩余再试；三次后仍不足 → `Err(NAKED_FIRST_LEG)`，qty 用剩余量。不要把 `Ok(_)` 当成功。

`fill_second_leg` 的 `Ok(f)` 同样要核对 `f.qty` 与第一腿量；不足走 `finished` 的 `unhedged_qty` 或继续平剩余，禁止当第二腿全成。

---

### P0-3 邻档增量对冲失败时丢掉已成功的 `acc`

- **位置**：`src/exec/limit_market.rs` `hedge_seen_increments`（约 947–960 行）；`controller.rs` `on_run_plan` NAKED 分支（约 2929–2939 行）
- **场景**：第一腿分笔成交，前几笔已市价对冲写入 `acc`，后续增量返回 `NAKED_FIRST_LEG` / `SECOND_LEG_UNKNOWN`（非 `EMERGENCY_CLOSED`）
- **现状**：`Err(err)` 直接上抛，`acc` 丢失，不走 `apply_fill`。NAKED 还用 `msg.plan.qty`（整格计划量）登记裸仓
- **后果**：所上已有对锁仓，内存为空，后续当空仓再开 → 叠仓。裸仓量被夸大

**修复：**

1. `hedge_seen_increments` 失败时：若 `acc` 已有成交，改为返回 `Ok(acc)`，并把剩余失败通过 `unhedged_qty` / 独立错误通道上报，**不要**整 task `Err`。
2. 若必须 `Err`：在 error 类型里带上 `partial: ExecResult`，`on_run_plan` 先 `apply_fill(partial)` 再处理失败。
3. `record_naked_from_failed_hedge` 用 `seen - hedged`（未对冲增量），禁止 `plan.qty`。

---

### P0-4 停止套利 `ARB_STOPPED` 同样丢弃已对冲结果

- **位置**：`src/exec/limit_market.rs` `execute_resting_adjacent`（约 760–768 行）、`hedge_seen_increments`（约 910–916 行）；`controller.rs` `on_run_plan`（约 2953–2958、2985–2991 行）
- **场景**：邻档已对冲若干增量后用户点停止；或停止瞬间第一腿又成交
- **现状**：直接 `bail!(ARB_STOPPED)`。`on_run_plan` 只打日志，不 `apply_fill`，不 `record_naked`。`finish_adjacent_slot` 在已 `claim` winner 时还不 `release_pending`
- **后果**：内存无仓 / pending 卡住；所上已有仓。与「停止后不再发新对冲」不矛盾——**已经对冲上的量必须入账**

**修复：**

1. bail 前若 `acc` 非空：先把已对冲结果作为 `Ok` 回传（或 error 携带 partial）。
2. 剩余 `unhedged > 0`：`record_naked(Foreign 或 BotFailure)` + 面板强告警。仍可不发新市价对冲。
3. `ARB_STOPPED` 路径强制 `release_pending`，不要依赖 `quote_winner_taken`。
4. 建议：停止时对「已成未对冲」允许 **emergency close**（只平这一笔，禁止新开/新挂）。见 P2-3。

---

### P0-5 成交均价回落成滑点保护限价

**Entropy / SoDEX 默认可触发。**

- **位置**：
  - `src/exec/limit_market.rs` `entropy_order_update_fill`（约 319–321 行）优先 `limitPx`
  - `scripts/exchange_sidecar/sodex.go` `avgPriceFromOrder`（约 1270–1279 行）缺 `ExecutedValue` 时回落 `o.Price`
  - `scripts/exchange_sidecar/lighter.go` `matchActiveOrder`（约 974–976 行）`price` 优先于真实均价
- **场景**：Entropy IOC 先到 `orderUpdates`、`userFills` 未到；SoDEX 历史单有量无名义；Lighter 查单拿挂单价
- **后果**：每腿多记约 `max_slippage_pct`（默认 1%）假滑点；往返 bp、加仓均价、面板盈亏全歪。第一腿监视若直接用 `ack.avg_price` 入账，会把保护价写进持仓

**修复（统一契约）：`avg_price` 只能是真实成交均价，否则空。**

1. Entropy WS：只认 `avgPx` / `userFills.px`。没有则 `None`，交给 `fill_price_for_pnl` 退回决策 BBO。改掉依赖 `limitPx` 的测试 `entropy_ws_fill_triggers_without_rest`。
2. SoDEX：无 `ExecutedValue` 返回 `""`，禁止 `o.Price`。
3. Lighter `matchActiveOrder`：只取明确均价字段（`avg_filled_price` 一类），不要 `price`。
4. 紧急平仓路径 `ack.avg_price.unwrap_or(price)`（`send_leg` emergency 分支）同样不要用保护限价；没有均价就用当时 BBO。

---

### P0-6 `split_order_size>0` 时 STEP +1 但数量只下拆单量

- **位置**：`src/domain/window_grid.rs` `emit` / `split_order_qty`（约 42–54、146–161 行）；`apply_fill` → `record_open(..., plan.grid_to)`
- **场景**：`split_order_size` 小于 `base_qty`（当前 yaml 为 `"0"`，**改配置即触发**）
- **现状**：Intent.qty 被截成拆单量，`grid` 仍变为 `k±1`。目标仓应为 `|k|×base_qty`
- **后果**：后续加减仓尺度全错；平仓按错误格数减，过零判断乱

**修复（二选一，推荐 A）：**

- **A**：拆单 **不推进** STEP。同一 `k` 多次 Open，直到 `qty ≥ |k+1|×base_qty` 再 `grid = k±1`。
- **B**：Intent 仍带满格 `base_qty`，由执行层拆单；`apply_fill` 按累计成交量反算 grid，只有凑满一格才改 `grid`。

`dex_test_mode` 截断 `plan.qty` 不改 `grid_to`（`controller.rs` 约 2425–2427 行）同一类，一起修。阶段 2 `adjacent_quotes` 未读 `split_order_size`，要在同一约定里写明。

---

## P1 — 逻辑错误

### P1-1 `activate_pairs` 未固定 L/R

- **位置**：`src/app/controller.rs` `load_available_pairs`（约 666–669 行）vs `activate_scan`（约 874 行）
- **场景**：页面 `active_venues` 顺序与 `config.venues` 不一致，例如勾选 `[entropy, lighter]`
- **现状**：`match_all_pairs` 按传入列表下标 `i<j` 定 `legs[0]/legs[1]`。扫描路径会 `order_pairs_legs(..., &self.cfg.venues)`，套利激活不会
- **后果**：正 STEP = 空 L 多 R。L/R 对调后开仓方向相对 yaml/扫描反号，跨重启或与扫描结论对不上

**修复：**

```rust
self.available_pairs = order_pairs_legs(match_all_pairs(&listed), &self.cfg.venues)
    .into_iter()
    .filter(|p| wanted.contains(&p.legs[0].base.to_ascii_uppercase()))
    .collect();
```

激活后断言：任意 pair 的 `legs[0]` 在 `cfg.venues` 里的下标 ≤ `legs[1]`。

---

### P1-2 阶段 2 负仓减档缺滞后，负向开仓滞后方向反了

- **位置**：`src/domain/adjacent.rs` `adjacent_quotes`（约 64–87 行）
- **场景**：`symmetric_limit=true` 且 `h>0`（当前 yaml `h=0` 时两档重合，不触发）
- **现状**：
  - 正仓减档 Minus：`μ+(k−1)Δ + hΔ`（对）
  - 负仓减档 Plus：`μ+(k+1)Δ`（缺 `−hΔ`）
  - 负仓开仓 Minus：`μ+(k−1)Δ − hΔ`（阶段 1 是 `+hΔ`，相邻更远，滞后反了）
- **规格**：阶段 1 `raw_plus ≥ k+1−h`、`raw_minus ≤ k−1+h`；负仓应镜像

**修复：**

```text
Plus 目标：
  开仓 (k≥0):  μ + (k+1)Δ
  减仓 (k<0):  μ + (k+1)Δ − hΔ     // 向 μ 靠

Minus 目标：
  减仓 (k>0):  μ + (k−1)Δ + hΔ
  开仓 (k<0):  μ + (k−1)Δ + hΔ     // 不是 −hΔ
  k=0:        μ − Δ
```

补单测：`h=0.25`、`k=-1` 的 Plus 减档与 Minus 开档，与阶段 1 阈值互为镜像。现有 `hysteresis_shifts_reduce_line` 只覆盖了 `k=1`。

---

### P1-3 平仓不写 `grid_to`，只靠 qty 反推 STEP

- **位置**：`src/app/positions.rs` `record_close` / `apply_qty_scale`（约 156–164、236–250 行）
- **场景**：部分成交、拆单残量、对账后 `qty` 与 `|grid|×base_qty` 不一致
- **现状**：日志 `step=plan.grid_to`，内存 `grid = sign * ceil(qty/base_qty)`。可能多减/少减一格，过零错乱

**修复：** `record_close(slot, qty, grid_to: i32)`。平仓成功优先用引擎 `plan.grid_to`；qty 到 0 则删仓并 `unfreeze`。对账路径才用 qty 反推，且反推后若 `|grid|×base_qty` 与 qty 差超过 `min_qty` 打告警。

---

### P1-4 阶段 1 裸腿不登记、有裸仓仍可开仓

- **位置**：`on_run_plan` NAKED（约 2929–2947 行）；`pair_has_naked` 只在邻档（约 4073 行）；`try_hedge_naked_exposures` / `process_pair` Open
- **场景**：双市价紧急平失败；或补对冲 task 飞行中再来开仓信号
- **现状**：阶段 2 会 `record_naked_from_failed_hedge`；阶段 1 只 `mark_intervention`。30 分钟 `AUTO_RESUME` 后 `hedge_failed_legs` 没有 BotFailure 可补。开仓不查 `pair_has_naked` / `naked_hedging`
- **后果**：介入解除后继续开，与未平裸腿叠加

**修复：**

1. 阶段 1 NAKED / SECOND_LEG_UNKNOWN 同样 `record_naked`（量用确认成交量，不是 `plan.qty`）。
2. `process_pair` 在合成 Open 前：`pair_has_naked(pair_id) || naked_hedging.contains(slot)` → Hold，面板「单边敞口」。
3. `spawn_naked_hedge` 期间把该 slot 标 inflight，与 `hedging` 同等待遇。

`EMERGENCY_CLOSED` 假成功（叠加 P0-2）目前只 `note_single_leg`，不满 3 次继续开。回滚校验全额后，假成功应变介入或强制对账。

---

### P1-5 两腿同向对账把内存抹成空仓

- **位置**：`src/app/reconcile.rs` `audit_position_qty`（约 101–103 行）
- **场景**：快照错腿、两所同向非零、内存仍有对锁仓
- **现状**：返回 `hedged=0` → `reconcile_qty` 缩到 0 → 策略当空仓再开，叠在同向仓上
- **注释本意**：同向对冲量视为 0，但调用方把它当成「实盘重叠量 = 0，把内存改成 0」

**修复：** 同向且都非零时返回 `None`（跳过自动对账），并 `mark_intervention(SameSignPositions)`。只有反向重叠量才允许缩/抬。

---

### P1-6 空账户快照被标 `fresh=true` + fallback 保证金

- **位置**：`src/exchange/{lighter,sodex,entropy}.rs` `account` 在无密钥 / 无 sidecar 时 `Ok(AccountSnapshot::default())`；`src/app/balance.rs` `refresh_accounts` 凡 `Ok` 都 `fresh=true`（约 129–135 行）；yaml `fallback_available_usdc: "500"`
- **场景**：四所都勾了，只有两所配了私钥；或 sidecar 暂时找不到
- **现状**：未配密钥的所余额走 500 USDC 兜底、持仓当空、fresh。容量校验能过。`keys_ready` 不挡 `process_pair`。双市价 `join` 两腿，无密钥那腿立刻失败，有密钥那腿可能已成 → 走紧急平（再叠加 P0-2）
- **另一条**：两所都返回空快照且都 fresh 时，`audit_position_qty` 会把内存仓缩到 0（两腿 qty 都是 0）

**修复：**

1. 跳过查询必须 `Err` 或 `fresh=false`，禁止 `Ok(空) + fresh=true`。
2. Open 前两腿 `keys_ready()`，否则 skip `keys_missing`。
3. `all_fresh()` 为 false 时不对账、不补裸仓（现有逻辑可保留），但不要用 fallback 假装有保证金去开仓。fallback 仅用于「查询失败且明确仍要联调」的显式开关，不要默认 500。

Lighter `collateral > 0` 才上报余额（`lighter.go` 约 211–224 行）：`collateral=0` 时像没钱，走 fallback。有 `available_balance` 或非零仓也应上报。

---

### P1-7 Entropy 签名与发单未串行（nonce 竞态）

- **位置**：`scripts/exchange_sidecar/entropy.go` `postAction`（约 936–965 行）
- **场景**：同账户并发 place/cancel（主路径 + 裸仓补对冲，或邻档两档几乎同时）
- **现状**：锁内只取 nonce，锁外 sign + HTTP。N+1 先到交易所会使 N 失败
- **修复**：对齐 Lighter `submitMu`，锁住「取 nonce → 签名 → HTTP 发出」整段。失败重试必须重新取 nonce，不要重放旧包。

---

### P1-8 Lighter 公共盘口在 snapshot 前应用 delta

- **位置**：`src/exchange/lighter.rs` `run_ws`（约 398–403 行）
- **场景**：重连后先到 `update/order_book`，后到 `subscribed/order_book`
- **现状**：`or_default()` 空簿上 `apply` delta，残缺 BBO 进决策
- **后果**：误开仓 / 阶段 2 误撤误挂

**修复：** 每个 `market_id` 在收到 snapshot 前忽略 delta；重连清空 `books` 并等 `subscribed/order_book`。没有 BBO 就不要 `tx.send`。

---

### P1-9 邻档 watchdog 超时直接摘 `pending`

- **位置**：`src/app/controller.rs` `watch_one_pending`（约 2481–2497 行）
- **场景**：邻档挂满约 24h，或非 rest 硬超时；`cancel` 已置位但撤单/对冲未完成
- **现状**：`pending.remove` + `finish_adjacent_slot` → 决策环立刻可重挂同侧；旧单仍可能成交
- **修复：** 超时只 `cancel.store(true)` 并告警，**等到** `on_run_plan` 回执再摘 pending。超时上限内不重挂。必要时挂介入 `WatchdogTimeout`。

---

### P1-10 `paper_trading` / `monitor_only` 已从代码消失，文档仍当开关

- **位置**：`src/config.rs` `ExecutionConfig` 无这两字段；`exec_worker.rs` `spawn_run_plan` 固定 `paper: false`。文档 `docs/项目说明.md`、`README.md`、`docs/配置参考.md` 仍写 paper
- **场景**：按文档设 `paper_trading: true`（serde 忽略未知字段）
- **后果**：静默实盘。`executor` 的 paper 分支成死代码
- **修复：** 二选一：
  - **恢复接线**：配置字段 + HTTP 页勾选 + `spawn_run_plan(paper)`；未知字段启动时 warn。
  - **明确删除**：启动时若 yaml 出现 `paper_trading` / `monitor_only` 则 **拒绝启动**；同步改 README / 项目说明 / 配置参考。推荐至少做拒绝启动，避免误开实盘。

---

### P1-11 阶段 2 全局 Δ 用「L 买 / R 卖」一套 F/C

- **位置**：`controller.rs` `pair_delta_inputs`（约 1207–1211 行）；`exec/sequence.rs` `symmetric_grid_costs`
- **场景**：两所 maker/taker/点差不对称，Plus 与 Minus 最优挂单所不同
- **现状**：固定 `(left,right)` 算一套 Δ，两档共用
- **修复：** Plus / Minus 各算一套 F+C，格距取 `max`（保守、不让便宜边格过窄）。改价/新挂时至少按当前 `q.buy/q.sell` 重算该档成本做校验。

---

### P1-12 Sidecar 超时后弃单，进程仍可能把单发出去

- **位置**：`src/exchange/bridge.rs` `call_on`（约 432–444 行），`spawn_sidecar` `kill_on_drop(true)` 但 Child 在 `Arc` 里；`scripts/exchange_sidecar/main.go` `handlerTimeout` = `fill_wait+15s`
- **文档**：写操作 100s、sidecar 90s，超时杀进程。现状 Rust place 80s、Go 默认约 17s，且单次超时只从 `pending` 摘 oneshot，**不杀 sidecar**
- **场景**：`sendTx` 不尊重 ctx、或 `DEX_SIDECAR_TIMEOUT_SECS` 偏短；Rust 已报失败，链上稍后成交
- **修复：**
  1. 对齐超时：Rust place 超时 > Go `handlerTimeout` 最长路径（建议 Go 写超时显式化，例如 90s，Rust 100s）。
  2. 超时后必须 `order_status` / 撤单闭环，不能只 `pending.remove`。
  3. 迟到的 sidecar 响应若找不到 oneshot，打 ERROR 并触发该 venue 对账，不要静默丢。
  4. 文档改成与代码一致的常驻 sidecar 语义。

---

### P1-13 成交量取 max 不求和（待确认推送形态）

- **位置**：`src/exec/limit_market.rs` `walk_generic_fills`（约 184–187 行）；`node_filled_qty` trade 分支
- **场景**：Lighter `account_all_trades` 一批多笔，或连续增量 `size`
- **现状**：`best` 取最大单笔。Entropy `userFills` 已累加
- **修复：** 订单快照用累计 `filled_base_amount`；trades 通道对同 `order_id` **求和**。禁止用单笔 `size` 覆盖累计。用实盘 WS 样本确认后再合入。

---

## P2 — 场景缺口

### P2-1 有仓时 stale/thin 会 `forget` 持续性

- **位置**：`controller.rs` `process_pair` gate 失败（约 2019–2024 行）
- **场景**：持仓中盘口短暂过期或变薄
- **现状**：`forget_persist` 清空 hits；价差已该减仓也要再攒 7 次
- **修复：** 有仓（或 Intent 方向是 Close）gate 失败不要 `forget`。空仓 Open 仍可 forget。

---

### P2-2 介入态挡平仓，敞口挂满 30 分钟

- **位置**：`controller.rs` 约 2358–2399 行；`intervention.rs`
- **场景**：裸腿 / orphan / SECOND_LEG_UNKNOWN
- **现状**：开平都挡（记账不可信，按错的量去平会放大敞口——有意）
- **修复：** 对「内存仓与交易所方向一致」的 Cause 允许 reduce-only 平到 0；面板加「只平不挂」；缩短 `AUTO_RESUME` 或必须人工点解除。不要在介入态自动开仓。

---

### P2-3 停止套利后第一腿成交故意不对冲

- **位置**：`orders_live=false` + `ARB_STOPPED`；阶段 2 文档 §4.4
- **场景**：停止瞬间邻档成交
- **现状**：符合「不再市价对冲」的规格，但是单边仓
- **修复：** 停止路径对已成未对冲允许 emergency close；禁止新开/新挂。与 P0-4 一起做。运营上停止前应先看到「邻档 0/2」。

---

### P2-4 文档中的强制离场未接入

- **位置**：`CloseReason::{FundingStopLoss,HoldTimeout,BalanceFloor}` 在 `grid.rs` 已定义；`process_pair` 只产生 `GridReduce`；`funding_*` 未引用
- **场景**：长持资金费翻转、余额触底（阶段 1 文档 §3.8）
- **修复：** 在格子 `decide` 之前插入强制 `Close { qty: held_qty, grid: 0 }`，不受 ±1 / persistence / 容量拦截。没做完之前不要在文档里写「已实现」。

---

### P2-5 未配密钥的所仍进入配对

- **位置**：`activate_pairs` 只按 `active_venues` + `base_qty` 过滤，不看 `keys_ready`
- **与 P1-6 一起修：** 实盘启动要求所选所全部 `keys_ready`；否则拒绝启动并列出缺密钥的所。

---

### P2-6 reduce-only 数量只向下截断，留下粉尘

- **位置**：SoDEX `roundToStep(..., false)`；Entropy `roundHlSz`；Lighter `baseAmount = qty.Mul(scale).IntPart()`
- **场景**：平仓量略低于 step
- **后果**：粉尘仓被当成新裸仓，或对账抬/缩乱跳
- **修复：** reduce-only 向「能平掉的最大合法步长」取整，夹到当前仓位绝对值。低于 min_qty 的粉尘标记 `NakedBelowMinQty` 并移出自动补对冲（现有 Cause 几乎没用上）。

---

### P2-7 保证金用跨所 mid，深度只用 L1×depth_pct

- **位置**：`src/app/sizing.rs` `check_capacity`（约 39–50 行）
- **修复：** 开仓名义分腿：买腿 `qty×ask`、卖腿 `qty×bid`；深度与 `l1_covers` / `sequenced_spread` 同一口径。mid 低估买腿名义时可能 `no_margin` 漏拦。

---

### P2-8 `max_concurrent_pairs` 已删除

- **位置**：代码中无此字段；文档仍写默认 `1`
- **后果**：勾选多币会同时开，保证金与在途单比预期多
- **修复：** 恢复槽位限制，或文档改为「无上限，靠保证金门」。推荐恢复，默认 1。

---

## P3 — 不一致 / 低危

### P3-1 停止边沿日志写「pair list cleared」，实际未清 `self.pairs`

- **位置**：`on_arbitrage_stopped`（约 619–625 行）
- **修复：** 改日志，或 `pairs.retain(slot_is_live)`。

### P3-2 扫描窗默认值分裂

- **位置**：`src/config.rs` `default_scan_window_samples = 60`；`src/app/control.rs` 同名默认 `120`；yaml `120`
- **修复：** 统一为 120（与 yaml / 页面一致）。

### P3-3 `scan.rs` 注释「每个 base 留两个所」vs `rank_bases` 只留一行

- **修复：** 改注释。实现是有意的（每币一行最优所对）。

### P3-4 Journal 不持久化 `grid_from`/`grid_to`；`Cause::EmergencyCloseFailed` 未使用

- **修复：** schema 加列，或文档写明读回 grid 恒为 None。NAKED 区分 `EmergencyCloseFailed` 与 `NakedLegUnrecoverable`。

### P3-5 yaml / 文档数字对不上代码默认

| 项 | yaml | 代码缺省 | 说明 |
|---|---|---|---|
| `grid.window_samples` | 300 | 300 | 与 yaml 一致 |
| `grid.persistence_min_hits` | 7 | 文档仍有 5 | 以 yaml 为准 |
| `scan.min_volume_24h_usdc` | 300 万 | 写成 0 时仍按 1000 万 | 配置参考已写，页面不要显示 0 |
| `order.ioc_fill_wait_ms` | 2000 | 2000 | 代码缺省 / 前端初值已对齐 |
| `cost.default_slip_pct` | `"0.1"` | `"0.1"` | 前端 `emptyParams` 已对齐 |

---

## 建议修复顺序

按「先堵资金漏洞、再修方向/状态、最后文档」：

1. **P0-1** 阶段 1 空仓改 `live_mu`（改动小、默认路径必踩）
2. **P0-2** 紧急平仓核对全额
3. **P0-5** 禁止保护限价当均价（三所一起改契约）
4. **P0-3 / P0-4** 失败/停止路径先 `apply_fill` 再报错
5. **P1-1** 激活所对固定 L/R
6. **P1-4 / P1-6 / P1-5** 裸仓、密钥、同向对账
7. **P1-10** 拒绝已删除的 `paper_trading`，避免误开实盘
8. **P1-7 / P1-8 / P1-12** sidecar 与盘口
9. **P0-6 / P1-2 / P1-3** 拆单、负仓滞后、平仓 grid（改配置或开阶段 2 才踩）
10. P2 / P3

每条合入应带：触发场景的单测或 sidecar 表驱动测试；涉及成交确认的用固定 JSON 夹具，不要依赖实盘。

---

## 审查后确认「不是 bug」的点（避免误修）

| 主题 | 结论 |
|---|---|
| 成功路径持仓回写 | `apply_fill` 用 `hedged_qty()`，不是计划量 |
| 过零 / 每拍 ±1 | `window_grid` 有测试，实现正确 |
| 满窗才开、有仓 C=0 可减 | `process_pair` 与文档一致 |
| 阶段 1/2 分流 | `symmetric_limit && enabled` 走邻档，否则双市价 |
| 双市价 Unknown 不再紧急平 | 符合「认不到 ≠ 没成交」 |
| Foreign 仓不自动补 | 符合 `hedge_failed_legs` 只补本进程失败腿 |
| `VenueSpreadBook` 同秒多币平均 | 文档写明，不是实现疏漏 |
| nat / res 不进 STEP | 有意，只给监控/扫描 |

---

## 未跟踪本地改动注意

`git status` 显示 `scripts/exchange_sidecar/lighter.go`、`src/app/controller.rs` 等有未提交内容。lighter.go 方向是修 WS 撤单假成交与 `order_status` 合并 trades，**不要当成新缺陷回滚**。合入前与本清单 P0-5 / P1-13 对齐：均价字段仍不可回落挂单价。
