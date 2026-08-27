# 整改方案：固定每格数量 + 交易对显式配置 + 平仓判据回退 + 提高兑现频率

**状态**：已实施（决策 **D1-c**：纯方案 A，止损和止盈闸都删）。逐项任务见第 5 节；配套实现见 [`整改实现细节.md`](./整改实现细节.md)。

**未做**：F3 双向网格。

**具体实现逻辑见配套文档 [`整改实现细节.md`](./整改实现细节.md)**，按任务编号一一对应，含现状代码、改法伪代码与边界情况。

## 目录

- [0. 总览](#0-总览)
- [1. 背景](#1-背景为什么改)
- [2. 目标配置模型](#2-目标配置模型)
- [3. 目标运行流程](#3-目标运行流程)
- [4. 需要先拍板的两个决策](#4-需要先拍板的两个决策)
- [5. 任务清单](#5-任务清单)
- [6. 附录](#6-附录)

---

## 0. 总览

四条主线，共 26 个任务，分七个阶段。阶段 A 与 F1 不依赖任何重构，可立即开工。

| 主线 | 动因 | 涉及阶段 |
|---|---|---|
| **定仓方式** | 废弃「总金额 ÷ 格数」，改为逐交易对指定每格数量 | B、C |
| **交易对管理** | 启动即加载交集全集供选择，选中并配置后才进决策环 | D、E |
| **平仓判据** | 移除方案 B 的往返止损，回退方案 A | A |
| **兑现频率** | 让减格子更频繁地兑现收益 | F |

外加 5 个已确认缺陷（附录 6.3）分散在各阶段修复。

**阶段依赖**：

```
A（独立）──┐
F1（独立）─┤
           ├─→ B ─→ C ─→ D ─→ E ─→ G
           └─→ F2/F3（需 B 的逐所对阈值）
```

每个阶段结束都应能 `cargo test` 全绿、可独立回滚。阶段 B 之后配置文件不兼容旧版。

---

## 1. 背景：为什么改

### 1.1 定仓：除法毁掉了格子边界

当前 `base_qty` 的来路：

```
resolve_qty(保证金, 名义上限, 深度) → r.qty（币数，已按精度取整）
base_qty = r.qty / max_segments        ← 除法，不再取整、不再校验下限
```

三个后果：

- `base_qty` 通常是无限小数（如 `0.00166 / 3`），每次下单都被 sidecar 按精度截断，**实际成交从第一笔起就不是 `base_qty` 的整数倍**，格子边界从一开始就对不齐。
- `r.qty` 刚过 `min_qty` 时，`base_qty = min_qty / 3` 直接低于交易所下限，而空仓开仓路径不检查 `min_qty`。
- 每格数量由行情和余额隐式决定，用户无法预知也无法复现。

改成显式配置后，`base_qty` 是用户填的、经过校验的合法数量，`base_qty × n` 精确可达，加减仓天然对齐。

### 1.2 网格用币数而非金额，这一点不变

需要澄清一个常见误解：**网格的尺已经是代币数量了**，本次重构不改变这一点，只是把这把尺的来源从"隐式除法"改成"显式配置"。

`Position.base_qty` 存的是币数且整个持仓周期固化（`position.rs:21-23`），`segments_held`、`close_delta`、`open_delta` 全是数量运算。金额只出现在两处且都合理：入口换算（保证金本就是金额计价）与风控记账（跨币种可比）。

固定币数的代价是名义敞口随价格漂移，见附录 6.3 的补充项。

### 1.3 兑现频率：先想清楚约束

每次减格子都要付一次完整往返费。把一次大收敛拆成三次小收敛，总收敛量不变但费付三次：

```
一次吃 0.09%：       0.09 − 0.016 = 0.074%
拆成三次各 0.03%： 3 × (0.03 − 0.016) = 0.042%
```

在**单边收敛**行情里，格子越密赚得越少。密格子的价值在**震荡行情**：价差在 0.06% 与 0.09% 之间来回摆五次，密格子能吃五次，稀格子因从未跌破 T0 而一次都吃不到。

所以目标不是"提高频率"，而是**在费率允许的下限内，让格子密到能吃住实际波动幅度**。这决定了阶段 F 各任务的优先级。

---

## 2. 目标配置模型

### 2.1 YAML

`pairs` 段重写，吸收原 `grid.base_qty` 与 `pairs.whitelist`，并支持逐所对覆盖：

```yaml
pairs:
  # 未单独指定的字段用这套默认值
  defaults:
    max_segments: 3
    initial_spread_threshold: "0.05"
    grid_step: "0.05"
    t0_ratio: "0.4"
    split_order_size: "0"
    scalping_enabled: false
    scalping_trigger_segment: 10
    scalping_profit_threshold_pct: "0.02"

  # 只有列在这里的交易对才进决策环。空列表 = 只监控不交易。
  enabled:
    - symbol: BTC
      base_qty: "0.001"            # 必填，每格币数
      max_segments: 3              # 以下均可省略，省略则继承 defaults
      initial_spread_threshold: "0.05"
      grid_step: "0.05"
      # 逐所对覆盖：便宜的所对可以开密档（见 6.1 费率表）
      overrides:
        - venues: [lighter, entropy]
          initial_spread_threshold: "0.02"
          grid_step: "0.02"
          max_segments: 6
          scalping_enabled: true
          scalping_trigger_segment: 2

    - symbol: ETH
      base_qty: "0.02"
```

`sizing` 段精简——不再定仓，只保留「校验开得起吗」所需字段：

| 字段 | 去留 | 新职责 |
|---|---|---|
| `mode` / `fixed_notional_usdc` | **删除** | 定仓逻辑取消 |
| `min_notional_usdc` / `max_notional_usdc` | **删除** | 同上 |
| `max_concurrent_pairs` | 保留 | 槽位数，与定仓无关 |
| `leverage_multiplier` / `leverage_by_venue` | 保留 | 保证金校验 |
| `margin_utilization_pct` | 保留 | 保证金校验留 buffer |
| `depth_pct` | 保留，语义变更 | 从「截断定仓量」变成「`base_qty` 不得超过一档量的这个比例」 |
| `refresh_balance_secs` / `fallback_available_usdc` | 保留 | 余额刷新 |

`grid` 段删除 `base_qty`、`close_stop_loss_pct`，其余阈值项移入 `pairs.defaults`。`close_take_profit_pct` 的去留取决于决策 D1（见第 4 节）。

### 2.2 页面

配置页新增「交易对」区块，数据源是启动时加载的交集全集：

| 列 | 来源 | 说明 |
|---|---|---|
| 勾选 | 用户 | 是否纳入套利 |
| 交易对 | `available_pairs` | 如 `BTC-USD-PERP` |
| 可用所对 | `available_pairs` | 如 `lighter↔sodex`、`lighter↔entropy` |
| 最小下单量 | 各腿 `min_qty` 取大 | 只读，用于校验 |
| 数量精度 | 各腿 `qty_precision` 取小 | 只读，用于校验 |
| **每格数量** | 用户填 | 前端即时校验 ≥ min_qty 且符合精度 |
| **格数** | 用户填 | |
| T1 / step | 用户填，可留空继承默认 | |
| 最大敞口估算 | 前端算 | `每格数量 × 格数 × 当前中价` |
| 逐所对覆盖 | 折叠展开 | 每个可用所对可单独配阈值，并显示该所对往返费 |

---

## 3. 目标运行流程

当前「点启动套利 → 才去 `list_perps` 匹配 → 顺带订阅 WS」全揉在 `apply_venue_match`（`controller.rs:416-512`）一个函数里，要拆成两段。

```
启动
 ├─ bootstrap()                建 bbo channel（不变）
 ├─ load_available_pairs()【新】对所有已配 venue 调 list_perps
 │                              → match_all_pairs 得到交集全集
 │                              → 存 self.available_pairs（含 min_qty / precision）
 │                              → 不订阅 WS、不进决策
 │                              → publish 给 API
 ├─ activate_pairs()      【改】按 pairs.enabled 过滤 available_pairs
 │                              → 逐对硬校验 base_qty（见 3.1）
 │                              → 只为选中的对订阅 WS
 │                              → seed_matched_pairs / panel.resize
 └─ 决策环
```

页面侧：

```
打开配置页   → GET /api/pairs/available    展示全集 + 各腿限制 + 各所对费率
勾选并填参数 → POST /api/config            写入 pairs.enabled
点启动套利   → POST /api/arbitrage/start   置 rematch → 下一轮 activate_pairs
```

### 3.1 校验分三层

`base_qty` 的合法性依赖 `min_qty` 与 `qty_precision`，二者要 `list_perps` 之后才知道，**所以不能在 `AppConfig::load` 阶段校验**。

1. **前端即时校验**：用 `/api/pairs/available` 的限制做输入框校验，填错当场标红。
2. **`control::validate`**：`base_qty > 0`、`max_segments >= 1`、`T1 > 0`、`step >= 0`。不查精度。
3. **`activate_pairs` 硬校验**：逐对比对 `min_qty` 与 `qty_precision`，不合格的**跳过该对并打 ERROR**，不阻断其他对。

第 3 层必须有：没有定仓兜底之后，一个非法 `base_qty` 会让该对每轮发单每轮被拒。

---

## 4. 需要先拍板的两个决策

其余任务都是纯工程性的，这两个不定下来会影响 A1、B4、F1 的做法。

### 决策 D1：平仓判据保留到什么程度

移除方案 B 后，盈利性由阈值参数自己扛，没有逐笔检查。**收敛量是下界不是定值**（详见 6.2），方案 A 在实盘中可以盈利，它赌的是"实际收敛量的期望 > 往返费"。真正的麻烦是**所对之间费率跨度 5.7 倍**（详见 6.1），而 `grid_step` 现在是全局的。

| 选项 | 做法 | 代价 |
|---|---|---|
| **D1-a（推荐）** 方案 A + 止盈闸 | 只删往返止损，保留 `close_take_profit_pct` 那道门 | 保留一处方案 B 残留；仍有锁仓区间但无止损兜底 |
| **D1-b** 纯方案 A + 抬高全局阈值 | `step` 按最贵所对配（>0.092%），`T1 > step/(1−t0_ratio)` | 便宜所对的机会全部放过 |
| **D1-c** 纯方案 A + 逐所对阈值 | 阈值下沉到所对级（B4 已在做） | 配置与 UI 复杂度上一台阶；无任何逐笔兜底 |

删除止损意味着唯一的亏损出口消失，剩下 `HoldTimeout`（168 小时）、`FundingStopLoss`（费率不利持续 60 分钟）、`BalanceFloor`（当前配 `0`，等于关闭）。**若选 b 或 c，务必把 `min_balance_close_usdc` 配上非零值**，别让 7 天超时成为唯一兜底。

### 决策 D2：是否做双向网格

当前网格单向：方向锁在 `pos.buy`/`pos.sell`，格数走 `0..max_segments`，平到 0 才重新双向择优。双向网格允许穿越零点反向建仓（格数 `-n..+n`）。

**收益比直觉小**。从 +1 到 −1，单向是「减 1 格 + 开 1 格」两个动作，双向是「一笔跨 2 格」一个动作，**交易量与费用完全相同**。真正差别只有三点：

- 平到 0 时 `grid.forget` 清持续性，反向建仓要重新攒（bucket 模式 1 秒）
- 重新占 `max_concurrent_pairs` 槽位、重新过 `daily.allows` 配额
- 中间一瞬的空仓期

**成本很高**：`segments_held`/`target_segments` 要从 `u32` 改带符号，`Position.qty` 要允许负值，`record_open`/`record_close` 要合并成 `apply_delta`，`planner` 要按 delta 符号决定腿方向。

建议先做完 F1、F2 看实际数据再定。默认按**不做**推进（F3 标为可选）。

---

## 5. 任务清单

### 阶段 A：平仓判据与判据层缺陷（独立，可立即开工）

#### A1 · 移除方案 B 的往返止损

- 文件：`src/domain/grid.rs`
- 删函数 `round_trip_stop_hit`（154-175 行）
- `decide_held` 删止损分支（353-364 行）
- `GridParams` 删 `close_stop_loss`；`CloseReason::StopLoss` 删除
- `controller.rs` 的 `forced_exit` 判定（1571-1580 行）移除 `StopLoss` 分支
- `config.rs` / `control.rs` / `default.yaml` / `web/index.html` 删 `close_stop_loss_pct`
- 若决策 D1 选 b/c，同时删止盈门（`grid.rs:414-420`）与 `decide_scalp_close` 的 round_trip 检查（467-471 行）
- `Intent::Close.round_trip_pct` **保留**为纯记录字段（journal 与面板要展示）
- 删除对应单测：`round_trip_stop_loss_flattens`、`open_fill_slip_does_not_stop_while_raw_still_at_t1`，若删止盈门再加 `grid_reduce_waits_for_round_trip_take_profit`

**验收**：`cargo test` 全绿；`grep -r "close_stop_loss\|StopLoss" src/` 无残留。

#### A2 · 减仓量补最小下单量检查（缺陷 1）

- 文件：`src/domain/grid.rs:424-428`
- 减仓分支补 `qty < params.min_qty` 判断，与加仓分支（374-380 行）对称
- `decide_scalp_close`（472-476 行）同样补
- 追加防重试：失败计数达阈值后触发 `intervention` 或退避，避免每轮重发同一张必拒单

**验收**：新增单测「`close_delta` 低于 `min_qty` 时返回 Hold」。

#### A3 · 统一加减仓的量口径（缺陷 3）

- 文件：`src/domain/grid.rs:368-387`
- `open_delta` 改为量差：`base_qty × add_to − pos.qty`
- 触发条件由 `add_to > current_segments` 改为 `base_qty × add_to > pos.qty + ε`

理由：`segments_held` 向上取整会把"欠了半格"的持仓算作满格，格数比较发现不了欠仓，导致部分成交后永远补不齐。

**验收**：新增单测「`pos.qty = 0.5 × base_qty` 且价差仍在 T1 时，能补到 1 格整」。

#### A4 · 第二腿部分成交的敞口登记（缺陷 2）

- 文件：`src/exec/executor.rs:350-354`（`hedge_second_leg`）与 `422-479`（`dual_taker`）
- `second.qty < first.qty` 时，把差额登记为 `NakedSource::BotFailure`，让 `try_hedge_naked_exposures` 能自动补对冲
- 注意 `controller.rs:2258-2260` 成功路径会 `retain` 清掉该 pair 的裸敞口记录，顺序要调整

**验收**：新增单测覆盖「第二腿部分成交 → `ExecResult` 带上未对冲量」。

### 阶段 B：配置结构重构

#### B1 · 新增 `PairSetting` 与 `PairDefaults`

- 文件：`src/config.rs`
- `PairSetting { symbol, base_qty, max_segments?, initial_spread_threshold?, grid_step?, t0_ratio?, split_order_size?, scalping_*?, overrides: Vec<PairOverride> }`
- `PairOverride { venues: [String; 2], ...同上可选字段 }`
- `PairsConfig` 重写为 `{ defaults: PairDefaults, enabled: Vec<PairSetting> }`，删除 `whitelist`

#### B2 · 精简 `SizingConfig`

- 按 2.1 表格删字段，`SizingMode` 枚举整个删除
- `control.rs` 的 `ArbitrageParams` 同步删 `sizing_mode`、`fixed_notional_usdc`、`min_notional_usdc`、`max_notional_usdc`

#### B3 · `grid_for` 改签名

- `grid_for(base, min_qty)`（`config.rs:689`）→ `grid_for(symbol, venue_a, venue_b, min_qty) -> Option<GridParams>`
- 找不到配置返回 `None`，调用方跳过该对（不再拿 `base_qty = 0` 的参数继续跑）
- 新增 `pair_setting(symbol) -> Option<&PairSetting>`
- 适配调用点：`controller.rs:1287`、`1316`、`2080`、`2732`，`risk.rs:47`

#### B4 · 逐所对阈值覆盖

- `grid_for` 内按 `venue_a`/`venue_b` 查 `overrides`，命中则覆盖对应字段
- 匹配时 venue 顺序无关（与 `slot_key` 一致，字典序归一）
- `control::validate` 增加提示：某所对的 `step` 或 `T1 × (1 − t0_ratio)` 低于该所对往返费时告警

这是解锁阶段 F 的前提，也是决策 D1-c 的实现基础。

#### B5 · `whitelist` 退场

- `make_adapter(venue, whitelist)`（`exchange/mod.rs:21`）改传空列表——新流程要展示**全集**供选择，过滤下沉到 `activate_pairs`
- `live_test.rs:150` 同步改
- `controller.rs:427-431` 的 `whitelist_allows` 过滤移除

#### B6 · `default.yaml` 与文档同步

- 按 2.1 重写 `pairs` 段、精简 `sizing`、清理 `grid`
- `docs/配置参考.md`、`docs/项目说明.md`、`docs/本系统套利流程.md` 同步
- `docs/平仓判据备选方案B.md` 标注为已废弃并说明去向

**阶段 B 验收**：`AppConfig::load` 单测通过；新 yaml 能解析；`cargo build` 无警告。

### 阶段 C：决策环接线

#### C1 · `sizing` 从定仓改为容量校验

- 文件：`src/app/sizing.rs`
- `resolve_qty` → `check_capacity(cfg, base_qty, add_segments, buy_margin, sell_margin, books, mid, legs) -> Result<(), &'static str>`，返回 `Err("no_margin")` / `Err("thin_book")`
- 保留 `LegMargin` 与 `free_notional`；删除 `preview_segment_qty`、`BindingLeg`、`ResolveQtyResult`
- 11 个单测重写

#### C2 · 替换 `process_pair` 的定仓段

- 文件：`src/app/controller.rs:1347-1430`
- 不再 `resolve_qty`，改为从配置读 `base_qty` 后调 `check_capacity`
- 校验不过则 `bump_skip("no_capacity")` 并返回
- `record_ui_pair` / `fill_monitor_row` 里 `preview_segment_qty` 的调用（2741 行）改为直接读配置值

#### C3 · 加仓路径补保证金校验（缺陷 5）

- 文件：`src/app/controller.rs`，加仓分支
- 当前 `resolve_qty` 只在 `pos.is_none()` 时调用（1347 行），加仓完全不校验保证金
- 新方案下这是唯一的敞口闸门：`base_qty × max_segments` 不再有任何隐式约束

**阶段 C 验收**：paper 模式跑通一轮开→加→减→平；加仓超保证金时被 `no_capacity` 拦住。

### 阶段 D：加载与激活流程拆分

#### D1 · 新增 `load_available_pairs`

- 文件：`src/app/controller.rs`
- 从 `apply_venue_match` 拆出「拉市场 + 匹配」，启动时执行
- 新增字段 `available_pairs: Vec<Pair>`，不订阅 WS、不进决策

#### D2 · 新增 `activate_pairs`

- 从 `apply_venue_match` 拆出「订阅 WS + seed UI + resize panel」
- 按 `pairs.enabled` 过滤 `available_pairs`
- 实现 3.1 第 3 层硬校验，不合格的对跳过并打 ERROR

**阶段 D 验收**：启动后 `/api/pairs/available` 有数据且未订阅任何 WS；点启动后只有选中的对建立订阅。

### 阶段 E：API 与前端

#### E1 · 新增 `GET /api/pairs/available`

- 文件：`src/infra/api.rs`
- 返回全集，每项含 `pair_id`、可用所对列表、`min_qty`、`qty_precision`、当前中价、**该所对往返费**（供前端提示阈值下限）

#### E2 · `ArbitrageParams` 改造

- 删 `base_qty`（HashMap）、`whitelist`，新增 `pairs: Vec<PairSetting>`、`pair_defaults`
- `validate` 补 3.1 第 2 层校验
- `POST /api/config` body 结构随之变化

#### E3 · 前端交易对配置表格

- 按 2.2 实现，含逐所对覆盖的折叠区
- 敞口估算与阈值下限提示实时计算

#### E4 · 前端清理

- 删除 `f-sizing-mode`、`f-fixed-notional`、`f-min-notional`、`f-max-notional`、`grid.base_qty` 逐币输入、`whitelist` 输入、`close_stop_loss_pct` 输入
- `renderVenueChips`（`index.html:1155`）补上遗漏的 `entropy`
- `venueQuote` 的 sodex fallback 从 `vUSDC` 改为与 yaml 一致的 `USDC`

**阶段 E 验收**：页面能列出全集、勾选、填参数、启动，且非法输入当场标红。

### 阶段 F：提高兑现频率

#### F1 · 启用剥头皮模式（零代码，优先做）

这是现成但被参数关死的高频兑现机制。正常减仓用 `close_thresholds`（滞后一格），剥头皮用 `open_thresholds`（`grid.rs:451-461`）——**滞后被完全取消**，第 3 格跌破 0.09% 就减而不必等跌破 0.06%，且**不走持续性门**，响应更快。收敛下界改由 `scalping_profit_threshold` 单独把关。

现在等于关着的原因不是 `scalping_enabled: false`，而是 `scalping_trigger_segment: 10` —— 需要价差达到 `T1 + 9×step = 0.30%` 才触发，实盘几乎不出现。

- 改 `config/default.yaml`：`scalping_trigger_segment` 调到 2~3，`scalping_enabled: true`
- **只在便宜所对上开**：`scalping_profit_threshold_pct: 0.02` 对 lighter↔entropy（往返 0.016%）净赚 0.004%，对 sodex↔lighter_rh（往返 0.0916%）是纯亏损
- 在 B4 完成前，先用 paper 模式单独测便宜所对

**验收**：paper 跑一段，对比启用前后的兑现次数与单笔净收益分布。

#### F2 · 提高格数而非缩小 step

缩小 `step` 受费率硬约束，提高 `max_segments` 不受。价差从 T1 走到 T1+5×step 的路上，6 格能分 6 次进场 6 次兑现，3 格只能 3 次，**每次收敛量不变**，吃到的波动更多。

- 依赖 B4（逐所对阈值）：便宜所对开 6 格密档，贵的所对保持 3 格
- 依赖 C3：格数提高会线性放大敞口（`base_qty × max_segments`），必须有加仓保证金校验兜底
- 前端敞口估算（E3）要显眼

#### F3 · 双向网格（可选，见决策 D2）

默认不做。若 F1/F2 的数据显示"平到 0 后频繁错过反向机会"，再按 D2 的成本评估决定。

### 阶段 G：收尾

#### G1 · 保证金占用按市价重估

- 文件：`src/app/positions.rs:158-161`、`reserved_margin_by_venue`
- 改用 `qty × 当前 mid` 估算占用；`entry_notional_usdc` 保留原义只做盈亏记账
- 现状：持仓期间币价上涨会低估占用，进而高估可用额度

#### G2 · `Position.grid` 减仓后更新

- 文件：`src/app/positions.rs:151-164`
- 只影响 `/api/snapshot` 显示（判据一律用 `segments_held`），低优先

#### G3 · 文档最终同步

- 本文标注为已实施
- `docs/套利流程对比.md`、`docs/参考项目V3套利流程.md` 中涉及定仓与平仓判据的段落复核

---

## 6. 附录

### 6.1 各所对往返手续费

`sequenced_fee = maker(先挂侧) + taker(后吃侧)`，先挂的是 taker 费率更高的一侧（`sequence.rs:75-80`）。按 `config/venues/` 的实际费率：

| 所对 | 单次 | **往返** | `step=0.03%` 覆盖得住吗（临界） |
|---|---|---|---|
| lighter ↔ entropy | 0.008% | **0.016%** | ✓ 富余 0.014% |
| lighter ↔ sodex | 0.0158% | 0.0316% | ✗ 差 0.0016% |
| lighter ↔ lighter_rh | 0.017% | 0.034% | ✗ 差 0.004% |
| sodex ↔ entropy | 0.0198% | 0.0396% | ✗ 差 0.0096% |
| lighter_rh ↔ entropy | 0.021% | 0.042% | ✗ 差 0.012% |
| sodex ↔ lighter_rh | 0.0458% | **0.0916%** | ✗ 差 0.0616% |

最贵的所对是最便宜的 **5.7 倍**。旧文档里「往返约 0.034%」只是 lighter↔lighter_rh 那一对，不能当全局常数。

费率变动后此表需重算——`venue_fees` 是从各 venue yaml 读的，不是交易所实时查询。

### 6.2 收敛量是下界不是定值

每次「开一格 → 减一格」的价差收敛量：

| 格 | 开在 | 减在 | 收敛量**下界** |
|---|---|---|---|
| 第 1 格 | `T1` | 跌破 `T0 = T1 × t0_ratio` | `T1 × (1 − t0_ratio)` |
| 第 n 格（n ≥ 2） | `T1 + (n−1)·step` | 跌破 `T1 + (n−2)·step` | `step` |

**实际收敛量 = 开仓超调 + 下界 + 平仓超调**，通常明显大于下界：开仓要过持续性门（bucket 模式至少 1 秒），价差穿越阈值后还要等，实际成交价差往往已超出阈值；减仓同理，100ms 一轮的决策环发现跌破时通常已经跌过头。

下界只在临界情况（恰好在阈值上开、恰好在阈值下平）出现，是最坏情况而非常态。所以方案 A 在实盘中可以盈利，它赌的是"实际收敛量的期望 > 往返费"。

### 6.3 缺陷清单

| # | 缺陷 | 位置 | 归属任务 |
|---|---|---|---|
| 1 | 减仓量可能低于 `min_qty`，失败后每轮重试 | `grid.rs:424-428` | A2 |
| 2 | 第二腿部分成交，多出的第一腿敞口不登记 | `executor.rs:350-354` | A4 |
| 3 | 加仓用格数差、减仓用量差，欠仓补不回 | `grid.rs:370` vs `408` | A3 |
| 4 | `base_qty` 除法未取整未校验下限 | `controller.rs:1388-1389` | **B/C 自动消除** |
| 5 | 加仓不复核保证金 | `controller.rs:1347` | C3（优先级升高） |

补充两项非阻塞：

- **`entry_notional_usdc` 不按市价重估**（`positions.rs:158-161`）→ G1
- **`Position.grid` 减仓后不更新**（`positions.rs:151-164`）→ G2

### 6.4 影响面统计

| 文件 | 改动量 | 涉及任务 |
|---|---|---|
| `src/config.rs` | 大 | B1 B2 B3 B4 |
| `src/domain/grid.rs` | 大 | A1 A2 A3 |
| `src/app/controller.rs` | 大 | A1 C2 C3 D1 D2 |
| `src/app/sizing.rs` | 大 | C1 |
| `src/app/control.rs` | 中 | B2 E2 |
| `src/infra/api.rs` | 中 | E1 |
| `web/index.html` | 中 | E3 E4 |
| `src/exec/executor.rs` | 小 | A4 |
| `src/app/positions.rs` | 小 | G1 G2 |
| `src/app/risk.rs`、`src/bin/live_test.rs`、`src/exchange/mod.rs` | 小 | B3 B5 |
| `config/default.yaml` + 5 份 docs | 同步 | B6 G3 |
