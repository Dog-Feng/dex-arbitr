# 监控统计与异常检测方案

> 分支：`fix/monitoring-analytics-20261016`。状态：设计稿（待实现）。
> 目标：给系统加一套可运维的监控统计——各种"率"、时间分桶、异常检测、告警与离线分析，用来回答"什么时段赚/亏、失败率为什么高、步数为什么堆满、反转是否摩擦"这类问题。

## 1. 原则与边界

- **monitor 只读**：所有统计、异常、告警都进 monitor；它永远不下交易指令、不 import engine。业务模块（套利/刷量/扫描/监控）互不影响。
- **engine 不实现业务逻辑**：engine 只补发事件字段（如补救滑点、每腿延迟），不写统计代码。
- **统计是观察，不是决策**：将来若要做"按会话调参/暂停开仓"的策略，那是 strategy/risk 层新注册实现，只读消费本方案的桶数据；统计模块本身不触发任何交易动作。
- **存储用 UTC、展示用北京时区**：桶按 UTC 存，会话标签（美股时段/亚洲时段）在查询/展示层按配置打标，改时段定义不需要重存数据。

## 2. 现状盘点（数据源）

已有：

- monitor 事件流：`step_opened/step_closed/flip/executed/rescue/status/emergency/error/pair_*`，已聚合 opens/closes/flips/executions/one_leg/rescues/errors/open_steps/volume/realized/latency(avg/max)/μ/σ/tick_k/fail_rate(终身+滚动)。
- `samples_dir`：100ms 盘口（两腿价、深度、时间戳）落盘，行情侧（μ/σ/深度按小时）可离线复算。
- state 文件：步账本、日滚动量/盈亏审计、`pnl_uncertain`。
- status 日志：每分钟 per-pair 快照（journald）。

缺口：

- 没有时间维度：无法看"每小时/每会话"的率与盈亏；
- 没有生命周期指标：单步持仓时长、步龄分布、回本速度；
- 没有分位数：延迟只有 avg/max，没有 p50/p95/p99；
- 没有异常规则：失败率尖峰、满仓锁死、risk 抖动、连续亏损桶等只能靠人肉看日志；
- 交易明细不可重放：step 的开/平、入场 bps、持有时间、realized 没有结构化留存（只有 samples 有行情，没有交易事件流）。

## 3. 指标与"率"设计

### 3.1 质量率

| 指标 | 定义 | 用途 |
|---|---|---|
| `one_leg_fail_rate` | 单腿失败 / executions（终身 + 50 笔滚动，已有） | 让价/深度健康 |
| `rescue_rate` | rescues / executions | 补救频率 |
| `rescue_success_rate` | rescues / one_leg_fails（应恒为 1；<1 立即 WARN） | 补救兜底有效性 |
| `partial_close_rate` | (executions - opens - closes) / executions | 部分平仓占比 |
| `flip_rate` | flips / opens，或 flips/小时 | 反转摩擦 |
| `depth_reject_rate` | 因深度不足拒绝的开仓信号 / 总开仓信号（需在 status 事件补计数） | 深度门槛是否过严 |

### 3.2 盈亏率

| 指标 | 定义 |
|---|---|
| `realized_bps` | realized_usd / volume_usd × 1e4（每单位双边量的净盈亏） |
| `realized_per_exec` | realized_usd / executions |
| `rescue_slippage_usd` | 补救滑点累计（executed 事件补 `rescue_cost_usd` 字段汇总） |
| `breakeven_close_ratio` | 平仓毛利 ≥ 0 的比例（close 事件补 gross） |

### 3.3 延迟

- 现有 avg/max 之外增加 **p50/p95/p99**；executed 事件补 `latency_l_ms/latency_r_ms`，支持分腿统计（两链确认延迟差异是单腿失败主因之一）。

### 3.4 持仓与生命周期

- 单步持仓时长：step_opened 记 `OpenedAt`，step_closed 记时长 → 平均/分位持仓时长、回本速度。
- 当前步龄：open_steps 的平均年龄与最长年龄（满仓时用"最老步龄"判断套牢）。
- 每桶步数峰值与 `max_steps` 使用率（满仓锁死预警）。

### 3.5 行情（离线）

- 每桶 μ/σ 均值、σ 阶跃、深度覆盖度——由 samples 离线复算，monitor 只存桶内 status 快照的 μ/σ。

## 4. 时间分桶与会话

- 桶粒度：**15 分钟**（可配），UTC 存储、有界内存环（默认保留 7 天）。
- 每桶每对累计 §3 的全部计数与求和项（计数、量、盈亏、延迟 sum/n、μ/σ 快照），全局桶 = 各对之和。
- 会话标签（配置项，仅展示层打标）：

```yaml
monitor:
  analytics:
    bucket: 15m
    retain_buckets: 672          # 7 天 × 96 桶
    sessions:
      us_cash: "21:30-04:00"     # 美股常规时段（北京时间）
      us_premarket: "16:00-21:30"
      asia_day: "09:00-16:00"
```

- 输出：`/metrics` 增加 `time_buckets`（近期有界），每个桶带 `utc_hour`、`session` 标签与全部指标；`anomalies` 近期异常记录。

## 5. 异常检测规则（阈值全部可配，按严重度 INFO/WARN/CRIT）

| # | 规则 | 级别 |
|---|---|---|
| A1 | 连续 ≥3 笔单腿失败，或 rolling_fail_rate > 阈值（默认 0.10） | WARN |
| A2 | rescue_success_rate < 1 | CRIT |
| A3 | 10 分钟内 flips ≥ 3（滞回摩擦，可能需调 `flip_hysteresis_sigma`） | WARN |
| A4 | `open_steps ≥ max_steps-1` 持续 > 30 分钟（满仓锁死） | WARN |
| A5 | 最老未平步龄 > 阈值（默认 60 分钟，套牢迹象） | WARN |
| A6 | 连续 3 桶 realized < 0 且累计 < -阈值 | WARN |
| A7 | 单次 rescue 滑点 > 阈值（默认 0.01 USD） | WARN |
| A8 | 延迟 p95 > 阈值 或 max > 2×p95（两链延迟漂移） | WARN |
| A9 | 盘口新鲜但 ≥30 分钟 0 执行（策略静默：门槛/配置/信号问题） | INFO |
| A10 | 1 小时内 risk blocked↔healthy 抖动 > 8 次（QQQ 零清算价边界摩擦类） | WARN |
| A11 | μ 相对前桶变化 > k×σ（默认 3σ，台阶跳变提示） | INFO |
| A12 | state 的 `pnl_uncertain` 置位超过 7 天未复核 | INFO |

告警统一走现有 `AlertFunc` 通道，消息带规则编号、pair、桶时间；`/metrics.alerts` 保留现有格式，新增 `anomalies`（有界，保留最近 64 条）。

## 6. 交易明细留存（"交易的信息"）

- 新增结构化交易事件 JSONL（append-only，放 `state_dir/trades/<pair>.jsonl`，由 monitor 定义 `BucketSink`/`TradeSink` 小接口，`run.go` 装配时注入 store 实现）：
  - step_opened：id、dir、qty、entry_bps、open_l/open_r、opened_at
  - step_closed：id、held_s、entry_bps、close_prices、gross、realized_usd、one_leg
  - executed：latency_l/r、rescue_cost_usd、tick_k、σ
- 用途：离线回放"每笔交易赚/亏了多少、持有多久、失败发生在哪条腿"，并作为未来按会话调参的数据基础。

## 7. 实施分期

- **P1（已实现）**：monitor 加 15 分钟桶聚合（计数/量/盈亏/延迟/μ/σ）+ `/metrics.time_buckets`；engine 在 executed/status 事件补 `rescue_cost_usd`、`latency_l/r_ms`、`gross`、`max_steps`、`oldest_step_age_s` 字段（纯事件字段，不动决策）。
- **P2（已实现）**：异常规则引擎（阈值入 `monitor.analytics.anomalies` 配置）+ `/metrics.anomalies` 输出与 ALERT 告警；规则触发有 10 分钟冷却，非正阈值关闭对应规则。
- **P3**：TradeSink JSONL 落盘 + 会话标签配置与展示。
- **P4（可选）**：`cmd/analytics` 离线命令：join samples（行情侧）+ 交易 JSONL（结果侧），输出每桶/每会话报表（μ/σ、成交、盈亏、失败率、flips、持仓时长），验证"RWA 美股时段 vs 白天"的时段假设。
- 测试：桶聚合单元测试、异常规则表驱动测试、JSON/JSONL 格式测试；全量 `-race`。

## 8. 与现有模块的关系

- engine：只补事件字段，决策逻辑零改动；
- strategy / scan / execution：不动；
- 配置：`monitor` 段下新增 `analytics`、`anomalies` 子段，默认全部关闭或为观察阈值，开箱不改变任何交易行为；
- 手动/自动模式共用同一套统计；auto 的 pair_health 数据可并入桶内的 depth/tick 质量字段。
