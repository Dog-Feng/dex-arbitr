# 阶段 2：Burst（L1 限价循环 + 市价对冲）

阶段 2 与阶段 1 **互斥**：`burst.enabled=true` 时不再走 μ / 点差窗 / WindowGrid 开仓，也不使用原 μ±Δ 邻档方案（已删除）。

## 开关与约束

| 项 | 说明 |
|---|---|
| `burst.enabled` | `true` 启用阶段 2；`false` 时仅阶段 1（STEP + 双市价） |
| 单币单 slot | `burst.enabled` 时 `pairs.enabled` **只能 1 个 symbol**；启动匹配时会校验 |
| 页面热改 | 配置页「阶段 2 / Burst」与 yaml `burst:` 字段一致，经 `/api/config` 写入 |

## 单次 rep（开或平一格）

1. **选方向**：按费 + 点差（`best_sequenced_spread`）决定买所 / 卖所；**第一腿**挂在限价所 **Bid1（买）或 Ask1（卖）**。
2. **首腿**：Post-only 限价；`limit_rehang_timeout_ms`（默认 2000）内未成则撤单，**无限重挂**直到该 rep 成交量 ≥ `base_qty`（或进程取消）。
3. **第二腿**：首腿确认成交后，对手所 **市价**对冲；失败重试至多 `hedge_max_attempts` 次。
4. **失败停手**：`BURST_HEDGE_FAILED` / 整 rep 零成交等 → `orders_live=false` + 人工介入（`BurstPhase::Stopped`）。

不看 μ 窗、点差窗、毛价差门禁；容量仍校验（含 `open_repeats` 峰值保证金）。

## 宏观周期

```
Opening：连续 open_repeats 次开仓 rep（每次 base_qty）
  → 每次 rep 后随机 pause（pause_ms_min ~ pause_ms_max）
  → 满 open_repeats → Cooldown（cooldown_ms_min ~ max）
  → Closing：按 base_qty 逐格平到 0
  → 再 Cooldown → Opening（open_reps_done 清零）
```

停套利：撤所有在途限价（`cancel_all_resting_limits`），已发出的单仍可能成交，但不再发新单 / 不对冲（与阶段 1 停手语义类似）。

## 配置（yaml / 页面）

| 字段 | 默认 | 说明 |
|---|---|---|
| `open_repeats` | 10 | 一轮宏观周期内连续开仓次数 |
| `pause_ms_min` / `max` | 3000 / 10000 | 每个 rep 完成后的随机暂停 |
| `cooldown_ms_min` / `max` | 60000 / 300000 | 开满或平完后的随机冷却 |
| `limit_rehang_timeout_ms` | 2000 | 首腿 L1 挂单最长等待（≥200） |
| `hedge_max_attempts` | 20 | 第二腿市价失败重试上限 |

## 代码入口

- 决策：`src/app/burst.rs` → `process_pair_burst`
- 规划：`exec/planner.rs` → `plan_burst`
- 执行：`exec/limit_market.rs` → `execute_burst_limit_market`
- 回调：`controller` → `on_burst_run_plan`

阶段 1 详见 [滑动窗口-阶段1-追STEP.md](滑动窗口-阶段1-追STEP.md)。
