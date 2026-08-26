# 平仓判据备选方案 B（往返净利 + 按格减仓）

**状态**：未实施。当前采用方案 A（完全对齐参考项目的格子阈值判据）。
本文档记录方案 B 的设计，供方案 A 实盘不理想时切换。

**背景**：2026-08-25 决定「全部对齐参考项目」，故先实施 A。B 保留为后备。

---

## 1. 两个方案的差别

判据的**量纲不同**，这是核心。

| | 方案 A（当前实施，对齐参考） | 方案 B（备选） |
|---|---|---|
| 平仓判据 | 归一化后的**毛价差** vs `T0/T(n-1)` | **往返净利** vs 止盈/止损线 |
| 公式 | `relative_spread = -closing_spread_pct × direction` | `round_trip = entry_net_pct + exit_net_pct` |
| 是否含手续费 | **否**（`spread_pct` 是毛价差） | **是**（两个 net 各扣过一次 fee） |
| 减仓粒度 | 按格（`actual - target`） | 按格（同 A） |
| 阈值来源 | `T0 = T1 × 0.4`，`close[i] = T(i)` | `close_take_profit_pct` / `close_stop_loss_pct` |

**两者都支持多格按格减仓。差别只在「这一格该不该减」的判据。**

---

## 2. 方案 A 的已知问题

参考项目 `_build_grid_thresholds`（`unified_decision_engine.py:1052`）：

```python
t0 = initial * 0.4 if initial > 0 else 0.0
close_thresholds = [t0]
close_thresholds.extend(open_thresholds[:-1])
```

`_check_grid_close`（`:824`）拿 `relative_spread` 和它比。`relative_spread` 来自
`spread_data.spread_pct`，是**毛价差**，没扣任何手续费。

按本项目默认费率：

- 开仓（先挂后吃）：`lighter maker 0.005% + lighter_rh taker 0.035%` = 0.040%
  实际按 `first_limit_venue` 选择后是 `0.012 + 0.005` = **0.017%**
- 平仓同样一次：**0.017%**
- 往返合计：**0.034%**

`T1 = 0.03%`，`T0 = 0.03 × 0.4 = 0.012%`。

**问题**：价差从 0.03% 回落到 0.012% 就触发平仓，此时毛价差收敛了
`0.03 - 0.012 = 0.018%`，而往返手续费要付 0.034%。**净亏约 0.016%。**

这正是本项目当初废弃 T0 的原因，见 `docs/套利流程对比.md` §2：

> 参考项目分段网格用 `T0 = T1 × 0.4` 对**毛价差**做滞后平仓；本项目的 `net`
> 已扣过 fee，套同一个比例会量纲错配且漏算平仓 fee（按默认费率每轮开平净亏
> ~0.016%），所以改成显式的往返净利。

参考项目能接受，可能因为其交易所费率结构不同（部分所有 maker 返佣），
或者靠高频次摊薄。**本项目费率下需要实盘验证是否成立。**

---

## 3. 方案 B 的设计

保留方案 A 的**全部多格机制**（阈值序列、`open_segments`/`keep_segments`
滞后、`actual - target` 按格减仓、拆单），**只把最后一道判据换掉**。

### 3.1 目标持仓仍按格算

完全复用方案 A 的 `target_position_by_spread`：

```
open_segments  = count_segments(relative_spread, open_thresholds)
keep_segments  = min(count_segments(relative_spread, close_thresholds), current_segments)
target_segments = if open_segments > current_segments { open_segments } else { keep_segments }
target = target_segments × base_qty
```

### 3.2 减仓前多一道往返净利闸门

```
close_delta = actual - target
if close_delta <= 0 { return Hold }

// ★ 方案 B 增加的一步
round_trip = pos.entry_net_pct + close_view.exit_net_pct
if round_trip < close_take_profit && round_trip > -close_stop_loss {
    return Hold    // 格子说该减，但往返仍亏 → 继续持有
}

close_qty = split_order_qty(close_delta)
```

即：**格子决定「减几格」，往返净利决定「现在能不能减」。**

### 3.3 止损独立于格子

`round_trip <= -close_stop_loss` 时**全量平**，不受格子约束。
参考统一引擎没有这条；本仓库热路径已关掉。切到方案 B 时再打开。

---

## 4. 切换成本

方案 A 实施后若要切到 B，改动集中在一处：

- `GridEngine::decide_close`：在算出 `close_delta` 之后、返回 `Intent::Close`
  之前，插入 §3.2 的往返净利判断
- 需要 `CloseView`（已有）和 `pos.entry_net_pct`（已有）
- **不需要**改阈值序列、目标持仓、拆单、持仓存储

预估 30 行以内，加 2~3 个单测。

---

## 5. 何时应该切换

实盘出现以下任一情况，考虑切 B：

1. **平仓后统计净亏**：`data/executions.sqlite` 里 `action='close'` 的记录，
   按 `entry_net_pct + exit_net_pct` 统计，若均值为负且接近 -0.016%，
   说明 §2 的理论亏损在实盘兑现了
2. 频繁在 `T0` 附近开平往返，手续费吞掉利润
3. 单轮往返净利为负但格子仍触发减仓（日志里 `grid: closing` 的
   `round_trip_pct` 字段为负）

**建议**：方案 A 上线后，先跑一段 `paper_trading: true`，用
`round_trip_pct` 日志字段统计分布，再决定是否切换。

---

## 6. 关联

- 方案 A 实现：`src/domain/grid.rs`
- 参考实现：`crypto-trading-open-main/core/services/arbitrage_monitor_v2/decision/unified_decision_engine.py:1052`（阈值）、`:911`（目标持仓）、`:684`（平仓判定）
- 历史决策：`docs/套利流程对比.md` §2
