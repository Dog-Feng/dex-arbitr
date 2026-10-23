# 恢复与预算约束

本次修复针对 main `742c99a` 复审确认的10条路径。只修改代码并运行离线测试/编译；不启动 arbd、scanner、selfcheck、HTTP监控或任何真实交易。

## 未决执行与外部干预

原IOC和补救单的clientID、实际请求量、累计成交与已计入贡献必须留在各自Result中。仓位增量与成交不一致时，系统先查询所有已保留订单的显式终态；三次不一致只是切换恢复阶段的条件，不能证明订单终结。

原单全部终态后，新的zero-target平仓使用独立Result。部分成交、未知结果、取消、缺行情或查询错误都会保留pending继续恢复。新平仓同样要求显式终态，不能在已知外部干预场景仅凭仓位归零推断新单完成并释放名额。这样迟到的reduce-only也不会影响释放后由下一actor建立的新仓。

本进程原成交与新平仓成交的费用/量合并一次；迟到的真实终态可替换原位置推断贡献，包括终态零成交。外部成交没有可靠价量时，确认两腿已平可以退账本，但PnL保持未知，不声称零损失。

## checkpoint期间的风险退出

跨进程仍不重放旧请求。旧版仅有Unresolved布尔值的状态继续兼容；正常恢复每15秒核对，默认连续15分钟一致后解除。该等待覆盖当前锁定SDK默认599秒签名期限，依赖正常时钟及交易所遵守有效期。

发生drain或应急请求时，先保存 `checkpoint_exit=true, unresolved=true`，再reduce-only减少已确认的当前仓位。即使当前已平、步骤为空，仍保持checkpoint、占用slot/预算且不认证Flat/Drained。隔离期内再次出现旧单迟到仓位会继续减仓；本进程新单未决必须先恢复。只有隔离期结束、新单明确终态、当前两腿均平，才持久化解除标记并释放。再次重启会重新开始完整隔离期，避免丢失的新请求早于其有效期释放。

PnL不确定标记为sticky：不会因日切或所有步骤退出自动解除。该标记仅作审计痕迹，不再参与开仓闸门（`daily_volume_target` / `daily_cost_budget` 已移除）；操作员核账后应在停止写入进程的情况下处理状态。不要通过删除未决标记强行恢复开仓。

## auto预算与定容

所有旧仓在任何恢复actor启动前先计入占用，包括尚不能完成drain的旧仓。可交易actor的固定预占为：

```text
sum(max(每个历史步骤左右腿名义额, 当前单步额度))
+ max(0, max_steps - 历史步骤数) × 当前单步额度
```

这既覆盖旧大仓与未来新小仓，也覆盖先关闭便宜旧步、再按当前额度替换的路径。未获足够未来额度的恢复对进入drain，直到确认退出才释放原占用；不会把正在退出当作额度已腾出。多个actor共享的固定分配在picker单owner中完成，不在并发开仓时临时借用其他actor的名额。

定容按两腿较高ask和可用深度限制数量。定容失败只禁止新增Open，现有步骤仍可Close。实际开仓按冻结的双腿IOC价格计划校验单步名义额度；为让价预留空间后按共同数量精度缩量，不足最小数量/名义才拒绝且双腿均不发送。恢复使用最终实际请求量，不能按缩量前的策略数量查单。

这些约束覆盖已知账本/查询到的入场基准占用及本进程计划开仓。签名身份未知的旧请求不能被精确计量；启动遗留checkpoint全部完成持久化恢复之前，auto全账户暂停新增Open和新actor晋升，已有仓位的Close/drain/应急退出继续执行。市场变动、卖出更优成交价及浮动名义额由账户风险轮询另行约束，不能把限价预算当作所有未来市值的绝对保证。

## 统计与数值

单槽SMA的连续采样不会因槽替换重置预热起点；按实际采样间隔识别连续性，允许半个间隔的调度抖动。漏掉一个完整tick或整窗中断仍重新预热。config/backtest同步拒绝桶跨度不足的参数，默认2h窗口/1h预热不变。

价格定向舍入保留合法原生单位，同时不放宽真正额外精度的边界。数量Floor不增加输入；原生仓位相减、原生价格加整数tick等运算先恢复其已知量格，避免二进制算术误差导致少一单位。VWAP等非原生量格结果继续保守舍入。

## 回归映射

| 复审项 | 验证 |
|---|---|
| N1 原IOC未知就认证Flat | `TestInterventionWaitsForUnknownOriginalBeforeFlatten` |
| N2 新flatten结果丢失 | `TestInterventionRetainsPartialUnknownFlatten`；新full-inferred订单终态回归 |
| N3 全旧仓未计入准入 | `TestAutoRecoveryCountsEveryRetainedPositionBeforeOpening` |
| N4 旧仓与未来步骤预占 | `TestAutoRecoveryReservesOldExposureAndNewSteps`；`TestRecoveryReservationCoversReplacementOfCheapHistoricalStep` |
| N5 定容失败用旧大额 | `TestAutoSizingFailureBlocksOpenButPreservesClose` |
| N6 checkpoint挡住应急 | `TestCheckpointRunCanReduceBeforeGraceWithoutCertifyingFlat`；迟到仓位/重启/持久化失败回归 |
| N7 已flat旧错误卡账本 | `TestInterventionAlreadyFlatClearsLedgerAfterTerminalOrders` |
| N8 外部零成交漏未知PnL | `TestUnwindExternalZeroFillMarksPnLUncertain` |
| N9 单槽预热/回放校验 | `TestSingleSlotEstimatorWarmsForAllStrategySelections`；`TestReplaySingleSlotWarmupAndGap` |
| N10 原生价格移动一tick | `TestZeroTicksPreservesNativeDecisionPrice`；domain/lighter原生单位与离线签名回归 |
| N11 空闲腿迟到成交卡死 | `TestFlattenRecoversLateFillOnIdleLeg`；flatten按实际仓位重新收敛 |
| N12 干预完成后永久暂停 | `TestInterventionWaitsForUnknownOriginalBeforeFlatten`；成功后恢复WARMUP |
| N13 低于最小额残量卡死 | `TestSubMinResidualClosesWithMinimumSizedOrder`；零目标按最小额 reduce-only 封顶清残 |
| N14 开仓回滚破坏旧仓 | `TestOpenRollbackRestoresPreOpenPositions`；回滚目标是开仓前仓位 |

验证使用内存venue、假行情、离线签名和临时时钟。测试通过不代表已经实盘验证远端延迟、撮合或所有错误码；没有调用真实交易接口。

## 迟到成交与干预收尾（2026-10-20）

HL 的订单状态可能早于真实成交落位一两个区块：一个已被判定终态的旧订单，其成交可能
稍后才体现在仓位上，使本批次“未下单的腿”（req qty=0）出现仓位变化，触发
`ErrUnexpectedPosition` 并让整个 pair 暂停。

- `flatten` 确认批次失败时，若错误是 `ErrUnexpectedPosition`、本批次所有已下单的腿
  均已显式终态、且变动只发生在未下单的空闲腿上，则把该批次记入 `unconfirmed` 并按
  最新仓位重新计算减仓量继续收敛，连续最多 3 次；超过仍不一致才上报引擎走外部干预。
  已下单腿的严格校验不变，未决/未知订单永远不会被丢弃（`TestFlattenRecoversLateFillOnIdleLeg`）。
- 外部干预抹平成功、两腿确认归零后，模式恢复 `WARMUP`（估计器重新预热后回
  `RUNNING`），不再永久暂停；PnL 保持 uncertain。干预长期未决时每 10 次尝试输出
  一条 WARN，避免静默空转（`TestInterventionWaitsForUnknownOriginalBeforeFlatten`）。
- 低于单腿最小可交易量（如 HL $10≈0.0051）的残量不再硬报错：目标是 0 时按最小额
  发 reduce-only 单，由成交封顶吃掉残量（连续被拒超过 3 次才上报）；目标非 0 时返回
  `ErrSubMinResidual`。该错误与 `ErrUnexpectedPosition` 一样累计 stall，三次后进入
  外部干预，不再无限静默重试（`TestSubMinResidualClosesWithMinimumSizedOrder`、
  `TestSubMinResidualStallsEscalateToIntervention`）。
- 开仓无法补齐/对齐时，回滚目标是“本次开仓前的仓位”而不是 0，既有步骤的仓位不再
  被一起抹掉；`ResolveOutstanding` 恢复路径同样按 `opening` 标记执行该回滚
  （`TestOpenRollbackRestoresPreOpenPositions`、`TestResolveOutstandingRollsBackOpeningToBefore`）。
  平仓路径保持原有零目标语义不变。

## 平仓越界与监控修正（2026-10-23）

- 平仓成交溢出选中步骤、按 FIFO 顺延消费其他步骤时，每个被整步清掉的步骤都发一条
  `step_closed` 事件，`closes` 计数与账本实际清掉的步骤数一致
  （`TestCloseSpillRecordsClosedEventForEveryConsumedStep`）。顺延仍然记录一条 error
  作为账本偏离的预警，不改变 FIFO 消化语义。
- 低于最小额残量的策略以 review 修复（405c371）为准：`ErrSubMinResidual` 保持
  PAUSED、告警一次并按 `subMinRetryInterval` 退避重试，不再升级为外部干预，也
  不在非零目标上尝试精确量下单；应急/退出请求绕过退避
  （`TestSubMinResidualStaysPausedWithoutIntervention`、`TestSubMinResidualRetryBackoff`）。
- 监控层：全局聚合桶在关闭时不再执行 pair 级规则，消除 metrics 里的空 pair
  幽灵条目和重复告警（`TestAggregateBucketDoesNotCreatePhantomPair`）；
  `latency_p95_ms` 阈值 600 → 1200，适配 HL 腿 750-870ms 的常态延迟。

## 本次离线验证

2026-10-15：`go test -race -count=1 -p4 ./...` 全部通过，19个有测试包、284个顶层测试，含子用例720项；没有失败或跳过测试用例，另2个包无测试文件。`go vet -p4 ./...`、五个命令的 `go build -p4 ./cmd/...`、gofmt和git diff空白检查均通过。没有执行应用main或实际交易。
