# 交易安全与生命周期修复说明

本轮修复对应 2026-10-14 对提交 `79fc0c484e21e76c22cf5a5ab82496388349bb80` 的代码审查编号 F01–F26。下表连接已确认的问题、当前实现和回归测试，便于检查变更范围。验证使用单元测试、内存交易所替身和可控传输；没有启动项目、HTTP 服务、scanner 或真实 selfcheck，也没有提交真实交易。测试不能代替交易所线上协议、成交和清算行为的验证。

## 修复对应表

测试名称可在对应包的 `*_test.go` 中查找；同一测试可能覆盖多个问题。

| 编号 | 原问题 | 当前实现 | 关键回归测试 |
|---|---|---|---|
| F01 | 应急退出误把两腿净额零视为全部平仓，删除仍持有的步骤。 | [executor.go](../internal/execution/executor.go) 为应急退出设置左右腿分别为零的目标，按各腿原生精度发送 reduce-only 保护市价单；[settlement.go](../internal/engine/settlement.go) 仅在两腿仓位及订单结果均确认后清空账本。 | `execution.TestEmergencyFlattensEachLeg`；`engine.TestSettlementDrainRequiresEachLegFlat` |
| F02 | 取消或仓位查询失败被当成零，虚构平仓成交。 | 执行结果显式区分 `PositionsKnown`、`SettlementKnown` 和错误。未确认执行保存为 pending，actor 暂停普通交易；不会因缺数据删除步骤。 | `execution.TestCanceledOrFailedPositionReadsNeverInventCloseFills`；`engine.TestSettlementUnknownOrCanceledCannotMutateLedger` |
| F03 | 双腿精度不同，残差在粗精度腿变成零下单量，补救无法完成。 | 补救计算共同可持有的对冲量，分别使用原生精度减仓；无法保留的新增零碎仓位退回原目标，而非重复发送零数量。 | `execution.TestMixedPrecisionPartialReducesToCommonHedge` |
| F04 | 补救成交缺少数量、价格，零价格产生虚假的巨大盈利。 | 保留原订单与补救批次，在恢复时先确认旧订单；只在充分仓位证据下估计完整成交，并使用已发送订单的保护价格，标记 `PriceEstimated`，不得用零价入账。未知零成交或部分成交仍未决。 | `execution.TestUnknownFullRescueIsRecordedWithEstimatedPrice`；`TestPartialUnknownRescueRecoveryDoesNotDuplicateOrdersOrAccounting`；`TestRecoveryPriceEstimateUsesOriginalOrderBound` |
| F05 | 入场、出场使用不同分母的 bps 相加，反转真实盈亏。 | [strategy.go](../internal/strategy/strategy.go) 按左右腿开平价格差计算 USD，再以固定入场参考价格转换 bps；预计平仓费用使用实际名义金额和存储的入场费用。账本使用相同现金流基准。 | `strategy.TestProfitUsesFixedEntryCashflows`；`TestCloseUsesStoredEntryFeesAcrossFeeTierChanges`；`engine.TestSettlementUsesCashFlowsWhenPriceLevelChanges` |
| F06 | 失败开仓回滚漏计手续费，有利回滚被绝对值记成亏损。 | 所有普通和补救成交进入名义量、手续费及每日 PnL；回滚价差保留方向。部分平仓分配已存入场费用并累积 `RealizedUSD`，最终关闭事件不重复计入 PnL。 | `engine.TestSettlementRollbackKeepsSignedProfitAndEveryFee`；`TestSettlementPartialCloseAndRescueWeightedPrice`；`monitor.TestPnLIncludesFeesAndPartialSettlementOnce` |
| F07 | auto 停用或替换 actor 后仍继续开仓。 | [auto.go](../internal/engine/auto.go) 发出 `Drain()`。actor 阻止新开仓，将进行中的执行交回恢复路径，确认旧订单后完成减仓，最后等待退出。 | `engine.TestAutoFailureGateDrainsWithoutAdaptivePricingOrDwell`；`TestAutoReplacementRetainsSlotAndBudgetUntilActorExits` |
| F08 | auto 重启只加载新入选交易对，遗留仓位无人管理。 | 选币前列出保存状态并恢复全部持仓和未决 checkpoint，包括白名单外的已匹配市场。缺少可恢复市场、重复使用市场或无账本仓位时启动失败；恢复的仓位先核对并接受风险监控。 | `engine.TestAutoRestoresOpenPairsBeforeWarmupAndSelection`；`TestAutoWhitelistRetainsMatchedRecoveryCandidates`；`TestAutoRejectsUnrecoverableExposure`；`TestAutoRestoresUnresolvedExecutionWithEmptyLedger` |
| F09 | 名义额度上限先返回，遮蔽清算触发。 | [margin.go](../internal/engine/margin.go) 先评估已知腿的清算 Trigger，再处理名义额度和缺失数据的限开仓条件。 | `engine.TestMarginTriggerPrecedesAccountCapAndMissingPeer` |
| F10 | 一腿刷新失败，丢掉另一腿成功返回的危险清算数据。 | 左右腿独立缓存成功数据与读取时间；失败腿保留旧缓存并接受过期判断，成功腿的新风险仍生效。风险 watcher 独立于执行等待。 | `engine.TestMarginRefreshRetainsSuccessfulLegWhenPeerFails`；`TestMarginExpiredCacheBlocksAdmission`；`TestRestoredActorProtectsMarginBeforeEstimatorWarmup` |
| F11 | WS 缺口或断线后，上游继续使用旧盘口。 | [stream.go](../internal/exchange/lighter/stream.go) 在序号缺口、重置、断线时发布无效盘口覆盖旧快照；缺少有效 nonce 的消息不刷新旧数据年龄。新订阅者保留原快照时间。 | `lighter.TestBookGapAndResetInvalidateQueuedSnapshot`；`TestWSDisconnectInvalidatesBookBeforeReconnect`；`TestBookMissingNonceDoesNotRefreshOldSnapshot` |
| F12 | sendTx 缺少业务 code 被当作确定拒绝，跳过结果确认。 | [rest.go](../internal/exchange/lighter/rest.go) 要求完整成功响应；不完整响应、读取失败和无法确认无成交的远端错误保留未知结果并使 nonce 缓存失效。本地确定未发送的校验失败仍可明确拒绝。 | `lighter.TestSendTxRequiresCompleteSuccess`；`TestSendTxReadFailureKeepsUnknown`；`TestSubmitRemoteErrorStaysUnknownAndResyncsNonce` |
| F13 | 首次开仓进行中，计数仍零，`Flat()` 返回真。 | `Flat()` 同时要求未执行、结算已确认、两腿原生精度下分别零仓位、步骤数零。完成结算/退出标记须在成功保存后设置；保存失败暂停、保持非 Flat 并向上返回。picker 只在 `Drained()` 后取消 actor。 | `engine.TestAutoReplacementRetainsSlotAndBudgetUntilActorExits`；`TestSettlementWriteFailureCannotAppearIdle`；`TestSettlementDrainRequiresEachLegFlat` |
| F14 | 旧 actor 未退出便晋升新 actor，突破 TopY。 | draining/stopping actor 持续占用 slot 和额度，直到收到退出结果。恢复历史仓位可能暂时超过 TopY，多余 actor 在启动前即进入 drain，只保留风控与退出能力；期间不新增晋升。 | `engine.TestAutoReplacementRetainsSlotAndBudgetUntilActorExits`；`TestAutoRestoresExcessInventoryWithoutExceedingTradingCapacity` |
| F15 | 多 venue 同 base 相互覆盖，选错组合或重复管理市场。 | candidate、active 和监控按完整交易对/venue 组合索引；排序确定，晋升和恢复都检查 `venue + marketID` 是否重叠。 | `engine.TestAutoSelectsBestVenueCombination`；`TestAutoPreventsSharedMarketActors` |
| F16 | 关闭自适应调价同时关闭 auto 失败率门控。 | actor 独立维护失败滚动窗口；adaptive 只控制调价。picker 读取 actor 的实际失败率，禁用时直接 drain，不等待普通替换 dwell。 | `engine.TestAutoFailureGateDrainsWithoutAdaptivePricingOrDwell`；`adaptive.TestRateIncludesWarmupAndAdjustsEachRollingOutcome` |
| F17 | actor 重建或同日重启丢失每日预算。 | [store.go](../internal/store/store.go) 保存 `Day`、`VolumeUSD`、`PnLUSD`、`PnLUncertain`；actor 加载并按 UTC 日期管理预算。跨午夜的 pending 成交按确认结算的 UTC 日入账。 | `store.TestSettlementAndUncertaintySurviveReload`；`engine.TestSettlementCompletesAgainstCurrentUTCDay`；`TestSettlementReviewCompletionDayBudget` |
| F18 | manual 吞掉 actor 错误，初始化失败留下运行中的 actor。 | [run.go](../internal/engine/run.go) 在启动前完成全部配置/市场/actor 构造；运行失败向上返回，取消并等待其余 actor。auto collector 初始化或运行失败也返回错误并清理。 | `engine.TestManualReturnsActorStartupFailure`；`TestManualStartupFailureCancelsOtherActors`；`TestManualValidatesAllPairsBeforeStartingActors`；`TestAutoReturnsCollectorStartupFailure` |
| F19 | 断流后 SMA、波动和评分永不过期。 | 均值 `StatsAt`、波动 `ReadyAt/VolAt`、健康 `SnapshotAt` 根据读取时钟判断新鲜度；盘口还校验 venue、marketID 和未来时间，过期候选不能维持 ready。 | `estimator.TestStatsAtExpiresWithoutUpdateAndDoesNotMutate`；`engine.TestCandidateExpiredDataCannotRemainReady`；`scan.TestHealthExpiresWithoutMutatingTracker`；`scanner.TestBoardExpiresLastCallback` |
| F20 | 按样本个数假设固定时间，scanner 把 500ms 变成 5s。 | [volatility.go](../internal/volatility/volatility.go) 使用真实时间戳和时间窗口；超过 1.5 倍采样间隔的缺样重置预热。scanner 按波动配置的 cadence 采样，显示频率独立。 | `volatility.TestRealizedVolUsesActualTimeWindow`；`TestSamplingGapResetsReturnHistory`；`TestReadyVolSeedsFullHorizonVariance`；`scan.TestWatchSamplesShortHorizonAndClearsOnExit` |
| F21 | Health 翻转/深度全期累积，首次上涨也算翻转。 | [health.go](../internal/scan/health.go) 按时间窗口计算深度、稳定性与每分钟翻转数；仅连续两个非零、方向相反的变化形成翻转，平台保留前方向。 | `scan.TestHealthUsesRollingDepthAndReversions`；`TestHealthMonotoneAndPlateauDoNotCreateReversions` |
| F22 | 全局档位被最后一对覆盖、方向符号变负数、Mode 未报告。 | [monitor.go](../internal/monitor/monitor.go) 累加每对绝对步骤数并记录运行模式；生命周期计数与 `KindPnL` 增量分离。告警在释放锁后调用。 | `monitor.TestStatusSumsAbsoluteStepsAndPreservesModes`；`TestPnLIncludesFeesAndPartialSettlementOnce`；`TestAlertCallbackCanReadAndRecord` |
| F23 | 负时间间隔、重名 venue、重复 pair 可启动。 | [config.go](../internal/config/config.go) 严格解码 YAML，拒绝未知/重复键、多文档、非有限数值、无效时间和重复/共享市场；默认值在解码前设置，保留有意义的显式零。 | `config.TestLoadStrictValidation`；`TestLoadVenueAndPairIdentity`；`TestLoadDefaultsAndExplicitZero` |
| F24 | Marshal 失败覆盖旧账本，落盘错误仅记录日志。 | 保存前验证及序列化；同 key 写入串行，临时文件 Sync、Rename、目录 Sync。发送前写未决 checkpoint；保存失败暂停并返回运行错误。采样写入/同步失败也传播。 | `store.TestSavePreservesPreviousStateOnInvalidInput`；`TestRejectInvalidSettlementBeforeReplacingState`；`TestSaveWriteFailureCleansTemporaryFile`；`TestStoreCloseAndReopen`；`TestSamplePropagatesWriteAndCloseErrors` |
| F25 | selfcheck 前置失败仍 live，有底仓或错误注入遗留仓位。 | [live.go](../cmd/selfcheck/live.go) 要求两腿零基线；任一前置检查失败即停止。rescue 注入是明确的本地拒绝；每次退出先恢复旧订单结果，再用独立有时限的清理 context 减仓并复核两腿。只读签名使用实际 venue/链配置。 | `selfcheck.TestRunChecksFailureStopsBeforeTrading`；`TestLiveFlowRequiresZeroBaseline`；`TestLiveRoundTripAndRescueAreFlat`；`TestLiveUnknownOpeningOutcomeRecoveredWithoutResubmit`；`TestReadOnlySigningUsesActualVenueEphemeralSigner` |
| F26 | 数量校验放过额外精度，空匹配后缀导致死循环。 | [matcher.go](../internal/symbols/matcher.go) 只容忍浮点表示误差，检查有限数值和精度；最小数量向上对齐。Normalize 忽略空后缀，配置入口也拒绝空后缀。 | `symbols.TestValidQtyAllowsRepresentationErrorOnly`；`TestValidQtyRejectsUntradableInputs`；`TestNormalizeIgnoresEmptySuffixes`；`TestPairMinimumDoesNotRoundDownFractionOfStep` |
| F27 | 重启后未决 checkpoint 与账本一致时永久暂停，没有自动恢复路径。 | [pair.go](../internal/engine/pair.go) 仓位与账本一致时先通过核对，但保持 PAUSED 并进入稳定性观察：每 5s 重新核对，连续 30s（均可配置）一致才清除 checkpoint，期间不认证 Flat/Drained；清除后每日 PnL 标记不确定并持久化。仓位出现分歧仍暂停待人工处理。 | `engine.TestCheckpointClearsAfterStablePositionMatch`；`TestCheckpointKeepsPauseWhilePositionsDiverge` |
| F28 | 恢复的历史持仓不占用账户名义预算，总额闸门只约束晋升。 | [auto.go](../internal/engine/auto.go) 恢复时按 `max(pair_budget, 实际名义)` 预留预算并累计；超出 `auto.max_total_notional` 的恢复对直接进入 drain，预算内且有空位才恢复交易。 | `engine.TestRestoredExposureRespectsAccountNotionalBudget` |
| F29 | 应急/降出平仓用账本数量与成交量强等，精度尘埃使退账本卡在重试循环。 | [settlement.go](../internal/engine/settlement.go) 按每条腿实际平仓量（直接成交+补救）计算均价与 PnL，账本数量与成交量超过原生精度时仅标记不确定，不再报错卡死。 | `engine.TestUnwindAccountingToleratesFinePrecisionDust` |
| F30 | `volume.min_inventory >= max_steps` 使库存地板永不可平，静默停止 churn。 | [config.go](../internal/config/config.go) 校验 volume 策略下 `min_inventory` 必须小于对应 `max_steps`（manual 逐对、auto 按 `auto.max_steps`），否则拒绝启动。 | `config.TestLoadVenueAndPairIdentity`（`volume floor at pair/auto max steps`） |

除上述映射外，适配器回归还覆盖 WS 在 Dial/Close 竞态下关闭连接、订阅取消与通道关闭、订单数值缩放边界、完整仓位响应、订单 marketID 隔离和查询历史范围不足。Tracker 覆盖流关闭、关闭时等待消费者、拥塞时保留终态与晚到消息。这些测试使用本地替身，不证明远端 API 在所有错误码下的行为。

## 重启、升级和持仓恢复边界

- 本轮仅修改代码和执行测试/编译，不会自动启动 arbd、scanner、selfcheck、监控端口或 systemd。README 中的运行/部署命令需要操作者明确选择执行。
- `state_dir` 应由单个进程独占使用。本版本已移除目录级进程锁（Windows 兼容性），并发双进程写入会互相覆盖状态；paper 与 live 状态仍分别使用原有文件 key。升级前保留完整状态目录和配置，不能把空步骤文件视为没有未决订单。
- 每次可能发送实盘订单前先保存 `Unresolved=true`。同一进程内保留原订单和补救批次，后续恢复确认后才结算，避免重复下单及重复记账。
- 即时 drain/应急退出按有限批次尝试。暂时没有有效盘口或已确认批次仍未达到目标时，结果保留在 pending，由 actor 后续周期继续恢复；未确认两腿平仓前仍占用 slot 和额度，不把一次尝试结束当成退出完成。
- **跨进程恢复：checkpoint 未序列化原请求的精确 clientID、订单参数及私有执行批次，因此新进程不重放旧订单。** 启动核对时若左右腿仓位与账本一致，actor 保持 `PAUSED` 进入稳定性观察：每 `CheckpointCheckEvery`（默认 5s）重新核对仓位，连续 `CheckpointGrace`（默认 30s）一致后自动清除 checkpoint、把当日 PnL 标记为不确定并持久化，随后回到 WARMUP。观察期内仓位出现任何分歧或查询失败都会保持暂停并告警，需人工核实交易所订单终态后再处理。所有本系统订单均为 IOC，观察窗口用于覆盖"已提交交易迟到落地"的窗口；若交易所实现提供订单终态查询，可在此基础上增加按 clientID 的显式复核。
- 实盘启动需要左右腿仓位查询成功、相互对冲并分别符合账本；不符合时保持暂停。auto 在候选预热/选币之前恢复历史持仓；不能匹配的已持仓市场或无可恢复账本的账户仓位使启动报错。
- 旧账本缺少 `OpenL/OpenR` 时，没有可靠的入场 PnL 基准，普通策略保持暂停。已成功核对的仓位仍允许显式 drain 和风险应急退出；完成退出必须确认两腿分别平仓且旧订单已完成确认，不会把“净额为零”当作退出。
- 旧账本的 `OriginalQty=0` 表示无法确认历史入场费用或此前部分成交的分配；即使价格基准存在，也标记为估计。缺少入场价格的退出不声称已知毛利；使用估计价格的普通/补救成交也标记为估计，并持久化每日 `PnLUncertain`。
- 使用 volume 策略并配置 `volume.daily_cost_budget > 0` 时，当日 PnL 不确定会阻止新开仓，减仓和应急退出仍可执行。预算按 UTC 日期更新；延迟结算记在确认日。历史估计持仓在后续日期结算时仍会使该日预算不确定，不能把估计盈利当成可靠的预算余额。
- auto 退出 actor 前保留它占用的 slot 和额度，只有 `Drained()` 成立并收到 actor 退出结果才释放。SIGTERM 负责取消、保存和等待消费者，不承诺主动清空现有持仓；若执行尚未确认，会留下上述暂停 checkpoint。

## 采样、回测和验证范围

新采样仍是逐行逗号分隔文本，文件后缀沿用 `.jsonl`，并非 JSON。十五列顺序为：

```text
ts,midBps,lsBps,slBps,Lbid,Lask,Rbid,Rask,refMid,LbidQty,LaskQty,RbidQty,RaskQty,LTs,RTs
```

时间列单位为 Unix 纳秒。价格必须有限且为正，深度可以为零；保留左右盘口各自的接收时间。写入、同步、关闭错误不会被当成采样成功。对应 `store.TestSampleQuotePersistsBooksAndZeroDepth`、`TestSampleRejectsInvalidQuoteWithoutWriting` 和 `engine.TestCandidateCollectorWritesCompleteQuoteAndPropagatesErrors`。

回测支持原四列和新十五列，禁止同一文件混用两种价格模型。旧四列没有实际 USD 价格、深度与盘口时间，回测明确显示 `approximate=true`，使用固定合成参考价与假设深度，USD 结果不代表历史真实成交。新格式允许按实价计算现金流和检查深度/新鲜度，但成交仍假设理想一档全量成交，不包含 IOC 失败、补救、执行延迟/随机等待、资金费或清算风控。期末库存保留并单独报告 MTM，不强制平仓制造已实现利润。

回测默认拒绝采样缺口；显式选择 `-allow_gaps` 后会报告缺口并重置采样统计，不能把长间隔误算成短期收益。校准工具也排除跨越长缺口的增量，并拒绝损坏、非有限和乱序数据。对应 `backtest.TestReplayChecksInputsAndGaps`、`TestReplayRejectsMixedPriceModels`、`TestReplayUsesDepthAndMarksWithoutForcedClose`、`volcal.TestIncrementsExcludeLongGaps` 以及 `backtest.TestRunReportsApproximateInventoryAndAssumptions`（命令包）。

2026-10-15 完成集成验证及 [四种策略/选币组合的补充检查](strategy-selection-flow.md)（Go 1.23.4）：18 个测试包的竞态检查通过，233 个顶层测试、含子用例共 595 项通过，无失败或跳过的测试用例；另外 3 个包没有测试文件。`go vet ./...`、五个命令的编译、gofmt 和差异格式检查通过。依赖声明和锁文件没有变更。即时救援还覆盖零成交、部分成交、缺少全部/单腿盘口时返回未完成结果，并在后续恢复中保留累计成交。

可以使用以下命令复核代码，这些命令不启动项目或提交交易：

```bash
go test -race ./...
go vet ./...
go build ./cmd/...
```

这里记录的是已确认问题的修复与测试边界，不表示所有潜在缺陷都已消除。价差决策仍主要使用一档信息，动态 IOC 的允许价格范围可能消耗信号利润；这轮修复没有加入完整的双腿 VWAP、执行延迟尾部成本或跨报价币汇率模型。
