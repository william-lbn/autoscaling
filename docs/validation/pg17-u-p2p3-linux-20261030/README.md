# PG17 P2/P3 Linux精确验收与版本归档

采用结果：root `{'pass': 17, 'fail': 0, 'skip': 0}`；subcase `{'pass': 64, 'fail': 0, 'skip': 0}`；判定`True`。root与子case分别计数，不把81条记录称为81次独立E2E。
15个root采用第二轮，2个自动组合root采用修正夹具后的第三轮；不是同一个纯HEAD快照全部执行。每个采用根及其准确来源见adopted-tests.json，两份实际COPY源码锁在contexts/。

| 编号 | 直接覆盖范围 | root通过/失败/跳过 | 子case通过/失败/跳过 |
|---|---|---|---|
| PG17-U-P3-001 | 策略窗口、冷却、低压客户端约束、资格与采样失效 | 6/0/0 | 28/0/0 |
| PG17-U-P3-002 | 未接受Begin原CAS迟到屏障、原身份和恢复 | 2/0/0 | 7/0/0 |
| PG17-U-P3-003 | 同一正式owner自动Grow/Shrink控制组合及来源失效 | 3/0/0 | 8/0/0 |
| PG17-U-P3-004 | Agent可信自动配置入口与UDS缺省关闭 | 2/0/0 | 4/0/0 |
| PG17-U-P2-001 | 无证明normal下降写前拒绝、旧未知可Observe而不可Replay | 2/0/0 | 3/0/0 |
| PG17-U-P2-002 | 持续聚合保留成本和无证明降限拒绝 | 1/0/0 | 0/0/0 |
| PG17-U-P2-003 | 原关闭命令/持续聚合接线保留其他成本 | 1/0/0 | 14/0/0 |

## 测试分类

- PG17-U：以上为真实Linux Go单元/协议组合，PG/Root/Kube/QMP是明确peer；Root拒绝测试只验证WAL/命令行为，不写物理memory.max。
- PG17-S-P2-004：TestNativeQuiesceConsumptionKeepsRetainedPayload需dsga_native_e2e标签、真实PG/生产Router/JWT与匿名stdin，native race二进制已导出且真实单服务随后通过，不计入17个U根。
- PG17-E：真实Guest/KVM/cgroup/HugeTLB/Host物理伸缩本记录未执行。

## 精确执行与尝试

第二轮：原完整选择，保留2根失败及tagged原生根未执行：

```sh
go test -mod=readonly -json -race -count=1 -p=2 -timeout=15m ./pkg/dsga/policy ./pkg/dsga/journal ./pkg/dsga/owner ./pkg/agent ./pkg/dsga/resourceguard ./pkg/dsga/budget -run '^(TestMemoryPolicyRejectsAmbiguousConfiguration|TestMemoryPolicyWindowsClientsCooldownAndEmergency|TestMemoryPolicyResetsOnChangedOrInterruptedFacts|TestMemoryPolicyRejectsStaleContradictoryAndRepeatedSamples|TestMemoryPolicyExactBoundsAndSliceIsolation|TestMemoryPolicySelectsNearestEligibleAndRestartsAfterNeutralLoad|TestUnacceptedBeginFenceOrdersDelayedCASAndRecoversLostReply|TestUnacceptedBeginFenceRejectsUnexpiredAndDifferentIdentityWithoutWrite|TestProductionAutomaticMemoryPolicyUsesOriginalOwnerAndBudgetChain|TestProductionAutomaticMemoryKeepsOriginalAddressSpaceAcrossGrowShrink|TestAutomaticMemoryPendingRejectsLostSessionBeforeAnyIO|TestDSGAAutomaticPolicyConfigRequiresExplicitTargetsAndOriginalQualification|TestDSGALocalControlConfigKeepsDefaultOffAndRejectsUnsafePaths|TestProductionNormalDecreaseRejectsBeforeIntentAndKernelWrite|TestLegacyPendingNormalDecreaseRemainsObservableWithoutReplay|TestContinuousQuiesceKeepsRetainedCostsAndRejectsUnprovedLowerLimit|TestNativeQuiesceConsumptionKeepsRetainedPayload|TestPGWorkerContinuousQuiesceUsesOriginalCloseAndRetainsAllOtherCosts)$'
```

Linux入口为dev-run.sh autoscaling；原始完整命令、日志、JSON事件、退出码：`/data/neon-build/validation/pg17-u-p2p3-linux-20261030t124057z`。

第三轮：只重跑2个失败根：

```sh
go test -mod=readonly -json -race -count=1 -p=2 -timeout=15m ./pkg/dsga/owner -run '^(TestProductionAutomaticMemoryPolicyUsesOriginalOwnerAndBudgetChain|TestProductionAutomaticMemoryKeepsOriginalAddressSpaceAcrossGrowShrink)$'
```

Linux入口为dev-run.sh autoscaling；原始完整命令、日志、JSON事件、退出码：`/data/neon-build/validation/pg17-u-p2p3-linux-20261030t124458z`。

首轮工具镜像无python3，实际root 0；随后改用已有Go标准库临时元数据helper，不改工具镜像。第二轮2根在业务链前因/private/tmp失败；P1代理按平台选择短Unixsocket目录修复后仅重跑这2根。所有失败/未执行收据保留。

## 源码、工具与机器

- Autoscaling HEAD基线02a1b0c90e58e3dfed2bb9f2c7c44e98a207ca28，功能分支codex/dsga-autoscaling-20261019；含P1并行工作树。源文件hash在实际BuildKit COPY容器内产生，不能仅用HEAD替代已测源码。
- Neon基线3aa86c056ee34bf2c8d4bda1659e897fdbf9e0d9；PG17准确锁79367196a74eef82e67cac917877d2e6271d3e70；U批次不启动原生二进制。
- Go1.24.3 linux/amd64，CGO_ENABLED=1，GOPATH=/go；持久Go module/build缓存不清理。新nonce及-count=1保证实际测试执行。
- 宿主6.8.0-147-generic、16CPU、MemTotal25,212,862,464字节、cgroup2fs；/dev/kvm存在，2MiB/1GiB HugeTLB预留均0。只读事实不代表使用KVM/HugeTLB，不能写成Guest Linux6.12认证。
- BuildKit前端解析曾停顿，显式代理四站检查全部可达；未重启服务或改网络。主代理授权移除共享Go测试模板#syntax标签，未来使用已安装builtin；当前已运行批次加载的是旧模板。准确前后hash见buildkit-frontend.json。
- 当前代理不改业务实现/测试；跨平台fixture和P1业务改动由独占P1代理完成。最终同分支提交、依赖锁和远程SHA由主代理统一。

## P2未完成门槛

当前Prepare、Quiesce预算、resourceexec、Root最终写者和自动eligible五层仍拒绝无证明normal下降。通过的是持续原额度/安全拒绝/旧Unknown观察恢复，不是安全降normal成功。实际文件、行号和hash见scope-audit.json。
仍需可执行持续私有/后台/PG域kernel与IO上界、必要后台及恢复活性、完整同源证据/最终写者/Unknown恢复，以及真实Linux normal cap下降无OOM或PG重启代偿的验证。
固定叶子M/可降父上限只是候选：缺原出生前拓扑、固定物理cap、force-charge及层级计数瞬态父内margin来源，不可启用或签收。即使实现，也只回收固定工作域以上的父预算余量，不宣称动态降低叶子M。

## P3及后续真实服务门槛

可信Agent automaticMemory已进入原ProductionPGWorker/Store.Begin/同一完整控制链；本批验证同normal额度的自动Grow/Shrink、来源失效、迟到CAS和原AS不漂移。Managed Compute停用旧Monitor是防止未认证memory.high/LFC并发写者的设计，没有重新开启。真实配置注入、PG/Guest/VM/Host链仍须后续单机与Helm门槛。

Native二进制：`/data/neon-build/validation/pg17-s-p2-004-native-go-20261030t124719z/export/quiesce-native.test`；SHA256 `a601939398b1537c48e2a8f8f716427a3eaf7853159b8b19644242e435af6b94`。准确编译命令与source hash见native-go-export.json和native-go-source-files.json；仅编译不计通过单服务测试。

## 单服务实际验收补充

PG17-S-P2-004已PASS：Go原生根1通过（0.06s），Rust精确harness 1通过/0失败/0忽略/9过滤（6.97s）；两者是同一次跨语言单服务门禁，不能算两次独立E2E。准确PG17、生产SQL/Router/JWT、匿名stdin、非root镜像USER=nonroot、RUN --network=none只用loopback已复核。源码、Go二进制/镜像digest及文件hash见native-service.json。

Root/Outside仍是明确模型；该根保留真实1个2PC和2MiB DSM，发送1次admission、0次resize，验证低normal被拒绝。没有物理memory.max下降、VM/Host动作，不能关闭P2或称KVM E2E。原始收据`/data/neon-build/validation/pg17-s-p2-004-http-20261030T125326Z`。
