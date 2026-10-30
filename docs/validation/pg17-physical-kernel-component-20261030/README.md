# PG17 物理 kernel 来源组件精确 Linux U 验收

本档案仅验收三个默认未接线的新文件。采用 5 个 root、28 个 subcase，race/count=1，全部 PASS；其中新增组件 3 root/25 subcase，原最终拒绝与未知恢复回归 2 root/3 subcase。用例是 Go parser/model 单元，不是 6.12.26 Guest 或 KVM 物理证明。

| 编号 | 精确 root | subcase | 分类/结果 |
|---|---|---:|---|
| PG17-U-P2-004 | TestPhysicalKernelObservationParsers | 12 | U/PASS |
| PG17-U-P2-005 | TestPhysicalKernelObservationMatchRejectsDrift | 13 | U/PASS |
| PG17-U-P2-006 | TestPhysicalKernelObservationRejectsSummedCapacityOverflow | 0 | U/PASS |
| 既有降限拒绝回归 | TestProductionNormalDecreaseRejectsBeforeIntentAndKernelWrite | 3 | U/PASS |
| 既有未知记录回归 | TestLegacyPendingNormalDecreaseRemainsObservableWithoutReplay | 0 | U/PASS |
| PG17-S-P2-005 | 新 profile 原 Guest 物理来源与 birth 封存 | — | NotImplemented/NotRun |
| PG17-E-P2-001 | 同 PG 工作域 memory.max 真下降并恢复增长负载 | — | NotImplemented/NotRun |

精确命令及实际 BuildKit/run 路径见 commands.json/summary.json。Go 实测版本 1.24.3；1.27.1 仅用于 gofmt，不混淆工具版本。第一轮新增 fixture 的 boot UUID 不规范导致三根失败，收据原样保留；只修 fixture 后重跑新增三根。首次与修 fixture 的第二上下文仅 fixture 文件 hash 不同；最终按准确 artifact 完整 release（不接受同前缀其他 build）、DMA 计入及中文注释修订后，只重跑受影响三根。采用首轮两个原拒绝根与最终三根；二者 package 内 46 文件的 43 个非本子任务文件逐字相同，差异仅本组三文件且没有生产消费者。三个历史上下文和失败均保留，最终现文件全部与第三上下文 hash 一致。

source/notes/cmdline/zone 的观察不是 original-birth seal。没有生产消费者，未改变 RootAggregate 或五层拒绝。真实原启动封存、全部在途成本、FS/AS-R/native filter、目标 MOVABLE 容量/水位门槛完成前，P2 继续不合格。
