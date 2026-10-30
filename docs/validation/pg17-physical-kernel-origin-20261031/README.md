# PG17 物理来源首次捕获组件 Linux U 精确验收（2026-10-31 北京时间）

最终 4 root/18 subcase，race/count=1 全部 PASS，Go1.24.3 linux/amd64，package 57 个文件的实际 container hash 在同目录。首轮 4 root/17 subcase 已 PASS；新增准确引用摘要与拒绝非 producer 规范字节后，仅复测受影响四根。没有失败/skip。

| 编号 | 精确 root | subcase | 结果 |
|---|---|---:|---|
| PG17-U-P2-007 | TestPhysicalKernelOriginCapturesAndKeepsOriginal | 0 | PASS |
| PG17-U-P2-008 | TestPhysicalKernelOriginRejectsSourceAndHistoryDrift | 10 | PASS |
| PG17-U-P2-009 | TestPhysicalKernelOriginPendingRequiresExplicitOriginalReplay | 8 | PASS |
| PG17-U-P2-010 | TestPhysicalKernelOriginSerializesWithOriginalResourceLock | 0 | PASS |

这些用例用真实 Linux flock/private files/fsync 并明确注入模型 kernel/ISO inputs，只属于 U。没有执行真实只读 runtime ISO 的生产 loader，也没有证明跨掉电持久性或原 Guest 真实首次来源封存。已有准确 KVM 观察 S 另有独立收据，不冒充此 Origin S。尚无 provision/HTTP/CLI/预算/normal consumer，五层降限拒绝保持。

API、字段、原 nonce/失败恢复和实际接入缺口见上层交接文档；精确命令、base HEAD/dirty状态、收据路径与准确三个文件 SHA 见 JSON。主执行并行源码若变化，summary 会逐一列 current difference，不混称同一源码版本。
