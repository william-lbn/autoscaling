# normal split 拓扑准备组件（2026-10-31）

本批只准备固定 `/sys/fs/cgroup/neon-postgres/{private,arrays}` 两个直属计费叶组，并记录原始创建阶段。它没有生产调用者，不生成 PG Birth、Bootstrap 或 normal/VM 缩容许可，不改父 `memory.max` 或 `state.json`，不改变原五处 normal 降限拒绝。PG17/PG18 的公共预算协议没有变化。

## 原来源与 API

`Guard.PrepareNormalSplitTopology(ctx)`、`ObserveNormalSplitTopology(ctx)`、`ReplayNormalSplitTopology(ctx)` 不接受 policy、路径、PID 或额度参数。生产入口只读固定 ISO `/neonvm/runtime/dsga-normal-split.json`，要求恰好一个原 boot marker `neonvm.dsga.normal_split=split-v1`，严格核 RuntimeDocument 准确字节摘要、原 Source 准确字节摘要、同 Config/原 Bootstrap，以及已确认的新 369 PhysicalOrigin。旧 0ab/552/3b profile 不可收养。

策略 `protocol_version=1`、`scope=normal-split-topology-preparation-only`、`physical_profile_id=neon-linux-6.12.26-dsga-normal-split-v1`；字段 `runtime_document_sha256`、`resource_source_sha256`、`private_memory_max_bytes`(P)、`initial_arrays_memory_max_bytes`(A)、`private_pids_max_tasks`(PGN)。64位数量为 canonical 十进制字符串；P/A 正、有限且64KiB对齐，两者和不溢出且不超过原 Source normal 初额，原 Source PidsMaxTasks 至少 PGN+1。

P 是显式新收费 profile 从出生就存在的固定私有配额；不能从 RSS 选择，不在 shrink 收紧，不能冒称完全兼容旧无限私人分配。上层 work_mem、maintenance_work_mem、max_connections 等若按总 normal/VM 大小自动增长，必须按原 P 合同约束并验证正常容量，这是后续接线门槛。A 是首次额度，后继仅可观察有限当前 A；真实授权/收费/降低 A 必须由后续独立 quota/WAL 链负责。

## 原锁、WAL 与未知恢复

复用原持久 StateDir resource flock，在锁内再次核原 PhysicalOrigin+confirmation 及实际同构建物理事实。首次必须无历史资源 state/Birth/bootstrap、父无直接任务且 recursive populated=0、memory/swap current=0、无子组/未知控制器。先 O_EXCL 写原 intent、fsync 文件和目录，才启用 memory/pids 与 mkdir。每个新子组读取实际 dev/inode 后另 O_EXCL/fsync 原身份 WAL，随后写原 cap。private memory.max=P/pids.max=PGN；arrays 初始 memory.max=A/pids.max=1；两叶 memory.swap.max=0、memory.high=max，均无后代且 root 控制。最终同步读回并 O_EXCL/fsync complete。

失败保留 Unknown。普通 Prepare/Observe 不重放 pending；只有显式 Replay 可延续同 intent、nonce 与已记录 inode 前缀。mkdir 后身份 WAL 前崩溃的现存子组没有原 inode 凭据，Replay 保 Unknown，绝不收养、删除或重建。独立审阅发现已完整可见的原身份文件仍可能上轮 fsync 失败；已修为每次 pending Replay 的 cap 写入前显式重同步该原身份文件与目录，失败继续 Unknown，不能用后继 complete 的 fsync 代替原文件 data 持久性。原身份 WAL 存在但对应组消失亦拒绝。已 complete 后 P/pids/父或两叶身份/控制权限/拓扑漂移拒绝；arrays 额度允许有限变化但不授予收费权。

`memory.current > A` 的实际观察不自动否认来源，也不能把一次 current 或 A 当作严格物理硬界；内核 force-charge、层级计数瞬态和全部 F/I/K 包络仍需完整来源。Observe 永远 `durability_confirmed=false`，本批所有结果 `normal_decrease_permit=false`。

## 实际验证及限制

精确记录见 `catalog.json`。7 个 U 根（含真实并发 flock、O_EXCL/fsync、未知前缀与旧来源拒绝）在 Git410+已核 physical8+本批3文件的隔离快照执行 race、零skip。U 内 cgroup 只由显式模型替代，不能当实际 Guest。首次编译失败与错误 gofmt 工具路径均完整保留于 `first-failures/`；不删除失败求绿。

S 是独立369 KVM Guest 的拓扑/真实ext4 WAL组件：2CPU/1GiB，QMP `query-kvm` enabled/present，准确 kernel/config/notes 与实际 Guest notes 匹配；CGO0静态 Go 测试二进制避免最小Guest动态loader依赖。额外测试源码只在 validation 附加构建 context，不增加第4生产源。实际生产三文件的编译前SHA与 U/当前Linux文件逐项相同。6个 S 编号验证原两叶+父不改、同原幂等、有限 A 观察、P 漂移拒绝、未知后代拒绝、原日志保留不建 Birth。Guest/Test/QEMU 全0，ext4卸载后 fsck0。

S 的 immutable Source、policy 与 Origin issuance 使用明确隔离夹具；只将实际 kernel/cgroup/dev/inode/WAL当真实事实。未执行固定 ISO 生产 loader、Root出生/完整授权、PG业务、完整 F/I/force/K/无新增OOM证明、安全 normal 降限或 Kubernetes E2E。S 不是 E，不算完整单机PG验收。
