# VM尚未派发时首次取消：Mac阶段验收

本阶段沿`codex/dsga-autoscaling-20261019`演进，后由Neon既有功能分支准确绑定。
源码提交、逐文件SHA-256、逐用例结果与日志摘要见
[机器收据](DSGA-VM未派发取消闭环-Mac收据-20261029.json)。本批不改PG17内核、
Rust或独立控制面，不新增Linux运行/物理回收结论；PG17冻结基线不解冻。

准确合同/WAL/Node提交`33b762100b5b96b405aa56c1247105fa51537440`，
生产接线与组合提交`91b84273deea8d6cc87467f6a43b295839d12a5d`。
共19个Go文件（6个测试文件）；文档归档提交不改变上述源码或测试采用范围。

## 关闭的具体缺口

首次服务已按完整Initial开放。随后Grow目标为1GiB，Prepare和Host峰值已经
完成，但VM步骤仍为Planned，原Runner从未有内存命令，实际仍512MiB。此前
恢复要求一份真实Resize终态来源；本场景没有该回执，既不能伪造NoEffect，
也不能改小原目标、丢弃Host峰值或永远借保守持有冒充已经结清。

新增`InitialVMSources`和`ReconciledCapacity v3`表达独立原启动来源：
固定原PG实例、Birth UID、Birth指定的State UID、完整Runner身份摘要和实际
启动容量，没有Resize Request/命令SHA/Receipt UID。v3与NativeCapacity互斥，
必须同时永久保留完整原Initial作为Outside资格和Root/PG/Host来源依据。

生产`vmguard.Reader`重新认证原Birth/State、原Lease UID、当前owner floor和
真实requested/actual；Pending、Last、HighWater、LastPublished必须全部为空。
仅无Pending或当前数字相同不够。旧命令已经完成、已取消或尚可重放时都不能
回退成启动来源；正常原命令仍走严格的普通/取消回执读者。

`admissionexec`在原VM仍Planned时选择该来源，保持原完整Prepare、Host峰值、
Root摘要和PG代次。原Cleanup Start回复未知不发送动作，重建SDK后先恢复原
字节；Record的同一CAS才用完整实际预算替换InitialHeld。原资格暂缺或进入
释放时拒绝推进，恢复准确原来源后继续，不能按同名新对象重新发现。

Host的`Settlement`另存可比较的独立启动来源，不填一份空或假的Runner回执。
真实Publisher核原VM关闭链、Host/资格/Pod/Node与原Runner，再以Node UID/RV
结清为实际512MiB并保留Host开销和审计，原请求的1GiB目标不改写。新来源要求
Node账本v8；旧v7读者拒绝，后继普通结清不降版本。该结清只释放原峰值承诺，
不是证明512MiB物理页已归还；本场景根本没有发送QMP resize。

## 合同兼容与边界

- 原capacity v1/v2保留原native来源与JSON形状；v3省略`native_vm`和旧Outside
  操作字段。严格解码通过“协议版本＋来源互斥＋完整结构”拒绝遗漏与混用，
  不因字段可省略而允许旧合同缺回执。重复/未知/null/错误来源仍拒绝。
- VM Model保持v9；新capacity版本和严格字段使旧读者不能误读。只有完整
  Initial合同允许Cleanup使用专用16KiB边界，普通命令仍为8KiB；注解总量、
  Observation和JSON深度/节点上限不放宽。
- Node v8的新启动来源按值比较；旧普通水位不输出零值新字段。Through关闭
  水位仍单调、不能重新授予旧请求。新协议上线前必须全链路升级；持有新账本
  时不能将旧程序回滚后继续调度。本轮功能仍默认关闭。
- PG17源码、gitlink和两份版本锁保持`b078736a4cfdfccce69914feefb5b5e56560c1e8`。
  没有改未变PG/Rust/GuestReady逻辑，不重跑相同完整矩阵。

## Mac采用验证

最终执行和计数以机器收据为准，不将父/子记录累加成独立E2E。
最终`-race`生产/Node组合日志为10顶层、20条含父子记录；其中完整生产组合
101.20秒、Node9顶层，两个包均通过。最终budget/journal/admissionexec定向
日志为13顶层、78条含父子记录，三个包均通过；两份最终日志均0fail/skip。

| 验证范围 | 实际覆盖 |
|---|---|
| 生产组合 | 首次预算/开放→Prepare/Host→VM仍Planned取消→Cleanup Start/Record丢回复→SDK重建→原资格恢复/释放中拒绝→永久实际预算→Host512MiB结清→后继完整Grow→Guest setter丢回复只读恢复→下一Shrink Prepare |
| 生产Runner读者故障 | State UID替换、requested/actual漂移、owner holder/floor冲突、自洽Pending/NotSent/Completed历史、保留来源State UID和身份摘要替换；准确恢复原对象后继续。组合内两个寿命点均核验 |
| 预算合同 | v3来源互斥、严格JSON、完整Initial、同数值实例/出生替换拒绝、调用方指针隔离；受影响原v1/v2定向回归 |
| WAL | 仍Planned才可用启动来源；已Start拒绝；Cleanup同CAS替换、后续零派发取消推进关闭水位与codec读回；专用字节边界 |
| Node账本 | v8升级、旧版本拒绝、按内容比较/重放、普通JSON兼容；原实际容量/结清/Through和未知CAS定向回归 |
| 静态 | 七包go vet；相对本阶段父版本65737287的增量lint。不是全仓lint清零 |

初轮严格JSON缺字段、测试误从独立Core夹具读取Lease、后续UID转string编译错误
与增量lint问题的原日志均保留；失败不计为通过。修复后采用最终成功日志，
没有放宽生产校验、请求期限或Lease时长。测试总上限继续3分钟/命令4分钟。
最终重跑仅因源码修正后的准确快照核验，未变PG/Rust矩阵直接复用原证据。

Go组合实际执行生产Worker/SDK/WAL/Runner读者/Host生产者；PG原生回复、Root/
平台资格、Kube/Lease和UDS QMP对端为明确协议夹具，不能称真实PG+Linux/KVM
端到端。`-race`也只证明本次执行到的内存访问，不能保证所有并发均已覆盖。

## 后续实现与目标环境

本缺口关闭不代表PG17核心整体完成。仍有缺原来源/Root不一致受控恢复、完整
Outside/资格/Runner持有与GC、安全降低normal和后台/私有持续上界与活性、
真正owner切换/Start或init失败/无Wait/跨boot迁移恢复、Monitor自动策略与可信
请求/取消/重放生产入口、全部写者与CPU隔离接线。这些是代码或组合工作，
不是仅待Linux验证。PG17核心完整归档后再PG18、PG16，不移其当前子模块。

Linux/K8s仍必须验证cgroup v2/身份权限、HugeTLB各hstate/物理页/恢复、
NeonVM/KVM/QEMU实际热插拔、RBAC/etcd/真实Lease与分区、终止GC、存储兼容、
性能与长稳。Mac不能证明这些。持续Goal保持active，原每30分钟接续任务复用；
不创建重复自动任务，不构建镜像、不准备/执行部署、不合主分支、不发布生产。
