# 原QEMU确定未启动：停止来源和Host结清阶段验收

日期：2026-10-29，Mac Darwin arm64。准确Autoscaling代码`0710d8cdc1a7c956742c9706076732cecf190416`，
沿既有`codex/dsga-autoscaling-20261019`；Neon沿原功能分支绑定。
PG17 b078及冻结点不变。本批是一个恢复子路径完成，**整体核心仍未完成**。
逐源码/日志SHA256、实际测试映射与编译产物见[机器收据](DSGA-QEMU确定未启动关闭-Mac收据-20261029.json)。

## 原问题和最小修复

原Runner先持久启动意图，再调用同步exec.Start。Start确定失败时，旧实现仅返回
错误：没有QEMU，但缺原Wait退出来源，Scheduler即使等到整个Pod终态也无法结清
原Host承诺。新实现由同一调用产生ProcessExit v2，只接受两种本地可知结论：
已经写完原启动意图但尚未进入Start即被取消；或Start失败且没有Process和
ProcessState。后者不凭错误文本、PID消失、QMP断连、Pod404或超时推断。

v2所有ProcessID/ExitCode/Signal为0，不伪造Wait结果；原Birth/State UID、容器
实例、随机launch nonce、argv摘要、当前完整内存WAL序号及摘要继续保留。
v1正常/错误/信号Start/Wait的原序列化格式不变；旧校验器明确拒绝版本2。
CM保护/标签仍是原process-exit-v1来源族，实际payload逐版校验；不升级VM9/
Node8或Runner内存record2，因为未引入内存动作语义。升级时先更新消费者。

## 顺序、失败和恢复不变量

原Runner文件锁→原完整启动来源与WAL→关闭文件fsync→解锁→不可变CM发布。
本地关闭独立于已取消的调用者，有10秒上限；文件失败不发布外部来源。发布丢
回复只以同一名称和原文GET/CREATE恢复，不重启QEMU或另造nonce；结束仍返回
原启动错误，不能因归档成功吞掉失败。没有完整原启动来源时不制造NoStart。

关闭不改Pending/Last/内存WAL序号，不把未知resize补成Completed或NoEffect；
Tick、第二次RunQEMU及旧实际publication在关闭屏障后拒绝。Controller同样先
查原停止来源，再拒绝旧QMP实际值。持久盘丢失/损坏不重建空日志。

Scheduler仍须原Pod全部app/init容器的真实终态、固定原容器ID和RestartPolicy
Never；NoStart只证明QEMU，不替代Sidecar或Pod停止。Node UID/RV结清、Pod保护、
先写VM关闭锚点、再移除Pod保护保持原流程。Node CAS回复丢失保留保护，下轮按
同一关闭意图只读确认；不重复退还，不动其他Pod成本。Kubelet/CRI是否能作整Pod
物理成本释放依据仍须目标Linux资格验收。

## 实际验证与复用

最终Mac -race三包12顶层/54含父子记录，0fail/skip；vmguard 7.951秒、
hostscheduler 53.946秒、Controller 2.582秒。覆盖程序缺失、权限、格式错误、
已取消启动上下文、发布GET丢回复、本地持久来源拒绝、正常/错误/信号Wait、
原未知WAL保留、NoStart与Sidecar终态、Node CAS丢回复和VM关闭锚点。
三包vet及相对59b66847增量lint退出0，lint 0 issues；不宣称全仓清零。

vmguard两种Linux架构测试ELF已在Mac交叉编译并核架构/Go版本/摘要，均未执行；
不声称8服务或镜像已构建。首轮Scheduler在沙箱禁止localhost监听时panic，原
失败日志保留，最终获准本机临时端口后通过；不以其替换生产检查。未改PG17、
Rust和预算/GuestReady的逻辑复用准确已有证据，不重复未变矩阵。

测试执行了真实Mac exec.Start/Wait、文件锁和fsync、生产来源发布及typed HTTP
Node/Pod/VM条件写；测试子程序不是QEMU，Kube/Lease/容器终态是明确夹具。
本批不能认证Linux fork/exec、真实K8s RBAC/CRI、QEMU/KVM或物理回收。

## 还未关闭的范围

此批不解决没有原启动来源的早期init失败、Runner被杀而无Wait、丢失Root/资格/
Outside来源、异常swap/任务/系统域、跨boot/迁移；不能从NoStart v2反推这些情况。
安全降低normal和持续上界、完整owner切换及Monitor/策略/可信操作入口仍须
代码与组合。PG17核心完整归档后继续PG18→PG16，逐版内核/扩展/发行接入及
专属回归，不以PG17替代PG18 AIO或PG16。持续Goal active、原heartbeat沿用；
默认关闭、不构建镜像、不准备/执行K8s、不合main、不发布。
