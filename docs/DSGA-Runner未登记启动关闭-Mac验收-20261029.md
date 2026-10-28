# Runner完整出生后、未登记启动时的关闭与Host结清

日期2026-10-29，Mac Darwin arm64。准确源码95e8e2dbf7a3892461af78ffa2dc885bab611af5；
七个Go文件、逐用例、日志及ELF摘要见[机器收据](DSGA-Runner未登记启动关闭-Mac收据-20261029.json)。

## 问题与完成的代码

此前Runner在init脚本、磁盘/网络和QEMU命令准备后才固定出生，完整出生后尚无
launch时也没有可认证关闭来源，失败Pod可能永久占用Host。现在原生产入口先
固定出生，再执行这些初始化；错误退出用独立10秒期限执行CloseUnlaunched。
同原目录flock内核完整WAL、确认process-launch.json不存在，再fsync永久关闭
屏障。与RunQEMU的写前启动登记只有一方能获许可；关闭赢后所有迟到Start/Tick
拒绝。原内存WAL/Pending/Last/Sequence原样保留，不凭当前PID消失补NoEffect。

ProcessExit v3明确表示没有launch，无launch nonce/argv摘要/PID/Wait结果，不伪装
成v1 Wait或v2已持久launch的NoStart；旧读者不识别版本即拒绝。原Pod删除时可
由同UID/owner/node、未重启且仍Running的原容器保存关闭，仍不允许新Start。
锁内不发网络；先本地后发布原不可变来源，发布未知有界只补同一事实，已有
关闭重试不重新发现Pod/容器。已有launch返回未关闭，缺/坏出生/WAL也不造来源。

同时发现并修复真实启动契约错误：生产生成器保留固定SIGTERM私有QMP socket，
原最终校验却只接受内存socket，正确受管命令必然被拒。两端现共用固定powerdown
路径，只额外接受一次该完整私有socket；重复/额外选项/任意TCP或其他路径继续
拒绝。实际处理器仅发送固定system_powerdown，不新增内存写入口。两个私有
socket均限可信Runner容器，不能挂给Guest/租户。真实QEMU行为仍待Linux验证。

Scheduler/Controller消费同一原出生的v3来源。关闭只证明原Guest未获启动许可，
Host仍须原Pod全部容器终态、原Node身份和最终CAS；sidecar仍运行时拒绝。Node
落盘丢回复保留保护，重读恢复后才移除，原闭合同一来源幂等不重复退还。

## 实际Mac验证和质量

新增及受影响去重16顶层/41含父子记录、三包采用结果0fail/skip；不是41次独立
产品E2E。覆盖真实Mac子进程Start/Wait/信号、未launch与Start互斥、迟到启动、
本地先持久/发布丢回复、原Pod删除、已有launch/坏日志/符号链接/换Pod拒绝，以及
真实Scheduler/Host Node/Pod CAS和Controller来源消费。Kube服务仍是明确peer，
子进程是测试二进制，不是QEMU/KVM。只复验新增/修改及受影响路径，不跑PG或
其他旧大矩阵。

初次双socket用例误把QEMU参数放在Go test的--之前，子程序exit2；修正夹具后
实际Start/Wait通过，并验证已有launch不能改写成v3。原首次日志包级失败保留，
失败不计通过；准确去重映射采用最终子树，未变通过路径按源码等价范围复用。
Controller初始筛选未命中，已单独执行新增两子场景。lint仅将测试if/else换等价
switch，不重复6秒Scheduler组合。三包最终vet0、相对08d87557增量lint0；Linux
runner包增量lint0，diff通过。

## 准确Linux仅编译与剩余边界

准确95e8e2db生产Runner Linux amd64/arm64两次编译退出0，ELF machine、GOOS/
GOARCH、VCS revision和摘要核对；未执行。唯一未提交所有者AGENTS修改导致
vcs.modified=true，七个源码摘要与commit一致。两次均有离线模块stat缓存写
权限警告，原日志保留，不能称无警告构建。不构建镜像或准备/执行K8s部署。

本批只闭合完整出生后的未launch init失败；出生尚未完整、Runner被杀无Wait、
分区/跨boot迁移、缺原Root/资格及异常关闭、安全降normal/私有后台持续上界、
Monitor/自动策略/全部写者/CPU仍须核心代码及组合，不能归Linux待验。PG17
整体未完成，核心完整归档后再PG18→PG16。Linux/K8s/NeonVM/HugeTLB/物理回收
及性能仍须目标环境验证；Goal active，上游冻结、独立控制面及不合main边界保持。
