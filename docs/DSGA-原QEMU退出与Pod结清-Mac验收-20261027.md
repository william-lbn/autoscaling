# 原 QEMU 退出、Pod 承诺结清与 VM 来源清理

本批实现受管 Runner 的真实子进程退出事实、Scheduler 的原 Pod 分配承诺结清、
Controller 的停止身份保留及删除清理。**这是关闭协议及分配账本的实现，不是
Mac 已证明 Linux 物理内存回收，也不是整个内存伸缩核心已封版。**

基础代码 `e3cc8b66501a853a6c8d1d09b64a777b60045753`；实际入口/组合代码
`08f3ebcb7e1b03930a840acb2d2aad45a677e731`。继续既有
`codex/dsga-autoscaling-20261019`，未追入冻结后上游，未修改 PG17 或独立控制面。
配套 JSON 收据固定 23 个 Go 文件、最终用例→日志映射和 8 个仅编译 ELF 摘要。

## 关闭顺序与不变量

1. 实际 Runner 入口使用同一个 `exec.Cmd.Start/Wait`。启动前固定原容器 ID、
   随机 launch nonce、原 Birth UID、QEMU UUID/私有 monitor 参数及 argv 摘要；
   启动意图先同步。禁止 daemonize/迁移接收/第二组 UUID 或私有 monitor。
2. 原子进程被真正回收后，先在原目录 flock 下同步不可覆盖的退出文件，再向
   Kubernetes 发布不可变 `process-exit-v1`。保留原 WAL 序列/摘要；旧 pending、
   may_have_sent 和结果原文不改。退出后普通派发/读回/发布均拒绝旧 QMP 状态。
   父 Runner 被杀、Start 失败或没有原 Wait 结果时不猜补退出证明。
3. 调度成功前保护原 Pod；受管 Controller 创建时即加保护。节点 owner 只有在
   原 Pod 终态、全部 app/init/sidecar 均 terminated、容器 ID/时间完整，且原
   VM/Birth/Exit/Node/Runner 容器实例一致时，才持久保存 Pod 关闭意图。
4. 原 Node UID/RV CAS 移除这一个 Pod 的 Placement 及全部旧峰值；其他 Pod、
   系统域和其他 VM 不动。首个整 Pod 结清升级 **Host Node Model v5**；v5
   所有容量写者重新读取原 Pod，终态/删除/新 UID 均不得因旧队列重开额度。
   成功运行操作的 v4 结清在 v5 节点上保持 v5，不能降版本绕过屏障。
5. CAS 确认后，先将同一关闭锚点条件写入原 VM 并实际读回，再移除 Pod 保护。
   Pod 可以随后被 GC；Controller 不依赖已消失的 Pod 来重造身份。Node/Pod/VM
   写丢回复均恢复同一原意图，原 Exit 来源被替换则保留删除保护。
6. 受管停止不走旧 Cleanup/自动重启，保留 VM/PG/WAL 原关系，等待显式受控
   恢复。删除 VM 时先停原 Pod，核原关闭锚点及全部实际 Node 已无该 VM 持有，
   才按原 UID/RV 移除本协议 Runner/Host/Outside CM 的保护并删除。别的 VM、
   无关 CM/Secret、其他 finalizer 不接管。未开放且从未派发的原始 bootstrap
   可撤销；仅把 Ready 改回 false 不能冒充从未运行。清理未知回复可续跑。

VM Model v4、Runner record/Publication v1、Host CM v2、PG ABI 不变。新增
退出 CM 和关闭元数据；285fa2e6 旧节点读者源码只接受 1..4，因此拒绝 v5。
此兼容判断是静态源码核对，未执行旧二进制。Agent 不增加 Node/CM 写权；
Scheduler 增加 Pod 保护/关闭与原 VM 关闭元数据写，Controller 增加关闭来源
的分页读取及按 UID/RV 清理。真实角色权限隔离须在 Linux 验收。

## Mac 实际结果

Mac arm64、Go1.25.13，新增/受影响最终去重 **20 顶层、68 含子项通过，0
最终失败、0 skip**；5 包 vet 通过，6 包 Linux 增量 lint `0 issues.`。
未改功能复用原证据，不与前批的 68 项叠加冒充唯一总数。

覆盖实际 Mac 测试子进程正常/非零/信号退出、原 Wait 身份、flock/fsync、
拒绝覆盖/符号链接/权限/超长文件，旧未知动作原文保留、退出后禁止新动作；
组合 Runner 原来源→Scheduler 原容量 CAS→VM 先于 Pod GC 的锚点，含 Node/
Pod/VM 写丢回复、来源替换、未停 sidecar、容器 ID 不符、新 Pod 可用归还额度、
其他 Pod 隔离；Controller 源清理/未知恢复、冷 bootstrap/Ready 回退拒绝和
当前仍持有时阻断。受影响首次容量、旧 VM 批准、Runner 活动执行及 Controller
状态展示/Pod 构建路径已选择复核。

子进程运行测试可执行文件，**不是 QEMU**；真实文件持久化/进程 Wait 与 typed
客户端路径执行，CM/Kubelet 终态与平台开销为明确夹具。Controller 清理另以
Scheduler 同一关闭结构验证，不称真实集群完整 E2E。

保留原失败：HTTP 测试夹具 Pod PATCH 分支最初放错路由导致编译错误；初轮
组合夹具在 Running 后初始化新 journal，被生产首次启动屏障拒绝，现改为
先 Initialize/ReleaseBootstrap 再标记派发；生产拒绝未放宽。初轮 lint 两项
已修复。最终日志按完整用例替换失败，未把空运行或重复父子事件另行累加。

准确 08f3ebcb 的 Scheduler/Runner/Controller/Agent 四程序，Linux amd64/arm64
共 **8 个 ELF 仅交叉编译通过**；ELF 架构、`vcs.revision` 和 23 文件逐字节核验。
modified=true 仅因所有者保留的 AGENTS.md；该文件未编辑/暂存。Mac 原生 Runner
CLI 依赖 Linux cgroup 符号，不能执行；便携核心与 Controller 测试实际在 Mac
运行，交叉编译不替代 Linux 运行。

## 复核入口与复现

工作区根 `build/dsga-process-retirement-test-audit-20261027.json` 固定每个最终
用例的真实日志；`build/dsga-process-retirement-final-audit-20261027.json` 核源码、
8 产物及冻结官方祖先，完整内容已镜像到本目录配套收据。

Mac 使用锁定工具链及依赖缓存，例：

```sh
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 -timeout=120s \
  ./pkg/dsga/hostscheduler -run '^TestSchedulerWholePodRetirement'
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 \
  ./pkg/neonvm/controllers -run '^TestManagedVMFinalization'
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOTOOLCHAIN=go1.25.13 \
  go build -mod=readonly -p=2 -o /tmp/dsga-runner ./neonvm-runner/cmd
```

其余完整用例/日志以收据映射为准；不用为重现单个修复重跑全部 PG/HTTP/存储。

## 尚未完成，不能归为仅待 Linux

代码仍须：失败/取消结清、无 Wait 回执异常恢复所需的原 Node/CRI 证据，迁移/
受控跨 boot、全写入者准入、独立 CPU 联动；Scheduler 有界公平推进与历史终态
Pod/空闲 VM GC 扫描收敛；可信完整 Prepare/系统剩余/过渡/安全预算，首次预算/
准入、安全 normal 降额及后台/私有持续约束；Monitor/策略/公开操作完整闭环。
Start 失败、init 失败导致未完整终态、Runner SIGKILL 等未知路径保留容量，
受控恢复生产者未补齐前，不能声称完整销毁/异常恢复能力已交付。

目标环境仍须验证真实 Kubelet/CRI/Lease/RBAC/GC、cgexec 与 QEMU 前台 exec
关系、cgroup/Pod sandbox/页缓存残留及 Host 开销上界。**app/init 终态或 QEMU
Wait 本身不认证全部 Host 物理开销已归还**；平台资格必须覆盖这些残留成本，
不成立时需补节点证据与分阶段结清代码，不能启用整份结清。KVM/NeonVM/
HugeTLB/virtio-mem、真实物理回收与性能长稳另验。

不构建镜像、不准备或执行部署、不合主分支。控制面/其他 PG 继续暂缓，整体
核心仍未完成；本批只提交真实已验证的关闭协议阶段，后续沿原分支继续。
