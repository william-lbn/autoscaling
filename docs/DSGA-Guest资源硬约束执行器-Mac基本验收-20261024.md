# Guest资源硬约束执行器与Mac基本验收

本批实现`pkg/dsga/resourceguard`及独立root程序
`neonvm-daemon/cmd/resource-guard`。这是实际Linux内核文件写入适配器，**还没有
接入完整生产启动/预算控制链**；不是把采样、DTO或旧`memory.high`当作硬约束。
默认不启动，不改变旧neonvmd无认证HTTP接口，也未构建镜像或部署。

## 实现与不变量

- 固定`/sys/fs/cgroup/neon-postgres`，检查真实cgroup v2文件系统、root目录/控制
  文件权限及Postmaster PID/starttime/cgroup成员身份。旧官方cgconfig将管理权
  交给postgres，该状态明确拒绝；受管启动的root权限接线仍须完成，不能自行
  宣称已有root约束。请求不能指定任意内核路径或shell命令。
- 实际写`memory.max`、`memory.swap.max=0`，拒绝已有swap占用或明显不适合目标的
  当前占用。要求Guest实际基础页整数倍；每次读回准确上限与占用。memory.max
  可能触发OOM或暂时超限，不能证明永不OOM或物理页已经离开Guest。
- 枚举全部HugeTLB hstate，要求命令完整列出并按页大小排序；全部禁用surplus
  overcommit，再调整持久池。写前拒绝目标小于已用/预留，写后要求目标读回吻合、
  overcommit为0且surplus为0。部分分配、HVO延迟释放、池写失败均保留未知。
  只有没有surplus并准确匹配时才证明所报整个池上界；空闲池也在成本中。
- 当前适配器只接受HugeTLB未计入memory controller的核算模式；检测到
  `memory_hugetlb_accounting`则明确拒绝，不能盲加导致不清楚的重复核算。
  资源总量为memory.max+整个Guest池+外部保留上界；最后一项必须由调用方另行
  建立真实承诺。本回执**不单独构成FinalizeBudget**，也不发VM许可。
- 资源floor绑定VM/Compute/Guest boot/cgroup设备与inode，独立于PG boot。
  持久盘私有目录中的跨进程文件锁覆盖读日志、原意图fsync、同步内核写和最终
  读回/完成fsync；取消不会把系统调用分离到后台或提前放锁。Linux6.12不假定
  支持较新`memory.max`的O_NONBLOCK语义。
- 写前保存完整原意图与上一份Held约束，任何部分写/回复丢失/最后持久化失败都
  不回滚放宽、不返回NoEffect。普通重试拒绝未知；可按原意图观察，只有当前
  floor可显式原样重放。新owner可观察旧意图，不能修改epoch后重放。Revision
  连续且有溢出检查；原约束/身份丢失、Guest重启均要求受控恢复。
- 日志缺失不能由普通Fence补建；原bootstrap必须显式Initialize，已存在、损坏
  或非私有日志均不覆盖。原创建流程仍须证明初始化授权，不能在恢复错误后自动
  调用。root状态目录不在/tmp、/run或PG可删除范围；文件原子rename并fsync目录。

## 实际验证

最终源码8个Go文件摘要前后相同；证据在工作区
`build/dsga-resourceguard-final2/`（race.log、results.json、source-sha256.json）。

- Mac `go test -race -count=1 ./pkg/dsga/resourceguard`：7项通过，0失败/跳过。
  覆盖真实文件写前日志/重建恢复、部分副作用未知、显式原样重放、旧Held保留、
  换owner只读对账、两个独立Guard/文件描述符的锁串行化与取消、旧epoch拒绝、
  Guest换boot拒绝、丢日志不自动重建、路径符号链接/权限、严格JSON/溢出与真实
  `/proc/stat`格式解析（含带括号comm和uint64边界）。**内核是明确的测试夹具**。
- 新包及root程序`go vet` exit0；Linux amd64、arm64程序交叉编译均exit0。
  交叉编译不等于执行了Linux syscall；此前一次Linux测试编译也通过，不重复计数。
- 初版Mac `Stat_t.Mode`宽度差异导致编译失败，显式类型转换后修复；随后Guest
  boot_id错用了32字符incarnation校验，改为保留内核36字符UUID。原失败日志
  `dsga-resourceguard-first/second`及中间检查保留，最终通过不抹掉历史失败。

## 继续编码与Linux后验的界线

仍须编码：root受管Postmaster出生/重启权限、Compute固定授权通道、PrepareBudget/
GuestReady/FinalizeBudget与独立启动准入的真实接线、完整quiesce、外部系统/Host
承诺、跨PG/Guest boot与部分旧owner命令的受控关闭恢复。这些是业务缺口，
**不能改记成仅Linux待测**。本程序未自动装入启动/发行镜像。

已写内核适配器的真实cgroup权限/成员、memory.max回收/OOM、全部hstate池调整、
预留/surplus/HVO、swap、进程退出/重启、SIGKILL持锁恢复及物理回收，后续必须
在实际Linux NeonVM/K8s上验证。Mac不构造K8s模拟验收，也未重复未变PG/SDK测试。

依据：[Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)、
[Linux HugeTLB](https://docs.kernel.org/admin-guide/mm/hugetlbpage.html)。本实现保守
拒绝未知/不支持情况，仍需目标内核验收，未宣称可直接生产发布。
