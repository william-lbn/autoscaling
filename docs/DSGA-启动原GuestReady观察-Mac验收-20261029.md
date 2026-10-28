# Agent启动期原GuestReady只读恢复

日期：2026-10-29，Mac Darwin arm64。准确Autoscaling源码
`4f85588c5f29c822a91917d89a99e3605243b145`，原分支
`codex/dsga-autoscaling-20261019`。PG17三处仍为b078736a，不移动子模块。
逐用例、十文件源码摘要、日志和新Agent产物摘要见
[机器收据](DSGA-启动原GuestReady观察-Mac收据-20261029.json)。

## 实际问题和恢复约定

生产Agent先运行Root恢复门槛，再创建正式PGWorker。外层GuestReady已写前持久，
Root设置已经生效，但Root自身完成日志尚未确认时，ConfirmedLimits保留未知；
旧恢复门槛只Inspect，原Observe却依赖尚未创建的PGWorker，造成启动环依赖。
之前Guest失租组合直接创建PGWorker，没有覆盖这个真实启动顺序。

本次启动循环仅对外层完整原GuestReady命令执行Observe。构造SDK与启动查询共用
来源校验，绑定原VM/Guest、预算、资源revision、派发owner/epoch和完整命令。
查询前后重读原UID/RV日志，核当前Session/owner、活动请求和原命令；不从Root
自报Original收养孤立效果，不把PG的revision当资源revision，不换PID或目标。

Root Observe可根据完整实际读回确认Root自己的Applied/Held，并清该SDK同一
原Pending；这不是内核setter。启动阶段从不Apply/Replay，不Record外层步骤，
不写预算终态、不派发PG。正式执行器接手后仍重新核VM/PG/预算/出生，再只读
原命令并Record外层。缺原命令/部分效果继续未知，没有按NotFound补发的路径。
独立PendingHeldRepair仍须显式Observe，不能被启动轮询顺带清理。

继续复用同一执行槽和Run/Session/调用者取消边界；恢复结束才交接PG端点。
没有新增wire版本或扩大管理权限，既有默认关闭策略不变。

## Mac实际验收

去重10个顶层测试、33条含父子记录、1包，最终0失败/0跳过。分三次执行，按
包/测试名采用最新子树，父记录只是测试树元数据，不表示33次独立E2E：

1. 新恢复场景、既有Root管理/取消/未知来源、GuestReady整组及并发和私有UDS：
   9顶层/32记录，包21.087秒。覆盖完整原效果可解除启动等待，外层日志不变；
   正式PG执行器再核完整来源后才结清，setter总共一次。
2. 两个新增负向子场景增强后单独实测，包4.335秒：缺Root原命令/部分效果的
   第一轮完整HTTP响应确已被SDK处理，第二轮才取消，原Pending与外层未知保留。
   没有仅取消第一个HTTP就宣称未知路径通过。该子树替换首次对应记录，不累加。
3. 受影响真实失租接管组合69.31秒、包71.049秒：原30/20/2秒选主时限不变，
   新资源SDK先从原WAL恢复Pending，经同Agent的启动gate Observe后再建正式PG。
   原GuestReady仍Unconfirmed；正式Record落盘丢回复后，按已增长VM/实际Root/
   未Grow PG取消结清预算与Host，再接受下一新Key Prepare。Root setter/QMP各1，
   PG Resize0；启动及正式执行器各Observe一次，没有重发旧epoch命令。

Session、SDK、外层WAL、领域执行器、预算与Host实现真实执行；Kube、Root内核、
PG和QMP为明确peer。短场景beginGuest使用显式前驱领域夹具，完整接管组合走
实际首次服务/Prepare/Host/VM/Guest链，但同样不是Linux物理内存或完整存储E2E。

owner包最终go vet退出0；相对45c88cdb增量lint为0 issues；diff通过。首次lint
有两个测试格式/等价switch建议，已修，日志保留；业务测试没有失败。最终仅
import与if/switch等价语法变化不重复69秒组合，也不重跑未变PG/Rust大矩阵。

复验命令（在Autoscaling根目录，使用本机Go1.25.13与既有离线缓存）：

```sh
go test -race -count=1 -json ./pkg/dsga/owner \
  -run '^(TestResourceRecovery|TestGuestReady|TestPrivateControlSocketRecoversRootBeforePGExists)'
go test -race -count=1 -json ./pkg/dsga/owner \
  -run '^TestProductionInitialOwnerTransferObservesLostGuestReplyWithoutSendingPG$'
go vet ./pkg/dsga/owner
```

日志前缀为工作区build/dsga-startup-original-*；准确采用及首次lint日志摘要已入库，
不是只在build目录留一句“通过”。中文注释修正另有177e8372提交，仅解释已存在
Runner v3消费语义，不重复未变Runner/Scheduler测试。

## Linux仅编译与后续边界

准确4f85588c的Agent已编译Linux amd64/arm64两个ELF，exit0；核架构、GOOS/
GOARCH、VCS revision和SHA256。没有执行。唯一未提交文件为保留的所有者
AGENTS.md，故modified=true；十个源码摘要与准确提交一致。两次均出现离线
模块stat缓存写入权限警告，原日志保留，不声称无警告或Linux已验证。

本阶段只闭合有完整外层派发来源的Root启动确认窗口。缺/坏Root来源或资格、
异常限额的受控关闭、安全降低normal及后台/私有持续上界、分区旧进程、完整
出生前失败、无Wait及跨boot/迁移、Monitor/自动策略、全部写者/CPU仍有代码或
组合缺口，不能写成仅待Linux。PG17核心未完整，归档后继续，随后PG18再PG16。

Linux/K8s/KVM/NeonVM/HugeTLB/物理回收/性能必须目标环境验收。上游冻结、独立
控制面边界不变；不构建镜像、不准备部署、不合main、不发布生产。
