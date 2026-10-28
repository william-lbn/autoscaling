# 原GuestReady未收到时的持久取消与启动交接

日期：2026-10-29，Mac Darwin arm64。Autoscaling准确接线源码
`371949821689adf6012d93d4c13d0cecaab99069`，Root primitive为其父提交；Neon
认证路由为`b2a3eb131237f8ab6f589bf80da7cc829187e633`。准确逐文件、逐用例、
过程日志和Linux交叉产物摘要见[机器收据](DSGA-原GuestReady未收到取消-Mac收据-20261029.json)。

## 问题和行为

原GuestReady已由VM UID/RV CAS写前持久，但回复丢失、Root尚未收到，普通Observe
一直未知。原启动顺序要求先Root确认再创建PGWorker，使正式原重放/取消入口永远
不可达。现先核当前Session、完整原PG身份、Guest身份和完整原GuestReady WAL，
只读Observe；未知时将同一个Pending SDK交正式PGWorker。无完整原来源拒绝，
初始无Pending仍走原Root恢复gate，已有执行循环拒绝交接。此选择不写资源、不清
未知、不改epoch/PID/预算、不记外层成功；正式执行器保留全套领域核验。

管理取消先持久AbortRequested。Root固定认证cancel-original在与Apply同一flock内
核原身份、确认Bootstrap/旧Held、出生、当前floor、连续资源revision和实际旧约束，
仅“从未接收”才fsync取消屏障，零内核setter。Apply先入锁则取消拒绝；取消先落盘
则迟到Apply拒绝。已有Original或部分效果不能猜NoEffect，新owner不能重放旧epoch。
屏障消耗revision、保留原Held；新命令同时越过Original和Cancelled水位，日志只保存
最近取消正文，避免无界增长。取消回执只证明此GuestReady NoEffect，不证明PG/VM
取消或Host可退；SDK丢回复保留Pending，只读完整取消屏障后才清除。

首次取消屏障同时把Root日志v1升级v2，旧Guard回滚必须失败关闭，不能让旧程序
忽略新字段后重发旧命令。不得删除日志或降格式恢复运行。新代码读取未取消v1及
带取消v2；正式Root路由仍经原scope/冻结实例认证和私有UDS，网关不生成领域事实。
外层Record前重载原WAL并核同owner/身份/完整命令/Abort，CAS未知仍保留原文。

## 实际Mac验证

采用59顶层/146条含父子记录，最终0fail/skip；并非146次完整产品E2E。

- Root整包54顶层/119记录：真实文件锁、持久日志、HTTP和SDK，防迟到Apply、
  部分效果拒绝、当前floor、取消丢回复只读恢复、v2旧版回滚拒绝及既有回归。
- 启动交接1顶层/5记录：Start CAS成功丢回复、同SDK交接、原重放、正常取消、
  Root取消回复丢失及外层Record回复丢失；PG仍Planned，普通未知不自动取消。
- 受影响回归3顶层/21记录：既有GuestReady 18子场景、孤立Pending拒绝及执行槽门禁。
- 新真实失租组合1项65.14秒（包66.908秒）：完整首次→Prepare/Host/VM→GuestReady
  写前成功丢回复且Root未接收→真实生产30/20/2秒租约失效/同Lease接管→同Agent
  启动交接→禁止旧epoch重放→取消屏障落盘丢回复→只读恢复→Record落盘丢回复→按
  已长大1GiB VM、未Grow PG和旧Root实际成本结清预算/Cleanup/Host→下一Prepare。
  Root setter零次、取消POST一次、PG Resize零次、QMP一次。

Guard采用最终v2整包替代早期子树；handoff日志中的SDK子树由Guard覆盖不重复计数。
四包vet通过，相对c7855907增量lint最终0 issues。首次lint11项规范问题已修，首次
rustfmt递归命中未改support子模块既有格式；原失败日志保留。最终仅3修改文件格式
检查及真实UDS转发/正式认证HTTP各1项通过，受影响库Clippy退出0，保留既有警告。
最后等价revision抽取、方法重命名、显式零字段及错误包装规范修正后复核vet/lint，
未重复长租约业务组合。可复现入口如下，准确采用范围与日志摘要以机器收据为准：

```sh
go test -race -count=1 -json ./pkg/dsga/resourceguard
go test -race -count=1 -json ./pkg/dsga/owner -run '^(TestUnreceivedGuestStartupHandoffReplaysOrCancelsOriginal|TestProductionInitialOwnerTransferCancelsUnreceivedGuestAndSettlesActualBudget)$'
go vet ./pkg/dsga/resourceguard ./pkg/dsga/resourceexec ./pkg/dsga/owner ./pkg/agent
```

Root锁/持久化为真实本地实现；owner组合执行正式业务，但PG/root/Kube/QMP为明确
协议peer。Mac -race只证明执行到的并发，不能外推Linux内核/物理回收。

## 编译、发布边界及剩余工作

准确37194982的Agent/Root Guard各Linux amd64/arm64共4个ELF交叉编译退出0，
核架构/VCS/SHA256，未执行。唯一所有者AGENTS修改导致modified=true，离线模块
缓存权限警告原样保留。未改Controller/Runner不重编，未改PG不重跑。

本窗口实现和Mac组合闭合，不表示PG17整体完成。异常Root/资格/日志的受控关闭、
安全降normal与持续私有/后台上界、分区旧进程/出生不完整/无Wait/跨boot迁移、
Monitor/自动策略和全部写者/可信CPU写通道仍须核心工作；随后PG18→PG16各自实现
并验证。真实Linux/K8s/KVM/NeonVM/HugeTLB、物理回收、性能与长稳后验。PG17
gitlink与两份锁保持b078；上游冻结、独立控制面边界保持，不构建镜像、不部署、
不合main、不发布，默认关闭。阶段归档后持续目标仍active。
