# 首次预算持久化、独立开放WAL及SDK恢复：阶段验收

源码`dfe62868aded79b978f5ee52c01effa73b3cc2d3`，沿用原自有功能分支和
冻结上游。本阶段完成永久初始预算、独立首次开放WAL、SDK原未知恢复，
**实际首次服务生产循环尚未接入，不能宣布首次服务完成。** 该生产者以及
原资格/root/PG/Runner/Host实时复核与组合验收继续实现。

## 实际变化与不变量

VM Model新增v9 `InitialHeld`，保存完整原Initial预算、原owner、摘要及落账
序列，与普通BudgetHeld/实际CapacityHeld互斥。没有Resize Request、假HWM
或假Host操作。首次开放有独立原命令/摘要、派发owner/序列和完整原审计；
无回执保持Unconfirmed，不能从空Active推断无未知。命令8KiB，预算/审计
各32KiB；整体有界codec及VM UID/RV CAS沿用原实现。

`BindInitialBudget`只在原PG/Guest已绑定、无原操作/HWM/历史时保存一次，
原完整预算不能被另一份替换。`StartInitialAdmission`持久化后才有真正派发
票据，回复丢失不返回票据。`RecordInitialAdmission`只接收完整匹配的原生
Applied/Replayed、当前floor/Shared/root审计；NotFound/Expired、布尔Open
或更高floor都不能独立完成它。网络来源认证仍须真实生产执行器负责，不能
将模型校验或测试夹具升级为平台许可。

首次开放只针对初始容量及revision1，绑定完整初始预算摘要和原bootstrap
预算ID；维护门禁已前进时不能制造更高revision覆盖。原预算已保存而尚未
派发时，新owner可先认证当前事实再新建首次命令；已经派发则完整原命令/
epoch不变，新owner只能只读确认。未知首次开放阻止新Resize Begin。首次
完成后首个请求仍固定原初始容量代次；正常Finalize或实际取消预算完成时
与替换永久头同一CAS清除InitialHeld，不提前删除任何外部持有对象。

格式升级与首份InitialHeld同CAS；现有无初始预算日志不自动升级。父版本
Model.Validate明确拒绝`protocol_version>8`，旧读者不能通过省略新字段把v9
投影成空闲VM。Node仍v7。独立控制面后续统一移植，未在本阶段改它的业务。

实际Session.NewPGWorker从InitialHeld独立WAL恢复同一SDK admissionPending，
即使Active=nil也不清空。普通Status NotFound保留SDK pending；原Status确认
后只清本SDK相同未知，不写VM完整预算完成。重建仍按原WAL恢复，不能继承
旧SDK进程的内存状态或自动发setter。必须提供原已绑定ResourceWorker，
没有为修测试省略root域或放宽原实例检查。

## 验证证据与范围

本阶段采用**81顶层测试/种子入口、293含子场景/种子记录pass；0 skip、
0采用失败**。其中预算/journal受影响全包78入口/290记录，新SDK1场景，
旧构造/绑定及原重放2场景；fuzz仅执行种子，不是连续模糊测试。完整映射与
源码/日志摘要见同名JSON收据，不与历批结果相加算整体完成率。

- 原Initial/开放合同拒绝NotFound/Expired、不同PG观察/预算/floor/VM/Guest
  与代次，合法新floor只能只读确认旧原命令。
- 真实Model/codec验证独立预算、深拷贝、未知阻止Begin、原预算不可重绑、
  接管不改旧命令、完整原生只读确认、版本/摘要/序列/owner篡改拒绝，以及
  普通完整预算完成前保留、完成同CAS替换初始预算。
- 真实typed HTTP VM客户端/RFC6902 UID/RV Store覆盖预算保存、开放Start、
  Record提交后各自丢回复；没有确认的Start无票据，客户端重建仍有完整原头。
  这里没有执行native setter，原审计是明确领域夹具。
- 真实Session/SDK/本地TLS覆盖没有Active时恢复原pending、NotFound不清、
  Status确认仅本地清空、VM原WAL仍未知、再次重建再恢复；setter计数恒0。
  这不是实际首次服务生产者或Linux平台验证。
- 三包lint为0 issues，vet通过。按准确源码构建并核验Agent/Scheduler/
  Runner/Controller各Linux amd64、arm64共8个ELF的架构、VCS SHA及摘要；
  全部只编译、没有执行。Go/模块源码与提交一致，vcs.modified=true来自保留
  的所有者AGENTS.md改动。未构建镜像。

保留的失败及修正：新测试重用了固定断言v3的旧helper，改为直接Record并
保持v9；fixture初始epoch1与真实Session接管后的epoch2不符，按真实floor
构造；SDK夹具漏传已绑定ResourceWorker，补回原资源域。旧helper/生产检查
未放宽，失败场景定向复验后执行必要格式回归。lint只涉及两个局部变量
遮蔽内建copy，重命名无业务变化。原失败日志不记为通过。

## 尚需完成的代码与目标环境验证

下一阶段将本持久入口接到实际首次服务循环：认证当前PG/root/进程出生、
原资格及初始Host/Runner真实容量，先保存完整预算/原命令并取得票据，后
开放原生准入；普通循环先查未知，独立可信选择才原样重放，来源漂移不
自动重建或改参数。随后验证首次服务、三阶段未知/重建、后继正常操作，
并接初始预算的取消、长期当前预算与Host来源生命周期。不能把这些代码
工作归为Linux待测试，也不能只靠本Store/SDK阶段宣布功能完整。

安全降normal、缺源/真实换主/启动失败/跨boot/迁移、其他来源回收与全部
写者/CPU、可信请求/Monitor/策略接线等既有缺口继续。Linux资格实际数值、
cgroup/全部HugeTLB、真实物理回收、NeonVM/KVM/QEMU、K8s RBAC/接管、存储/
负载长稳与控制面移植后产品E2E仍后验。默认关闭；不部署、不合main。
