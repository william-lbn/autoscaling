# PG已派发与容量竞态安全关闭：Mac阶段验收

日期：2026-10-29。基于AS `3f8528c2427cd470c6ec657044e8fda081eec130`、
Neon `0baa8a5820efcc9c69c3b273f2cdd7176dded8a7`，继续既有自有功能分支。
PG17实际源码、gitlink、两份锁及本次原生产物均为
`325aeb42a6080d86902d51892b2518a70ec2512b`；本批没有改PG源码或追新上游。
完整摘要及原失败见[机器收据](DSGA-PG在途跨容量关闭-Mac收据-20261029.json)。

## 正式行为

PG Resize已派发或完成、尚未进入最终结算/开放时，来源失效不再只拒绝后续动作。
生产PGWorker先认证同boot并原生fence，再UID/RV写前CAS登记独立关闭；原Active、
原PG命令、所有领域未知、永久预算和历史逐字保留。只允许该原Resize的原容量或
提交后下一代；差值比较避免u64加一溢出。换boot、额外代次或原命令不符明确拒绝。

关闭撞上原PG提交时，同owner可在容量严格前进后持久选择更高revision的后继关闭。
同代次未知仍只查询/明确重放原选择；更高owner必须选择最新完整前驱。最初、直接
前驱、当前关闭一起保留，有限两代集合使自动选择有界。关闭不代表原Resize无效果、
完成或预算可释放；来源恢复、SDK重建或接管均不自动重开，也不发送反向资源动作。

Go/Rust共享包装格式2准确标识跨容量接续，原生子请求仍格式1。格式1既有同容量、
更高owner约束完整保留；格式2要求容量单调，同owner还须容量及revision严格提高。
响应回显所选格式，SDK允许显式升级并禁止已保留2降回1。Journal格式17独立约束
原Resize前后两代，不把宽于业务的协议校验当作资源授权。OpenAPI同步描述。
这些编号用于识别持久格式和安全恢复语义，交付目标始终为完整PG17自动扩缩容。

## 实际执行的Mac验证

- Go采用44个顶层、310条按(package,test)去重的父子记录，全部-race通过，
  0fail/skip；不是310次产品E2E。合同/共享向量、TLS SDK、持久模型和真实HTTP
  typed Store UID/RV CAS均实际执行。模型首次失败与旧合同overlay失败另行保留。
- 七个生产模式：applied、unreceived、lost-start、lost-close、race、
  race-lost-continuation、transfer全部通过（包249.847秒）。覆盖原PG两代、
  同owner接续、丢回复、SDK恢复、真实Lease失效/接管和来源恢复后继续关闭。
- 正常自动Grow→Shrink与生产结案/Finish丢回复组合2顶层8条记录通过，包232.583秒；
  旧关闭HTTP Store 4顶层24条记录通过，包20.666秒；旧纯模型12顶层72条通过。
  缩容PG完成后的ReleasePG/GuestReady/VM/ObserveVM Planned及未知模型也已通过。
- Rust共享合同5项、准确PG17原生3项、真实Compute Router准入4项全部通过，0ignored。
  Router命令显式include-ignored；24个无关用例被过滤，不计为通过。原生PG提交与
  旧容量Stale、原关闭回执、同owner接续实际运行；11个有效摘要由真实PG JSONB/SHA
  独立核对，37个共享向量两仓逐字一致，旧格式1向量字节保持。
- 四Go包vet退出0、增量lint最终0issues，Rust SDK/HTTP目标Clippy退出0；依赖既有
  warnings保留。共享消费者22包pass/12包无测试文件仅编译，不计业务执行。
  最终复核后只修正两处SDK中文注释的epoch描述，没有修改已验证业务字节。

复现新增Router：工作区执行`python3 build/run-dsga-capacity-continuation-http.py`，
只使用`build/pg17-initial-close-exact-install`私有临时PG，显式核准确SHA。
Go补充命令在AS目录执行，配置工作区GOCACHE、Mac14.4 SDK和本地已有工具链：

```sh
go test -race -json ./pkg/dsga/owner -run '^TestProduction(NormalCompletionRechecksSourcesAndRecoversLostFinish|AutomaticMemoryPolicyUsesOriginalOwnerAndBudgetChain)$' -count=1
go test -race -json ./pkg/dsga/journal -run '^Test(InitialServiceClosureStore|ResumeServiceClosureStore|PlannedResumeClosureStore|PendingFinalizeClosureStore)' -count=1
```

原阻塞是自动审批额度拒绝，用户批准后实际执行通过；没有绕开审批或将Clippy冒充
执行。首次摘要生成、沙箱监听、原生测试编译、lint、消费者路径等失败原日志保留，
采用的是修正后的明确终态结果；详细日志摘要和准确源文件SHA见机器收据。

## 仍需继续的完整产品工作

本批关闭PG已派发/容量竞态这个代码缺口。Cleanup恢复开放异常、坏缺日志独立关闭、
受控重开与有界重复生命周期、安全降normal及后台/私有持续上界与活性、出生崩溃
无失败屏障、Runner无Wait/分区/跨boot迁移、Monitor/WSS/配额/RW-RO/可信CPU和
全部最终写者仍须代码，不能归入Linux待测试。整体未完成，Goal保持active。

Go组合中的PG/Root/QMP/Kube为明确peer；真实HTTP/CAS/Lease执行不能外推成物理
认证。Linux/K8s/KVM/NeonVM/HugeTLB/物理回收/性能另行验收。默认关闭，不建镜像、
不部署、不合main、不改独立控制面；PG17全面实现/Mac验证/远程归档后才暂停，
不自动进入PG18/16。准确提交和远程SHA以Git、远程refs及Neon发行锁为准。
