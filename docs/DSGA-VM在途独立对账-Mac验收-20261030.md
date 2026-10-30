# VM 在途独立对账：Mac 阶段验收

本批继续 PG17 内存伸缩 P1，补齐服务关闭后原 Runner/VM 单领域的独立
对账。完整原生完成与原文件锁内永久封存未发送分别记录，原 Active、
Abort、Unknown、旧预算和关闭均保留。单 VM 证明不授予开放、普通 Finish、
VM 缩容或 Host 释放；完整实际预算及后继恢复仍须后续业务代码。

本批 Autoscaling 代码、中文说明与验证已准备模块化提交；远程归档 SHA 在
Neon 发行锁及最新进度记录固定。准确源码、采用用例和失败日志摘要见 Neon
同名机器收据。不能据本批通过宣称 P1 或 PG17 全面完成。

## 实现和不变量

`ServiceClosure.VMRecovery` 先保存独立写前选择，固定原操作摘要、完整
ResizeVM 外层摘要、最近完整关闭、原 owner/序列及是否选择未发送封存。
选择与落账各自通过同 VM UID/RV CAS；未确认回复不返回可采用的新快照。
Runner 自己 GET 到已持久选择后，还须通过真实 Lease、VM/Runner/Node 身份、
当前 floor 及原文件锁内的重复认证，不能拿调用方的返回值代替执行授权。

原完整 Prepared、原 VM before、Request、Prepare/Guest 摘要、Host 与出生引用
交叉绑定。journal 使用既有 v1 命令的只读 JSON 投影，避免反向依赖 vmguard；
原完整几何、Boot/Node 身份、回执原文及物理值仍由固定 Reader 验证。规范
投影同时绑定 Runner 域 SHA 和外层 journal SHA，不能以相同目标或相同数字
收养另一条原生命令。

独立选择的相位只允许 Unknown 和 Settled。Settled 保存既有 NativeCapacity、
真实观察 owner，以及取消时原回执中的见证摘要；不新增发送协议，不伪造原
ResizeVM 或 ObserveVM 成功。完整原 VM 步骤已 Completed 时仅可读取完成来源，
不得再把它解释成未发送取消。接管可以重新认证同一来源，不能覆盖已保存的
观察者、回执 UID、取消见证或将旧 owner 原命令换 epoch 重发。

| Runner 当前事实 | 本批行为 | 仍然禁止 |
|---|---|---|
| 原命令完整 completed，当前准确 requested/actual 都等于原 target | 只读认证原不可变回执、原 State 和实际容量，另存独立完成来源 | 改原 Unknown、自动开放或释放 Host |
| 原命令未接收，或仍在 prepared/not_sent 且有明确封存选择 | Runner 在原 flock 内重新认证并封存 cancelled_before_send；准确 before 另存来源 | 把单纯 not_sent、当前 before 数字或关闭本身当取消证明 |
| 已越过 may_have_sent | 继续只读观察原效果；仅完整原生终态可以形成来源 | 再次 Set、反向回滚、按当前 before 取消晚到 QMP |
| 原来源丢失、UID/Boot/Node 变化、部分效果、当前 floor 未确认或实际漂移 | 保留未知和原持有，报告 AwaitingFact | 修补来源、复用同名新对象或清理旧预算 |

Runner 最后写者显式检查 ServiceClosure。关闭后即使预算来源恢复，原命令也
不能新派发或消费旧 not_sent 重放提示；先保留完整原命令及合法取消权限，
使已发送动作仍可观察、明确未发送选择仍可在原锁内裁决。Store 的显式
SelectVMReplay 同样拒绝关闭中的原操作，覆盖新的重放选择入口。

## 持久和生产顺序

1. 显式管理读取原关闭、原派发及原 Runner 出生；同 VM 快照检查后保存
   唯一恢复选择。普通 Run/轮询不能自行创建选择或授权未发送封存。
2. Runner 持原文件锁，通过已认证原命令和独立选择裁决；may_have_sent
   只观察，取消原命令不得越过发送屏障。封存使用既有原生取消终态格式，
   不改 Active.Abort，不写新 QMP 容量目标，不改 VM spec。
3. vmexec 以 CapacityForOriginal 认证完整原回执、原 UID 和当前实际值；
   包围同 VM 快照检查再次 CheckCapacity，来源逐项相同后才 CAS 落独立事实。
4. 已保存 Settled 后每轮仍重新认证准确来源和原取消见证，接管 floor 未获
   Runner 确认或物理值漂移均拒绝；保存证明不是永久活性许可。
5. PG、VM、Guest 单领域可以独立推进，但开放前仍要求全部领域和完整实际
   预算闭合。当前 CheckStableServiceRecovery 继续拒绝仅有本批 VM 证明的
   类别 16/17；普通 Record/Abort/Finish 继续禁止改写冻结原操作。

网络请求不持进程内 mutex。QMP 与原生取消的线性化仍属于 Runner 原 flock；
持久选择、独立事实和 VM 操作分别由各自的 UID/RV 条件写保护，不比较不同域
Sequence，也不把 opaque ResourceVersion 当数字。取消见证必须不早于选择、
严格早于独立落账；原操作已有 Abort 时允许消费之前已认证封存的原取消来源。

新增字段属于内部持久证据，不新增 PG/Compute HTTP wire 版本。旧严格读者
遇新字段会拒绝；无新字段旧账本沿用原规则。深拷贝隔离观察者、容量及取消
指针，调用方修改返回值不能改已保存证明。当前生产范围仅类别 16/17 的原
ResizeVM；类别 18 永久实际预算及完整后继消费者不由本批自动补齐。

## Mac 验证及证据口径

最终定向 race 按 package/test 去重为 **22 个顶层、122 条含父子记录，
采用用例 0 fail/skip**。部分原日志保留失败子树；机器收据明确选择通过子树，
并用修正后的精确子根替换失败/过期子树，不把原失败隐藏在总体数字中。
仅执行本批新增和直接受影响路径；不重复未改 PG/SQL/Rust 原生矩阵，不执行
官方已有未改服务的大矩阵。最终数量按 package/test 去重，含父子记录不能
称为相同数量的产品 E2E。

本批 journal 定向验证使用真实 Model/Store/codec，覆盖：

- Grow 完成/未发送封存、相同 owner/更高 owner，以及 Shrink 已有 PG/Release/
  Guest 前驱后的完成与封存；原 Unknown、Abort、关闭、旧永久预算逐字保留。
- 已 Settled 原 VM 仅能读取 completed，选择取消和改成 cancelled 均拒绝；
  新独立证明不能补成完整稳定预算、普通 Finish 或 Abort。
- 原请求/双域摘要/Prepared/Guest/Host/出生引用、关闭/序列/owner/观察者及
  取消见证篡改拒绝；指针深拷贝、严格字段、重复键、缺字段和大小上限。
- 选择与落账两处真实 HTTP/clientset/RFC6902 UID/RV CAS：正常、提交后丢回复、
  旧 RV、VM 重建；未知写不返回确认快照，重读保留原操作及实际已持久相位。
- 直接受影响的旧 Replay 选择/严格 wire，以及服务关闭后相同 owner、Key、
  摘要仍不能创建 Replay，拒绝时原模型、VM RV 和选择注解均不改变。

journal 领域输入是明确测试夹具；其中低层 PG/Release/Guest Completed 不代表
真实原生执行。Store 验证是真正本地 HTTP 和条件 JSON PATCH，不能称作 K8s/
etcd 认证。Runner/QMP/Root/PG 协议 peer 的采用场景、写次数和真实 Lease 接管
证据由主任务另行汇总；协议 peer 不能代替真实 Linux/KVM 内存返还。

复现新增与直接受影响 journal 范围：

```sh
go test -mod=readonly -p=2 -race -count=1 ./pkg/dsga/journal \
  -run '^(TestServiceVMRecovery|TestStoreServiceVMRecovery|TestVMReplaySelection)'
```

实际采用结果：

- journal 10 顶层/58 记录，38.694 秒；Grow/Shrink、选择及落账两处 CAS 四窗口通过。
- Runner 新关闭根 11 子场景，31.12 秒；包括有效旧重放、prepared后二次认证、
  may_have_sent 保留、原取消和独立选择。受影响旧 Grow/Shrink 2 根/7 记录、
  原取消/flock 5 根/23 记录通过。Runner 的 typed fake Lease 接管不冒充真实选主。
- 正式 owner 新 VM 五模式 59.87 秒通过；增加旧 floor 明确拒绝后，仅替换重跑
  接管子根 36.54 秒。覆盖未收到、选择丢回复、落账丢回复后真实失租接管、
  已完成、部分效果；SDK重建、来源漂移及原 Unknown/预算保持均通过。
- 直接受影响旧 Guest 六模式 56.68 秒、两个真实接管 82.23 秒通过；PG 五模式
  采用四个原通过子树及修正断言后接管子根 42.56 秒。接管前先证明新SDK恢复
  Pending；PG独立对账可先清该项，而VM仍须等待原Runner确认新floor。
- 四包 vet、相对父提交的最终增量 lint 0；33 个消费者仅编译通过。完整未改
  包的历史 lint 告警保留，不将编译成功称为业务通过。

该 HTTP 测试需要 Mac 允许本地回环监听。PG/Root/Kube/QMP 是明确协议 peer，
正式 Session/选主、文件锁、持久记录、HTTP客户端和 UID/RV 写前路径是真实实现。
本轮没有改PG/SQL/Rust产品路径，复用准确PG17既有原生证据，不重跑无关矩阵。

反向验证使用 Go overlay 只移除关闭 gate；closed-unseen 确定性失败，生产
源码没有修改，随后原源码同项通过。原失败分开保留：初轮并行半写编译、
夹具缺永久预算/Compute身份不符、测试错误要求Runner拒绝发布当前漂移、
旧PG接管遗漏Runner floor及在对账后误断言Pending仍存在。均修正准确前置或
断言顺序，未放宽产品守卫。首轮全量lint漏增量过滤报59项；增量4项零字段/
格式问题修正后为0。采用及失败日志SHA与最终源码SHA均在机器收据固定。

## 剩余业务和环境边界

P1 仍需完整当前实际预算、Host 峰值结清、关闭后晚到 PG 提交的实际代次与
rearm/reopen 衔接、类别 18 永久预算消费者、历史淘汰和后继完整资源组合。
这些仍是实现任务，不能归入仅待 Linux。P2 安全降低 normal 上限及持续
private/background 预算、P3 直接 Agent/Monitor 接线、P4 修改范围内准确 PG17
最终组合、中文归档与远程 SHA 验证继续，以 Neon 当前剩余清单为准。

本批 PG17 实际源码、gitlink 和两份版本锁应保持
`79367196a74eef82e67cac917877d2e6271d3e70`，已核实际源码与 gitlink。
冻结后不主动追新上游；不改独立控制面，不构建镜像，不部署，不合 main。
仅提交自有功能分支，依赖先推送并核验远程 SHA，再更新 Neon 发行绑定。
所有者 AGENTS 修改不混入业务提交。

Linux cgroup/HugeTLB/KVM/K8s 的真实物理返还、规模、性能与目标环境验收单独
保留。PG17 全面完成并远程归档后才停止自动接续，不进入 PG18/PG16；当前
持续目标保持 active，本批单领域完成不改变该停止门槛。
