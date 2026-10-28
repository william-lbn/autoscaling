# Controller的CPU故障与内存状态隔离

日期：2026-10-29，Mac Darwin arm64。准确源码
`4d8033b298bf88b9e101cabd1a799f2517274af1`，沿原自有Autoscaling功能分支。
四文件源码、逐用例、过程失败与最终采用、日志及ELF摘要见
[机器收据](DSGA-CPU故障隔离内存状态-Mac收据-20261029.json)。

## 问题与实现

旧受管Running先查询CPU，Scaling也在CPU失败时提前返回；正式Reconcile随后
不执行Status().Update。因此，即使原Runner已经发布实际内存，CPU HTTP失败
仍会使VM.Status停在旧值，Agent预算结算可能一直等待。这是代码中的领域耦合，
不是Mac缺少Linux环境造成的失败。另有CPU设置接受即当完成的问题。

两入口现共用原Runner内存消费及直接读取的原Pod；身份、Node、floor或原来源
不符仍整体失败并禁止CPU HTTP，不使用cached Pod/IP作为新目标。已核内存
事实先保留；CPU HTTP失败时写专属DSGACPUReady=False/CPURequestFailed，保持
Scaling及原1秒对账周期，返回nil只允许外层正常提交状态，不宣称CPU或伸缩完成。
没有修改VM操作WAL、预算、Host承诺或生产新内存效果。

CPU状态仅展示GET读回，POST目标被接受仍保持未确认；下一轮GET匹配才Ready。
即使CPU已确认，Runner仍有pending时也不能进入Running。普通非受管入口保留
原流程；统一CPU读取补上错误响应body关闭及4KiB回包上限，避免持续失败轮询
泄漏连接或无界读入。关键职责、跨域边界、失败保留和状态语义均有中文注释。

## Mac实际验证

按子树采用4顶层/27条含父子记录，最终0fail/skip；不是27次独立产品E2E。
正式Reconcile调用真实Status().Update，再从client重读实际保存的状态：

- Running/Scaling的CPU查询失败、设置失败，均保存2GiB原Runner内存，保持CPU
  未确认、Scaling和1秒重查，原journal字节不变；不把目标写成实际CPU。
- 合法CPU JSON后跟超量空白必须拒绝，不能因JSON合法而绕过4KiB上限。
- POST接受后第一次仍False；下一轮GET确认后才True/Running，不重复POST。
- CPU已匹配时可正常维持Running；替换原内存State UID时两个入口均失败，
  内存仍为旧1GiB，CPU GET/POST均为0。
- 受影响原内存来源14子场景及普通TestReconcile/TestRunningPod通过，保留此前
  严格来源/退出防线与非受管行为。

first整包因新增夹具Create错误携带resourceVersion失败，8个新增子场景都未
进入业务；该日志保留，不计通过，也不是业务反向证明。修正夹具后新增整组
second包2.687秒通过，含新增超长回包场景；采用其完整子树替换first对应树。
first中已通过且后续语义未变的来源/legacy子树保留，不重复测试。最终vet退出0，
相对b90afa0d增量lint0 issues，diff通过。主要执行命令：

```sh
go test -race -count=1 -json ./pkg/neonvm/controllers \
  -run '^(TestManagedCPUFailureDoesNotDiscardVerifiedMemoryStatus|TestManagedControllerMemoryConsumesOriginalRunnerPublication|TestRunningPod|TestReconcile)$'
go vet ./pkg/neonvm/controllers
```

本次采用两份日志而非声称上述完整命令最终重跑一次；工作区build日志前缀为
dsga-cpu-memory-isolation-*，准确映射及摘要已入库。

## Linux编译和仍缺的业务

准确4d8033b2的Controller已编译Linux amd64/arm64 ELF，均exit0并核架构、VCS
revision和摘要，没有执行。唯一未提交所有者AGENTS导致modified=true；四文件
摘要与提交一致。离线模块stat缓存写入权限警告如实保留，不称无警告构建。

**本批不等于CPU自动伸缩完成。** 受管Guest旧daemon无认证PUT仍被拒绝，不能
为了恢复CPU缩放放开它；原Runner CPU桥接的可信写通道仍是核心代码缺口。
当前修复确保这个失败不会吞掉独立已确认内存状态。认证/持续执行权、自动策略
和全部写者仍须后续实现，不能降格为Linux待测或等待控制面就自动完成。

安全降normal/后台私有持续上界、异常来源关闭、分区/出生前失败/无Wait/跨boot
迁移等仍按核心清单继续；PG17完整归档后PG18→PG16。真实Linux/K8s/KVM/
NeonVM/HugeTLB/物理回收/性能另验。上游冻结、独立控制面边界不变，不构建
镜像、不准备或执行部署、不合main、不发布，默认关闭。
