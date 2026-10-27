# Scheduler 有界轮询、历史过滤与 GC 收敛

代码 `521537186a337cefaeb3ce298ee09ea3e6aca36c`，继续既有
`codex/dsga-autoscaling-20261019`。本批只改两个生产文件和两个测试文件，
不改 PG、协议版本或默认启用状态，不重新同步冻结后上游。

## 实施约定

- 初次引导仍须完整恢复有界分配快照，一次 Node CAS 提交；不能分批授予
  初始容量或把未知初始化成空域。256 条活动账本上限不变。
- 运行期每轮最多发现两个 256 项页面、尝试 16 个 Pod，保留未尝试队尾。
  单项调用前出队；即使它耗尽当前 ctx，下一轮也先服务保留的队尾。失败项
  下一次发现时再试，不能每轮总从同一失败 VM 重新开始。
- 历史 Pod 只有同一原 Node v5 已确认结清、完整原关闭锚点一致且已无 Pod
  保护时才跳过；终态、缺行或 404 均不能单独证明关闭。已确认历史不再被
  计入活动 256 条上限，也不逐 Pod 重做 API 读取。
- API 快照过期只重置发现游标，容量/floor/原 WAL 不动；分页与排队信息
  都是可丢调度优化，重启不会丢预算，写前仍真实读原来源并 UID/RV CAS。
- 同一已结清预算的 GC 调度缓存精确匹配完整水位，最多 256 项，一分钟后
  重扫迟到垃圾。改变水位、缓存丢失或时钟回退立即重核。该时间只控制扫描
  频率，不是容量 TTL、不授予执行许可、不降低任何持有。
- 单节点 reconcile 原子互斥，不跨网络持 Mutex/PluginState.mu；不同节点
  与 Framework Reserve 不共享这个工作队列。真正 owner/Node 验证保留。

## 实际 Mac 证据

Go1.25.13、Mac arm64，最终去重 **8 顶层、16 含子项通过，0 最终失败、
0 skip**；两个受影响包 vet 通过，最终 Linux 增量 lint `0 issues.`。

覆盖确定性取消慢项后服务队尾、512 条关闭历史分三页后仍可授予新 Pod、
410 恢复不重置原账本、GC 精确水位/过期/时钟回退/缓存上限；实际 Agent/
Runner/Scheduler 缩容闭合后跨真实 ticker，namespace LIST 在两次确认后
稳定。受影响完整引导、实际选主和普通 Pod 结清仅选择三项复核。

512 条历史为基于一次实际 Node 结清的协议夹具，不是 512 次物理退出；
typed HTTP、race 与实际协调路径执行，PG/Root、QMP peer、Kube及平台资格
仍按原组合明确使用夹具。未把本批计数叠加此前重叠测试。

原失败保留：抽取后未使用 metav1 import 导致首轮未编译；两个新增断言漏算
保护 PATCH 后必须执行的真实 GET，修正断言未取消生产读回；lint 补显式零值
ListMeta 字段，未改变行为。完整最终用例→日志映射见配套 JSON。

准确 52153718 的 Scheduler Linux amd64/arm64 两个 ELF 已核版本/架构/摘要，
**只编译，未 Linux 执行**。modified=true 是所有者 AGENTS.md；该文件未改动
或暂存。只构建受影响 Scheduler，复用未改服务及 PG 的既有证据，不构建镜像。

工作区 `build/dsga-scheduler-work-test-audit-20261027.json` 与
`build/dsga-scheduler-work-final-audit-20261027.json` 保存最终映射、4 源码及
2 产物摘要；准确内容镜像本目录配套收据。单项复现可运行：

```sh
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 \
  ./pkg/dsga/hostscheduler -run '^(TestSchedulerWork|TestSettledScanCache)'
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 \
  ./pkg/dsga/owner -run '^TestProductionAgentRunnerShrinkRechecksGateAndOriginalObserve$/^host-settle-scheduler-loop$'
```

## 边界与后续

有界轮询不承诺所有规模/负载下的完成延迟。真实 API QPS、分页/410、持续
变动、选主和高密度节点进度/长稳须在 Linux 验收；分页发现是提示，不能
充当实时全域容量事实。真实权限/CRI/GC、Host 残留成本/物理回收/KVM/HugeTLB
仍未认证。

取消/失败与无 Wait 异常受控恢复、迁移/跨 boot、其他写者/CPU，可信完整
Prepare/预算、首次及 normal 准入、后台/私有持续约束和 Monitor/策略公开
生命周期仍须代码。**本批修复轮询及扫描缺陷，整体核心仍未完成。**
维持默认关闭，不改独立控制面/其他 PG，不部署、不合 main、不发布生产。
