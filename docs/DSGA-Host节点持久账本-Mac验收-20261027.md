# Host节点持久账本原语：Mac验收（2026-10-27）

本批代码提交为`6159109676a178c5294d0d78625651e812116714`，基于自有原分支
`codex/dsga-autoscaling-20261019`的`a1a2660ff41b2e781431f7ad5db1e3e387411ff1`。
本轮上游继续冻结于2026-10-26 17:29:30（Asia/Shanghai），没有再次fetch。

## 交付行为与边界

新增`pkg/dsga/hostguard`保存原Node UID下的完整容量持有：原请求、VM UID/
incarnation、Runner Pod UID、前后内存及非零Host开销。单个原VM/Pod按多个
承诺最大值计账；同VM不同Pod分开相加，不假设旧QEMU已经退出。完整请求
改变、同键换上下文、旧高水位、同Pod换PG boot、整数溢出均拒绝。

真实Core/Coordination客户端直接读取固定Node/Lease UID，以Node UID/RV JSON
Patch CAS线性化更新。Lease切主先提升持久floor，保留全部旧持有，迟到旧RV
写不能覆盖新floor。回复丢失不返回完成票据，恢复只读查询原持有。来源丢失、
空值、损坏、同名重建或保护标记丢失均不自动重建。初始化是单独显式操作；
条目/原文/注解大小及序列均有界，达到上限拒绝新持有，不自动淘汰。

**这仅是持久账本原语，尚未接入实际Scheduler。** 域外保留额度只是初始化时
冻结的参数，本代码不认证它覆盖全部普通Pod、Host和旧VM成本。真实Scheduler
全部授予/恢复、选主初始化、可信原VM/Pod/Runner复核、Host来源发布和显式
释放仍须实现。不能把测试中的额度或一份immutable对象当完整Host资格。

本包也没有运行时创建节点finalizer的调用方；后续接线必须完成节点删除/替换
的显式结算，不能靠时间、历史淘汰、Pod消失或Lease过期释放物理持有。

## 实际执行与证据

Mac arm64、Go1.25.13，固定只读模块缓存，执行新增包的`go test -race -count=1
-json ./pkg/dsga/hostguard`。初批5顶层/14含子项、后增模型2顶层/12含子项，
按完整测试名去重共**7顶层/26含子项通过，0失败/跳过**；没有重复未变PG、
HTTP、core225或此前VM重放功能。准确测试名、源码及日志摘要在同名JSON收据。

| 日志（工作区根build下） | 实际结果 |
|---|---|
| `dsga-host-ledger-initial-20261027.jsonl` | HTTP/RFC6902并发CAS、丢回复、接管迟到写、原持有恢复及丢源边界，包PASS |
| `dsga-host-ledger-model-20261027.jsonl` | 完整上下文、别名/boot/方向/溢出、条目上限及严格解码，包PASS |
| `dsga-host-ledger-lint-initial-20261027.log` | 首轮23条静态问题，原日志保留 |
| `dsga-host-ledger-lint-final-20261027.log` | 中间轮1条OwnerReference显式零值问题，原日志保留 |
| `dsga-host-ledger-quality-lint-20261027.log` | 最终Linux目标增量lint：0 issues |
| `dsga-host-ledger-vet-20261027.log` | 最终原生readonly vet退出0 |

静态补正为错误文字大小写、测试辅助参数/显式零值、格式与HTTP状态常量，
未改变生产业务语义，不因此重复原通过场景。质量脚本
`build/run-dsga-host-ledger-quality-20261027.sh`已退出0，禁止为观察状态再执行。

`dsga-host-ledger-linux-{amd64,arm64}.test`是双Linux架构**测试程序仅编译**，
不是Scheduler业务二进制，也没有在Linux执行，不是镜像或平台认证。

HTTP服务器模拟apiserver的UID/RV和RFC6902冲突，fixture明确提供Node、Lease、
域外额度、VM/PG身份和容量；没有真实K8s/RBAC、QEMU/KVM或物理内存。本批
没有声称真实Scheduler容量分配已经被验证。

## 后续实施与平台验收

核心代码继续：Scheduler全授予/重启恢复与选主、完整Host源生产/持续复核及
显式释放，可信Prepare/剩余系统上界、安全降normal与持续私有/后台约束、首次
预算/准入、策略/Monitor/Agent公开操作、取消未知结清及受控跨boot。原VM明确
未发送的显式重放传输已交付，本批不重做。上述是待实现业务，不能归到Linux待验。

Linux后验：真实apiserver/RBAC/Lease故障、节点压力、KVM/NeonVM/异步或部分
unplug、HugeTLB/普通页物理回收、Host完整持有/迁移和性能长稳。PG17固定
`b078736a4cfdfccce69914feefb5b5e56560c1e8`，控制面保留
`d84c0b73004a7d8cc3e89f3c3f0e3f06cfa8cba9`，均未改动；不构建镜像、不部署、
不合主分支、不发布。默认关闭，整体内存伸缩核心仍未完成。
