# Host来源发布与Agent消费：Mac验收（2026-10-27）

本批沿`codex/dsga-autoscaling-20261019`在已发布`4e9656a3`上演进，代码提交
`0d9fc2c9`的完整SHA、13文件摘要、实际用例、原日志和产物在同名JSON收据。
本轮继续使用2026-10-26 17:29:30（Asia/Shanghai）冻结基线，没有fetch新上游。
PG17仍`b078736a4cfdfccce69914feefb5b5e56560c1e8`，控制面仍
`d84c0b73004a7d8cc3e89f3c3f0e3f06cfa8cba9`，未修改其业务、SDK或分支。

## 实际交付

1. 原Node持有新增`Claim`原CM UID。先持久保留完整请求/VM/Pod/前后容量和
   Host开销，再创建保护immutable CM，最后以Node UID/RV CAS发布该UID。
   **发布CAS是Host来源第一次生效的线性化点**；此前CM存在也不能被消费。
   发布升级Node模型v2；模型/单条返回都深拷贝引用，调用者不能改内部原UID。
2. 新`hostexec.Publisher`自行读取原VM WAL、VM Lease、完整Completed Prepare/
   Outside来源、Runner Birth/State的实际QMP观察及原Pod/Node。新分配核当前
   owner、期限、实际before、目标几何和原Runner身份。先容量CAS，后建CM并
   真实GET，再固定UID，最后再核两端来源和原VM；不发PG/QMP/VM spec动作。
3. CREATE回复丢失从尚未发布的同一完整CM恢复；Node发布回复丢失只读原UID。
   一旦Node固定UID，CM消失或同名重建均拒绝，不CREATE替代、不回滚/释放容量。
   Host Lease过期不清原持有；源丢失不能解释为NoEffect或零预算。
4. `budgetclaims.Reader`及实际Agent/最终Runner共同要求Host v2，并读取原Node
   账本，核完整Plan/SourceID/真实CM UID/保护及当前节点容量。旧Host v1仅保留
   历史合同解码，生产消费拒绝；immutable、标签、数字相等都不能代替Node发布。
5. 实际`NewProductionPGWorker`接入只读`hostexec.Consumer`，自动结算Grow的
   ReserveHost：完整原CM引用和Prepare/Host摘要先写VM UID/RV命令，复核来源
   后落Completed。Start/Record未知只查询该原命令/UID，丢源不换来源、不新分配。
   Shrink没有独立ReserveHost步骤，发布者仍可在Prepare完成后提前保留原前后容量。

Agent不增加Node PATCH、CM CREATE或释放权限。Publisher由独立可信Host分配者
调用，读取身份不接受远程调用者传入；它仍不能自行认证配置中的Host开销/域外
上界。Node域须显式引导且完整覆盖其他成本，失败不得自动初始化或重置。

正常跨对象窗口单独处理：CM尚未存在，或原Node已持有同一完整计划但未发布
Claim UID时，只等待，不Start、不完成、不误退出owner。已发布后换UID、坏源、
丢保护或超节点容量仍拒绝。VM Model保持v4，Runner record v1和PG ABI不变。
新Host v2要求读者先升级；真实旧29读者本批实测接受原v1、拒绝v2。

## 实际Mac结果与失败保留

Mac arm64、Go1.25.13，固定只读模块及缓存，新增/受影响测试使用`-race
-count=1 -json`。按“包+完整测试名”取最后结果，最终**20顶层/142含子项通过，
0最终失败/跳过**。逐项用例在JSON收据，不按重复父项累计。不重复未变PG、
HTTP、core225等；不同源码/夹具的已通过用例保留原日志，不冒称都在同一最终
测试二进制中执行。新增Consumer后仅重验受影响Grow和其余尚未执行的扩缩分支。

| 工作区根build日志 | 最终采用的证据 |
|---|---|
| `dsga-host-source-initial-20261027.jsonl` | Runner受影响69项通过；Owner当轮构建失败，不算通过 |
| `dsga-host-source-owner-20261027.jsonl` | Owner35项通过，含预算与新Host发布到扩缩/最终准入；后续同名Grow证据被替换 |
| `dsga-host-source-new-20261027.jsonl` | 新Node发布10项通过；新Host故障夹具当轮失败，不算通过 |
| `dsga-host-source-final-20261027.jsonl` | 新Host发布8场景及父项通过 |
| `dsga-host-source-consumer-20261027.jsonl` | 新Consumer Start未知/丢源及更新Grow通过，Record夹具失败被后续替换 |
| `dsga-host-source-delivered-tests-20261027.jsonl` | Record未知、正常未发布窗口及Node等待类别通过 |
| `dsga-host-source-agent-remaining-20261027.jsonl` | 未重复complete的其余受影响扩缩14场景及父项通过 |
| `dsga-host-source-old-reader-20261027.jsonl` | 真实已发布29旧Host读者拒绝v2 |

首轮Owner有两个测试局部变量未使用，修正后仅重试未执行的Owner。新Host故障
测试的Guest boot夹具UUID不合法，随后修正；另一轮因client-go fake的锁内回调
递归调用同客户端而卡住，精确停止本任务测试进程，保留
`dsga-host-source-final-new-20261027.jsonl`的中断结果，改为回调直接读tracker。
新Consumer Record故障夹具误把压缩envelope当Model，改为真实VM Store读取。
这些是未通过的初始记录，均未删除/覆盖，仅用准确同名后续结果替换。

首次增量lint报告1条测试无效赋值，已修正；格式器首轮缺Go PATH的错误未被
当作成功。最终格式日志为空、7包readonly vet退出0、Linux目标增量lint 0 issues。
准确Agent/Runner双Linux架构4个业务程序仅交叉编译通过，未执行、不是镜像；
最终产物前缀`dsga-host-source-delivered-`，摘要见收据。生产Publisher也由该Go
包和实际组合测试编译执行，尚未接入Scheduler运行入口，不将其称Scheduler实机产物。

## 证明边界与仍须完成的核心代码

测试执行实际Host Publisher/Consumer/Node Store、Agent/Session、Runner本地
WAL/文件锁和Unix QMP协议，并验证到预算/准入的扩缩组合。Kube服务用HTTP/
fake RFC6902 CAS，QMP peer替代QEMU/KVM；Prepare/Outside资格、域外额度、
Host开销及PG/Root内核事实仍是显式fixture。`internal/dsgatest`只供测试导入，
不是生产资格生成器。Node物理内存、真实apiserver/RBAC和Scheduler未被认证。

**核心整体仍未完成，不能宣布完整功能封版。** 新Publisher是可执行的生产组件，
还没有实际Scheduler调用入口；Scheduler全授予/重启恢复、选主/显式初始化、
旧/普通工作负载完整计账、首次VM持有、显式释放/迁移互斥仍须代码。固定
Host账本上限不淘汰持有，不能以达到上限时拒绝代替长期回收功能。

其余核心代码继续：可信完整Prepare及系统剩余/过渡/安全上界生产者、安全降
normal和持续私有/后台约束、首次预算/准入、Monitor/策略/Agent公开操作、取消
未知结清与受控跨boot。已交付的原VM重放不重复实现。控制面独立开发后移植
时须升级其旧SDK565读者，再统一端到端；这些不是“仅待Linux验证”的缺口。

Linux后验另列：真实K8s/Lease/RBAC/节点压力、KVM/NeonVM/部分或异步unplug、
普通页/HugeTLB/实际物理回收、Host/迁移与性能长稳。本批默认关闭，不构建
镜像、不准备或执行部署、不合main、不发布生产。
