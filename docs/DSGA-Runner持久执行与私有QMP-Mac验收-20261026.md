# DSGA Runner持久执行与私有QMP：Mac模块验收

本模块在2026-10-26冻结基线上完成最终Runner**消费/执行/发布**路径，实际接入
Runner启动和Controller状态读取。只在自有`codex/dsga-autoscaling-20261019`
演进；PG17仍为b078736a，未再fetch/merge上游。完整源码哈希、分项日志和双
Linux架构编译产物见[机器收据](DSGA-Runner持久执行与私有QMP-Mac收据-20261026.json)。

**本模块通过不等于整体内存伸缩封版。** Agent的typed ResizeVM/ObserveVM派发、
最终来源结算和真实Prepare/Host生产者仍有代码待接，详见文末；不能归为仅待Linux。

## 实际代码与协议

- `pkg/dsga/vmguard`：原Runner出生、固定Kube来源、单VM文件锁、持久floor/完整
  原命令、私有QMP、不可变回执及持续状态。JSON全字段严格有界，数字不经float64。
- `neonvm-runner/cmd`：原ready启动完成后、QEMU启动前初始化出生；同一随机boot
  派生QEMU UUID，开启实际消费循环。受管模式关闭TCP QMP和manual入口，保留
  私有终止socket并设置0077 umask；普通未启用VM保持官方旧控制路径。
- `pkg/neonvm/controllers`：只用直接APIReader取得原Runner/Pod/Node及私有QMP
  发布，更新实际内存；受管内存不再因spec.Use而调用旧QMP。CPU只支持原sysfs
  模式；缺显式SA、default SA、nil/QMP CPU模式在Pod组装前拒绝。
- Lease名称、marker和完整UID/owner/schema规则集中到journal；owner原API保持
  委托，算法与选主行为不变，避免Agent与Runner复制出不同授权规则。

Agent先把完整原命令通过VM UID/RV CAS写入既有ResizeVM步骤，Runner用自身
InCluster Kube身份拉取，不新增控制面接口、公开memory HTTP或请求自报授权回调。
核验顺序是Lease→完整VM账本→Lease，再核原Birth/状态CM、真实Pod/Node及来源。
Lease UID首次持久固定；同名重建、同epoch换holder、旧epoch均拒绝。提高floor不
清原pending，不改原owner/request/摘要。未来接管仍须独立处理旧未知。

Grow必须已有原Prepare和ReserveHost；Shrink必须已有原Quiesce/ResizePG/
ReleasePG/GuestReady及完整原生物理释放、Root约束/出生来源。原预算、Guest审计
和Host全文摘要均绑定原命令；读取原immutable Outside/Host CM并再次核原UID/
真实Pod/Node。**动作未结清前Host须持有max(before,target)+overhead**，不能先
按缩容目标释放原Host额度。该消费校验不能代替可信Host/预算生产者落实真实持有。

## 写前顺序、不确定性和恢复

1. 固定目录由原Pod状态卷提供，0700子目录由原启动流程安装。出生nonce先以
   O_EXCL写入birth-intent并fsync，再创建/GET确认受保护Birth及State对象真实UID。
   缺WAL、已有出生、同名新进程/新Pod/新Node不自动收养或重新启动QEMU。
2. 每VM独占文件锁覆盖floor、完整prepared WAL、二次来源复核及有期限的同步
   私有QMP IO。状态文件使用O_NOFOLLOW、原owner/权限核验、完整写入、文件
   fsync、rename和父目录fsync；目录未挂载/损坏/序号溢出均拒绝，不自动重置。
3. before的requested和actual必须都等于原B。非no-op先持久may_have_sent，再
   **只尝试一次**qom-set。普通超时、短写、错误回包、取消和丢回复都不变NoEffect。
   私有适配器仅在qom-set写入尚未开始时可给出明确not-sent证明。
4. may_have_sent恢复只读原目标A，不重发。唯一写者、B≠A且只有一次set的条件下，
   requested与实际device size、base+plugged均到A，才证明原写已经执行。部分
   热拔/仍在B继续未知并持有预算。no-op B=A从来不发set，只做读回。
5. 崩溃留下prepared可证明尚未发送，保留not_sent。内部状态机仅允许当前原owner、
   完整同命令/摘要/Key、不变before和新的显式128位nonce重放；生产跨进程显式
   重放选择通道尚待Agent生命周期模块接入，正常轮询绝不自动重放。
6. 完整原结果先创建并GET确认immutable回执，再持久其UID，才允许后续新命令。
   State对象以原UID/RV CAS发布当前floor/pending/实际值。CREATE已提交丢回复
   只能GET恢复同一nonce/全文；Update未知也不据此制造Completed。

每次私有连接核原QEMU UUID、唯一vm0/原memdev、原min/max/block几何；观察
requested-size与实际size及summary分别取值，不能将请求量当实际量。语义参照
[virtio-mem维护者用户指南](https://virtio-mem.gitlab.io/user-guide/user-guide-qemu.html)
和[QEMU QMP协议](https://www.qemu.org/docs/master/interop/qemu-qmp-ref.html)。
本批没有升级/下载QEMU或改冻结清单。

## 实际Mac验证

Go1.25.13、Mac arm64、race、readonly模块和已有依赖缓存。**去重20个顶层/
110个含子项完整测试名通过，最终0失败、0跳过**；后续同名复核替换旧证据，不
累加。未变PG/HTTP/Provider/准入测试未重跑。

| 范围 | 去重通过（顶层/含子项） | 关键断言 |
|---|---:|---|
| Runner文件状态机/私有QMP/生产Kube消费 | 14/78 | 单次set、真实Unix socket、丢回复读回、接管旧未知、锁覆盖IO、prepared显式重放、no-op零写、严格JSON/整数/序号和权限、Pod/Node/Lease/CM替换拒绝、完整扩缩容来源 |
| Controller读回、旧副作用隔离与实际Pod组装 | 3/16 | 直接API来源、实际内存、错误原UID/floor拒绝、Downward身份、原Resource解码、状态卷、移除公开QMP、拒绝SA/CPU模式 |
| 共享Lease规则合同 | 3/16 | 原名称/全绑定规则保持、缺失/外来/畸形/重建Lease拒绝、跨journal读取换Lease拒绝 |

新增组合用例同时覆盖状态/Birth/回执CREATE已落库丢回复、QMP set已执行丢回复、
State Update已落库丢回复，原回执只产生一个内存写。Host源失联不挡旧may-send
动作只读恢复；但不能据此结算外层预算。Shrink短Host持有、PG物理未验证、Root
限额漂移、错误原生fence均在内存写之前拒绝。

真实Mac文件锁/fsync/rename和Unix socket不是mock；**Kube API/UID-RV服务、
QEMU/KVM、Prepare/Host资格与PG/Root物理来源是明确协议夹具**。不称为完整
Neon端到端，也不把这些结果外推Linux热拔、Host真实预留或生产可用。

readonly vet覆盖vmguard/journal/owner/Controller，最终Controller补检也通过。
仓库Linux目标增量golangci-lint v2.4.0最终**0 issues**，未关闭既有规则；补齐
显式零值、测试IO检查、CREATE/GET错误链、导入/八进制格式和清理说明。质量
补正后只替换受影响的3顶层/21含子项证据，不扩增20/110。准确源码双架构
程序重新编译，原产物和原lint问题日志保留；机器收据同时固定前后哈希。
Runner、Controller的Linux amd64/arm64均编译成功（buildvcs=false，源码逐文件
哈希固定），未执行ELF。首次默认Go缓存受权限限制，未进入业务；改显式已有
GOPATH。首次Controller夹具漏注册core Scheme而中止，修夹具后通过；两份原
失败日志保留，未冒充通过。生产树哈希描述交付树，不追溯宣称早先测试二进制
拥有完全相同测试源；受影响回执/缩容/Pod用例已分别补检。

## 尚须核心代码和目标环境验收

仍须实现：Agent typed VM写前派发与当前Root/PG复核、原UID回执结算及长期VM
来源绑定；可信完整Prepare/剩余系统/过渡/安全资格生产者；真实Host/Scheduler
持有/释放；显式Runner重放传输、取消/未知结清；安全normal下降、首次准入和
受控跨boot/完整公开生命周期。本模块不能使用临时自报CM补齐上述来源。
独立控制面业务/SDK仍未改动，等另一任务完成移植后统一E2E；其他PG版本暂缓。

Linux后验单列：真实apiserver CAS/RBAC及领域CM写权限保护、显式Runner SA绑定；
实际QEMU/runas/私有socket权限和KVM启动、virtio-mem异步/部分热拔与连接故障；
cgroup/HugeTLB/真实物理归还、Host预留/迁移互斥、性能与长稳。当前emptyDir只
在原Pod内保存状态；Pod/WAL丢失由受保护Birth阻止重新出生，不能声称已经实现
跨Pod/Guest受控恢复。没有构建镜像、准备/执行部署、合主分支或生产发布。

下一批只关闭上述核心缺口；不重新追上游、不重复无变化功能。整体Goal继续，
本页只确认这个模块及明确边界。
