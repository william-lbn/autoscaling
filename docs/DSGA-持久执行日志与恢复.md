# DSGA 持久执行日志与恢复

日期：2026-10-20。实施标准为Neon仓库V3/R03/R09；代码位于本fork
`pkg/dsga/journal`，公共标记与严格JSON工具位于`pkg/api/dsga`。
这是可信协调器的恢复基础，尚未接入实际伸缩协调循环，**不代表完整DSGA交付**。

## 已实现的职责

同一VM UID、incarnation内只有一个活动意图。持久状态保存完整原请求、领域分离
SHA256、最初owner、全部动作及命令、请求取消意图、16条终态上限、单调高水位和
结果过期计数。历史同时受字节预算约束，可能早于16条淘汰；原意图中的PG boot
和Compute身份不随重连/接管被改写。官方传播Revision仍保持旧进度语义，DSGA
使用完整独立Key，不把同Revision的旧分批资源批准解释成同一业务命令。

每个动作的顺序是：准备完整业务JSON→`Start`原子写入Unconfirmed→得到确认
票据→外部执行→核验组件事实→`Record`。日志本身不发送PG/VM命令。目标实例、
完整命令字节、派发owner、动作序号、请求摘要全部绑定命令摘要；回执不能借用
另一动作/实例/意图，终态事实不能被后一次观察覆盖。凭据不属于命令或审计数据，
未来适配器必须使用明确DTO与独立鉴权头，不得直接保存含凭据的HTTP日志。

增长计划固定为预算准备、Host预留、VM增长、Guest就绪、PG增长、预算完成、恢复
准入；缩小固定为预算准备、排空、PG逻辑收缩、Provider释放、Guest就绪、VM缩小、
实际VM观察、预算完成、恢复准入。前一步未得到核验成功结论，不能派发后一步。
其中Guest/Provider/预算/实际VM事实仍需后续适配器实现，不能用这组枚举代替它们。

取消只保存AbortRequested。已派发但未知的动作须先查询/对账，不允许直接删除；
已有副作用的取消还须完成预算与准入清理回执。新owner只增epoch并保留所有旧
事实，不能继续派发旧owner的剩余正常动作，可以在实际效果查明后进行清理。
未完成活动意图，即使PG换boot，也阻止冲突新意图。期限/租约到期不会自动撤掉
预算、抹去未知动作或宣布取消完成。

`Completed`/`Rejected`/`NoEffect`是**可信协调器已经验证的结论**；本包不验证
回执来源、密码学签名或物理条件。HTTP200、旧Ok、当前PG容量、not-found、单次
RSS均不足以构造完成/无效果。模型测试的数据明确标为state_machine_test_only，
仅证明状态不变量，不能生成生产VM许可。账本摘要也不是PG jsonb context摘要。

## 实际持久化与并发边界

`Store`通过仓库生成的VM clientset执行真实GET/JSON Patch，固定namespace、
name、VM UID与incarnation。Patch测试UID、原resourceVersion和固定模式标记，
apiserver CAS是跨进程线性化点；不跨网络持本地锁，不把RV当可运算整数。
CAS失败不自动采用新对象重试旧动作。即使是幂等无写入，也重新读取检查RV，
避免旧快照将过期owner或结果冒充当前事实。

API错误、写入成功但回复丢失、返回对象损坏/不符，均无派发票据，返回
UnconfirmedWrite；调用者必须重读原意图对账。重新构造clientset后仍可恢复同一
未知动作，不能自动重新派发。成功返回Snapshot/Dispatch为深拷贝，外部修改不会
改变已记录的原命令或快照。票据只表示写前记录已确认，**不是VM执行许可**。

存储使用两个annotation：`internal.autoscaling.neon.tech/dsga-incarnation`与
`internal.autoscaling.neon.tech/dsga-journal-v1`。封装有版本、incarnation、zlib和
原始JSON SHA256，最大192KiB；解压最大1MiB、16384值节点、32层。命令和审计
body各8KiB，任意层重复键、未知/缺失字段、非法UTF8、尾随压缩数据均拒绝。
同时从Kubernetes 256KiB annotation总预算扣除其他键值，活动意图预留4KiB关闭
空间。字节不足只淘汰最老终态，保留活动意图/高水位；仍不足则拒绝新写入，不截
掉未知动作。checksum/压缩不提供身份认证，模型有效也不等于允许发布生产。

初始化是显式步骤，只接受已有准确UID/RV、尚无Runner/重启/删除、初始阶段的新
VM。`Load`遇到缺失或损坏日志失败关闭，不能初始化为空账本。Webhook禁止在
Create预填旧账本，初始化后标记不可移除、incarnation不可改变；旧调试豁免也不能
跳过该检查。这个Webhook只保护模式与存在性，不认证完整日志变迁。实际协调器
仍须实施唯一可信写入者、RBAC和领导权认证。

## 阻止旧路径回退

任一标记存在即视为受管，包括空值、缺半边和损坏值。旧Agent资源Patch、Controller
Scaling分支和旧QMP内存函数拒绝处理受管VM，检查先于旧资源副作用。此批没有
自动打标/开启功能，没有运行中的VM无重启切换。旧二进制/旧快照已发出的QMP、
迁移路径及外部直接连接，仍必须由后续Runner私有通道、持久fencing和实际动作
串行化封闭；这些guard不能撤销在途QMP，不能据此关闭V3/R03。

## Mac验证与证据边界

测试覆盖有序写前记录、回执串用、不可变终态、取消清理、换owner/boot、16条及
字节淘汰、uint64溢出、严格解码、解压炸弹、快照深拷贝。存储测试通过真实
clientset、本地HTTP与RFC6902实现，覆盖写成后500丢回复、并发owner、同名重建、
旧无操作快照、损坏回复、annotation额度及新clientset恢复。它们没有运行真实
apiserver/etcd、VM、PG、Host或Scheduler。

实际反向证据：分别仅移除resourceVersion或UID条件，定向CAS用例均失败。
UID反向还断言新对象的账本被实际覆盖，避免只检查写后返回错误掩盖误写。
脚本finally已逐字节恢复源码；最终完整受影响组合在恢复版本上执行。最终结果、
命令和日志索引见[当前执行记录](DSGA-执行状态与恢复.md)。

## 必须继续实现

- 协调器启动时的显式初始化/加载、唯一owner认证及真实控制循环；Takeover目前
  只持久化上层已认证决定，不检查领导权租约。不是一个自足的选主服务。
- Go实际Compute HTTP消费者、Monitor协议、PG issuer接管fence及领域回执核验。
  不把任意调用者能构造的Observation当成可信数据。
- 完整硬预算/排空/承诺生命周期、Scheduler/Controller/Runner实际执行与恢复；
  最终私有QMP边界和持久串行fencing，跨重启/迁移对账。
- PG其余版本、control-plane Driver、Helm功能配置和真实Neon存储本地E2E。

以上是代码/集成剩余项。Linux/KVM/HugeTLB/物理回收/迁移/K8s/性能认证后验；
本轮不构建镜像、不准备部署、不合主分支，不以缺代码冒充环境限制。
