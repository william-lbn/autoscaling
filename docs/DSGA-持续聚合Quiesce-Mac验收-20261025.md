# 持续聚合约束下的实际Quiesce与Shrink派发

沿既有 `codex/dsga-autoscaling-20261019` 增量演进。fresh fetch确认官方main
a4bce09e完整包含；其他相关维护五线均missing0，未重建/rebase/强推分支。
只做PG17，默认关闭，不构建镜像、不准备或执行K8s部署、不合main。

## 生产代码及兼容边界

新增 `budget.Quiesced` 与 `quiesceexec.Executor`，接入实际Agent/PGWorker。
只接受原VM账本完整Completed PrepareBudget，原Guest/PG boot/Runner/当前owner
及完整请求必须一致。先核验原root聚合约束与真实出生、fence PG，再以独立准入
revision构造关闭命令；必须UID/RV CAS写前持久取得票据，才发真实SetAdmission。

Setter成功不等于Postmaster ack或前台排空。正式DSM成员内嵌一次原生采样的
Prepared1/Connection1/Work2；核验原关闭revision、当前floor/容量、私有管理入口、
持续公共连接阻塞、排除观察者后前台idle。2PC/后台/DSM/会话私有成本不补0，
由未撤销的原PG计费域+整Guest池约束持续覆盖；不暂停检查点和必要后台。
其他计费域及系统成本仍必须由完整Prepared受信生产者覆盖，结构本身不认证。

本策略只支持保留或提高原normal memory.max。降低normal额度所需的可执行
后台/私有上界及安全降限协议**仍未完成**，不能用当前RSS低、前台idle或OS OOM
来冒充实现。Quiesce在任何关闭前拒绝该模式，GuestReady也在新派发及显式重放前
再次拒绝降限；旧未知的只读查询始终可达，不自动撤销Held/原预算。

关闭写前/写后丢回复保留Unconfirmed；普通循环仅查询原命令，NotFound不自动
重发。PGWorker从原步骤恢复关闭方向及SDK Pending；显式ReplayQuiesceOriginal
与最终开放/资源/Resize重放分开，只发送当前owner原持久命令。新owner只读旧
关闭，不换epoch/revision。原关闭/资源身份不因PG重连或本地对象重建而消失。

ResizePG新Shrink派发与可能形成新效果的显式重放，必须重新验证原Quiesced、
当前原root来源/出生和新的同点PG观察。资源替换或门禁重开不能借历史Completed
放行。PG逻辑提交后仍保留ReleasePG/Guest/VM/Host/Finalize步骤；无VM许可。
旧未携带本合同的领域fixture/历史不被自动补写成新证明。root协议、PG ABI、
Neon SQL1.15不变；未绑定资源的旧测试/非生产路径不据此获得受管VM能力。

## Mac实际执行

1. 新来源消费和实际执行器/受影响GuestReady、最终准入恢复共5顶层/31含子项
   去重PASS，0测试跳过。quiesceexec无独立test文件，初轮1包级skip如实保留；
   实际执行器由owner的真实Session/TLS/双客户端/typed Store与UID/RV CAS调用。
   覆盖非零保留成本、关闭回复丢失重建、写前丢回复/NotFound/显式重放、前台忙、
   Postmaster未ack、root/Runner漂移、预算不完整、降normal拒绝、并发一次派发、
   关闭后正向ResizePG派发及重开后拒绝。物理释放仍为下一独立Planned步骤。
2. 首轮发现新执行器局部err遮蔽：真实关闭503后保持Unconfirmed，但上层误得nil
   错误；已修复。另一个单位fixture把normal提高到512MB却仍用256MB Guest，
   正确被拒，修正仅测试容量。仅替换失败项和增强后的门禁漂移项，再单独增加
   正向/降限/并发/预算拒绝场景；已过等价测试不重复。首轮失败日志完整保留。
3. readonly vet预算/Quiesce/PG/资源/owner/实际Agent退出0；实际Agent Linux
   amd64/arm64交叉编译退出0，未运行ELF，不是Linux或镜像认证。
4. 新原生门禁1项：Go race→本机TLS→生产Rust Router/JWT→Neon1.15正式SQL→
   准确PG17 `5462f1de54a97b9a9235c69d64d31cb0825dcea5`。实际fence后关闭1次，
   正式同点观察确认门禁及前台idle，同时真实2PC槽1、DSM pinned 2MiB/4MiB额度
   非零；新Quiesced消费通过，降normal拒绝、零Resize POST。root/外部成本在
   此门禁明确为模型，实际WAL/CAS由第1组覆盖，不能相加声称全栈集群E2E。
5. 原生首轮测试漏调用生产执行器已有的fence，native如实返回Stale；测试补齐
   先fence顺序后仅替换失败1项通过。产品权限/协议未放宽。受影响Rust测试
   clippy `--no-deps -D warnings`退出0，remote_storage依赖原27告警保留。

实际日志与配方在工作区 `build/dsga-quiesce-*`；Neon机器收据列精确哈希。
新Go native binary来自提交前业务工作树，不宣称发布制品或VCS标签认证。
未改PG/Native DSM实现，不重复原8/core225/isolation5和未改HTTP测试。

## 完整功能仍需的工作

本批完成的是持续聚合策略的Quiesce/新Shrink派发链，不是完整伸缩交付。完整
可信Prepare/Finalize和SystemOutsidePG来源、可降低normal的后台/私有预算与
安全降限、首次准入、Host/最终VM/公开worker和受控跨boot仍需代码。
Linux cgroup/HugeTLB/内核暂态/PTE/THP/物理回收、KVM/NeonVM及目标性能长稳另验。
保持原Held、未知成本和所有平台门槛；Goal active/incomplete，不用“Linux待测”
掩盖缺少业务实现，也不用此有限策略代替V3最终要求。
