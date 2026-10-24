# Agent原Guest绑定、资源代次与PG派发门槛

本批沿既有服务分支接入实际Agent，不只是独立HTTP客户端；整体在线伸缩仍未完成。
PG17内核未改，不构建镜像、不准备/执行K8s部署、不合main，默认关闭。

## 运行链与持久顺序

Agent已确认owner Session和短凭据后，先创建长期ResourceWorker，再恢复PGWorker。
首次只读发现完整Guest boot/cgroup身份前，VM完整账本必须没有HWM、活动操作、
历史或过期记录；root也不能已有孤立运行期原命令。初始限额实际读回不匹配则拒绝。
有过操作的旧未绑定账本须受控迁移，不能自动收养当前Guest。

完整ResourceIdentity通过同一VM UID/resourceVersion条件写入原账本，再发送root
fence。CAS回复丢失不产生派发票据；重读恢复已写原身份，不重新发现。已绑定身份
在接管、PG boot改变、历史淘汰及Clone中保留，不能覆盖或清空。新增optional字段
保持新代码读取旧未启用日志；旧代码不认识已写字段时会拒绝，不能无条件降级。
原始pristine bootstrap必须完全没有此运行期绑定；Sequence1夹带绑定会拒绝。

ResourceWorker以长期Session Context耦合取消，短凭据回调逐次检查当前owner。
root floor必须等于已提交owner epoch，接管不清Root Original/Held/未知状态。独立
于SQL连接/PG boot，不因PG不可用重造资源身份。未确认GuestReady完整资源命令从
同一账本恢复到客户端Pending，校验原目标、预算引用、原派发者epoch及完整身份；
调用方自报Pending与原记录不同则拒绝。

固定GET `/dsg/v1/resource/state`返回同一资源锁内的原Bootstrap、Original、Held、
Birth、floor和当前实际限额。资源revision独立于Native PG revision；不能按外层
历史空或PG revision猜计数。View不是原命令完成回执；新PG实际读回不能替代旧
PID原Observe，缺失不是NoEffect。只有原Applied/Bootstrap及当前实际限额一致才
能确认当前约束；未知、占用暂态或限额漂移均保持ErrUnknown和预算，不为暂态
不断换owner/floor。

实际PG执行器在新POST及显式重放前校验资源floor和持续存在的已确认约束；本地
资源Pending亦阻止新PG写。已有PG未知结果仍可原查询，不能因root不确定隐藏原
SQL效果。已持久绑定的PGWorker必须携带同一Session资源客户端，不能省略回退。
这是最低派发门槛，不替代PrepareBudget、Host、GuestReady、排空或VM完整证明。

## 必要Mac证据

go1.25.13 race分批只执行新增/受改用例：去重后8个顶层用例通过，0fail/skip。
包括真实Mac日志/fsync、Root HTTP/TLS客户端、typed Store/Lease与RFC6902；root
内核、Kube服务及前驱预算/VM步骤明确为协议fixture，不是K8s/完整物理链。

| 本机检查 | 结果及去重说明 |
|---|---|
| state-race | View两项、资源身份账本一项，3pass |
| worker-race | 写前绑定、孤立效果拒绝、完整Pending恢复，3pass |
| pg-gate-race | 新PG门槛1pass；含账本复验1项不重复计数 |
| lost-bind-race | 绑定已写但回复丢失时不派发fence，1pass |
| drift/pristine-race | 受改用例各1pass，不相加计数 |
| vet-final | 5个相关包，退出0 |
| Linux目标编译 | 实际Agent、Controller、Runner、root guard ×amd64/arm64，8个退出0，仅编译 |

证据在工作区build/dsga-resource-owner-*.jsonl及各目标*.log；最终源码SHA见
build/dsga-resource-owner-source-sha256.json。编译预检没有执行用例，不计测试通过。
未重复旧PG/SDK/原资源认证/原生SQL测试；同一功能未因日志分批而累计重复数字。

## 仍须实施及Linux后验

本批实际完成资源身份与floor的Agent接线及PG最低门槛。GuestReady的实际资源
派发/原观察领域执行器、完整预算/系统上界生产、首次准入和全排空、Host/最终
Runner-QMP、公开worker/生命周期及跨boot/owner受控关闭仍须编码；不能把资源
View、写入绑定、floor成功或仅cgroup上限当作完整预算/VM完成。

真实Linux root/SUID/cgroup/全部HugeTLB hstate、NeonVM/KVM、K8s身份/RBAC/
服务路由、物理回收/迁移及性能长稳按后续目标环境验收。Mac和目标编译不提供
这些认证。本轮不为后验准备镜像或部署资源。
