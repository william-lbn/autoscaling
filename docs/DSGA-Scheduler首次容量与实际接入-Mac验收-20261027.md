# Scheduler 首次容量与实际接入：Mac 验收及后续实施约束

本批完成实际 Scheduler 的节点选主、显式引导、首次容量、重启恢复和 Host
来源发布接线，以及最终 Runner 的首次容量核验。**完整内存伸缩核心仍未完成**；
以下未实现项须继续编码，不能归为仅待 Linux。沿既有自有分支演进，官方基线
保持 2026-10-26 17:29:30 冻结；不改 PG17、独立控制面或其他 PG 版本。

代码提交分为 `44bddd7f2a4f4c35fc22264cde73fb5d115f4764`（Host 基础）、
`55cec6aa`（实际 Scheduler）、`76f8dc05e6fb18da489ac285cc034e47b125c486`
（Runner 最终核验及组合）。本页配套 `DSGA-Scheduler首次容量与实际接入-Mac收据-20261027.json`
保存最终 24 个 Go 文件、日志与 6 个产物的 SHA-256、完整用例映射。后续文档提交
不改变这份代码 SHA；产物的 vcs.revision 准确指向 76f8dc05，modified=true 仅因
所有者保留的 AGENTS.md 本地说明，所有 Go 文件均逐字节对照该提交核验。

## 实际行为及边界

1. `hostguard.Model` v3 固定原 Node/Lease UID、平台配置、系统域上界与完整
   Placements；空列表必须显式为 `[]`。同一原 Pod 只增高水位，不因 stale 注解、
   Lease 到期、Unreserve、删除事件或响应丢失释放。不同 Pod 即使归属同一 VM
   也分别计账；操作持有与同 Pod 首次持有取最大值，Host 开销只计一次。
2. 全局 Scheduler leader 与节点 owner 可以是不同副本。可信副本只可通过原
   Node UID/RV CAS 单调增加容量；节点 owner 才可 Fence/发布。独立 client-go
   选主锁固定原 Lease UID/owner/marker，不在 404 后 CREATE 同名替代。
3. `hostscheduler.Service.Run` 已接 `NewAutoscaleEnforcerPlugin`。节点 owner
   从实际分页 Pod LIST 和原 VM GET 恢复；首次仅用配置固定的原 Node RV 引导。
   账本已存在则核同一配置并 Fence；损坏、丢失或旧 v1/v2 域不自动重建/改义。
4. 实际 Framework.Reserve 在忽略命名空间和本地 mutex 之前核持久容量，未知
   写不允许绑定。事件恢复和旧 VM 批准复用同一容量入口；网络间不持 plugin mu。
   普通 Pod 要求每个 app/init 容器有正数硬 memory limit，按官方 PodLimits
   的 sidecar/init 峰值及 Pod overhead 计账；不接受无界/ephemeral/迁移目标。
   VM 从原 VM 的 Use、实际 memorySize、原批准、本次目标取最大值，不信 Pod
   传播资源注解，不打折 memory overcommit，再加固定 Host 开销。
5. 旧 VM 批准先保留目标，再对原 VM UID/RV 条件 PATCH；冲突/丢回复不清容量。
   受管 VM 通过新 Host 发布链，排除旧批准和自动迁移。当前也关闭其旧 CPU
   批准旁路；独立 CPU 策略接线仍属后续代码项，不能称已完成所有原 CPU 功能。
6. 实际节点循环在原 Prepare 完成后调用已有 Publisher，完成 Node 持有→原 CM
   →Node 固定 CM UID；实际 Agent 只读消费。最终 Runner 在 birth-intent、来源
   CREATE/QEMU 之前，再以自己的 API 读取核原 Pod/VM/incarnation 和首次内存，
   防止直接 nodeName 启动绕过预留。此首次检查不提供后续缩容许可。

## 配置和后续实现必须遵守的约定

`plugin.Config.dsgaHost` 默认 nil，不启用节点循环或改变未配置域的官方行为。
启用配置使用 `hostscheduler.Config/NodeConfig`：原 Binding、Profile、SystemBytes
和首次 InitializeAtResourceVersion。字节数字遵守 DSGA 规范十进制字符串。
Profile.ID 仅引用真实平台认证；非零开销、自报 ID、RSS 或剩余 allocatable
均不证明成本上界。SystemBytes 在 v3 只包含固定系统域，全部其他工作负载另计；
v1/v2 的 Outside 含义不同，禁止直接改版本号升级生产域。

完整分配域要求所有入域 Pod 使用同一 schedulerName，普通容器必须有硬限额；
平台必须隔离其他 scheduler/binding、原地资源变更、直接旧 VM 写入者及特权旁路。
目前客户端并未实现这些全部准入约束，不能仅配置 taint/ID 就宣称生产隔离。
目标环境还须实际认证 Host 系统/Runner/QEMU 开销、内核行为、HugeTLB 和权限。
本批不编制/执行部署清单、不构建镜像。

每域最多 256 条 Pod、256 条操作，Node 原始账本上限 192 KiB/总注解 256 KiB。
超限拒绝且保留原事实；**显式结清/历史收敛尚未实现**，这不是可长期生产运行的
替代方案。释放须证明原 VM 未知动作已闭合、当前预算持续有效及实际资源归还，
还须防止旧来源再次授权；不能只删除 Node 行或依赖 Pod 404/TTL。

## 本次真实验证

Mac arm64、Go 1.25.13；模块保持原 go.mod 版本，不升级依赖。最终按完整
package/test 名字去重 **20 顶层/98 含子项通过，0 最终失败、0 skip**。范围包括
原 UID/RV 并发争抢、另一副本授予、未知回复后只读恢复、容量缩小后拒绝、
首次绑定/原身份、严格 JSON 和 256 行往返、原 VM/普通 Pod 恢复、旧批准、
实际 Framework.Reserve/Unreserve、原节点选主、首次 Runner 拒绝早于写入，
以及受影响 Agent/Runner 的扩/缩/未知原动作对账。

实际 Service.Run→节点选主→恢复/Fence→Publisher→原 CM/Node 发布组合已通过，
没有手工 Publisher 调用代替此用例入口。Kube HTTP/UID-RV、fake tracker、Unix
QMP peer 是明确夹具；VM Lease 在两个测试 API 间的镜像、Prepare/PG/Root 和
平台成本资格也为明确夹具，未把它们计作真实 apiserver/KVM/内核。

六个受影响包 vet 通过，最终 Linux 增量 lint 0 issues。初次两个测试导入错误、
OwnerReference 深拷贝指针比较缺陷、v3 未发布夹具误标 v1，以及 lint 失败日志
全部保留；修复后只替换受影响完整用例，不累加通过数。CPU 测试输入按官方
JSON 单位校正后单独复核。未变 PG17、存储、HTTP 和原 142 项不重复全量运行。

Scheduler/Runner/Agent 六个准确代码 Linux amd64/arm64 ELF **仅交叉编译通过，
没有在 Linux 执行**，摘要在收据。默认 Go 代理限时探测不可达，准确终止该次
下载进程，改用可达公共代理补齐 go.sum 已有校验的锁定依赖；go.mod/go.sum
均未改变。原失败和成功日志分别保留。

日志位于工作区 `build/dsga-host-scheduler-*.jsonl`、`*.log`，权威最终映射为
`build/dsga-host-scheduler-final-audit-20261027.json`。通过收据的 test→log 映射
复用本轮已通过证据；只有源代码改变、故障或新疑点才重跑相应场景。

## 尚需代码与 Linux 验收

继续编码：Host 显式释放/收敛及迁移/跨 boot 结清、其他写入者准入隔离和独立
CPU 联动、可信完整 Prepare/系统剩余/transition/safety 来源、首次预算/准入、
安全 normal 降额与后台/私有持续约束、Monitor/策略/公开操作/取消及受控恢复。
先收尾这些核心项，不扩到独立控制面或其他 PG 大版本。

Linux 后验：真实 apiserver/RBAC/Lease/Node 压力，KVM/NeonVM/QEMU 部分热拔，
HugeTLB/真实物理回收、平台上界/调度域隔离、性能与长稳。独立控制面完成后再
移植协议/依赖并统一 E2E；旧 Model/Host 读者须先升级，不能放宽原来源校验。
本批交付不等于整体封版或生产认证，不合主分支、不发布。
