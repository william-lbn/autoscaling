# Scheduler 启动恢复：普通 Pod 完整占账与退休事件

现场 Scheduler 在新增 Host 预算插件初始化时遇到旧 DiskPressure Pod 的 ContainerStatusUnknown，不能完成 initialEvents，健康接口尚未启动便被原 liveness 重启。旧新正式编译 profile、SDK、K8s依赖、镜像 Config 相同，不能归因于缺失 linker flags。此次只修内存预算直接初始化路径，不改 probe，不新增整机内核证明。

## 安全行为

- 原 ordinary 行存在时，以原 UID、Namespace/Name、Node 和配置域、完整有限 app/init/sidecar、Pod/Profile overhead 及原 Lease→Node→Lease 校验确认。完整保留原水位，不把 Unknown 当停止，不减少额度、不新授予。
- 已绑定 ordinary 的原保护仍在、无退休意图但缺行时，两次 fresh Pod 绑定 UID/RV/Spec/owner/finalizer，按完整硬限及 overhead 单调补占账；复用原 Node UID/RV、多写者 CAS、256行、capacity、overflow 和 typed Unknown。新 UID 独立相加；不生成终止票或释放。managed/migration、缺硬限、漂移、过期都保持拒绝。
- 已经完整退休的 ordinary 初始事件，必须经原 InspectRetirement/InspectRetiredPod 的 Binding/Origin/Node序号/摘要/noRow/noHolds，以及当前配置、Lease 和 fresh Pod 全部确认。返回 canonical Pod 与 nil allocation；不能伪造零 Placement 或复活旧行。同步初始化成功门保留，未放行半初始化状态。

不改 Host Model/公共协议字段、PG/Guest 限额与证明、VM/migration 边界、Filter/Reserve、health/probe 配置。只需更新 Scheduler 镜像；其它严格读者不需因新增私有函数重复构建。

## 实际验证和失败归档

最终生产源码一致，共 12 个去重根、57 PASS events，来自两个明确批次：a97f9f 的 10 Host 根/51 PASS，与 22ce04 的 2 Plugin 根/6 PASS，不冒同一轮 12 根全绿。

a97f9f 使用原 reserveHost 签名，真实旧调用方先编译后行为 RED。其 GREEN 原 whole FAIL 保留：唯一正例在 NoError 已通过后，fixture 内 metav1.Now 的 Go monotonic 信息与真实 HTTP JSON canonical 时间不同而完整 Equal 失败。只将 fixture 末尾用原 typed Get 规范化，保留完整 Equal；生产五文件和 Host 测试逐字不动。22ce04 最小复验两 Plugin 根与正式 Scheduler，0 skip/0 OOM。没有用 undefined 新 API 当 RED，没有把历史失败改成成功；race detector 未启用。

最终目录：/data/neon-build/validation/scheduler-retired-caller-canonical-fixture-GREEN-20261104T215447Z-22ce04。
最终收据 SHA：ad0e4307d913e5295ff089f0ddfde7f483750e985a569abcdb270287ce95f967。
Source-lock SHA：5fa2681be28000ee62ec4199e642723daa5e41698c656ee82d20e7120f5e2a11。
正式 Scheduler SHA：babc421033d7f6c70ba49d2a6e18f98f2c79a560694187f286af66d1446000d5，68559032字节；Go1.24.3/CGO0/原 trimpath/buildvcs=false/-s -w，实际 SDK 3CPU/3Gi/swap0，最终原 unit dead/exit0/reconcile0。7 owned=5生产文件+2测试文件，未混入历史 native WIP。

## 现场范围和未验收项

当前三 Node 初始化输入 57 项：41 active、11 Unknown 已有完整行、2 Unknown 原保护缺行、3 完整退休。每类均按其实际原记录和完整上界判定；无现场 managed/migration/unscheduled 数据时只陈述原代码路径，不伪造覆盖。

这是组件源码与定向测试门，不代表 Scheduler 已实际 Ready。新图尚须 118 薄包、准确 OCI、SSH 117/119、实际 k8s.io/CRI/PIN、仅 Scheduler Helm 升级及真实 Ready。当前 93bad VM 仍 held，未 Seal/PG，PG17 Grow、安全 normal Shrink、Regrow、自动策略四项 E2E 仍 0/4。旧 Unknown 账本和全部失败证据保留。
