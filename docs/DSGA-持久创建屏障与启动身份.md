# DSGA持久创建屏障与启动身份

2026-10-21，V3增量。当前是API/Store/准入/Controller/Agent的实际保护模块，
**Driver创建流程尚待接入，完整内存伸缩仍未完成**。字段默认nil，不启用任何
现有VM，不构建镜像或执行K8s部署，不合主分支/发布生产。

## 持久状态与不变量

`VirtualMachine.spec.dsgaBootstrap`从CREATE即存在，含版本1、固定incarnation、
固定ComputeInstanceID、ready、VMUID及ConfigSecretUID。创建时ready=false，
两个UID为空，随机身份已经持久化；实际VM UID由apiserver分配。普通legacy VM
字段为nil，且没有DSGA标记。已有对象不能补加/删除字段，ready后整个记录不变。
不能在线给Running VM打标，也不能因同名重建或日志丢失再造一套空历史。

held不允许任何Runner/Node/重启、资源/revision、网络/SSH/TLS分配事实。允许
普通metadata、finalizer及初始Pending状态。Controller在删除清理后、默认值/
IPAM/Secret/Runner创建前检查；`doReconcile`自身也保护，避免未来重构绕过入口。
初始或DSGA对象用5秒直接API reader复核缓存中的固定UID，拒绝收养同名新对象。
一次检查不是跨对象原子执行票据，最终资源域的fencing仍必须完成。

## 初始化、解除与丢回复

可信Driver先用实际VM UID/准确RV调用`journal.Store.Initialize`，只向原held
VM条件写入完整日志和incarnation标记，保留其他metadata。初始owner epoch及
sequence均为1。held期间不能Takeover/Begin或派发，初始化本身不构造资源许可。

安全Compute配置持久化并按同UID读回后，Driver以实际Config Secret UID调用
`ReleaseBootstrap`。它先解码同一原完整日志，要求原始epoch/sequence、无HWM/
Active/History/ExpiredResults，再以VM UID、RV及整个原held记录做JSON Patch
条件写，同时固定VMUID、ConfigSecretUID和ready=true。不得在内存中清空历史。

写入丢回复返回`UnconfirmedWrite`且不返回可执行成功票据；调用方只重读同UID
原事实。同一绑定已ready且ConfigSecretUID完全相同时可确认已有事实，不再PATCH，
不改owner/日志，也不将其当VM、PG或预算成功。不同Secret UID不能借幂等入口通过。

新PGWorker即使HWM尚空，也必须先核对创建时固定的ComputeInstanceID，不能先
调用凭据/HTTP发现再等Begin才拒绝。真正空历史允许首次PG boot协商；有HWM仍
沿原boot及SDK Pending，后续跨owner/boot受控关闭协议不能用重造Client替代。

## 准入与配置核对

Webhook的创建屏障、完整初始化日志与owned Secret检查在旧debug skip之外。
Controller也直接读取同namespace固定`<VM名>-config`，核对实际Secret UID、
唯一控制器OwnerReference的VM UID/名称/版本、业务归属标签、未删除以及原生
v2配置的VMUID/ComputeInstanceID。唯一`journal.Inspect`复用有界解压、校验和、
完整模型规则，不在不同组件复制日志解析器。Secret或历史不明就停止，不回落legacy。

配置核对只承认启动身份，不伪造完整R02预算。安全启动规格仍由生产Compute原生
规范验证；全部运行时私有/并行/DSM/维护/后台预算及quiesce继续实施。Kube API
权限与Driver实际调用链由目标平台配置，不能把本机fake reader当RBAC认证。

损坏对象删除时，仅完全不改变spec/journal/status的metadata清理可移除finalizer，
避免创建屏障阻止清理；删除分支不允许解除屏障或启动Runner。

发布CRD增加原生CEL：屏障不能事后加/删、身份不变、ready不可回退、两个UID须
同时绑定，held不得写入Pod/Node等资源事实。它补充Go/Webhook保护，不能撤销已
发送的外部动作。实际混合版本、RBAC、admission与Controller发布仍须整链验收。

## 生成与Mac验证

按仓库原锁定的`controller-gen v0.16.5`在Mac原生生成DeepCopy及CRD，未运行
含Docker的旧`make generate`。嵌套API字段由现有生成clientset正常序列化，
无新REST方法。格式化→生成两轮必须逐字节一致。

一致性检查曾发现GoDoc会把空串引号改成Unicode排版字符，使后续生成的CEL失效。
现改用`size(...) == 0`，并验证格式化/生成幂等。先前有效但不可重现的生成物不
作为最终发布依据。实际发布YAML经锁定Kubernetes0.31.7的原生CRD定义验证器、
CEL编译/成本检查和解释器运行，正向释放/运行及11类不安全变更逐项验证。

五项实际生产故障注入：去掉释放UID条件、RV条件、完整输入准入、发布CRD held
规则及初始CIID检查，均应被对应业务断言捕获，不能用编译错误充数。UID夹具刻意
保持opaque RV相同，检查客户端即使因回包不符报错也没有已经写坏同名新VM。
finally精确恢复、源码hash核对后才运行最终全量。当前schema修复后另重验该反向。

最终Mac受影响全量race为350个pass事件（包含父/子用例），14个有测试包通过、
10个无测试文件包单列，0用例fail/skip；带native标签的同范围lint为0问题。
源码hash与最终执行完全一致。格式化/生成两轮逐字节一致后，发布CRD反向另重验，
再执行上述全量最终回归。真实Go→TLS→Rust Compute→SQL→准确PG17
`0806b4ea3229255b3ec25ea791b128a05b5bdd17`门禁1Rust外层/2独立Go race进程
9.80秒通过、零skip/ignored/filtered，实际包括持久初始化/释放、epoch2 owner
循环、2048→4096及丢回复对账/显式重放。Kube与非PG领域仍为协议夹具。
根工作区日志入口为`build/bootstrap-final3-*`、`bootstrap-negative/`、
`bootstrap-owner-binding-*`、`bootstrap-schema-stable-negative/`及
`bootstrap-final3-native-go.log`。Controller/Agent Linux交叉编译及既有Controller
functests程序编译不是Linux执行或实际envtest认证；没有Mac独立apiserver运行证据。

## 后续代码与目标验收

Driver必须实际按“CREATE held→同UID身份/安全Compute spec/Secret ownerref与完整
journal→准确UID/RV释放”接线，并核对目标CRD/组件支持，不能只调用一个独立库。
当前Control-plane还固定旧f60模块，后续先推自有Autoscaling依赖，再对齐模块并
补真实Driver、签发器对创建身份的绑定及完整中心→Rust/PG端到端。

完整预算/Monitor/Scheduler/最终Runner、受控恢复、多PG版本和真实Neon存储E2E
继续编码；这些不是只待Linux测试。Linux K8s/NeonVM/KVM/HugeTLB/物理回收/
迁移与性能长稳按V3后续目标环境验收，默认关闭与生产发布门槛保持。
