# Host关闭水位与取消请求防重放：Mac阶段验收

代码提交`4a8b9cd609770add0c3d0083e670f6862039d0ba`，继续自有
`codex/dsga-autoscaling-20261019`。官方基线保持2026-10-26冻结值；本批只改
9个Go文件，不改PG17、独立控制面或其他PG，不构建镜像、不部署、不合main。

## 问题及最终行为

VM永久BudgetBoundary能证明当前预算成功后直到较新取消Key都没有容量效果，
但原Host结清仅保存预算所属Key。若不把整个关闭水位传到Node，较新但已关闭
的Key仍可能被旧队列或迟到授予再次占用容量。只删除一个Hold不足以解决问题。

本批将原预算与关闭水位分开：Settlement保留原Request、Host CM UID、Runner
来源、预算序列/摘要，另附ThroughKey和ThroughSequence。它们只能从原VM已
验证的永久边界派生，不从当前before、超时、普通NoEffect或HWM推测。

Node同UID/RV结清CAS保存该水位，移除水位以内非当前预算的持有，保留当前
来源及所有更晚在途承诺。Reserve永久拒绝水位以内Key，包含取消Key本身及
相同epoch/revision换operation ID的别名。同预算只允许关闭链前进；新成功
预算的Key与序列都必须晚于整条旧关闭链。CAS丢回复后重读，不补发旧授予。

原VM关闭证明先于Node CAS出现时，只读当前预算可核同原预算的旧Node来源；
不能因此取得执行权限，执行消费者继续拒绝已结清来源。GC在水位持久后才能
清同VM/incarnation/Node/Pod的失效Host CM，范围包含Through本身的迟到CM，
保留当前预算和其他Pod来源。退休流程保持新版本，不因另一个Pod退出而降级。

## 兼容性与Mac证据

有Through证明的Node升级v6；原无Through结清输出原JSON形状，不写空Key污染
旧v4/v5数据。旧v5版本门槛拒绝v6。VM仍v6、Host来源仍v2、Runner record仍v2，
PG17 ABI不变。独立控制面冻结d84c0b73，后续统一移植相关读者并执行E2E。

完整机器证据见[收据](DSGA-Host关闭水位与取消防重放-Mac收据-20261027.json)。

| 验证 | 实际结论 |
|---|---|
| Host模型与Scheduler | Mac `-race -count=1`通过；含版本/持久水位、原预算保留、未来持有、回退/别名拒绝、丢回复及Node v6退休不降级 |
| 实际Agent/Runner/Publisher组合 | 正常成功、完整容量后取消、真实历史字节淘汰后的取消结清通过；后者新增迟到Reserve拒绝与Through本身迟到CM的真实GC |
| 去重用例 | 36项顶层、123项含子场景，通过123、失败0、测试用例跳过0 |
| 无独立测试文件的包 | budgetclaims被Go记为包级skip 1；明确保留。其真实读取由组合路径覆盖，并通过vet，未当作独立用例通过 |
| 旧版本门槛 | overlay使用前一提交fff41693的model.go原文，确认v5接受、v6在版本门槛拒绝；未运行整套旧服务 |
| 静态检查 | hostguard/hostexec/owner/hostscheduler四包增量lint为0 issues；加budgetclaims共五包vet通过 |
| Linux可编译性 | 准确4a8b9cd6源码的Agent/Scheduler/Runner/Controller，amd64与arm64共8个ELF编译成功，核内嵌VCS SHA及架构；**未执行Linux二进制** |

保留初始失败：lint的空Key显式字段及错误字符串大小写；组合在Node v6结清后
发现旧v4夹具的waiting Pod根本不存在。已补原UID/保护标记的Pending Pod；
v5/v6授予前重读Pod和终态的生产检查没有放宽。只重跑变动/受影响场景，未改
PG、Rust和其他已通过用例复用原记录；GC补验只重跑对应组合。

主要复现命令（锁定Go1.25.13及Mac SDK，autoscaling仓库）：

```sh
go test -mod=readonly -p=2 -race -count=1 -timeout=3m -json ./pkg/dsga/hostguard
go test -mod=readonly -p=2 -race -count=1 -timeout=3m -json ./pkg/dsga/hostscheduler ./pkg/dsga/budgetclaims
go test -mod=readonly -p=2 -race -count=1 -timeout=2m -run '^TestProductionAgentRunnerShrinkRechecksGateAndOriginalObserve$/^(host-settle-after-cancel-history|host-settle|abort-committed)$' -json ./pkg/dsga/owner
go vet -mod=readonly ./pkg/dsga/hostguard ./pkg/dsga/hostexec ./pkg/dsga/owner ./pkg/dsga/hostscheduler ./pkg/dsga/budgetclaims
```

## 当前实现边界与后续必做

本批是取消结清所需的Host持久防重放机制，**没有完成全部部分效果取消**。
真实组合的Through生产者来自原VM已实现的零派发取消边界；纯Node测试的
其他关闭证明是明确领域夹具。不会因为Node接受Through，就给VM的部分效果/
普通NoEffect补造关闭证明。已有完整容量后取消仍按前一批真实协议执行。

尚须代码：原预算Runner完成回执与后来的不可变未发送取消回执/实际值共同
认证、已证明无容量变化时的原预算边界恢复、完整准入/Host/来源结清；PG/
Guest/VM部分效果的真实重整预算；异常启动/跨boot/迁移及完整来源生命周期；
可信Prepare和剩余/过渡/安全上界生产者、首次/normal预算及开放；持续约束、
全部相关写者/CPU、Monitor/策略/公开生命周期。缺失实现不能归为Linux待测。

Mac运行真实Go/HTTP/TLS/JSON PATCH UID-RV/选主/本地WAL及Runner/QMP协议、
Host Publisher与Scheduler；PG/Root内核、Prepare/平台资格、Kube及QEMU为
明确协议夹具。Linux/K8s/RBAC/Lease/GC/CRI/真实故障与规模延迟、NeonVM/KVM/
virtio-mem/cgroup/HugeTLB/物理回收及内核残留资格、存储/性能/长稳，均未
验收；控制面移植后再完整E2E。功能默认关闭，整体核心和完整取消尚未封版。
