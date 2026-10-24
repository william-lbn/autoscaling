# 认证Guest资源通道与Mac基本验收

本批是V3的实际运行通道增量，不是整项在线伸缩交付。PG17源码未改，默认关闭。
不构建镜像，不准备或执行K8s部署，不合main。所有者的AGENTS.md修改保留。

## 产品职责及授权

原Guest immutable来源冻结Ed25519公钥。root程序新增serve，真实和有效UID均须
root；普通postgres即使通过SUID入口也不能启动服务或执行资源动作。生产服务
重读只读ISO、封存原文件及既有日志，确认原Bootstrap一致；允许PG运行或未知
操作存在时对账。没有网络initialize/bootstrap动作，不初始化缺失日志，不自动重放。

固定UDS为`/run/neon-dsga/resource.sock`，root目录0755、socket0666。可连接不
代表获权：每请求重新核验原公钥Ed25519签名、派生kid、唯一Authorization、固定
aud=neon-dsga和scope=compute_ctl:dsga、VM/Compute实例、非零字符串epoch、
严格整数nbf/exp、最多300秒寿命且无宽限；拒绝额外、重复、缺失及null字段。
初始化只读scope无此权限。签名不是租约、完整预算或排空证明；网关仍逐次核验
原VM及current owner，root再以独立持久floor阻止已被接管的旧代次迟到写。

独占服务flock先于旧socket清理，第二进程不能unlink存活服务。目录不能由
postgres替换；最多16连接，读头5秒、读写及空闲15秒、请求16KiB。root同步内核
写期间不提前放资源锁，HTTP断连或写超时不代表未发生。接管保留Original/Held/
未知状态。FenceBound在同一锁内比较完整Guest boot/cgroup身份再落盘floor，避免
Inspect与Fence分离时的身份竞态。

正式Compute新增五个固定管理路径，复用专用认证层，将原始Bearer交root重新
验签；独立于SQL连接和PG running。没有TCP兜底、任意socket或命令。Rust只转发
限长JSON及明确状态，不转发cookie、重定向、自报epoch头；20秒上限，取消时
终止连接驱动，不自动重试写。资源动作失败均保留不确定性，不发VM许可。

| 路径（前缀/dsg/v1/resource） | 方法 | 语义 |
|---|---|---|
| /identity | GET | 原Guest身份，不代表约束已落实 |
| /fence | POST | 完整Guest身份和当前授权epoch，同锁推进floor |
| /apply | POST | 当前epoch完整原意图，连续revision，写前持久化 |
| /observe | POST | 完整原意图读回，新owner可读旧epoch，缺失不是NoEffect |
| /replay-original | POST | 当前floor完全相同原意图，必须显式操作 |

Go独立资源Client以原Guest身份冻结，执行和观察独立连接池，标准TLS且禁止
不安全配置、环境代理和重定向。owner Context取消传播，每次获取短凭据。
Pending深拷贝、构造时显式恢复；失败不清空，普通Apply不隐式重放，显式Replay
才允许再发送。200亦逐字段核验完整Evidence、整池、上界、占用及身份，VM许可
必须false。严格Evidence解码使缺字段和null不能静默补成0占用。外层必须先持久化
原意图，内存栅栏不能替代完整journal；失败后仍可能需要受控关闭，不提供清空
Pending或凭not-found宣称NoEffect的接口。

Compute SDK新增资源身份读取，不自动改写Guest或PG boot。BusyBox inittab通过
固定脚本respawn root服务，vm-builder实际嵌入、输入归档和安装源接线。这里只修改
产品构建源，未构建镜像或在Mac安装特权程序。

## 必要Mac验证

Go工具链go1.25.13，GOCACHE为工作区build/go-cache，GOMAXPROCS=4。

```sh
go test -race ./pkg/dsga/resourceguard ./pkg/dsga/compute \
  -run 'TestResourceHTTP|TestResourceIdentityDiscovery|TestResourceGuardInFlightWriteKeepsCrossProcessFence' \
  -count=1 -json
go vet ./pkg/dsga/resourceguard ./pkg/dsga/compute \
  ./neonvm-daemon/cmd/resource-guard ./vm-builder
```

最终组合6顶层/14含子项pass，0fail/skip。签名、TLS、客户端和Mac文件锁/fsync
真实；内核为明确fixture。最后追加畸形JSON、数值epoch、null、重复键及重复认证
头后，仅复跑受改认证项：1顶层/9含子项pass，不与组合结果相加计数。vet退出0。
Linux amd64/arm64的CGO_ENABLED=0 root程序目标编译均退出0，仅编译未执行。
服务脚本sh -n及diff检查通过，未重复未变PG、预算和旧SDK功能。

证据：工作区build/dsga-resource-http-{go-final,go-auth-final}.jsonl、vet.log及两
目标二进制；13个AS实现、测试、构建源SHA见build/dsga-resource-http-source-sha256.json。
Neon另执行固定UDS转发和五路认证各1项，均通过；clippy all-targets退出0。
控制面实际TLS固定资源路由及受影响broker签发2项race通过，见各仓同批文档。
它们是分段真实传输和协议检查，不是完整K8s端到端。

## 尚未交付和Linux后验

本批没有接入Agent的资源领域journal/executor循环。首次准入、完整运行期预算
及系统上界生产、所有查询/DSM/维护排空、Host保留和最终Runner-QMP、公开创建
worker/生命周期及跨boot/owner受控恢复仍是代码缺口，不能称为只待Linux测试。

真实root/SUID、UDS属主权限和服务崩溃恢复、cgroup2、全部HugeTLB hstate、KVM/
NeonVM、K8s身份/RBAC、物理回收/迁移及性能长稳留Linux K8s目标环境验收。
Mac通过或Linux目标编译不能保证内核行为，不宣称生产可用或无需Linux验证。

## 首次提交审批记录

首次提交命令未执行：自动审批服务因额度限制未能完成审查，提示12:02后重试，
不是认定代码或提交不安全。不得绕过审批或将工作树代码称为已提交/推送。
所有者随后要求继续执行；后续状态以工作区build/dsga-resource-http-checkpoint.json
及实际Git/远程refs为准，不能把首次失败当作后来成功提交/推送的证据。
