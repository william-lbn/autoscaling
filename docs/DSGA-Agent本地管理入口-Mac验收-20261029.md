# Agent私有管理入口与串行协调接线

日期：2026-10-29，Mac Darwin arm64。公共模块`f94d4e86`、Agent接线准确源码
`12a993e7677817e9d936c79f072df98be9670afe`，沿原自有功能分支。默认关闭。
PG17仍为b078；PG/VM/领域协议版本不变。本阶段完成不表示PG17核心完整。
源码、逐用例采用映射及日志/ELF摘要见[机器收据](DSGA-Agent本地管理入口-Mac收据-20261029.json)。

## 实际新增行为

可信Agent配置增加可选`controlSocketPath`。不配置时不监听，既有受管模式和
legacy拒绝边界不变。配置时，Agent主taskgroup运行本地Unix socket服务；当前
正式Session创建的PGWorker注册唯一端点，退出注销。所有管理决定通过无缓冲
通道进入同一个`PGWorker.Run`，HTTP处理器不直接并发调用SDK、不产生后台队列。
普通轮询不构造新目标、不因HTTP错误重放原生动作。

每次POST固定完整VM/Compute/PG boot及预期当前owner代次。Begin携带完整
Request；取消/各领域恢复只选原Key，首次开放只选已持久命令SHA256。不接受
替代URL、PID、限额或回执。旧Key可被新owner查询/取消，但原生重放资格仍由
各领域原日志核验，不能改epoch继续旧动作。Begin先按原Request幂等查找，再
判期限；丢接受回复后可用同Request查询，不能新造操作ID。

HTTP成功只代表这次管理选择处理完。必须读`disposition`及`state`：Busy、
Conflict、Stale或期限拒绝不是接受；Accepted也不是整次resize或物理回收完成。
任何错误都可能发生在写后，503/超时不得当NoEffect。普通Run继续对账已持久
意图；恢复操作各自仍须完整预算/准入/Host/Runner/PG/Root来源及最终fence。

## 受信调用约定

socket的规范绝对路径最长100字节。其父目录须预先存在、归Agent服务UID、
无组/其他用户权限；socket为0600。拒绝符号链接及可被非管理者替换的祖先，
仅允许root/self持有的sticky临时祖先供开发测试。此服务信任同UID及root；不得
把其目录暴露给Guest、租户或不受信sidecar，不把Pod exec权限当租户授权。
本阶段未提供TCP/租户HTTP接口，未更改独立控制面、RBAC、Helm或部署manifest。

若路径已存在，启动失败且不删除它。崩溃遗留socket由管理员确认原进程退出后
处理，不能靠超时猜测旧执行者消失。正常退出关闭并清理本监听；Agent其他初始
化在Serve前失败也关闭监听，不泄漏文件。注册表小锁不跨网络/等待，不与全局
pods锁或Runner锁嵌套；旧注销按端点比较，不移除后继注册。

接口：

- `GET /dsga/v1/owner?vm_uid=<原VM UID>`：返回本Agent端点的完整实例/owner
  绑定，仅供发现；随后的POST仍要认证Session，发现不是执行许可。
- `POST /dsga/v1/command`：`application/json`，严格完整字段/唯一键，16KiB
  原始正文上限，最多16个在途请求，单次总期限130秒。路由和参数严格校验。

POST示例（值必须取原实例及当前owner，不复制示例身份到实际VM）：

```json
{
  "protocol_version": 1,
  "instance": {"vm_uid": "original-vm", "compute_instance_id": "original-compute", "pg_boot_id": "3"},
  "expected_owner_epoch": "7",
  "action": "state"
}
```

可选动作与选择值：

| action | 附带值/含义 |
|---|---|
| state | 无Request/Key/selector，读取原VM日志 |
| begin | request：完整原Resize Request |
| cancel | key：请求原操作取消，不证明已派发动作无效 |
| replay_pg / replay_quiesce / replay_admission | key：各自原PG动作或门禁 |
| replay_resource / replay_prepare / replay_vm | key：各自原Root、预算生产或VM动作 |
| replay_initial_admission | selector：原首次开放命令的小写SHA256 |
| replay_abort | key：原Cleanup开放命令 |
| recover_unchanged_admission / recover_capacity_admission | key：可信服务策略明确选择的原取消恢复，不自动生成 |

单个Run不可重入；端点只用一次，不跨owner复用。管理ctx同时关联Run、Session
和HTTP调用者：Run先停而其他两者仍有效时也取消并join管理网络调用。调用者已
退出时单容量回复通道不会阻塞循环；已写原WAL/未知结果不撤销。错误管理选择
只回复本次调用，不以坏Key或旧epoch杀死健康owner。

## 实际Mac验收

四包去重16顶层/43含子记录，最终0fail/skip；四包go vet退出0，相对909756fe
包含新暂存文件的增量lint0。按机器收据逐用例选择最新日志；后续发现接口和
Run取消变更只复验受影响场景，未重复未变PG/Rust或其他领域矩阵。初轮lint
显式字段/预定义名称及格式问题已修正，旧通过日志不重复累计。

实际组合包括：严格动作/绑定/来源选择；正式Elector、UID/RV Begin接受丢回复、
完整Request幂等和冲突；全Planned取消进入历史；错误恢复选择不退出owner；
原PG丢回复后只读对账、显式选择才幂等POST；Run取消时正在真实HTTP中阻塞的
管理调用被取消并join，Session/调用者仍活着；真实私有UDS→HTTP→正式注册
端点→同一Run→原VM WAL的Begin/查询/零派发取消/历史。配置默认关闭及旧
受管模式拒绝legacy回退受影响回归通过。

UDS组合不执行PG或VM物理动作；PG域组合的TLS原生事实和非PG前驱是明确夹具，
不能称全预算/真实Neon存储产品E2E。现有真实PG/Rust证据按未变范围复用。

## Linux编译与剩余工作

本提交准确业务源码的Agent Linux amd64/arm64两个静态ELF交叉编译退出0，架构
及VCS revision均核对；**均未执行**。构建时唯一未提交文件是保留的所有者
AGENTS.md，故VCS modified=true；14个Go源码摘要与提交一致。受限模块统计缓存
写入可能有权限警告，构建仍退出0且产物已验证，不能把它称无警告构建。

Linux仍须验证真实UID/挂载/文件系统和K8s权限、实际服务生命周期及全业务链。
KVM/NeonVM/HugeTLB/物理回收、存储负载/长稳/性能另验，不构建镜像或准备部署。

本批关闭一个可实际调用的可信协调入口，未完成Monitor/自动策略、Root漂移发生
在worker建立前的管理恢复、其他异常来源/启动/无Wait/跨boot/迁移、安全降normal
与持续私有/后台约束、全部写者/CPU。它们仍是代码或组合缺口，不是仅待Linux。
PG17核心完整后PG18→PG16，逐版内核/扩展/发行及差异验证。上游继续冻结，
独立控制面保留原分支，不合main、不发布；Goal active，阶段归档后继续。
