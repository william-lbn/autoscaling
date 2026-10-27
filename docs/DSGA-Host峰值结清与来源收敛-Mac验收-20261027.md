# Host 峰值结清与来源收敛：实施约定及 Mac 验收

本批接通**运行中 VM 成功操作后的峰值分配承诺结清**、当前预算只读核验和
旧 Host CM 清理。目标容量与当前来源继续持有；不把该结清称为 Mac 已证明
物理内存归还，亦不包含 VM 销毁、取消或迁移的整份额度释放。

代码：`39839c1f93976b92aa9dc4d3d7685bec1b39f119`（账本基础），
`efc385a77412379987ee14ba9054ffc22152a2de`（实际来源/Publisher/Scheduler）。
配套 `DSGA-Host峰值结清与来源收敛-Mac收据-20261027.json` 固定 12 个 Go 文件、
完整用例→最终日志映射、原失败日志及 6 个仅编译产物摘要。

## 必须保持的语义

- 节点首结清升级 Host Model v4；VM Model v4、Host CM v2、Runner record v1
  和 PG ABI 不变。原节点 v1/v2/v3 的读取语义保持；ae4c9376 旧节点读者源码
  明确只接受1..3，故拒绝v4。此为静态核对，未执行旧程序，不记录为旧二进制测试。
- 每个原 Pod 保留完整请求、原 Host/Runner UID、VM 当前预算落账序列及最终
  命令摘要的结清高水位。目标容量不释放；更晚在途 Hold 的峰值另计。旧 Key
  永久不能再次 Reserve，历史条目收敛不能变成旧请求重新获得执行权的机会。
- 原 `InspectClaim`/`budgetclaims.Reader.Read` 对已结清请求返回 ErrSettled。
  新 `ReadCurrent` 只能从原 VM 永久 BudgetHeld 派生期望值，再真实读取原
  Outside/Host CM、Pod/Node 并核水位，维持当前预算。它不是新的派发接口，
  不交给 Runner/VM 执行路径使用，不靠旧 Finalized 成功推定来源持续活性。
- `Publisher.SettleCurrent` 已被实际 Scheduler 调用。顺序为原 Node 快照/Fence
  →原 VM 无 Active、最新成功终态与当前预算→原 Runner 不可变完成回执、
  无 pending、真实 requested/actual 目标→两端重读→同 Node UID/RV 结清 CAS。
  任一未知/新授予冲突都保留事实；不因 Lease、删除事件或超时释放。
- 确认水位后才清严格早于它、同原 VM/incarnation/Pod/Node/PG 实例的 Host CM。
  当前预算/另一 Pod 或迁移域/Outside/Runner 来源不动。去保护与删除均固定
  UID/RV，丢回复或重启可从持久水位重扫；不会为了清理恢复旧执行许可。
- 永久预算存在后 Scheduler 按原 Runner 实际值与当前预算恢复，保留更晚峰值；
  不再用滞后的 spec.Use、status 或旧批准注解把已结清峰值重新抬高。

Host 分配者增加旧来源 List/Patch/Delete 的使用；Agent 不增加 Node/CM 写权。
真实 RBAC 必须按角色隔离，配置/部署与权限验收留 Linux；本批不准备或执行部署。

## 真实验证与失败保留

Mac arm64、Go1.25.13，新增/受影响用例按完整 package/test 名最终去重：
**19 顶层、68 含子项通过，0 最终失败、0 skip**。包括：

- 旧峰值归还但目标、系统域和开销仍持有，旧执行回执拒绝而当前预算读取成功；
- 结清与并发新授予的 Node CAS、丢回复只读恢复、篡改原水位/身份拒绝、深拷贝；
- 连续超过256次成功操作后仍只保留当前操作承诺，最早旧 Key 仍不能重开；
- 实际 Agent/Runner 缩容→预算/准入闭合→Host 结清→等待 Pod 可使用归还额度；
- 当前来源丢失或 Runner 实际值漂移时不结清；结清 CAS/旧来源删除丢回复恢复；
- 实际 Scheduler 选主/恢复/结清/GC，下一轮不被旧 spec.Use 抬峰值；
- 受影响旧 Node 账本、首次持有、严格解码及少量 Scheduler/Framework 路径。

原失败不覆盖：新单元测试初次有未用绑定/整数类型错误；组合 `-run` 初轮没有
执行 Owner，用例空运行不计通过。实际循环暴露双测试 API 中静态复制的 VM
Lease 过期，改为转发实际续约的原 Lease GET；保留生产新鲜度拒绝。随后夹具
只等待结清 CAS 就取消循环，导致 GC 尚未完成；改为两阶段确认后才取消。
均只复核失败场景，最终完整用例映射替换原失败、不累加通过数量。

5个受影响包 vet 通过，最终 Linux 增量 lint 0 issues。Scheduler/Runner/Agent
的 Linux amd64/arm64 共6个 ELF 按准确 efc385a7 编译，架构与 vcs.revision 核验
一致，**仅编译、未在 Linux 运行**。modified=true 对应所有者保留的 AGENTS.md，
12个Go文件逐字节对照代码提交一致。未改 go.mod/go.sum，不重新同步冻结后上游，
不重复未变 PG17、HTTP、存储及原整批测试，不构建镜像。

日志：工作区 `build/dsga-host-settlement-*.jsonl`/`*.log`；最终权威审计
`build/dsga-host-settlement-final-audit-20261027.json`。前批验收结果仅在未改功能
中复用，不将各批含重叠项目的计数相加冒充唯一测试总量。

## 后续仍须完成

代码：VM/整个 Pod 停止销毁后的额度释放、失败/取消结清、迁移/受控跨 boot，
全部分配写入者准入隔离及独立 CPU 联动；可信完整 Prepare/系统剩余/过渡/
安全上界来源、首次预算/准入、安全 normal 降额与后台/私有持续约束，以及
Monitor/策略/公开操作/取消/受控恢复。**整体核心仍未完成，不能称只待 Linux。**

平台验收：memory-backend-ram/virtio-mem discard 的实际 Host 归还、系统与
Runner/QEMU 开销资格、K8s/RBAC/真实 Lease/GC、KVM/NeonVM/HugeTLB/性能长稳。
QMP 相等本身不认证物理 Host 空闲；可信平台配置必须经过目标环境认证。
原操作 CM 的收敛不代表所有旧 Runner/Outside/VM 生命周期来源已清理。

仍在既有自有分支演进，PG17与独立控制面保持原版本；控制面完成后再移植统一
E2E。默认关闭，不构建镜像、不部署、不合主分支、不发布生产。
