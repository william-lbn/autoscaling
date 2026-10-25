# Root聚合上界生产与GuestReady审计v2

本批沿既有 `codex/dsga-autoscaling-20261019` 增量开发。fresh fetch后官方
main `a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f` 已完整包含，无空维护提交。
仅PG17当前链路；其他PG暂缓，不构建镜像、不准备或执行K8s部署、不合main。

## 实际实现

`pkg/dsga/budget/root_aggregate.go` 从实际认证root `InspectState` 结果生产
PG计费域的聚合硬约束引用。先经 `ConfirmedLimits` 核对原Bootstrap/Held、
当前限额、swap=0、全部HugeTLB hstate及禁用overcommit/surplus，再要求原
Postmaster出生身份和populated资源域。不能从请求参数或当前RSS生成来源。

数额只为 `PG memory.max + 整个Guest HugeTLB池`；整池只加一次，包含空闲池，
不再叠加主段、四数组、DSM、私有或LFC。`OutsideUpperBytes`只是既有外部预留
声明，本源明确排除，不能把它升级成已经认证的系统成本。全部乘加检查溢出。

引用绑定原Guest/cgroup设备与inode、Bootstrap、Postmaster PID/starttime及
完整Held命令。源ID为这些不变字段规范JSON的SHA256；owner floor升高或瞬时
占用变化不替换源。同数字的新命令仍是新源，未知Original不能借旧Held确认。
对象深拷贝持有数组，不依赖调用方切片寿命。严格解码拒绝缺字段、篡改和许可。
摘要和 `Validate` 只是结构校验，**不认证生产者**；后续预算必须把原来源与新
认证View执行 `Match`，约束漂移/Unknown时保留原承诺，不能当作释放或0。

`pkg/dsga/resourceexec/executor.go` 在现有Agent/PGWorker的GuestReady实际路径
接入，完整原命令持久化、资源效果、PG/root复读、原VM UID/RV CAS核验后，把
该来源写入新审计v2的必选 `pg_aggregate`。旧v1保持历史原样，不转换成证书。
该审计记录一次已核验采样，不是跨PG/root/Kube原子快照，也不代替最终动作
之前的原来源复核。新增类型不改root网络协议、PG ABI、SQL版本或VM许可。

## Mac实际验证

- `go test -mod=readonly -race -p=2 -count=1 -json` 对budget/resourceexec/owner，
  筛选 `^(TestRootAggregate.*|TestGuestReady.*)$`：5顶层/14含子项PASS，0失败/测试跳过。
  resourceexec本身无test文件，Go有1条包级skip；真实执行器由owner测试调用。
  新增3个来源测试，受影响GuestReady实际Session/TLS/双客户端/账本CAS路径2个；
  覆盖扩/缩、丢回复、原命令查询、VM/PG漂移和不完整预算。
- 随后只增测 `TestGuestReadyWorkerUsesOriginalBudgetAndActualDomainOrder/root-empty`：
  PASS，原资源效果后空域不得落Completed；恢复后只Observe原命令，POST仍1，
  记录v2来源与当前认证View一致。没有重复上一组已经通过的子项。
- `go vet -mod=readonly -p=2 ./pkg/dsga/budget ./pkg/dsga/resourceexec ./pkg/dsga/owner`
  退出0。日志位于工作区 `build/dsga-root-aggregate-*`；机器收据列精确SHA256。
- 实际 `./autoscaler-agent/cmd` 以Go1.25.13、readonly、CGO关闭，交叉编译
  Linux amd64/arm64均退出0；仅编译，未执行ELF，不是Linux运行验证或镜像。

测试使用真实客户端/TLS/Session/typed Store和UID/RV CAS；root内核、PG成本及
Kube服务是显式协议模型，不能写成Linux真实cgroup/HugeTLB/内核或集群端到端。
未改DSM原生、HTTP和PG，不重复已经通过的Native8/core225/isolation5等测试。

## 仍需代码及目标环境验收

这只生产PG计费域聚合来源，完整可信Prepare/Finalize、SystemOutsidePG来源、
BG/私有持续上界或Quiesce及降低上限不造成OOM的保证、首次准入、Host和最终
VM/公开worker/受控跨boot仍须实现；不能把这些缺代码记作Linux待测试。
memory.max是计费域约束，不是严格瞬时物理上界或免OOM证明；PG迁入也不迁移
既有共享/file页收费。其他计费域与系统成本必须由互补来源覆盖，不能漏算。
这些边界依据[Linux官方cgroup v2文档](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-ownership)。

Linux后验包括实际权限/出生/cgroup计费、上限暂态和回收/OOM、全部hstate、
内核PTE/THP/未归属内存、真实物理释放及KVM/NeonVM/性能长稳。全部功能仍默认
关闭，`vm_shrink_permit=false`；本批通过不代表整体完成或获准发布。
