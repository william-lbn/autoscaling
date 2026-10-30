# 资源在途关闭后的 Guest 独立对账：Mac 阶段验收

本批属于 PG17 内存伸缩 P1，只补齐类别 16/17 关闭后的 Guest 域独立恢复。
PG17 保持 `79367196a74eef82e67cac917877d2e6271d3e70`，不改 PG/SQL/Rust/
原生 HTTP，不重复官方未改矩阵。完整 PG/VM/实际预算恢复仍须后继实现，
单 Guest 对账不能算 P1 或 PG17 全面完成。

## 问题和行为

GuestReady 已写前持久但 Root 回复未知，随后来源异常导致独立关门时，
原 Active 被完整摘要冻结。普通 Record、Abort、Finish 都不能再改写它。
此前缺少在保留该未知的前提下，对账 Root 原效果的生产入口。

现由已有显式服务恢复管理入口先保存 ServiceResourceRecovery：固定原
操作/Guest 命令摘要、原关闭与序列、选择者，以及是否允许取消未接收命令。
UID/RV CAS 未确认时不发 Root 取消；原 Abort 标志不改。仅持有当前
Session 的串行执行器消费该选择，网络期间不持本地锁。

| Root 原状态 | 独立恢复行为 | 保留的边界 |
|---|---|---|
| 已接收且实际完整吻合 | Observe 原命令，保存完整 Applied 事实 | 不重发 Apply/Replay；原 GuestReady 仍 Unknown |
| 从未收到 | 先持久取消选择，再由 Root 文件锁内封存原命令取消屏障 | 普通 NotFound 不算 NoEffect；不改外层原操作 |
| 原 Apply 抢先于取消获得锁 | 取消失败；后续只读核原 Applied | 不把失败取消补成取消成功 |
| 部分应用或来源/出生不符 | 保留独立 Unknown，等待真实原效果可核验 | 不用旧 Held、低占用或相同数值伪造终态 |

Root 的真实文件锁继续裁决 Apply/Cancel，调用方的旧 View 不授予取消
成功事实。Root 取消回复丢失后，普通轮询只查已保存选择及原取消屏障，
不再自动发送取消。选择本身丢回复、且 Root 尚无屏障时，必须再次显式
管理选择。Root 完成而独立 Record CAS 丢回复后，仅重读原独立证明。

落账前核原 VM UID/RV、owner、Root 身份/floor、原 Postmaster 出生与
实际进程、完整命令和所有限额。Settled 证明不被新观察覆盖；接管可在
当前更高 floor 下重新核相同聚合来源，原命令 issuer 和原观察者各自保留。
来源漂移仍拒绝，不能借旧 Settled 退休其他领域。View.Clone 深拷贝全部
嵌套命令、池、出生、任务/地址空间/系统证据，避免调用方污染持久快照。

## 兼容性和当前限制

新增字段属于内部严格持久恢复证据，不新增产品 wire 版本；旧严格读者
遇到未知字段必须拒绝，没有该字段的旧合法关闭仍按原规则运行。原生
PG、Root 命令和 Compute HTTP 合同不变。

Guest 证明不授予开放、预算释放、PG/VM 完成或 Host 结清许可。完整
Active、旧预算和关闭均保留，CheckStableServiceRecovery 继续拒绝。
类别 16 已验证生产协调链；类别 17 缩容后边界只新增 Model/codec 验证，
不能把它写成完整缩容恢复 E2E。PG/VM 在途、原 PG Unknown/结果淘汰、
独立完整实际预算及最终重开结案仍属于 P1，不能归入仅待 Linux 测试。

## 实际验证

按 package/test 去重采用 14 个顶层、98 条含父子记录，0 fail/skip。
这是不同层的验证记录数，不是 98 次产品 E2E。

- 新 Model/Store 覆盖 Applied/Cancelled、相同 owner/更高观察 owner、
  原未知/预算保留、严格字段、深拷贝、原生代次 17 缩容后边界；选择与
  Record 两处 CAS 的正常、丢回复、旧 RV、VM 重建全部通过。
- 六种生产故障组合：已应用、Record 丢回复、未收到且选择丢回复、取消
  丢回复、原 Apply 抢先生效、部分应用。实际执行正式 Elector、Session、
  SDK、typed CAS、管理和普通轮询；SDK 重建后仍保留原未知与关闭，
  恢复不新增 Apply/PG Resize/QMP 写，出生漂移拒绝且证明不改。
- 两种真实 Lease 失效接管：已完成独立 Record 丢回复、未收到且选择
  丢回复。原 Session 停止后，新 owner 沿用原选择，只读核原 Applied
  或显式取消同一旧 issuer 命令；两项合计 87.91 秒。
- 直接受影响的五类旧 Model/关闭/预算根及旧在途关闭、原未派发 Guest
  取消接管通过。Owner 定向过滤中可选后缀实际选中了整个旧关闭根的
  七个子场景；按实际结果记账，只执行这一轮，没有额外重复该矩阵。
- 四包最终 vet、增量 lint 0 问题，33 个实际消费者仅编译通过；编译
  不算业务测试。最终源码、用例过滤和日志摘要由 Neon 同名机器收据归档。

主要复现命令（固定本机 Go 1.25.13、Xcode 14.4 SDK、race）：

```sh
go test -mod=readonly -race -count=1 ./pkg/dsga/journal -run '^(TestServiceGuestRecovery|TestStoreServiceGuestRecovery)'
go test -mod=readonly -race -count=1 ./pkg/dsga/resourceguard -run '^TestResourceViewClone'
go test -mod=readonly -race -count=1 -timeout=10m ./pkg/dsga/owner -run '^TestProductionGuestServiceRecovery(Takeover)?$'
go vet -mod=readonly ./pkg/dsga/journal ./pkg/dsga/resourceguard ./pkg/dsga/resourceexec ./pkg/dsga/owner
```

Root/PG/Kube/QMP 在生产组合中仍是明确协议 peer；这不证明 Linux cgroup/
HugeTLB 物理返还、KVM/K8s、规模或性能。它们继续由目标环境独立验收。

## 原失败和收尾边界

首轮沙箱禁止 httptest 本机监听，失败保留；获准后完成实际测试。首次
lint 子进程受宿主 PATH 影响，后续固定 Go PATH；cache 改为工作区路径，
显式字段/格式及错误文字检查全部修正，最终零问题。缩容后新夹具初版
混用旧 Root 与启用系统计费域的原身份，被完整命令校验拒绝；改用同一
原创建的限额/出生/系统证据后通过，未放宽产品校验。上述失败不计通过。

本批继续冻结基线；只推自有既有功能分支，不构建镜像、不部署、不合
main，不改独立控制面。P1 后续领域、P2 安全降限/持续预算、P3 直接接线、
P4 修改范围最终 PG17 组合仍继续。全面完成并远程归档后才暂停自动接续，
不进入 PG18/PG16；所有者已有 AGENTS.md 修改保持原样且不混入业务提交。
