# PG 在途与历史淘汰后的独立对账：Mac 阶段验收

本批继续 PG17 内存伸缩 P1，实现 PG 单领域的独立恢复。原执行未知、原生
终态和历史结果淘汰分别处理，不将某种状态改写为另一种。PG17 保持
`79367196a74eef82e67cac917877d2e6271d3e70`；未改 PG/SQL/Rust 产品实现，
Neon 只新增严格原生组合测试。原未知/预算/关闭保留，完整 VM/实际预算及
重开仍需后继业务代码；本批不代表 P1 或 PG17 全面完成。

## 问题和实现

类别 17 关闭冻结原 Active 后，普通 PG Record/Abort/Finish 均不能再改写
原步骤。此前完整原生终态对账只接受已经落账的结果，明确 Busy/Rejected
又有独立拒绝屏障；原执行 Unknown 或原生历史已淘汰仍缺少独立恢复入口。

现在保存 ServicePGRecovery，逐字固定完整原派发/操作摘要、最近关闭、
原观察 owner、序列、原生 Fence 和原 Key 查询。原 Active、ResizePG
Unknown/原回执、Abort 和旧预算全部保持。两类证明分开：

| 当前事实 | 独立恢复条件 | 不代表什么 |
|---|---|---|
| 完整 Committed/Denied 原生终态 | FENCED 串行权、完整原 Key/context 及同点稳定容量；已保存原生终态时再次逐项核一致 | 不是 Guest/VM/Host 完成 |
| 无完整结果，包括历史淘汰 | 真实严格更高 floor、FENCED 已退出旧执行者、同 boot 稳定实际容量、原 Key 仍无结果，且当前头不冒充原 Key/PREPARING | 不证明原命令 NoEffect、Denied 或成功 |

隔离模式只说明旧执行权不能再产生未来效果。历史效果仍为未知，必须按
当前准确资源和预算独立结算。没有更高实际 owner 就拒绝；不自动换 owner
来通过检查。原代次和原操作下一代通过减法比较，避免 uint64 加一回绕；
下一代必须是原目标前缀，Fence→查询期间容量变化或不稳定均拒绝。

原关闭已确认后，网络中的原 Resize 仍可能在相同 owner 下晚到并提交。
独立证明保存当前同 revision 仍关闭的实际容量，允许它在原操作有限范围
内前进；不改写原关闭的 expected 代次，也不将旧关闭当作新容量预算。
后继完整恢复必须消费这份准确当前容量，不能继续沿用旧容量永久头。

## 持久、SDK 和执行顺序

1. 显式恢复先隔离/查询原 PG，不能让 Root 部分效果遮住原 PG 事实。
2. 用原 VM UID/RV CAS 保存独立证明；CAS 未确认即停止，不清 SDK Pending。
3. 保存后重新执行当前 owner Fence，再查询完全相同原 Key。只有证明
   类型、不可变终态和逻辑容量均吻合，且本地没有执行在途/不同 Pending，
   才清该 SDK 的一项栅栏。原 WAL Unknown 从不因此改变。
4. 普通轮询只复核已持久证明，不创建新恢复选择、不换 owner、不重发
   Resize/Cancel。SDK 重建仍从原 WAL 恢复 Pending，沿用相同证明再查询。
5. Pending 已清时仍核当前 floor、容量和原关闭；门禁漂移不能借空 Pending
   被忽略。原独立证明不被后来查询覆盖，接管保留原观察 owner。

网络期间不持本地 mutex；SDK 锁只保护执行槽和相同 Pending 的检查/清除。
原生 Fence 自身会推进隔离元数据，不能把它称作零 RPC 或零原生写；没有
新增 Resize、Cancel、准入 setter、Guest Apply/取消或 QMP 容量动作。

新增字段是内部严格持久证据，不新增 PG/Compute wire 版本；旧严格读者
遇到未知字段拒绝，没有该字段的旧账本仍走原规则。新证明与原明确拒绝
屏障互斥。证据深拷贝数组、ReleaseKey、Operation 和历史 Result；删除证明
只能退回原关闭/未知，仍不能 Finish 或开放。

当前生产入口接通类别 17；类别 18 的完整预算恢复、原生终态淘汰后的
持久预算消费者仍需后继接线。Guest 与 PG 的单领域证明都不构成完整稳定
预算，CheckStableServiceRecovery 继续拒绝。跨 PG boot 的安全重建也不由
本证明替代。

## 实际验证

普通定向 race 按 package/test 去重 16 个顶层、97 条含父子记录，0 fail/
skip，不是 96 次产品 E2E；最终源码、过滤、逐用例及日志摘要在 Neon 同名
机器收据归档。

- 合同/SDK：原生提交、拒绝、旧/新实际容量隔离、后继 owner、原 Key 缺失/
  历史淘汰、同 owner 拒绝、PREPARING、floor/boot/容量/上下文变化、同容量
  下证明类型被替换、本地执行在途或 Pending 变化，以及 uint64 最大代次。
- Model/Store：原 Unknown/原生终态保留、Grow/Shrink 晚到提交的独立实际容量、证明
  深拷贝/codec/严格字段、owner/序列/门禁/摘要变化拒绝；CAS 正常、丢回复、
  旧 RV、VM 重建均通过。
- 五个正式生产组合总计 150.09 秒：已提交、证明丢回复、证明后接管、未收到、
  历史淘汰。后三类包含真实 Lease 失效接管；旧 Session 停止，新 owner
  沿用原操作/证明，SDK 重建不抹未知。恢复不新增容量/Guest/QMP setter，
  服务保持关闭，原 Active/预算逐字保留。Pending 清除后门禁漂移仍拒绝。
- 直接受影响的旧 Guest 恢复、PG 拒绝、PG 在途关闭/容量接续及执行器通过。
  五包 vet、普通及 native tag 增量 lint 0，33 个消费者仅编译通过；Rust 新测试目标 clippy --no-deps 退出 0，27 条既有 warning 保留。

新增严格原生门禁实际执行 1 个 Rust 外层、3 次独立 Go race 进程，最终
11.87 秒，0 fail/ignored；其余 8 个旧 Rust 场景被明确过滤，没有重复。
三次均使用准确 PG17，经 Go TLS→正式 Router→JWT→管理 SQL→真实 PG：

- 原请求从未收到，更高 floor 后可独立隔离；刻意晚到旧请求返回 Stale。
- 原生真实提交后代理丢回复，SDK 保留未知，原 Key 终态对账不重发 Resize。
- 实际执行超过原生历史容量的同容量 no-op 请求，确认原结果真的为
  ResultExpired；原 SDK Pending 始终保留。更高 floor 后独立隔离，晚到旧
  请求 Stale，实际容量保持原提交后的值。填充历史仅为测试安排，不是生产
  协调器绕过 Pending 的许可。

反向验证暂时移除严格更高 floor 检查，同 owner 拒绝用例确定性失败；
源码 SHA256 逐字恢复后同项通过。该预期失败单列，不混入采用的通过计数。

主要复现范围：

```sh
go test -mod=readonly -race -count=1 ./pkg/api/dsga -run '^TestResizeRecovery'
go test -mod=readonly -race -count=1 ./pkg/dsga/compute -run '^TestRecoveryReconcile'
go test -mod=readonly -race -count=1 ./pkg/dsga/journal -run '^(TestServicePGRecovery|TestStoreServicePGRecovery)'
go test -mod=readonly -race -count=1 -timeout=15m ./pkg/dsga/owner -run '^TestProductionPGResourceRecovery$'
# 预编译Go dsga_native_e2e race二进制，指定准确PG与二进制后：
cargo test --locked --offline -p compute_tools --features compute_api/postgres-client --test dsg_go_http native_go_resize_recovery_keeps_unknown_and_isolates_expired_result -- --ignored --exact --nocapture
```

生产组合中的 Root/Kube/QMP/PG 是明确协议 peer，严格原生门禁中的 PG/SQL/
Router/JWT 为真实实现；两者证据分别记账。都不替代 Linux cgroup/HugeTLB/
KVM/K8s 物理返还、规模或性能认证。

## 原失败和剩余业务

首轮未收到场景误沿用 Start CAS 丢回复前的旧测试句柄；该 SDK 尚未进入
Resize，本来就可能没有 Pending。按正式从原 WAL 重建后复验完整五场景
通过，未放宽产品检查。三项 lint 的无效赋值/等价布尔与错误 helper 返回值
已修正。首次 rustfmt 递归发现旧支持文件格式，已只保留目标新测试的格式
改动，支持文件逐字恢复；最终使用 skip_children 精确检查。native tag 最终 lint 首次与普通 lint 争用工具锁，顺序重跑通过；原日志均保留。

P1 仍需 VM 在途独立对账、完整当前实际预算/Host 结清、关闭代次与重开
衔接、历史淘汰后的永久预算消费者和后继业务组合；不能归为仅待 Linux。
P2 安全降 normal/持续预算、P3 直接接线、P4 修改范围最终 PG17 组合继续。
不追冻结后上游、不改独立控制面、不构建镜像、不部署、不合 main；PG17
全面完成并远程归档后才暂停，不进入 PG18/16。所有者 AGENTS 修改不混入
本批业务提交。
