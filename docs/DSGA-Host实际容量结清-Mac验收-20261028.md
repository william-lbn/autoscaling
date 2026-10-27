# Host实际容量结清与Scheduler消费：Mac阶段验收

源码提交：`f2a96d0216edf6ce9520c70107b417a360ad8d4c`，在既有自有
`codex/dsga-autoscaling-20261019`继续演进。上游仍按2026-10-26冻结点，
没有fetch/merge新官方提交；PG17、独立控制面和其他PG版本未修改。

本阶段完成Host/Scheduler对永久实际预算的消费。**自动部分效果取消执行器
仍未接齐，不能据此宣布整个部分取消或内存伸缩核心完成。**

## 实现与不变量

- 原Request保持不可变；Node v7新增可选`actual_memory_bytes`。零值保持旧
  普通成功合同；实际预算显式保存当前VM容量，只允许原Host计划的before或
  target。缩容目标1GiB、原VM仍3GiB时仍记3GiB，不错误退还2GiB。
- Publisher从VM v8 `CapacityHeld`的原Cleanup、原来源及永久关闭链派生结清，
  独立认证实际合同，不将其投影成普通目标成功。Cleanup落账但Active未关闭
  时仍不能结清；业务终态保持`AbortedAfterReconcile`。
- budgetclaims重读原Outside/Host不可变CM、UID/RV、保护及Pod/Node身份。
  Outside的资格目标必须等于实际VM；小VM资格不能覆盖仍大的原VM。
- Runner当前读者允许实际预算的原生来源为Completed或不可再发送的原域取消，
  不把cancelled_before_send标成Completed。普通执行/Finalize来源不放宽。
- 顺序仍为Node快照/Fence→原VM关闭→原来源/Runner当前值→重读→Node UID/RV
  CAS→再读→旧Host来源GC。丢回复只恢复原结清；替换来源不清账。当前来源、
  Through之后的未来峰值继续持有，迟到旧请求不可重新占用。
- Scheduler恢复和Reserve读取真实实际头，不能用旧spec.Use恢复较小记账。
  普通旧JSON不输出空actual/Through字段；完整旧v6源码接受v6、拒绝实际v7。
  Node读者需随版本整体升级；VM保持v8、Runner保持v2，PG17 ABI不变。

## 实际执行与证据

机器收据：[逐场景、源码、日志与产物摘要](DSGA-Host实际容量结清-Mac收据-20261028.json)。
本机根目录为`/Users/williamlee/github/neon-self-dev`。Go1.25.13、Darwin arm64；
14个Go文件，相关六包增量lint/vet通过，准确提交四个CLI×Linux amd64/arm64
共8个ELF编译通过，并核ELF machine及嵌入VCS SHA；**没有执行Linux产物**。
构建VCS dirty来自未提交的所有者AGENTS.md，业务Go/module逐文件核对提交一致。

去重采用39项顶层、128项含子场景的通过记录；它们不是128项产品E2E。
没有测试用例skip；hostexec、budgetclaims没有独立测试文件，仅包级编译，其
新增业务读取由Owner组合实际调用验证。失败调用没有整体改写为通过：

| 日志（位于本机build/） | 结果与采用方式 |
|---|---|
| `dsga-host-actual-node-first.jsonl` | Node/Scheduler共119个具体场景通过；唯一旧future-version夹具仍把已支持v7当未知而失败。复用119条，不把原全调用标为成功 |
| `dsga-host-actual-retirement-final.jsonl` | 未知版改为v8后仅重跑该Pod回收用例，通过；拒绝断言保留 |
| `dsga-host-actual-wire-final.jsonl` | 导出真实v6 codec输入，并覆盖Through/迟到请求 |
| `dsga-host-actual-old-reader-final.jsonl` | 精确旧提交`7ff783947a2639a563b98ddb2710586c87b66c86`的完整源码接受v6/拒绝真实v7；未混用新类型覆盖单一旧文件 |
| `dsga-host-actual-owner-second.jsonl` | 原SDK pending恢复通过，新增Host组合的Scheduler名称夹具失败；仅采用原pending子场景 |
| `dsga-host-actual-owner-final.jsonl` | 修正名称后新增组合通过：实际Agent/Runner/QMP扩VM、PG未扩→Cleanup Start丢回复→typed Record/Finish→Publisher结清→Scheduler Reserve→同名Outside新UID阻断且账本不变 |
| `dsga-host-actual-legacy-final.jsonl` | 普通成功Host结清、完整目标后取消、原容量未变取消丢回复三条受影响回归通过 |
| `dsga-host-actual-lint-final.log` / `dsga-host-actual-vet.log` | 六包增量lint为0 issues；vet退出0 |

首次新增组合还存在一次编译字段误用，已改为原夹具Pod身份；初始失败日志
保留。未为通过而删除断言、放宽身份/来源或把Unknown标NoEffect。

命令统一`go test -mod=readonly -p=2 -race -count=1 -timeout=… -json`；具体包、
用例和采用日志见JSON映射。`build/audit-dsga-host-actual.py`核对本机全部采用
来源、14个提交文件及8个产物；`build/build-dsga-host-actual-linux.py`为本机
交叉编译记录，不构建镜像。未改PG/Rust/控制面功能不重复跑。

## 结论边界与后续代码

组合中的Agent/Runner、本地持久日志、VM UID/RV WAL、Publisher及Scheduler
为实际实现；PG/Root/资格与Kubernetes/QMP对端为显式协议/领域夹具。
本组合**人工组装typed Cleanup审计**检验消费链，没有代替自动取消执行器。
该区别必须保留，否则会把尚缺代码误报为仅待Linux测试。

仍需编码：自动实际预算Cleanup/准入及完整部分效果、Unknown/重建/后继操作
组合；可信Prepare、系统剩余/过渡/安全资格生产者、首次完整预算/开放；安全
降normal/后台私有约束；无Wait/Start/init、跨boot/迁移及完整来源生命周期；
全写入者/CPU、Monitor和自动策略/公开可信请求接线。独立控制面完成后另行移植。

Linux仍需验证：真实K8s RBAC/Lease/CAS/GC/CRI/分区、KVM/QEMU/NeonVM热插拔、
cgroup/HugeTLB与物理回收、Host开销资格、存储/负载/性能长稳。Mac纯逻辑结果
可以复用；系统调用、页回收、持久化和真实资源效果仍可能有平台差异。
默认关闭，不构建镜像、不准备或执行部署、不合主分支、不发布生产。
