# DSGA 原 Runner CPU 带宽与事实

本合同补齐分数 CPU 从真实 VM 意图到原 Runner 控制组、Guest 整数 CPU、读回和未知恢复的执行链。额度作用域是原 Runner 与其 QEMU、控制和 I/O 线程所在的同一 cgroup v2；它包含控制开销。weight 只观察相对权重，配额是带宽上界，不能据此承诺拥挤宿主上的最低性能。0.5 核 / 1 GiB 的出生 normal、查询、内核与 SLO 仍须独立验收。

受管 Runner 在 QEMU 启动前固定原 cgroup 的设备与 inode，只写同组 cpu.max，不改 memory.max、不迁移进程、不使用旧四倍 CPU 超配子组。cpu.max 必须是可精确表示为 millicores 的有限 quota/period；实际 Guest CPU 必须是整数。CPU 包络由冻结的 QEMU 最大拓扑与已有 Pod CPU 硬上界共同限制，不能自报新预算。

现有 CPU HTTP 消息增加可选 managed 合同，protocol_version=1，scope=original-runner-cgroup-v2。DSGA 必须包含原 VM、Compute、incarnation、Runner Pod/boot、Node 身份，以及真实 VM generation；普通 legacy 消息和旧路径保持兼容，旧逻辑回包不能使 DSGA 就绪。Runner 写前直接读取原 InCluster VM、Pod、Node，拒绝不匹配的目标、旧/伪造代次、迁移中和重启实例。Controller 复用同次已核内存 Birth，额外核实 CPU 来源，但不以 CPU 成功结清内存预算。

原固定 Runner 状态目录 cpu-state.json 保存执行 floor、原目标和 pending。每次先原子写入并 fsync 意图，再改同组 CPU 配额与原 Guest sysfs；所有完成要求实际 quota、Guest online 与原目标匹配，并持久确认。写后读回丢失、Guest 部分完成、持久化失败均保持原未知；不能换目标覆盖。原目标重试幂等，或者原事实明确匹配后读回恢复。此日志不是新的 Owner、Host 预算或 Runner 重新出生入口。

回执同时提供原组 dev/inode、quota/period、weight、在线/最大 vCPU、原代次和 pending，以及实际 CPU 节流和 pressure 观察。HTTP 接受目标不等于 Controller 完成，后者仍须下一次实际读回。缺失、旧合同、错身份、max 或不完整事实保持 Scaling，不回退 lastValue。

产品前置：原 Runner 的同一 cgroup v2 cpu.max 必须可受控写入，cpu.stat、cpu.pressure、cpu.weight 可读；Kubernetes Pod 的固定 CPU 硬上界仍是 Host 已承诺的包络。若站点 kubelet/上级控制反复改回配额，则读回保持未收敛并停止宣称完成，需在该站点资格与部署前置中处理，不通过移动进程规避。

验证分层：Go 合同和故障单元覆盖实际值与逻辑值分离、原身份/代次、未知前缀、原日志恢复及 Controller 拒绝旧响应；专属 systemd 单元验证真实 cpu.max、节流及 memory 组不移动。它们不能代替 228 的真实 QEMU task 集合、Guest SQL、控制线程开销与双版端到端资格。Host 大页 backing、跨 Host 迁移和 1 GiB 出生资格独立推进。


## Helm 与站点接线

最新 AS Controller 仅对显式 RuntimeContractVersion=1 的 DSGA VM 强制挂载原 /sys/fs/cgroup；legacy VM 仍遵守既有 controller.runnerCgroupEnabled，不会因本合同启用旧外部 CPU 子组。无新增公网入口或 SYS_ADMIN/privileged 权限。原 Node cgroup v2 的 CPU 控制器、叶组写权限和 PSI 是平台前置；部署后必须检查实际动态 Runner Pod，不能只看 Helm 静态 Controller 参数。

优先使用 /proc/self/cgroup 的原路径并核本 Runner 成员。如果私有 cgroup namespace 只报 / 且 HostPath 根并非自己的叶组，则仅从已验证原 Pod 的真实 containerd ID 与 QoS 构造对应 systemd/cgroupfs 两条候选。所有路径逐段 nofollow，先核原 Runner，再在原 Start/Wait 的 pidfd/procFD 观察窗口核原 QEMU 同组，固定 dev/inode。无全局扫描、Host 根写入、PID 重新绑定或进程迁移。启动前尚无 QEMU 时只安装原 Runner 初始有限额度；就绪和运行回执必须另有真实原 QEMU 存活/成员事实。

正式 Helm 固定同一 AS 候选的 Controller/Runner/Daemon 镜像，并验证新 managed 协议。228 验收需关联 VM/Pod/Runner boot/Node/QEMU PID 与 start ticks，核原组的 cgroup.procs/tasks、quota/period、实际 Guest整数CPU、SQL与控制开销及节流；确认整个链的 memory 归属保持。单元的受控 child 或 native CPU 组不代表真实QEMU/Guest业务已验收。


## 显式 Runtime 合同及调度承诺

DSGABootstrap.runtimeContractVersion 是创建即冻结的 uint32：省略/0 保留原历史启动合同，1 明确选择本发行严格 CPU profile，未知版拒绝。held 阶段也不能改版；新镜像、可变 annotation 或 HTTP 不能给旧 VM 自动升级。CP v6 才渲染版本 1，历史 v5 仍按原冻结 PodResources 恢复。

版本 1 在创建/启动前要求整个 Runner 的 CPU request==limit，且完整覆盖冻结 Guest.CPUs.Max；精确 millicores、最大 64 vCPU/64000m，不把过低 Pod limit 用 min 截成较小合法范围。Pod 承诺一直保留至原 Pod 真实退出，CPU shrink 只降低同原组带宽，不声称向 Node 返还调度承诺。回执 maximum_millicores 为最大合法权益，committed_millicores 为固定 Pod 请求/硬限，target/quota 为当前实际 cap，在线 vCPU 为独立实际整数。读回和写前都重核原 VM、原实际 Pod 请求/硬限、容器运行状态及 Node，不能从旧 intent 推导当前承诺。weight 不等同 CPU 保障，Kubernetes request 不是独占核或最低查询性能的证明。

Controller 的 DSGACPUEntitlementReady 专门表示严格合同下实际 cap/承诺/Guest 拓扑都完成。版本 0 新 Runner 提供 historical_guest，只读实际原 Guest 整数拓扑并关联原 QEMU/Birth，不提供带宽资格，也不接受新增 CPU 写；DSGACPUReady 仍可按此历史实际拓扑确认业务，EntitlementReady 始终 false/HistoricalContract。旧二进制缺新拓扑字段时，只有原 Running 且原 CPU 确认代次不变的连续服务状态可保留，明确标 HistoricalTopologyUnobserved；新代次或在途伸缩不据 logical VCPUs/lastValue 完成。缺少这段历史确认的旧 VM 无法宣称无缝升级，应维持原版本后由原受控替换流程迁移，不能在线补写不可变合同或 PodResources。

站点 kubelet/运行时若把 cpu.max 改回 Pod 最大限额，Node 最大承诺仍持有，但当前 target cap 未完成，新的严格 CPU 就绪为 false，须用持续实读及节流/SLO现场资格解决。受控 native kernel 通过与真实 QEMU/Guest/SQL资格分别归档。


## 原Node唯一CPU账与调度视图

自定义调度器关闭了默认 NodeResourcesFit，单独的 Pod request 不能替代真实调度承诺。Host model v11 将每个原 Placement 的 cpu_accounting_version=1 与 committed_cpu_millicores 持久入原 Node UID/RV CAS，Profile 固定 cpu_accounting_version=1 及 allocatable 内额外 system_cpu_millicores。所有新授予同 RV 同时核 CPU allocatable 与原 normal 内存，CPU不另建账本，不重新扣 Capacity-Allocatable；Host HugeTLB 尚未启用，不从旧normal值投影成池额度。

普通Pod用锁定Kubernetes完整 PodRequests（含init/native sidecar/overhead）；VM至少持有声明最大CPU与完整Pod请求的较大值，严格Runtime1另核实际Runner request==limit。CPU持有一直到原Pod/QEMU退出协议完整结清，内存Grow/Shrink/PG缓存结算及本地cache缺Pod都不会退还。Filter只消费同原Node的只读帧，保留缺cache的旧持有；Guest CPU目标与Host固定承诺分开，最终仍由原Node CAS授予。

KubernetesBudgetPolicy v2 冻结 capacity_cpu_millicores/allocatable_cpu_millicores；旧v1及CPU版本0保持原冻结投影，未知版本拒绝。已有v10只能在显式 cpu_accounting_upgrade_mode=original-placement-snapshot-v1 下，完整重读每个原Pod/VM来源后由原Node Owner做同RV CPU轴升级；原Origin/内存/holds/Claim/Settlement/Barrier不变。任一来源未知不补0，保留旧账和不可资格；需要先完成原生命周期清理再启新profile。CPU升级丢回复从原Node恢复，不重复创建域。

Node回执 node_commitment 给出原账Origin/sequence/Node RV及整个Pod固定承诺。Runner每次实读和写前核原v11同Pod持有，Controller另向原Node/Pod直接读取比对后才允许新CPU权益条件为真。此链不依赖PG初始化完成，出生只需原VM/Birth/Pod/Node预算、原QEMU和Guest实际CPU；避免CP等待Running后才配置PG的环。Condition不是无限期采样新鲜度证明；本次nonce只读reader接线完成前不能据滞留True发布新路由。
