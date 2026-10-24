# GuestReady资源执行与原操作恢复

本批在既有 `codex/dsga-autoscaling-20261019` 演进。重新 fetch 后确认官方
main `a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f` 已完整包含，未制造空维护合并。
预算独立提交 `cb1fe5b8`；执行器和本记录随后独立提交。只修改 PG17 当前服务链，
没有移植其他 PG，没有构建镜像、准备部署或合并主分支。

## 实际代码与职责

- `pkg/dsga/budget/prepared.go`：完整 Prepared 目标承诺绑定原请求、原 Guest、
  Runner、原 VM 内存和目标可用 Guest 容量。系统、过渡、安全类别严格去重并
  等于 Limits.outside；PG 聚合硬上限和整个 HugeTLB 池只计一次。
- Grow 使用 ABI6 固定四数组前缀不变量：每数组当前已保留上界乘
  `ceil(target/high)`，再只加一次固定主段。含保留尾部和页对齐，检查乘加溢出；
  可能保守拒绝规格，不猜结构尺寸、页大小或 RSS。原生精确 quote 尚未新增。
- `pkg/dsga/resourceexec/executor.go`：实际 PGWorker/Agent 循环中的 GuestReady
  领域执行器。它只接受同一账本的完整 Completed PrepareBudget，不存在临时
  参数或“返回成功”的注入回调；没有该合同的旧草稿拒绝推进，不能伪造预算。
- Grow 必须 VM 实际达到目标且 Guest 当前可用容量足够，再提高资源约束，
  后续才轮到 ResizePG。Shrink 必须原 PG 提交及四数组物理释放回执仍有效，
  再设目标约束；VM 尚须保持原实际内存，后续才轮到 ResizeVM。
- 资源 revision 来自独立 root 原日志加一，检查耗尽；不采用 PG revision、
  capacity generation 或外层历史计数。PID/starttime 来自实际 root 原 Birth。
  目标必须列出全部实际 hstate，包括零页池，outside 不得低于原启动承诺。
- 完整 root 命令经原 VM UID/RV CAS 写前持久后，只有真实派发票据持有者发送
  Apply。写前或写后回复丢失都保留 Unconfirmed；普通轮询只 Observe 原命令。
  重建 Resource/PG worker 从原 WAL 恢复 Pending，不发现另一 Guest/PID。
- 独立受信 `ReplayResourceOriginal` 入口显式选择同 owner 原意图。root 已留
  同原命令则 ReplayOriginal；可能尚未收到则 DeliverOriginal 发固定 Apply，
  仍由最终 root 锁内验证 floor、连续代次和未知栅栏。没有自动 fallback，
  not-found 不产生 NoEffect，另一个本地未知命令不可被替换。
- 写后重新核验完整 PG 成本/代次、root Original/Held/约束/Birth 和当前
  Runner/VM，再以 fresh UID/RV CAS 保存完整审计。PG/Runner 漂移保持未知。

## Mac 实际验证

最终去重 **4 个顶层测试，含子项共 21 个测试事件通过**，0 未修复失败、0 跳过。
测试执行真实 Session、client-go Lease、typed Store/RFC6902、双客户端、TLS；
资源客户端测试还执行实际 Ed25519/Guard/Mac 文件锁、写前 WAL 和 fsync。
PG 成本、前驱预算/Host/VM/排空、Kube API 和内核均明确为协议夹具。

| 检查 | 真实结论与日志（工作区 build/） |
| --- | --- |
| Prepared 完整预算、向上取整、未知尾部、去重、溢出、同 boot 映射不变量 | 1 顶层/9 含子项通过；`dsga-guest-ready-budget-final.jsonl` |
| 实际 worker 的 Grow/Shrink、两处丢回复、预算缺项、PG/Runner 漂移、unsupported、缺 hstate | 1 顶层/10 含子项通过；`dsga-guest-ready-race.jsonl`；重建 worker 的丢回复子项以 `dsga-guest-ready-restart-final.jsonl` 最终结果替换，不重复计数 |
| 同 owner 两个实际 worker 并发，只有一个 CAS 票据派发和落账 | 1 项通过；`dsga-guest-ready-concurrent-final.jsonl` |
| 原资源显式首次派发、未知不可覆盖、普通重试拒绝、root 部分效果后的显式原重放 | 1 项通过；`dsga-guest-ready-recovery-race.jsonl` 的 resourceguard 包 |
| budget/resourceexec/resourceguard/owner/pgexec/实际 Agent vet | 退出 0；`dsga-guest-ready-vet.log` |
| 实际 Linux Agent、root guard × amd64/arm64，CGO=0 | 4 个目标编译退出 0；`dsga-guest-ready-{agent,guard}-linux-{amd64,arm64}.log`，只编译 |

首次并发测试的屏障与生产单观察连接池互等，测试失败；已修正为同 owner 两个
真实 worker、各持有自己的观察池，最终 race 通过。没有放宽产品连接池或忽略
失败日志。初次编译 `-run ^$` 是零项预检，不计业务通过。最终源码 SHA256 在
`build/dsga-guest-ready-source-sha256.json`，未重复旧 PG、UDS 路由或鉴权全套。

## 尚缺代码与后验边界

Prepared **合同/消费者不等于真实预算生产者**。尚需实现可信 Prepare/Finalize
生产、系统 outside 上界的实际保留、首次独立准入/完整后台与并行/DSM 排空、
Host/Scheduler/最终 Controller/Runner/QMP、公开创建 worker/生命周期及受控
跨 Guest/PG boot/owner 的未知效果关闭。当前 GuestReady 执行器已接线，但因为
生产者未齐，不能称整体在线内存伸缩已经完成，也不能把缺代码改写为待 Linux。

Linux K8s/RBAC/NeonVM/KVM、真实 root/SUID、cgroup2/HugeTLB 全池、物理回收、
迁移及性能长稳均未实测，后续目标环境验收。真实内核物理释放 unsupported 时
拒绝进入 GuestReady。默认关闭；任何本记录或 Completed GuestReady 都不发 VM
许可、不证明无 OOM、不开放 SQL、不释放 Host 承诺。Goal 保持 active。
