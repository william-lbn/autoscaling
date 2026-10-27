# 原 VM 未发送取消：Runner 终态与 Agent 消费

代码 `1e0d432d8ac6cd39c07e10c94f9227ebaf707a0a`，沿既有
`codex/dsga-autoscaling-20261019` 演进。14 个 Go 文件（10 生产、4 测试）完成
一条 VM 命令的原域取消，**不代表整条伸缩取消或整体核心已完成**。

## 原问题与不变量

VM WAL 的 ResizeVM 可以已经 Unconfirmed，但 Runner 尚未收到命令，或
本地只到 prepared/not_sent。普通 not_sent 仍可显式重放，不能凭 VM 的
AbortRequested、超时、404 或当前 QMP 容量等于 before 就写 NoEffect。
已越过 may_have_sent 屏障的动作可能晚到，取消不能抹掉它。

本批接通固定生产路径：

1. Runner 通过原 Lease/WAL/Lease、Birth/State/Pod/Node 读者认证取消标志和
   完整原命令，保存原派发 owner 与当前取消 owner，普通 Denied 不算取消。
2. 在与正常 QMP 发送相同的原 flock 内，只处理 prepared/not_sent 或尚未接收
   的原命令；两次 Kube 认证包围原 QEMU before 读回。身份/floor/取消改变、
   容量漂移或未知均不写终态。过期命令可以取消，但不能延长期限再发送。
3. 写前持久为 `cancelled_before_send`，保留完整命令/高水位及取消快照的
   owner、VM 序列和 RV。VM 与 Runner 序列属于不同域，禁止互相比数字。
   本地终态不可再转为重放，迟到选择也不能再次 qom-set。
4. Runner 继续通过原不可变 Receipt 和可变 State 发布。Agent 固定原 UID，
   两次核对取消终态及真实 before 后，以原 VM UID/RV CAS 只记录该 VM
   步骤的 NoEffect，完整审计保留命令、原来源和读回。
5. may_have_sent 仍只读原效果；真实目标完成只能保持 Completed。原回执
   丢失、UID/来源改变或写入回复丢失均保留 unknown/原持有，不猜测完成。

本模块不发送 PG Cancel、开放准入、降低 Root 限额或释放 Host。Shrink 此前
已发生的 PG/Provider/Guest 效果完整保留。VM NoEffect 之后仍须整份预算/
准入 AbortReconcile，不能直接 Finish 或删除原意图。

真实组合还发现 Host 消费者把其他域的 NoEffect 当成致命错误，取消落账后
会退出 owner；已改为 ExternalStep，交后续取消协调，不允许跳过前驱去
Start ReserveHost。正常 Host 原来源校验及缺源保留 Unconfirmed 不变。

## 格式与接管

首次新取消终态升级 **Runner record v2**，新增取消证明；新普通结果不降级。
原派发者仍来自原命令，接管者只能取消/对账，不能改原 epoch 后重新执行。
深拷贝包含 Last 内部取消指针；校验拒绝可能发送、错误容量、错误 owner、
空序列、旧 record 投影以及给普通结果附加取消证明。

VM Model v5、Node Model v5、Host CM v2、PG ABI 不变。外层 Publication/
Receipt 对象类别仍为 v1，严格旧读者会拒绝 record v2/新取消字段；这不是
允许旧程序忽略新终态的兼容扩展。Linux 验收须同批服务，独立控制面冻结
d84c 不修改，后续移植须同时支持 VM v5 与原 Runner v2 的完整语义/引用。

## 实际 Mac 证据

Mac arm64、Go1.25.13，最终去重 **32 顶层、153 含子项通过，0 最终失败、
0 skip**；四个受影响包 vet 通过，Linux 增量 lint `0 issues.`。
持久格式/执行器变化影响 vmguard 全包，因此执行该包 29/139；其余只选择
新增或受影响场景，未重复 PG17 原生及前批未改矩阵，不把重叠测试相加。

- 真正 flock/fsync/rename/严格解码测试：尚未接收、prepared、not_sent、
  接管、过期、no-op；迟到重放、may_have_sent 不清除、晚到真实完成、认证
  窗口变化、发送锁竞争、错误证明、深拷贝及新普通动作不降级。
- 实际生产 Agent/Runner/选主/私有 Unix QMP 协议组合：Grow 四种未发送
  取消/写入回复丢失/原不可变回执丢失、Shrink 保留此前 PG/Guest 的 VM
  取消、取消过晚只能落 Completed。节点峰值和初始持有均未改变。
- 受影响正常 Grow/Shrink、原 not_sent 显式重放、Host 正常消费及原来源
  丢失继续保留 Unconfirmed，各仅复核必要选择。

Kube/PG/Root/Prepare 与平台成本仍为明确协议夹具；Shrink 前置原生释放
在本组合为先前领域夹具，不能写成本轮真实 PG 收缩。真实 Mac 文件和网络/
生产协调代码执行，不代表 KVM、真实 apiserver/RBAC 或 Linux 物理认证。

原失败完整保存：首轮组合测试漏写 `types.UID` 到 string 转换，未编译不计
业务测试；随后实际组合暴露 Host 致命分流，已修并只重跑失败场景。此前通过
的原回执丢失路径在改动分支之前失败关闭，复用原通过证据。完整用例→日志、
14 源码 SHA256 和产物摘要在配套 JSON，原失败未删除。

可按选择复现：

```sh
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 ./pkg/dsga/vmguard
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 ./pkg/dsga/owner \
  -run '^TestProductionAgentRunnerGrowClosesBudgetAndAdmissionOnOriginalWAL$/^cancel-'
GOTOOLCHAIN=go1.25.13 go test -mod=readonly -race -count=1 ./pkg/dsga/owner \
  -run '^TestProductionAgentRunnerShrinkRechecksGateAndOriginalObserve$/^cancel-before-runner$'
```

准确 `1e0d432d` 的 Agent/Runner/Controller/Scheduler 双 Linux 架构共 8 个
ELF 已核 VCS、架构和 SHA256，**仅交叉编译，未 Linux 执行**。构建时只有
所有者 AGENTS.md 未提交，原 SHA256 不变，未暂存；不构建镜像。

## 尚未完成与目标环境后验

仍须代码：完整已派发取消的预算/准入 AbortReconcile、容量/来源显式结清，
无 Wait/Start/init 原 Node/CRI 受控恢复，跨 boot/迁移、其他写者/CPU，完整
可信 Prepare/系统剩余/过渡/安全预算生产者，首次/normal 准入与持续私有/
后台约束，以及 Monitor/策略/公开生命周期。原预算来源不会因 VM NoEffect
自动换为新事实；接管、恢复和历史消费者须按原完整来源继续认证。

Linux 后验：真实 Lease/RBAC/CAS/GC/CRI、故障/规模时延，QEMU/KVM/NeonVM/
virtio-mem/HugeTLB/cgroup、物理回收与残留成本、性能长稳/存储端到端；独立
控制面完成移植后再统一 E2E。Mac 结论只覆盖上述逻辑及明确夹具边界。

默认关闭，Goal 继续 active；不追冻结后上游，不改其他 PG/独立控制面，不
准备或执行部署、不合主分支、不发布生产，不将本条 VM 结清外推整功能完成。
