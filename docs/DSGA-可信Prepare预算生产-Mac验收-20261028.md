# 可信Prepare预算生产与原资格持续复核：Mac阶段验收

本阶段源码为`da291ce8f8549f5c225a3b6b00ce2efbf6dcd9ec`，在原
`codex/dsga-autoscaling-20261019`继续演进。上游冻结点、PG17、独立控制面和
部署边界不变。只签收本阶段，整个内存伸缩核心尚未完成。

## 已完成的实际代码

此前Prepare由测试或外部生产者直接提供Completed和Outside。本阶段新增
`prepareexec`，并通过可信Agent配置的`prepareQualifications[VM_UID]`及
`NewProductionPGWorker`可选参数接入实际执行链。未配置时保持ExternalStep，
不生成默认额度，不从普通VM注解、HTTP变配请求、RSS或当前空闲页选资格。

独立平台签发者提供immutable、受保护、同VM的`platform-qualification-v1`
ConfigMap。可信配置固定其原UID与完整原始JSON摘要；内容固定Guest身份、原
root bootstrap摘要、Runner出生引用/身份摘要、Pod/Node及确切VM规格。每个
规格给出经过平台认证的可用Guest下界、全部hstate目标、normal/有限任务/系统
组额度和互不重复的系统外部、过渡、安全承诺。没有相应规格就拒绝，不按比例
外推。系统组包含于外部承诺，整HugeTLB池只加一次。

签发者必须在目标Linux环境取得实际资格记录，本程序不自动认证或签发资格。
ReportID和摘要不是物理证明。Mac用例使用明确标注的资格/PG/root/内核夹具；
不能将本阶段通过写成“生产平台资格已取得”。首次接入和换boot/Pod等选择
仍须独立可信配置，不能让普通请求者自签一份声明放行。

实际顺序如下：

1. 读取原资格、已认证PG floor/稳定成本、root确认限额/出生和原Runner实际
   容量；核VM状态、几何、目标上界和完整预算。Grow按固定前缀保守报价；
   Shrink使用当前保留成本作安全上界，不拿逻辑下降提前扣款。
2. 先将完整原Prepared模板、资格引用、当时root摘要及确定性Outside名称写入
   原VM UID/RV WAL。Start回复未知没有票据，绝不继续Create。
3. 再次复核事实，创建受保护immutable Outside v2，只持承诺；不改PG/Guest
   限额、不发QMP、不开放准入、不退Host。服务端UID经真实回复或原保护对象
   读回获得，Completed将该UID存入Prepared，后续不能按同名换来源。
4. Create/Record丢回复后普通轮询只GET原对象。对象缺失不当NoEffect，也不
   自动重建；独立`ReplayPrepareOriginal`只选择原Key，原派发Session才可
   原样创建。`PGLoopOptions.PrepareReplay`独立于PG/Root/VM重放通道。
5. Host、Finalize、永久预算及实际取消重整的读者每次传递复核原资格UID/
   内容/保护寿命及原Pod/Node，避免只在Prepare时核验一次。

资格签发、Outside生产和Host分配是不同权限。配置新生产者需要相应预算对象
创建授权，不能给它资格签发、Host/Node写入或释放权限；本阶段未生成或安装
RBAC/部署文件，真实K8s权限与准入策略留目标环境验证。保护finalizer没有TTL；
原命令未知期间禁止释放后重建原来源，当前预算/未知引用仍存活时不能清源。

## 兼容与明确限制

历史Outside v1仍按原完整绑定读取，新生产者只生成带原资格的v2；不能将v2
删引用降级或让v1夹带新来源。旧严格JSON读者拒绝新格式。当前VM模型仍v8、
Node仍v7，没有放宽普通预算或改变原取消/Host领域的资格要求。

本阶段要求有限任务与系统计费组，目标不得改变bootstrap后冻结的相应限额，
全部hstate必须列全。**安全降normal仍未实现，Producer与GuestReady继续拒绝
降低normal memory.max。** 本组合只证明保持/提高额度的正确顺序，不能签收
完整缩容范围、连续后台/私有活性或无OOM保证。共享成本报价仍保守，不能用
测试中的字节常量或省略尾部/安全项来放宽生产规格。

正常请求的Begin/Finish仍由外层受信协调入口负责，本PGWorker不自动规划请求
或关闭完整业务。Mac组合使用实际Store.Begin/Finish；没有把ExternalStep当成
已自动Finish，也没有改独立控制面来掩盖该边界。

## 实际验证与采用结果

完整日志/源码摘要和逐项映射见同名JSON收据。本阶段采用去重结果为
**23顶层、117条含子场景pass记录；0 skip、0采用失败**。其中预算/来源两包
回归22顶层/115含子场景；新增组合是**1个场景及父记录共2条pass**，不是2个
产品E2E，更不与历批数字相加计算完成率。

- 资格/来源校验覆盖缺寿命、旧无系统/任务约束、系统未覆盖、总额不符、
  可用内存不足、重复规格、溢出、缺bootstrap、不支持的确切规格、v1/v2降级，
  以及原UID/内容/释放/保护/Pod/Node漂移。原预算/实际取消合同回归一并通过。
- 新增`trusted-prepare-replies`使用真实Session、typed VM客户端、UID/RV CAS、
  PGWorker各领域执行器、持久Runner与本地TLS/Unix QMP。Start提交丢回复时
  零Create；两次查询仍零Create；错Key重放拒绝；原重放Create提交后丢回复；
  只读恢复Record提交后丢回复；SDK重建得到同一原UID。然后真实执行完整Grow、
  预算/准入、显式Finish、Host结清，长期预算拒绝释放中的原资格。最后普通
  Start/Create完成后继Shrink Prepare，后继PG/Guest/VM动作保持Planned。
- 最终组合日志`build/dsga-prepare-owner-final.jsonl`，场景20.21秒。相同逻辑
  未改的其他取消/PG/Rust/历史组合不重复。Kube/PG/root/平台/QEMU peer仍为
  明确协议夹具，不证明etcd、RBAC、Linux内核或真实热拔。
- 五包lint为0 issues，vet通过。Agent配置选择已编译/静态核验，组合入口为
  Session/PGWorker，没有冒称执行了完整`agent.Run`或外层自动策略入口。
- 本阶段准确提交的Agent/Scheduler/Runner/Controller各Linux amd64、arm64
  共8个ELF另核编译/VCS身份，均不执行；详见JSON收据。没有构建镜像。

保留的失败不计作通过：pure-first编译时测试helper名称写错；owner-first/
second捕获新代码回执SourceID与原TargetID不符，已按领域绑定修正；owner-third
暴露测试误以为PGWorker自动Finish，改为既有显式Store.Finish。owner-fourth及
最终调用通过，lint首轮错误链/结构显式字段/格式/未使用值修正后复验通过。
没有删除失败场景或放宽身份/未知规则。

## 仍需编码与后验

本阶段关闭“已有资格下缺实际Prepare/Outside生产”的缺口。首次完整预算与
服务开放、安全降normal和后台/私有持续保证、缺失来源或Root变化的生产恢复、
真实换主、无Wait/Start/init失败、跨boot/迁移、全部来源生命周期、外层可信
请求/取消/重放与Monitor/自动策略/CPU接线仍有代码工作，不能归为Linux待测。

Linux需实际填写并验收平台规格/资格、cgroup/全部HugeTLB、真实物理回收、
NeonVM/QEMU/KVM、K8s RBAC/接管、Host开销/规模/长稳/完整Neon存储负载；控制面
完成后再移植统一产品E2E。默认关闭；不部署、不合main、不发布生产。
