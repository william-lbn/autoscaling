# 独立平台资格发布器

生产Agent、Runner和Host调度消费者已存在，此前没有独立的资格ConfigMap发布入口，完整Linux组件测试借用了显式模型资格。本批新增一次性`dsga-platform-qualifier`及qualificationissuer包；不修改既有上层资格DTO或PG版本协议。

发布器仅接受独立平台主体提供、由root保护启动配置固定原SHA的报告及两份原始artifact（真实Linux验收与持续硬约束）。SHA和报告格式只能固定可信来源，不能证明报告内容真实；本程序不生成平台认证、不从RSS/空闲值推算或外推目标。缺实际来源时保持未签发。没有新增签名/密钥生命周期或默认Source成功开关。

实际读取原VM/journal、健康owner Lease、Agent私有UDS的resource_state、Root出生及已确认限额、原不可变Runtime Secret、Runner Birth/实际发布、Pod/Node、Host账本/Lease与完整Pod硬限额清单。写前两次、写后一次核稳定来源。仅创建和读回既有保护型immutable资格ConfigMap并复用budgetclaims原消费者；没有Root Apply、PG/QMP写、Node初始化或VM/对象删除入口。Create丢回复保持Unconfirmed，须显式固定原UID恢复，不能按同名自动收养。

使用：先将可信配置、报告与artifact放在root私有且祖先不可替换的原目录，再以`dsga-platform-qualifier --config <原配置路径>`一次运行；集群内用原ServiceAccount凭据，集群外可显式提供root保护的`--kubeconfig`。配置/报告结构见Config/Report源码。成功只输出原claim UID/SHA及来源摘要；未知结果只输出原对象定位信息，不输出报告、Root状态、Kube错误正文或凭据。独立最小RBAC与实际Agent UDS挂载须由部署者显式接通。

验证：Linux隔离源码快照，10个唯一根测试、15子例race全通过，无skip；CLI静态构建0。新增正例不替换inspect，完整经过真实生产读取者和UDS协议peer的三次capture，再Publish并经原Reader核对；Node替换在Create前拒绝。其余覆盖输入保护、缺来源、Create后丢回复/固定UID恢复及前后漂移。Kubernetes/Root在本轮是明确protocol peer，不能称真实Source认证。首个capture夹具编译失败保留，不计通过。

现场前置仍未满足：三节点14个既有系统Pod缺完整hard memory limit，118也没有可复用完整non-Pod系统硬域。发布器仍按现合同拒绝，未修改CNI/系统服务、未向真实集群签发资格、未证明内存E或K8s通过。原证据与独审摘要见receipt.json；后续Helm和实际平台来源独立收口。
