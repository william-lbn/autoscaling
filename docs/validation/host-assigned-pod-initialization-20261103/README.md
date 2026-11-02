# Host初始账本：已绑定普通Pod兼容

问题：真实三节点已有默认调度器创建的系统Pod。首次Host建账会扫描原Node全部已绑定Pod，旧检查却要求每个Pod都声明自有调度器，因而在核内存上限前即拒绝合法存量对象。

改动：只允许已经绑定到原Node的普通Pod沿既有完整hard limit路径入账；未绑定候选和VM仍必须属于原调度域。同UID读取前后schedulerName漂移也拒绝。没有namespace白名单，不跳过Pod，不把requests或RSS当hard limit，不改Node预算协议。

验证：独立HEAD归档加本次两个源码文件，真实Linux Go `-race` 定向运行4个根测试、16子例，全部通过且无skip。新增正例核原账本确实保留2GiB及既有Pod overhead；负例覆盖未绑定/异Node/无limit/调度器漂移/VM外域，保留原完整初始化及11项拒绝回归。见receipt.json及/data原日志。

边界：现场14个既有kube-system Pod未具备完整内存上限，修复后仍会按原安全合同拒绝；本提交不能证明平台资格或真实K8s端到端通过。没有修改现有集群、CNI或宿主网络。
