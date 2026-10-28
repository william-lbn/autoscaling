# Root原持有约束的显式管理恢复入口

日期：2026-10-29，Mac Darwin arm64；准确代码提交
`463c1ad8031de4d15bc6e7c8ced6b165ff13ca40`，沿自有Autoscaling既有功能分支。
PG17 b078保持不变。阶段通过不等于PG17核心完整。

## 实际行为与恢复不变量

在默认关闭的Agent私有管理入口增加三种动作：`resource_state`读取原Root资源
View及可修复来源摘要；`repair_held_resources`显式选择原Held来源的小写SHA256；
`observe_held_resources`只读确认同一修复。状态漂移不自动发修复，原命令未知而
无法形成可修复摘要时，不返回虚构selector。修复和观察复用构造时的同一个
ResourceWorker/SDK，全部进入当前正式Session的同一个PGWorker.Run串行循环。

原VM/Compute/Guest boot、当前owner、Root最终fence和逐维单调检查继续生效；
管理调用者不能替换限额或原命令。修复回复丢失保留PendingHeldRepair，只有显式
原摘要观察恢复；不创建第二个客户端绕过未知栅栏。Reconciled仅表示Root独立
恢复完成，不写外层预算、GuestReady、PG、VM、Host终态，不发VM缩容许可。

## Mac增量验收

采用既有终态日志，不重新执行：两包-race去重4顶层/20含父子记录，0fail/skip；
四包vet退出0，四包相对b9f9ab7f增量lint为0 issues，diff检查通过。准确5个源码
摘要、逐用例和日志摘要见[机器收据](DSGA-Root管理恢复入口-Mac收据-20261029.json)。

新增正式Elector→ResourceWorker→PGWorker.Run/管理端点组合：实际Root约束漂移后
状态可查；错摘要零POST；原摘要修复已生效但回复丢失；保留原Pending；显式
Observe只读清修复栅栏，总修复POST为1；外层WAL全部字段不变。协议选择校验及
既有ResourceOwner同域丢回复场景为受影响回归。Root内核行为为明确TLS夹具，
复用已有真实Guard证据，不能将本组合称实际cgroup或完整Neon E2E。

## 尚未完成与Linux边界

本入口依赖PGWorker已经成功构造；建立前Root异常的管理恢复仍需代码。
安全降低normal、完整私有/后台持续上界及活性、其他来源/启动/无Wait/跨boot/
迁移恢复、Monitor/自动策略、全部写者与CPU接线仍需实现或组合，不能写成仅待
Linux验证。PG17完整实现归档后继续PG18、PG16，各版单独适配和回归。

本阶段没有新Linux交叉编译或执行；前阶段Agent双架构ELF源码是12a993e7，
不能作为本次463c1ad8的编译证明。真实Linux/K8s/KVM/NeonVM/HugeTLB/物理回收、
系统生命周期和性能仍须目标环境。持续目标active，上游冻结，独立控制面边界不变；
不构建镜像、不部署、不合main、不发布生产，阶段归档后继续核心代码。
