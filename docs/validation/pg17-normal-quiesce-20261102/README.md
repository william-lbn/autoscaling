# normal1 降限前的关闭与排水接线

真实首 Grow 完成后，第二次 Shrink 已准备目标 N=1006632960，当前已应用 Held N=1077411840。旧 Quiesce 校验无条件拒绝目标 N 更低，使已支持 normal1 的关闭与排水步骤无法开始。

本次保留完整命令、Prepared、原请求、epoch、身份及固定任务限制校验，仅在完整 normal1 资格下允许继续 Quiesce；legacy0 仍拒绝未经证明的降限。共享校验也用于 Quiesced 事实和下游匹配，所有这些事实仍要求 VMShrinkPermit=false。

Quiesce 只关闭准入并排水，不写 memory.max，不缩 VM。真实降限仍须 PG 数组释放、Root 原身份和真实内存来源、固定 P+A+F+counter 连续覆盖、A/N 持久事务与实际读回，以及后续 GuestReady 和 VM 原授权。此次接线不替代这些独立证明。

60aa97：4 根直接耦合 race 测试、共 22 项通过，零失败、零跳过；Host 与 seed 编译阶段均零退出。覆盖完整 normal1 的关闭/排水事实、legacy0 拒绝及资格缺失拒绝。实际新整链 97583d 已启动，安全 N 降低和 Linux/K8s 端到端尚未验收。

独立调用边界证明、完整日志和私有输入保留在 /data/neon-build/validation。
