# 原CPU本次读回合同

`cpureader.New(core,vms).Observe(ctx,binding,computeInstanceID)` 和Controller的非缓存
`NewForAPIReader` 共用同一生产实现。URL仅由原VM/Birth/Pod派生，单次12秒总界、
5秒HTTP界、4KiB响应界；不接受外部URL，不跟重定向，不使用环境代理，不内部重试。

每次crypto/rand nonce只关联本次实际Runner读取，不能生成Owner/floor或预算。
Runner读原QEMU/PID/cgroup、cpu.max、Guest整数在线和原Node v11承诺后回显nonce。
kernel boot-time仅审计，不设任意TTL，不能将旧Condition持续True当当前读取。
reader前后核原VM UID/generation/frozen spec、Birth UID/原文、Pod UID/容器/Node UID和
原Node账origin/同Pod完整固定承诺。其他Pod推进Node RV/sequence不等于重新授权。

出生只需原创建journal、原Birth、原Node/Pod既有完整持有和真实原QEMU/Guest；
不依赖PG boot、初始化后的runtime selection或PG Ready。有效部分事实返回
observation+ErrPending；CP不得据此发布新路由。Controller只按已fsync旧pending的
原generation/target精确恢复，未知未结清前不能用新目标覆盖。

该reader验证本次事实，不补足混合调度器/resize/force-delete的全入口平台资格。
原生kubelet admission对其跟踪集合有串行预算检查，但forced-deleted尚活进程可能
被GetActivePods忽略。v11承诺继续持有；完整平台资格须原精确支持配置和真实验收，
不能仅把profile标记、Pod Scheduled/Running或AllocatedResources当全部物理证明。
