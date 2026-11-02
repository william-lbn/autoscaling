# 原QEMU pidfd的有界中断重启

问题：原私有进程存活检查仅Poll一次，EINTR会被同死亡/未知一样立即返回。不能因信号中断直接授予alive，也不应将一次可重启读取当作终局。

实现：仅在同原私有pidfd上最多4次Poll0，每次重建PollFd。只有EINTR可继续；其他errno、可读/退出、NVAL或耗尽仍拒绝。必须观察真实Err=nil/N=0/Revents=0，再执行原signal0与proc启动身份核验。原processGuard读写锁和唯一close语义不变，不按PID重开。

验证：23d205的4race根/4子项、Host/seed编译通过。真实SIGURG中断同一Linux Poll，110次调用观察1次EINTR后真实成功；活进程、已退出、关闭fd、身份漂移都按原边界验证。不是模型Poll序列，原大日志留Linux /data。

边界：这是已证源码缺陷的独立修复；旧r3失败的真实errno仍未观察，不冒称它就是EINTR。本次也没有实际触发4次EINTR耗尽场景。normal降限和Linux/K8s全链仍待验收，不移动发行锁。
