# PG17-S-WIRE-001：准确6.12 KVM同步修复复验

状态：**PASS_COMPONENT_ONLY**。AS7ed9356424474f64489dec1f92a9eb864aaead71完整已提交源的独立只读git archive，CGO0/Go1.24.3禁网静态编译。5根、41子例全PASS，Guest exit0、QEMU正常关机exit0；无skip或TCG。复用原稳定case IDs，不重复增加覆盖数。

生产wire三个源未变；仅native fixture增加明确父子0x7a屏障。1/64个SCM_RIGHTS发送后子进程/stdio/pidfd保持存活，父端仍严格比较前后FD相等，完成后通知子进程退出并严格Wait。不是把断言改<=或sleep掩盖。原deadline负测的150ms延迟用于真实50ms超时，作用未改变。真实child UID/GID65534、SO_PEERCRED实际PID、wrong peer/type/短长帧/rights/deadline全部通过。

原attempt01失败原样保留，不能被本次通过替代：/data/neon-build/validation/pg17-native-birth-wire-kvm-20261030T163112Z（rights FD10→9异步stdio竞态，Guest1）。源码状态变化已准确区分：7ed继承已提交Origin三源，因此本次687份Go/mod实际COPY包含它们；仍排除所有活动untracked/uncommitted源。最初沿用1f“Origin必须不存在”旧断言导致前置停止，已记录preparation-issue.json，未在失败断言前执行编译/VM，没有删已提交源码伪造snapshot。

本次源manifest SHA08d288ed31c907470c7cae8230ed52de1aec646118f5de10ebdc7fbad24584d7；687份逐SHA匹配immutable snapshot。静态ELF SHAe0957dc3b08feb053792e34d1efc0459b7ac18a139c110f357ab0f3609ae2512。kernel SHA0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0，实际release6.12.26-william-bd33f7c78026，notes SHAb52a6321c44be2b8d138e9ca990ec86413c3744a189a118ed69aa295cb8ef03d。QEMU8.1.5 sha8d8d8ee83471749d0b882ce34344638e0ac0fa2390cd822b98d9b6314e2f9cb5来自锁digest f170364e...镜像，直接entrypoint启动QEMU，没有运行旧Runner业务。

2CPU/1GiB、Docker network none/Guest -nic none、Guest /tmp root0755；KVM硬件加速强制、150秒外部/90秒Go保护。实际3.60秒结束，本测试容器清理成功，没有宿主网络/配置/已有VM或K8s变化。

同产物既有PG17-S-KERNEL-001物理观察根也PASS：真实K272228352B，Movable800976896B，stock524288B，VMShrinkPermit=false；不产生新的覆盖ID或normal授权。没有PG、生产listener/ACK发送者、Root Pending/Enrolled、原maps/filter全认证、normal限额写、VM resize或E2E。

完整构建命令、固定工具digest、源码archive/COPY清单、CPIO算法/条目、原console、二进制及清理状态：
/data/neon-build/validation/pg17-native-birth-wire-kvm-attempt02-20261030T164834Z
