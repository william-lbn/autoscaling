PG17普通九角色内核控制机制组件（2026-11-01）

本目录归档已实际完成的组件，不增加任何新执行。准确Git3c2a5f3+独占normal_kernel_control_source前缀9源；当前9源与实际编译source-lock31ec9b6逐字相同，完整4806 COPY哈希及OK逐项匹配。固定工具镜像c940750e/Go1.24.3，BuildKit明确2CPU/2GiB resource flags、离线缓存；此批没有另存cgroup memory.max/cpu.max采样，不能声称有该采样证据。最终104172实际5race根6子例PASS，无fail/skip；原03同逻辑旧字节通过只留历史，不叠加U数量；04/05是native编译，不伪装新增U。

静态manifest路径/neonvm/config/dsga-kernel-control-source.json，构建期输出非授权固定模板，无Runtime/System反向自引用；运行期必须原只读System库存准确绑定。九枚举为acpi/allow-start/clock/daemon/kernel-version/logs/network/udev/udev-init；Root/resource、Compute、sysinit、shutdown拒绝。显式cap最小表：network NET_ADMIN|NET_RAW，clock SYS_TIME，udev/init MKNOD，acpi SYS_BOOT，其余0。所有五cap集合/NNP按原exec-stop重核；不靠仅CapEff或Root自报。

实际安装同一编译规则顺序为准确369内核及ABI6/固定路径检查→所有原Go线程NNP→所有线程Landlock→bounding/ambient/securebits/能力收敛→唯一TSYNC cBPF。程序固定90指令，x86-64/x32门禁、迁移/NUMA/swap/mount/ns/BPF/perf/注入/uring等拒绝，clone3 ENOSYS兼容旧clone；namespace clone拒。Root审核进程始终Seccomp0，原停点GET_FILTER逐指令比较完整程序并拒多余继承filter。Landlock限制的是受支持文件访问，不等于覆盖所有元数据或同UID进程控制。

f234e0在准确369实际KVM（QMP enabled/present true，QEMU8.1.5）运行：1536MiB/2CPU，outer4GiB/2CPU/noNIC，Guest0/唯一docker-QEMU退出0、OOMfalse，Go ELF9188ef38。一个S根九个role子例，全部actual cap/90指令/child Wait0。安装后同时持有四个新Go M实测继承拒；真实第二次exec-stop核完整filter、五cap集合、NNP1/Seccomp2/Threads1/TracerTid、FD仅0..2；执行后再核受保护写open EACCES、迁移等syscall EPERM、clone3 ENOSYS、ns clone EPERM，tmp/null正常。network实际写隔离Guest resolv.conf、clock实际写两固定目录正控；udev/init仅创建再删隔离char，未列入允许规则的新alias写open仍EACCES。没有向受保护sysctl/sysfs/cgroup/rawdev写入。

边界必须明确：九子进程是DEV机制harness，调用真实installer但没有运行真正九服务或生产Gate，也没有构造并签发production opaque System/Kernel来源。原Root/原task/原静态Source组合的生产API已写，但此S只验证机制与读取，不代表真实生产调用已通。LandlockFromFixedGateSequence只能依赖后续唯一原RO Gate完整checked安装顺序，不能描述成内核直接读回Landlock/securebits规则。stdio检查只类型，缺原创建pipe/peer typed来源。metadata与同UID signal系列为静态待防护候选，本批未执行任何对应反例，不宣称已经验证可绕过；父任务安排后继防护和真实服务兼容验证。

保留01/02 TEST_BUILD_FAIL（unix.Errno与uint32断言编译类型），03较早5U/编译，04/05 native-only，首9a02e2 S整体FAIL（network原fixture缺resolv.conf，8role通过不代表整体通过）。首S虽然Root-unfiltered marker存在，但Guest非0，所以不能单看marker改判；最终f234在补准确fixture及新源码图后实际重跑并通过。原raw receipts与CRLF保持，不编辑旧记录/锁/镜像。

continuous资格仍Unknown，Root/PID1/Compute/原stdio/全部respawn及未来控制写者未闭。没有删除Inflight MissingSources，没有normal硬降限许可、完整Linux E、K8s或生产认证；E=0。此次静态审查与归档无生产源码修改、无新测试、无Git操作；主代理独占最终索引/阶段提交。
