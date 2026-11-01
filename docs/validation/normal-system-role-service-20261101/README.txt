固定System v3生产角色broker服务组件：阶段冻结

结论：最终b44dd6准确12源+Root8，实际5636输入COPY逐项匹配；27个Go race根/136子例，零失败零跳过。真实CLI静态编译0，native编译0，夹具导出0。准确369真实KVM单Guest执行一个TestNativeNormalSystemRoleServiceInitial根：001..004是该根的四稳定检查点，不能算四独立S。实际Guest/QEMU/原initialRoot均0，48.082秒。生产Start/RunBroker/Client和Root initial薄helper/FS adapter由真实TestMain调用；CLI二进制c42c只完成编译，未在Guest执行。其它daemon和compute为明确静态Go替身，BusyBox init/vminit/musl是真实字节；无PG、无完整Source/normal/K8s E。

实际覆盖：原KernelPrep→Origin→PreSys→Split→Provision→Check后原FS捕获；initialRoot与新broker原task配对；原clone PIDFD/procdir和FS namespace/root共4个SCM_RIGHTS对象实际交接；同flock两阶段且不跨进程等待持锁；真实candidate listener→原probe→ready持久→COMMIT ACK，原initialRoot正常退出，broker仍存活。固定13行v3由PID1直接启动空环境Root client；payload/broker保持原PATHonly。compute原FD9通过pidfd_getfd取得同OFD，固定tmpfs路径/devino/root0600/nlink1和实际全文件FLOCK WRITE；不要求锁记录的PID等于client（短命flock子进程可退出）。该锁保持到真实target Wait，未转9给target。stdio/其它FD尚非typed完整来源。

修复真实故障：
1. service锁名不得占system-role-<nonce>历史命名空间；保持strict scanner不变。
2. 旧finish先置Unknown再等Wait，新service不得对长驻服务立即调用它。现保持原known run与creatorM租约，原child.done+waited后才在同exec序列归档。下一spawn前只retire本broker持有的已退出原run，三阶段文件inode/ctime/hash及原资源身份齐全才调用旧finish；partial/缺见证/未知Wait继续Unknown，不扫描WAL收养。
3. packet/auth10秒与受限排队90秒、exec-stop30秒分离。取得exec序列锁后重新核原peer/固定Source/FD，再创建spawn；Accepted回复另10秒。Accepted后双端显式清deadline，只有原client死亡/断连、明确ctx取消或target真实Wait结束控制运行。取消只原clone pidfd+单一Wait；不以超时假释放。
4. DEV acpid先前漏分派导致-f进入testing parser；最终工厂开VM前精确核完整固定payload名/替身字节，acpid明确长驻。network在无NIC fixture可真实失败退出，不宣称9角色服务兼容性。

失败历史：旧目录/日志/磁盘/缓存全部保留；本档案只复制可审小收据。a0853e曾将fixture exporter错误归到test.exitcode，真实U无fail；stage-naming-erratum单列。8b4d1c原driver把VM source-lock覆写到同名build文件，原文件保持，build-source-lock-recovered与原metadata SHA核对，source-lock-naming-erratum单列；最终build与VM分目录，不再覆写。9ae594是U的proc-environ在exec用户ready前采样，修为原child USER_READY/ACK握手；其它旧FAIL、ae6313的10s排队pending未知、4e44b5修前acpid替身均只历史，不转签当前源。

当前边界和后续：普通9 Landlock/caps/seccomp/SIGNAL最终门尚未在此Gate接入；ACPI关机兼容、typed stdio、真实compute/PG、原Host与FS/memory持续writer、完整Source/I/normal降限与K8s E仍未完成。此模块不删除正常五处拒绝，不将System/FS组件raw摘要或一次census签成连续资格。FS reference当前只Protocol1/Layout2；Root后继selector2/MemorySourceSHA必须另批精确适配与验证。

所有Git/index/提交由Root统一。stage只本12源和本档案；Root8仅reviewed hashes引用，不擅自stage。当前精确图Darwin未额外运行，非Linux新入口是明确Unsupported。
