18c bridge：独立R2父进程门实际阻断修复（2026-11-01）

结论与历史
Root独审发现916818虽14race/36sub通过，生产KernelR2的readCurrentLocked误调用原R1 normalKernelPrepV2Caller。原R1函数要求PPID1；实际R2 bind发生于R1仍活且等待READY时，PPID=原R1，故必拒。不能先让R1退出规避门。原282冻结与其全部源码/原日志保持历史，不覆写receipt。本次只解冻newbroker+专属newtest两个文件，原KernelPrep5与其余29源不变；新候选manifest覆盖31当前源码+两批归档，Root唯一Git/index写者。

修复
新的normalKernelPrepV2R2Caller单独实读本线程/proc status和Capget，不约束父进程为PID1。仍要求当前真实UID/EUID/GID/EGID0、status四Uid/Gid全0、NNP0/Seccomp0及KILL/SYS_ADMIN/SYS_PTRACE/SYS_RESOURCE原Root能力。Source准入仍须原transfer/proof clone Task、当前R2 private nonce/start/PID与原8namespace；任意Root进程、JSON/current0仍不产生Source。没有放松原R1 caller，也不重开原sysctl路径。
kernelPrepV2BaseRead449..582与依赖Identity、kernelBootstrapNamespaces、sameSB preflight闭包已静审：实读精确18c/notes/marker、原硬PID和allocator控制、当前VM/boot/cgroup/state/mount身份与原PID1 namespace。sameSB preflight只pin真实PID1与mount graph、只读System库存；没有R1 task/PPID1、PGEmpty、旧floor/state.json依赖。此静态结果不是全生命周期S。
本R2 Source目前限定NNP0/Seccomp0/原Root caps阶段；未来Root自身TSYNC/降cap后需要显式受约束Root phase opaque证明，不能将seccomp2任意放行或清MissingSources。当前完整normal仍Unknown。

准确新验证
949aa7继承不可变916818全tree，仅叠加两个授权修复源；不抓live Root/P2。15race根51sub全部PASS、0fail/skip；新父进程87(代表R1活)与1正例、四Uid/Gid每槽、NNP/Seccomp和Root cap缺失拒绝均为纯status边界U，没有假称真实fork/R2 S。Linux tagged nativeCGO0只编译及Darwin arm64三包编译全部0；build0，58.494秒，实际RUN2CPU/2GiB，7458 COPY逐SHA检查。当前native入口依然只编译，未执行。原raw result仅记录test_exit，其余4compile rc已逐个核0，详catalog。

仍待真实S
原R1仍活时：通过真实decl3/transfer2第五原ROFD、原Gate/proof、typedFS2依次构造独立KernelR2与MemoryR2；R1正常退出后：同R2按原Source再次观察、procfs Pread0与原8WAL历史仍可用；PG populated/fence/resize后不重PGEmpty/floor。两phase必须实际同源ELF/18c KVM验证，不以本U的PPid字符串代替。普通9服务、Compute SUID、未来Root writer、完整safe normal以及三worker Helm E2E均未由本批通过，S=0/E=0/normal=false。既有failed617原历史记录保持。
