PG17普通角色单leader最终信号域门（2026-11-01）

本增量只有normal_kernel_control_signal_前缀5源，原9源/90指令/manifest/审计不改。准确Gitb73d77ef+5源、5644COPY完整核对；构建快照仅将.dockerignore覆盖为包含全部已列输入并明确记SHA，不改仓库.dockerignore。使用既有env.sh/缓存，BuildKit实际memory.max2147483648、cpu.max200000/100000，outer2CPU/2GiB/300s，Go1.24.3。最终fd25ad 7race根19子例PASS＝原KernelControl5/6直接回归+新2/13；不重复累计第一次98e85a同U结果。

准确369/ABI6本来已有SIGNAL scope bit1，但不能直接把现FS AllThreads ruleset改为scoped：每个原M独立restrict会成兄弟域，Go SIG33广播/SIGURG可能被拒。故所有原文件/namespace/parent/角色/向量/ctx/ABI检查、C字符串和nil结束指针分配、scope-only 24byte{fs0,net0,scoped2}原CLOEXEC FD准备都在收口前完成；原ordinary9 installer仍执行全部AllThreads FS/NNP/cap/TSYNC。最后仅原Gate leader进入不可返回ASM，landlock_restrict_self446→execve59；scope失败raw exit_group125，exec失败raw exit_group126；若未知filter连exit都拒则UD2，不回Go。新业务exec清旧M，后续线程/子进程从同一个域继承。

两层private API：通用Linux Gate调用execNormalSystemOrdinaryRoleWithKernelControls(ctx,originalSystem,enum)，内部从原RO库存构造normalKernelOrdinaryControlPlan，只接受System3普通9与原argv/PATHonly；amd64内部execNormalKernelOrdinaryRoleSignalScoped。Root/resource/Compute/broker/PID1/unknown没有ordinaryplan；其它Linux架构stub明确Unknown。原leader pinned、真Root和准确Go1.24.3前置拒绝。没有新外部路径/向量参数，也没有宽松未知版本。进入ASM后的Go defers和panic仅是不可达源代码，不构成回退路径。

ASM NOSPLIT/NOFRAME且显式PCDATA UnsafePointUnsafe，不含CALL/RET；实际ELF objdump已逐指令保存。以typed Go指针传image，image另持全部[][]byte和[]*byte，前三指针对应0/8/16布局；不以uintptr藏底层对象、不在scope后调用KeepAlive或调度。固定c940工具镜像只读导出的准确Go runtime源码保留：preempt.go isAsyncSafePoint拒UnsafePointUnsafe及FuncFlagAsm，signal_unix.go只在安全点注入异步抢占；funcdata.h说明当前包原型隐式ArgsPointerMaps，os_linux.go说明AllThreads SIG33。不能写成“绝对没有signal handler”；论据是该不返回unsafe asm不接受异步抢占、不回Go跨M，而不是声称信号不会到达。无需改变原信号mask/handler，成功exec后应用正常初始化。

真实S d00d6c、ELF8e08060e、4.202s，369 release/notes精确，QMP KVM enabled/present、Guest0/唯一docker-QEMU0、OOMfalse，1536MiB/2CPU Guest、outer2GiB/2CPU/noNIC。新1S根2子例：原scope FD坏/原exec目标不存在分别125/126且零Go返回输出；实际最终exec后四个同时Go M可tgkill SIGURG、单P紧循环依靠真实runtime异步抢占让timer/main G推进；同域后代exec后SIGUSR2→SIGUSR1往返成功。域外目标是本用例新建并由Parent持续持原PIDFD的专用worker，scope前kill0/tgkill0/PIDFD0成功，scope后kill/tgkill/tkill/pidfd_send_signal/rt_sigqueueinfo/rt_tgsigqueueinfo六实际入口EPERM，worker最终收到0个测试信号。没有向Root/PID1/PG/broker/任何既有服务发送信号。父审核保持Seccomp0；同一个新ELF原普通9机制S直接回归全部通过，不能增算九个新S。

边界：新S用DEV机制入口直接调用同一installer/ASM，不执行生产Gate原typed plan分支，也不构造production opaque；尚未执行真实九服务。acpi原poweroff脚本可能依赖向PID1发信号，scope会封跨域，所以必须后继实际服务回归和显式受控Root关机路径，不能假称功能兼容。真正来源须原唯一RO Gate固定分支+准确ELF+原Parent第二exec-stop审计组合；GET_FILTER不能直接读出Landlock。准确369原build里的landlock源码字节暂未追加导出；官网6.12 primary只是静态语义依据，实际369机制本次已验证，不能把官网源写成原编译文件读回。

失败保留：e51dc2缺env导致Docker Buildx plugin不可用，build125、U/native null；6712c8默认Dockerignore排23.github文件，源完整性校验在测试前拒；98e85a U7/19真通过、Native TEST_ONLY unix.Tkill封装不存在编译FAIL，改同一SYS_TKILL raw调用后最终fd25ad重跑，两生产文件不变。没有删断言、放宽过滤、修改任何旧receipt/镜像/发行锁，所有原nonce日志保留。

metadata、原stdio、Root/PID1/Compute/allrespawn及完整normal_authority仍未闭；normal未授权，Linux E/K8s E均0。此阶段只完成signal机制，不代表所有future writer持续证明。Root统一Git阶段提交，P2唯一负责生产Gate后继接线。
