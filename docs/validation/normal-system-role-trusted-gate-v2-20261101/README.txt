本批V2 supersedes旧138路径收口：Root独审发现Readdirnames返回partial names+非EOF错误可能漏读未知历史。仅journal条件改为 e!=nil&&!errors.Is(e,io.EOF)；其余14源逐字节不变。新Git3c2+仅自有15，4789实际COPY，同新GoELF57d781eff2befb181294bd91a2562815f677e136d97fc288fc217cacce39ecd5四个真实Guest全PASS；旧7096源码/结果原样作为历史，不转签。

本批固定System可信Gate组件：15源冻结，未接公共启动/Source/normal许可。

真实关键事实：
1. 原child原子CLONE_INTO_CGROUP+PIDFD出生为固定static guard Gate；不是让任意目标先exec。
2. Gate持原PIDFD_THREAD，能拒创建M已死而TGID仍活的早期竞态。原M在确认首exec-stop后设置TRACEEXEC|EXITKILL，再发送固定nonce/枚举GO。
3. 第二exec-stop按原RO闭包/同inode核主ELF与真实PT_INTERP入口及ASLR load bias，完整原namespace/4UIDGID/argv-env摘要/原PIDFD-proc身份核验，原pending计划SHA再核；先journal fsync后detach。
4. 普通角色原创建nonleader M租约直到原child真实Wait；函数先返回不归还创建M，避免晚Pdeath杀合法长服务。
5. broker仅由额外RO声明固定lifetime=initial-root-to-pid1-after-durable-release/GateABI1选择。普通role enum不接受broker。可信Gate在EXITKILL已装时清Pdeath，原第二stop+released持久后允许创建M结束；实际原initialRoot正常退出后broker仍活且PPID1、old/new consumer明确配对。
6. broker清Pdeath但released前原M单独raw SYS_EXIT，同TGID仍活，原target被EXITKILL杀死、零writer/零目标入口，pending保留。

验证计数：11个Go race根/53子例（每个同图任务均通过，不重复累计）；实际369四个Guest承载NEON-S-ROLE-GATE-001..010稳定检查点，其中普通故障矩阵另有4进程死亡+4原M死亡子场景。所有15 owned SHA与GoELF相同；4789实际COPY逐项核过；Guest/Test/QEMU均0且QMP KVM真实enabled/present。不是KVM Host rc单独判通过。

范围：006实际formal47ef镜像compute_ctl+glibc默认闭包，固定DEV --help向量，不是PG workload；其余System程序仍static Go stand-in。008/009是DEV broker ready/COMMIT prelude，不是生产listener/FS移交。普通9 controls尚未集成，Root审计仍Seccomp0，Compute保NNP0供既有SUID PG链。

旧6源/39档案的无Gate spawn存在真实父死阻断，保留历史不转签。首次broker9632ff因私有role分支漏接失败，保留；修复后新nonce实际通过。C独立反例/机制与Go真实路径分别归档，不互称。raw M死亡下可能保留等待goroutine，pending/预算Unknown；不宣称异常线程自动恢复、Wait即物理全部free或完整future-writer资格。

后继生产接入：Root固定main分别接role-exec-gate/system-role-broker/exec-system-role；StartBroker只能CheckGuest后FS original capture持原flock，转移原ns/root FD，严格originalInitialRoot/newBroker pair及真实listener ready/COMMIT；ordinary请求仅enum。此批未做。

持续安全normal、完整Source与K8s E2E仍未完成，原五安全拒绝保持。
