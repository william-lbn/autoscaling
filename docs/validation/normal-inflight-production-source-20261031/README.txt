normal物理在途来源与12GiB保守预算（准确369；组件归档）

范围和状态
本批独占normal_inflight_source四源，原五层normal拒绝、Kernel/Root/共享record均未修改。
6个精确U根、18个子例race通过；真实369 12GiB来源正控及target0 balloon负控通过。
这是有限来源读取+条件预算组件，不是PG工作负载、memory.max实际降限、全I资格、K8s或E2E。
AllSourcesClosed=false及NormalDecreaseAuthorized=false是生产未接线事实，不能由请求bool升级。

原来源前提
K是原base-only物理nonMOVABLE present容量；必须保持该物理域及原Root初期页数量来源。
reader当前只观察K和managed容量，不能补签Origin、PreSys先于第一次MOVABLE增长的历史。
页P=4096；准确config THP/KSM/CMA/ZONE_DEVICE关闭，swap0，全部Huge状态为0。
J=512来自原early硬PID补丁和readonly hardcap，晚写pid_max不成立；包含所有初始PID域任务。
C=64来自compiled CONFIG_NR_CPUS，当前possible=2不是未来硬界。后继只能以同VM原Host maxcpus来源优化。
Q=1仅覆盖普通匿名、tmpfs/shmem、RW ext4以及固定ramfs/iso9660普通文件的数据folio：
其配置/映射未启large-folio；raw设备、FUSE、DAX、未批准FS/驱动不在该闭包内。
其它可能高阶的opaque kernel/IO/slab/元数据持续在Normal物理域K覆盖，不能冒称nokmem关闭force。
原modules列表空再disabled=1、NUMA balancing0、demotion=false、memmap_on_memory=N、
无balloon及禁止未来设备/FS/BPF/cpuset/调优写者，必须共同由Root/Host原来源持续落实。
12GiB正控不是上述原启动顺序实证：其virtio-mem在fixture启动时即请求全region，未执行正式Origin/PreSys。

IRQ硬栈界（不用handler从不重开IRQ假设）
准确369 compiled DWARF：sizeof(pt_regs)=168；sizeof(exception_stacks)=32768，
DF/NMI/DB/MCE各8192，VC/VC2因AMD_MEM_ENCRYPT=n为0；IRQ_STACK_SIZE=16384。
同artifact/config/notes只读导出，原两次make均CACHED；不是另编TestOnly内核冒充原图。
entry_64.S/error_entry/paranoid_entry、calling.h保存15个通用寄存器、origAX及IRET帧，
在C体返回前保持完整168B帧。irq_stack.h在切栈前置inuse，嵌套IRQ不重置硬IRQ栈顶。
故在IRQ/全部物理异常栈上仍有活跃C体的嵌套入口数<=ceil((16384+32768)/168)。
另每CPU保守+3：最多两个留在task/外栈的首入帧（用户IRQ及首次切IRQ栈的内核IRQ），
以及无独立IDT帧的softirq。softirq禁止调度；ksoftirqd正常任务另已计J，重复预留允许。
NMI的nested_nmi只安排repeat_nmi并iret，不再次进入C；原活跃C帧保持在NMI栈。
异常返回/重复入口不把仍持未释放普通页的C体移到任意额外栈后忘记它；固定可信kernel，
无自定义栈切换/注入/module/BPF来源是必要限定，不涵盖损坏内核或硬件RAM故障。
H=C*(ceil((16384+32768)/168)+3)=18944。全35页/槽已保守包括31+single+failed+precharge+lookup。
旧真实31页GDB窗口IF=1说明0007只封counter撤账，不封物理retire；本式没有混用其4C。

占用中的在途I（不以Mmax充当I）
I_occ=P*[512*J + (1+31+1+1+1)*J + 35*H + floor(K/16384)]。
512J：每个migrate_pages调用最多512个Q1配对；转账前dst及转账后src是同一对的影子，
不叠加两份512。charge迁移后真正validated GUP/DirectIO/pipe保留引用使原ref freeze失败，
或仍留在原leaf账内；不能按所有已收费pin容量再加I。完成mapping/PTE复核前的投机引用每上下文一页。
31+single+failed：swap/vmscan的folio_batch最多31；single __folio_put及failed filemap_add单页。
准确全mm/fs/include caller收据已保存；正常free是uncharge先、PFN归还后，无反向free先行而counter滞后窗口。
filemap失败caller限固定普通FS通路，失败页不进入持久旁表；free路径不递归分配/持有新普通数据批次。
exit_notify释放PID后才有splice_pipe/task_frag put，不能继续以J约束这些晚退出者。
仍在执行或阻塞的任务持有原16KiB VMAP内核栈；栈是GFP_KERNEL非MOVABLE，所以晚退出数<=K/16KiB。
允许FS的pipe ops逐个buffer释放单普通页；task_frag及watchqueue等Normal分配留在K，非任意模块回调。
被普通进程/文件/RCU/IO保留的仍收费页由永久leaf cap覆盖，旧boot/旧FD不自动抹账。
内存硬件poison/memory-failure单页旁路不是普通I：真实故障必须停止资格/保Unknown；
不允许通过madvise HWPOISON/SOFT_OFFLINE、Root sysfs或Host注入制造未声明长期坏页。

allocator可用性预留（已空闲，不能叫占用I）
R_compact=P*J*(512+1024-1)=3219128320B。单cc只取一个pageblock，nr_migratepages<=512；
free isolation最后一个buddy最多1024页；order0目的页总可用既有任意高阶free list，故不持续叠加新pool。
同一cc里512迁移影子与free pool分别取最大，重复保守预留。Q1闭包下高阶direct-capture仅Normal/K；
PF_MEMALLOC阻止普通支持路径重入direct reclaim/compaction。禁止自定义回调/非常规GFP_NOMEMALLOC路径。
PCP：准确zone_batchsize<=63，原显式fraction f使high_min==high_max，zone high<=max(Mpages/f,4*63)。
对原single-NUMA唯一MOVABLE zone，不使用当前online CPU除数，所有64 possible CPU逐个保守计：
R_pcp=P*C*(ceil(Mpages/f)+4*63+3*63+8)。3类order0 list的refill及单个高阶free插入均留余量；
其它opaque高阶Normal缓存已在K。旧defaultfraction遗留页需原base-only K覆盖，不从当前空闲猜。
水位：MOVABLE min<=128页，但distance必须用未截断tmp=(minfree_pages*Mpages)/managedNonMovable，
不能错取128/4。保守计PROMO：R_wmark=P*[128+3*max(ceil(tmp/4),ceil(Mpages*scale/10000))]。
managedNonMovable是原固定物理容量floor，不是free/RSS；禁止Normal offline/balloon/后续hstate改写才持续成立。
watermark boost必须原固定0。PCP和水位在本批12GiB真实Guest实际读取/参数核对，但非压力/allocator无失败认证。

force与counter
F_force<=K需持续禁止force Normal页迁出：NUMA/demotion关闭；PG已禁相关迁移，Root/System须固定无迁移调用，
内存热移除只ActualMovable，compaction同zone。正常RW ext4允许：PF reclaim/evict不创建新MOVABLE数据，
NOFAIL buddy/bitmap等GFP_NOFS/GFP_USER metadata最高Normal；swap0及无FUSE排除replace_folio两条支持外入口。
任何未批准FS/设备/动态kernel代码会破坏该来源，不能由nokmem或采样counter代签。
D=128*(4*C)*P=134217728B来自准确0007完整IRQoff ancestor cancel两循环，仅counter parent-leaf正差。
CPU已付stock另保64*C*P=16777216B，未冒称完整I；leaf cap本来也含付账stock，重复保守允许。
normal候选目标N=P_leaf+A_leaf+K+D；Outside另保K_kernel+K_initial_root=2K；同K三份预留不相减。
opaque Normal/K容量上界不独立证明该区永不耗尽；本目标保持K物理域不变，且普通MOVABLE有独立空闲余量，
避免因本次缩容制造新fallback/Normal压力。固定Root/System资源与服务活性仍须实际回归。

实际12GiB组件结果（没有写normal/leaf max）
准确369，Guest base512M/kernelcore512M，唯一vm0 max/request11776M，CPU2/maxcpus2；预算仍取C64。
Docker限制16GiB/2CPU，无NIC、无TCG；启动前MemAvailable>18GiB。实际K=536334336B，managed=106082页。
原allocator候选：minfree4096KiB、scale10、boost0、proactive0、fraction1024；沿旧128会显著增大PCP。
输入仅条件规划：P_leaf+A_leaf合计1536MiB，System512MiB；必须实际包含helper reserve/数组最大范围。
若helper仍按AS+ELF保守预留，不能把包含数组VA的AS当RSS，或自报A1G足够；Root后续实际选业务参数。
实际总需求12140470272B，12GiB余744431616B；normal目标2281164800B，PCP923009024B，水位93814784B。
这些数来自真实读取+纯公式，Mmax本批来自明确fixture QMP/argv候选；未发原同VM Host→Guest来源资格。
8GiB/C64候选不通过，保留拒绝；有原Host maxcpus2后才能重算优化，不能仅改currentpossible。

剩余生产消费需求（精确有界；不要求新kernel）
1. Root同VM原argv/QMP base/max/双backend/唯一virtio-mem/无balloon/CPUmax，持久来源与Guest region互证。
2. base-only→KernelPrep→Origin→PreSys→初增→正式Bootstrap真实顺序；固定K及低区managed floor。
3. 固定System执行/配置manifest，禁自定义inittab/mount/sysctl/root交互，封设备/BPF/NUMA/FS未来写者。
4. 全FS/ELF/plugin闭包和split永久身份、P固定与A所有cap/计费写者marker接线；不冒称旧VM可补资格。
5. 以上齐备后才消费本公式并做真实PG降A/同normal父memory.max下降、故障恢复、PG17业务与Helm回归。
本批不改共享协议、不开放五拒绝、不更换369五元组；现有真实PG17恢复能力不因本profile被静默删除。
