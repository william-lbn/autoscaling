KernelPrep policy v2：原R1 user namespace未来写者屏障组件归档（2026-11-01）

结论与版本
本批仅新增normal_kernel_prep_userns_v2_前缀5文件；原policy1 JSON、源字节、8WAL与分派保持原样。严格新v2构造/reader仅接受显式18c元数据防护profile、protocol_version=2、必填max_user_namespaces=0及唯一新启动marker，不自动提升任何normal资格。准确源码hash见catalog.json与source/；准确base151加已冻结Root5叠加来源见go/source-lock.json。Linux功能分支沿用codex/dsga-autoscaling-20261019，无本agent Git/index写入。

实现与原始来源
任何actor前，原Root在资源原flock中捕获原VM/compute/boot/cgroup/state身份、PID/startticks/private nonce、原ELF、8namespace与初始userns原FD、原procfs leaf FD和资源lock inode。私有writer只允许固定user.max_user_namespaces对象写字面0；不存在任意值/任意路径公开setter。先持久新family intent再设置0和原8项allocator控制，读取原对象后写complete。
新WAL writer只允许固定2文件名且<=64KiB；只有实际O_EXCL成功的原FD、完整字节SHA和对象身份可登记private witness，并保留其只读FD pin；开前EROFS/EACCES/EPERM或EEXIST即刻失败，绝不通过可见同字节文件补登记。file/dir sync未完成时维持Unknown，保留实际创建来源供同Root显式重放。旧generic helper不改。
Prepare与Replay只允许preactor边界；Observe只读且零write/sync，不重新要求PGEmpty，可在模拟后继PG populated/Movable/history变化后观察原历史。该生命周期U是模型证据，没有实际启动PG。current0或反序列化raw证据均不能取得opaque Source；持续资格函数明确返回Unknown。

单元测试U
当前63b78f实际COPY5973、race 9根28子例、0失败0跳过，31.842秒；原生Go/C编译、ISO fixture导出均0。仅验证本批policy/事务/private witness/错误语义：版本兼容、原Prepare/Observe/Replay、partial、actor/history/身份/8ns/对象/第三值拒绝、同字节替换拒绝、资源lock替换拒绝、后继历史Observe纯读、开前错误不可收养、sync失败保留真实原FD。原Neon无修改逻辑未扩测。

独立真实Guest S
678594两台全新无PG隔离18c VM依次运行positive与partial-replay，均实际QMP确认KVM present/enabled、Guest/QEMU exit0，分别约6.039/5.916秒；每VM Guest512MiB/2CPU/kernelcore512M（Movable0），外层2GiB/2CPU/noNIC，原init与命令/资源/终态保存于guest/。准确kernel artifact18c9fd3a...、notes/config由既有冻结18c构建提供，未重编或替换旧369。activeLSM、原Root/procfs mode/8ns与原值1860由原日志和WAL保存；诊断before记录发生在flock创建前，Lock0仅诊断，持久intent/after绑定实际原lock。
独立C子worker先完整清空五capsets、四UID/GID1000、NNP0/Seccomp0；设置0之前真实单线程unshare(CLONE_NEWUSER)成功且已Wait退出。设置后unshare/clone/clone3 NEWUSER均得到内核ENOSPC，同时普通fork真实Wait0及Go四个并存锁M线程正控通过。partial模式在真实写0后注入DEV故障，Prepare/Observe保持Unknown，原Root显式Replay恢复且intent原字节不变。另建专用Root测试子进程被拒，无修改WAL。没有向现有Root/PID1/PG/broker发信号，无受保护metadata反例，无宿主sysctl/网络修改。
单台VM一个Go测试根，合计2个独立VM组件场景；C内部3拒绝入口和Go线程不虚报额外端到端S。raw QMP最初FileNotFound只是启动前轮询；最终完整握手已确认enabled/present，详见原帧。原ext4/ISO/ELF保留Linux nonce目录，仓库保存SHA、构造输入、raw console与只读debugfs导出的原2WAL；不提交大二进制/盘镜像。

历史与复核
history/initial-f3ef01保留先前源5、全部构建/测试日志与原终态，旧U通过36.868秒，但不作为收紧后当前源码证明。当前图重新编译与U/S使用同一准确ELF。本地native驱动最初Python嵌套引号语法错误发生在发送Linux命令前，修正后首次Linux nonce即678594，属于驱动准备错误，没有虚构VM/编译失败，也未改源码绿化。原receipt一律只复制不编辑。

仍未闭合的生产边界
目前opaque backend.verify绑定当前consumer==原R1且原task仍活；只能原R1生产/同Root重放，不是R2长期消费。后继R2必须通过原typed移交、历史投影和真实FD建立独立consumer，不能改R1source.root或重新打开sysctl读取0补签。新WAL family未接原initial8版本映射、typedFS Source绑定、生产profile/分派；未来Root可放宽该值的写者尚未全闭合。普通9实际服务、Compute真实SUID兼容、PG Birth/normal降限与后继resize/fence、117/118/119 Helm均未由本组件认证。policy1这里只验证原字节/解码兼容，未重跑其旧Guest矩阵。所有normal/allSource/VMshrink资格始终false，E=0。
