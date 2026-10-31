RuntimeBootRequirements：私有版本化启动意图（纯静态，不是资格）

DeriveRuntimeBootRequirements(raw,assets)内部重验RuntimeDocument/Source、Renderer与
整组逻辑资产准确字节。旧v1保原raw与旧模式，新v2只支持完整五策略canonical
normal-split-v1/准确369；未知/partial/漏改/短物理键代替逻辑键均拒绝，无静默降级。
对象字段私有，version1；getter只读，参数slice为副本，不新增HTTP/public协议或账本。

生成8参数：Managed、KernelPrep、Physical、PreSys(normal-split-v1)、NormalSplit marker，
原KernelPolicy.Controls.PIDMax early参数（本fixture512）、online_movable、
cgroup.memory=nokmem,nosocket,nobpf。生成值直接经过原四个reader marker validator。
最终-append校验须在Root省掉旧auto-movable分支之后调用，拒缺/重复/错值/裸参数、
hyphen别名覆盖、旧memory_hotplug.*、未知DSGA阶段、LSM与movablecore/movable_node
覆盖、引号/反斜线/控制字符/--和>2047字节。准确x86源码2048含NUL的来源SHA另列。
v1仍容许旧auto模式，但不能带新阶段marker；零版本对象不能通过最终检查。

BootMemoryUpperBoundBytes只取原PreSys上界。原validator是实际nonMovableUpper<=上界
且初期Movable0，不把上界当QMP实际base或选择值，不从其生成kernelcore/mem参数。
本对象没有ObservedBase/BusinessMin/Max/Permit等字段。Root还须独立核目标架构/准确
镜像、实际QMP base、原Host预算与首次增容；本组件不提供这些事实或任何normal授权。

准确Git508+仅4新源，797Go/mod实际COPY逐hash相等，networknone与原缓存。
新增5根47sub和直接Renderer5根66sub，整cohort10根113sub race全部PASS、零skip；
Go1.24.3，-p2/GOMAXPROCS2，实际10.948秒，源码检查/test/Darwin compile rc0。
Darwin/amd64仅Linux上的生产包交叉编译，没有Mac执行或Guest启动。

508基线实际缺口：Runner只加Managed且总带旧auto-movable串，sysinit尚未KernelPrep
→Origin→PreSys→Bootstrap→Split且缺securityfs。准确旧调用点与SHA在audit；Root的
后继consumer修改另批，不把此份旧基线审查说成当前后继工作树没修。
本批不改Runner/Config/现reader/Native/helper/P2/共享record或发行锁。S0/E0，持续
安全normal、Host原来源首次grow、完整Guest启动/恢复和K8s E2E均未因此完成。
