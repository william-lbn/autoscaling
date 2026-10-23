# 启动硬约束与原位PG出生

本批补齐实际PG出生前的硬约束核心和受限启动程序。仍保持默认不自动启用，
没有安装SUID、构建镜像或执行K8s。**Guest sysinit/发行安装与认证预算生产者
仍需接线**，不能因为此模块通过便宣称全功能完成或只有Linux待测。

## 已实现行为

`resourceguard.Bootstrap`在PG尚不存在时，按完整VM/Compute/Guest/cgroup身份、
原epoch/承诺及完整Limits保存写前意图，再实际执行memory.max/禁swap/全部池
约束并准确读回。初始化必须组内无进程，不能伪造一个PG PID走运行期路径；
与运行期共用真实内核限额适配器。先前裸Initialize只建立floor，不授权PG启动。

Bootstrap部分写或丢回复保留未知，普通重试不再写；ObserveBootstrap只按完整
原意图读回。ReplayBootstrap仅当前floor可显式原样重放。新owner可观察旧
原意图但不能改epoch再写。进入运行期后禁止把旧bootstrap覆盖回当前约束。
新可选bootstrap日志字段兼容旧私有记录，旧记录没有该事实便不能授权PG出生。

`launch-postgres`是唯一允许真实postgres UID使用有效root的入口：固定root配置
`/etc/neon-dsga/resource.json`、固定PG路径`/usr/local/bin/postgres`。其余资源
操作及LoadConfig/Open同时要求真实root；不能用SUID借--config调用apply/fence。
调用者只能是postgres或root，不接收任意目标程序/shell。

启动时同一资源文件锁覆盖：验证原bootstrap已完成、无未知写→核对**当前Held**
及实际限额/Guest/整个池→确认组内空闲→把本进程加入固定组→清能力保留/ambient→
通过Go标准库的全线程Setgroups/Setresgid/Setresuid降权→NO_NEW_PRIVS→exec固定PG。
prctl到exec固定OS线程；不能只改变某一个Go线程的UID。锁FD使用CLOEXEC，PG
不长期占用资源锁。没有sudo/代理父进程，Child PID、信号与wait仍指向实际PG。
降权完成后才执行PG路径；参数/存储token环境不打印，也不在root身份运行PG。

这只是PG进程出生，不打开普通SQL准入，不签发VM许可。PG重启采用当前Held
预算，不回退到原较大启动额度；缺少启动事实、实际限额漂移、组内仍有进程、
未知资源命令或Guest/cgroup身份改变均在exec前拒绝。

Neon Compute本批另行接入Linux三个PG启动调用点（包括sync-safekeepers）；
受管Linux使用固定帮助程序，缺能力不回退旧cgexec。Mac保留直接实际PG验证。
受管模式跳过原512MiB临时预热池，避免在原创建预算外预热；标记存在只抑制预热，
不能作为启动/预算/准入授权。

## Mac实际检查

最终日志：工作区`build/dsga-bounded-birth-go-final/`与
`build/dsga-bounded-birth-rust/`，源码前后摘要分别为12个Go、3个Rust文件。

- Go新增3项及受影响原7项，最终race **10项通过，0失败/跳过**。测试使用真实
  Mac私有文件/写前日志/锁，内核与exec边界是明确夹具；覆盖部分初始化未知、
  原样显式重放、换owner只读、旧Held/当前约束启动、漂移/活跃组/裸floor拒绝。
- 新/受影响Go组件vet通过；静态`CGO_ENABLED=0` Linux amd64/arm64程序编译通过。
  初次非提权编译出现模块stat缓存无写权限提示但exit0；最终受控编译日志exit0。
- Rust新增argv/不越预算预热2项通过，0失败/ignored，26项未选；clippy通过，
  依赖原有警告保留。没有跑未变全套SDK/Router/PG内核。
- 受影响真实Mac `start_postgres`→初始化/原角色/HBA用例**1项通过**，0失败/
  ignored，27项未选，1.93秒；实际PG版本准确cbabd5de541，Neon1.11。正常停止
  夹具PG时后台worker的FATAL是管理员停止日志，不是本用例失败。

不把上述夹具、交叉编译或Mac实际PG启动称作Linux SUID/cgroup/内核端到端。

## 尚未编码接齐与目标环境验收

生产接线仍须完成：原VM的可信sysinit配置与cgroup建立、关闭vminit旧根组全员
迁移权限、避免cgconfigparser重新委派限额、固定帮助程序/配置的root安装权限、
原创建限额生产及授权通道、完整预算/独立启动准入/排空/Host和最终VM执行链。
SUID入口只允许PG出生，不给Compute postgres身份资源写能力；后续须使用独立
认证的root服务通道，不能把root写动作接到旧无认证25183端口。

跨PG/Guest boot和旧owner部分资源命令的受控关闭恢复、公开worker及生命周期
也仍继续编码，不能改记成Linux待测。当前旧Guest权限会使适配器明确拒绝，
未经接线不能直接启用Linux受管模式。

代码接齐后在真实Linux NeonVM/K8s验证：root安装/SUID/nosuid、全线程身份与
能力清除、原位PID/信号/退出状态、实际成员/迁移拒绝、进程遗留/重启、SIGKILL
持锁、内核memory.max/swap/HugeTLB及整个池/物理回收/性能。这里没有准备镜像、
执行部署、改main或发布生产。
