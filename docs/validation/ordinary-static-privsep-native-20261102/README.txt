CPU0静态kind3：真实18c KVM中ordinary C特权分离机制验证

本批仅新增vm-builder/.../test_static_privsep_native.c，旧test_privsep_native.c与
 test_libudev_consumer.c逐SHA保持不变，不改六生产C/Go、Git/index或发行锁。生产C由Root
独立构建11e57a，udevd c93446d3...407880B、udevadm482c3049...保持；本fixture为Root MODEL，
不是生产角色lease/WAL/Source。-O2/-Wall/-Wextra/-Werror（仅unused function关闭）是本测试
编译标志，不将其混称生产eudev全构建-Werror或生产hardening证明；Root codec6U/46cases
是另一批独立证据，本目录只计两个实际机制S，不重复计算。

固定6.12.26/18c kernel18c9fd3a.../notesa496a3c4...；实际QMP query-kvm=true并逐byte核Guest
/sys/kernel/notes。每Guest2CPU/1GiB，Docker与预算2CPU/3GiB，NICnone，仅QMP capabilities+
query-kvm，没有Host网络/路由/时间/电源动作，没有QMP powerbutton。Guest最终正常poweroff
只结束自己的隔离VM，不操作宿主机。内核/Busybox/签名库/工具层全复用既有精确缓存。

最终稳定S两根：ORDINARY-S-STATIC-CPU0-ADD-CHANGE-001，以及
ORDINARY-S-STATIC-CPU0-LOST-ACK-001。旧e473实际拒只是ENV负对照，不增加S通过数。
真实旧control：同UID60004/NNP/cap0 main-ready且原kernel CPU0 add seq555已到，0emit/
0stockconsume、Guest实际1；不是不可达legacy_mode分支，也未重建旧C。

最终17a626普通场景3.115秒：main61→实际fork worker64/PIDFS364，原两代1/2、seq555 add及
556 change。Root从原main children列表及独立pidfd核同活task，逐请求SO_PEERCRED+
SO_PEERPIDFD和SCM_CREDENTIALS核原main；每包无SCM_RIGHTS/其它control，原ACK格式仍严格。
同UID非原main实际拒，已用seq重放和伪index1均拒。Main正常TERM后唯一waitpid0；worker
仅声称原fork/PIDFS存活两代，不冒Root唯一Wait。丢ACK场景只一代，不能借普通两代作两代证明。

独立kernel group1 stock monitor与原/sys/devices/system/cpu/cpu0/uevent pinned inode匹配；
SYSFS原MODALIAS双末LF、kernel原值单末LF均完整原字节保留。expected仅本fixture独立提取，
绝不调用生产neon_device_canonical算两边；接收wire保持原strict neon_canonical。kind3/index0、
CPU0无DEVNAME/MAJOR/MINOR、真实devnum0均由原kernel/libudev API核，不伪造设备节点。
Root MODEL只将合法同seq processed字段发实际组2；public consumer通过未改stock libudev
API（带真实cpu subsystem BPF filter）收到555/556，并核alias去末LF、CPU0 path和无devnode。

最终日志版把raw改为单次write，Host实际逐byte验证完整SYSFS671B和kernel alias（普通两份/
丢ACK一份）相符；不泛称任意kernel console长度均原子。Main/worker完整原/proc/status rawhex
保留并单独解码文本，Host独立解析四UID/GID均60004、NoNewPrivs1、CapInh/Prm/Eff/Bnd/Amb
五组均0000000000000000；worker实际PPid61。不是仅凭打印的CAP_ALL=0自报归档。

丢ACK最终3.242秒：原组1 seq555→Root已emit且原stock libudev实际consume555；故意不回ACK
后原main真实非零唯一Wait且无新请求/retry，保持UNKNOWN，不把未确认当失败回滚或授权成功。
Root MODEL没有真实lease/WAL，以上不授normal/VM回收许可或全stock Source。

stock消费库：55168B，原签名eudev-libs-3.2.14-r1.apk member lib/libudev.so.1.6.3；
ELF6a96205c57a74ec7d6150494d218e906f4656167b6b0ea2dab63b5f61555a795，
APK7f836c515bad8998158938a8c90ccf79b6738162d74d8fe9c9ce45652f817ea9。
复用原已验证签名收据、原header和Murmur源码，Guest库字节锁定，没有用新编196992B libudev
替代原public消费库，未修改公共库逻辑。

所有历史保持：9f2546 Docker CLI环境启动125、f77000资源精确guard失败（均未C编译），
bee0cd真实C新增诊断printf转义错误、5ec143严格编译及96abaa三VM全部真实通过；f825c6仅
日志升级编译，不冒S。最终df8c3a21... fixture在2375c7编译0，17a626只新两行为重跑、未重建
生产C或重复旧对照。96abaa原日志/原S不重标，最终两稳定ID替换分账，不累加运行次数。
编译BuildKit实际2CPU/2GiB在native2CPU/3GiB包络内，APK工具层CACHE命中、实际C RUN禁网，
显式127.0.0.1:7890沿原项目Docker环境，不打印凭据。所有nonce/spec/worker/source-lock/
initramfs成员SHA/命令/QMP/console/退出/资源cpu.stat memory.peak io.stat memory.events均保留。
不把ELF、initramfs、kernel或runtime库放进Git，不prune。原独审及最终raw补审均纳入。

准确边界：这是C机制S，不是完整Root身份/租约/WAL、PG进程、全stock Source、normal安全
降限、Linux整体E或三节点Helm K8s回归；这些仍由主任务门槛独立验证。本批Source=false、
normal=false、PG_executed=false、E=0。
