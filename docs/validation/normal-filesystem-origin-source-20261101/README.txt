PG17 文件系统原始来源组件 Layout2（2026-11-01）

完成真实原namespace/FD/持久来源，绝非仅parser或marker。默认无公共入口：Root须后继在固定initial-boot的原KernelPrep/Origin/PreSys、Split→Provision→CheckGuest全部完成后、PID1启动其它角色前，持原resource flock调用captureLocked。新10源含FS7及仅normal vminit/KernelPrep/PreSys三窄接线；旧通用vminit不变。普通normal五拒绝没动。

Layout2严格17mount：原RO ext4根、6个原同ext4 RW bind持久孔、3个tmpfs、原RO ISO、proc/sysfs/devtmpfs/cgroup2/devpts以及原LSM reader必需的只读securityfs。每个实际O_PATH/statfs/STATX_MNT_ID与完整图匹配；DAX任何显式模式除never均拒，实际STATX_ATTR_DAX也拒。原logical/canonical祖先检查保留；不把整个/lib、根或动态扩展目录变RW。未来动态扩展孔需独立版本化原plan。

原Root当前task和固定PID1由原self/Guest不复用的PID1 pidfd+procFD钉住；不是扫描PID认领角色。原Source为P2 opaque System：固定RO ISO→Native policy/build.RuntimeClosure→System2 manifest→准确vminit/BusyBox/Guard ELF字节与实际inode。所有线程ns/root/cwd及FD（含O_PATH、已卸载/隐藏对象）逐FD检查；RW ext4对象只准原6孔mountID，shared writable ext4映射即使其mount已RO也必须在原孔。私有COW不误拒。负向用户树census只排未知任务，不产生角色资格。

原8WAL当前Rootdurability见证+同issuer后才首拍，保存原state/lock devino、原producer/Guest/Source/namespace/root/17图与初始FD事实。固定intent→complete、逐10文件+目录同步、同步前后真实inode/ctime/准确SHA重核才登记本Guard/本Root私有见证。complete可见但sync失败、新Guard/Root或同字节换inode仍Unknown。Observe零写且不要求PG空/两task；只复核原引用与实际namespace/root/graph/字节。显式Recover只接受完整原prefix，绝不补partial或重拍初始FD。新Root需真实当前role opaque task，旧Root PID不重开；新broker专有exec proof尚未接，不能把它冒充旧resource/serve。pinOriginalNamespaceLocked移交真实原ns/root二FD；rebind先在os.File引用锁下dup并核原引用/实际对象，拒输入早于Sync。锁序resource flock→FS→System→Task。

实际25race根117子例通过，新增9FS根，旧KernelPrep7+PreSys9为直接回归；无skip。cb6925准确919COPY签生产，S用a8acdf仅native脚本变化，生产同字节。S为PG17-S-FILESYSTEM-ORIGIN-001：真369/KVM、PID1真正switch_root、单ext4/原LSM读者/同SB两原适配/全8WAL、正确Split→Bootstrap→CheckGuest、生产capture三类隐藏持有拒且零FSWAL、privateCOW、10WALsync/observe、实际UID1000 PGDATA重建与warmup、运行期有真实其它task、同Root新Guard显式Recover原JSON不改。Guest0/QEMU0，两报告分块全SHA完整。新Guard不是新Root进程，不把它称已证明跨Root broker恢复。Host challenge和System未运行程序为DEV stand-in；未实际PG/normal/VM shrink/K8s，E0。

真实发现并修正：旧DEV chroot夹具PID1仍initrd不能当新来源；实际LSM需securityfs使16→17；KernelPrep和PreSys原same-SB排除与原vminit6孔矛盾，现仅固定原ISO/manifest/vminit+实际完整图/状态孔FD预检，旧separate盘路径保留；原初始入口Bootstrap先于Split会被历史门禁正确拒，Root已交Helm修调用顺序，本批不放宽split。TGID map_files不能放到task/TID，准确6.12.26源码核修。全部原失败、错误夹具、unit变量遮蔽metadata、丢行transport都保原nonce/收据；最终数据父uid仅在DEV rootdisk构建时恢复原1000计划，未改任何权限门禁。

尚未：实际StartBroker新opaque exec proof适配/完整未来mount-FD-SCM控制写者闭合/normal持续费用/真实安全降限/全PG或K8s。requireContinuousQualification始终Unknown，事实对象不是normal票据。Root须独审10源及同依赖联合后统一阶段提交；本脚本不Git。
