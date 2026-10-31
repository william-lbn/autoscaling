# PreSys 显式369适配与实际Bootstrap衔接（2026-10-31）

修复完成后合法PG角色观察被早期census错误拒绝的问题。只改PreSys三源与新增独立native测试；Root桥接七源、Huge写者一源由主代理独占修改并以准确cohort合测。没有修改Config/schema、Root audit、已冻结物理来源/KernelPrep、正常五处降限拒绝或PG版本公共协议。

## 显式版本与阶段合同

同一PrepareSystemBeforeBootstrap、ObserveSystemBeforeBootstrap、ReplaySystemBeforeBootstrap API与结果形状继续使用。v1只接受原552 profile、原normal-v1 marker，JSON/WAL字段保持；v2只接受369严格IRQ构建和normal-split-v1新marker。内核差异放在私有适配器，旧来源/原nonce/typed与raw policy、Origin引用不得升级或混用；不按PG版本复制上层接口。

读者按适配器识别实际角色：旧552可观察PG父；369只允许private/arrays叶子角色，PG父直接任务、未知角色/更深域拒绝。KernelPrep使用的preSystemTaskCensus严格wrapper保留，仍只允许根/System。初次与pending继续要求recursive cgroup.events populated=0及所有层procs/threads为空，不能只看split父空。

完成后PG非空还须原formal日志身份/Source匹配、Bootstrap.Applied与SystemPlacementObserved均true；此前出现PG仍拒。无PG时保留原只读完成观察/同原恢复。准入、RootBirth和完整成本资格没有因此开放。仍要求原命名空间/持久state inode/readonly RootFS、原System cap、PID1归组及根用户清空等既有事实。

## 两处真实整合问题及修复来源

真实369 API链首次在PreSys已完成归组后被旧Bootstrap的System空组门槛挡住。主代理增加同锁、原固定ISO、canonical Origin/PreSys四文件和同源同cap验证器；正式intent先持久化私有stage_version=2及四引用，旧Bootstrap JSON省略新字段，旧空组安全门槛保留。

随后原2M0+1G0计划在gigantic nr_overcommit_hugepages重复写0触EINVAL。主代理保持计划与全部Huge校验，实际已等目标的overcommit/nr_hugepages只读，非等值仍正常写并由最终ReadBounds核所有hstate、actual/surplus/overcommit。不猜不支持等于0，不以计数写入声称物理释放。

Root七源与kernel_linux.go的完整hash、不可变Git31a+owned24快照及actualCOPY清单见[归档](validation/presys-v2-20261031/)；本批不抢改或代提交主代理的实现。

## 精确U及同源S

最终Linux Go1.24.3 race/count1限定20根90子例=110 PASS，0fail/skip：PreSys9根44子例、KernelPrep7根34子例、Root桥接4根12子例。实际COPY全Go/mod/sum逐hash相符。准确命令保存在go/source-proof.json，不复用缓存测试结果。

准确369内核/config/notes与actual QMP KVM启用，2CPU、256MiB base-only、无NIC/无TCG、原early PID512。三个独立Guest用同一二进制`f1db711b0eb5beab573c74f7fc549d1ff0d71cf982dc19145b0730e5d0db0e43`，Guest Go root与明确EXIT0，Host0并非唯一通过依据：

- 完成角色链：真实readonly RootFS视图、ISO9660四原文件及独立ext4 WAL→真实KernelPrep→CapturePhysicalOrigin→PreSys迁移PID1/用户→真实g.Bootstrap。实际state断言stage2/四SHA/Applied+SystemObserved；placeSystem回调未调用、state字节不变。private/arrays sleep角色公开Observe成功，未知角色拒、父直接task由层级拒绝；未造formal journal夹具。
- 初期private角色：PG父直接procs空但叶子有真实任务，recursive为空false，PreSys初次拒绝；无intent、System cap保持max。与完成角色同一原断言保留。
- 独立overcommit校正：同原2M0+1G0计划，Guest先实际设2M overcommit=1，正式Bootstrap真实写回0，最终ReadBounds及正式state通过；没有将全部Huge写者关闭。

只读debugfs从已停止Guest ext4提取实际formal、PreSys intent/complete、Origin/confirm原文件，独立重算四摘要相符，完整小文件随证据归档。没有以模拟formal日志替换生产API链。

## 适用范围与留存

角色为sleep夹具而非PG业务；原ISO预算为明确小型harness计划，非生产Host预留/原Runner发行来源。RootFS只读标记来自隔离initramfs真实bind-remount，只证明VFS视图，不声称完整不可变FS/允许写者资格。完整F/I/force/K、RootBirth、PG容量/正常降限、物理热移除和Kubernetes E2E仍未完成，normal_permit=false/AllSourcesClosed=false。

最终目录`/data/neon-build/validation/presys-v2-bridge-go-20261031T005614Z`与`presys-v2-native-20261031T005721Z`。初轮BusyBox remount夹具失败、真实System衔接失败、1G0写入失败均原样保留，四摘要已持久但Applied仍false的失败WAL也保留；不删失败求绿。先前真实主链PASS保留，最终仅新增独立校正分支后以同一cohort复测三Guest。所有异步任务独立systemd unit/log/result/exitcode，未清缓存、未改宿主控制、未操作Git索引/提交/推送。
