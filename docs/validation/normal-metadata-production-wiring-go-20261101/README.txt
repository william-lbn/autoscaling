18c KernelPrep2 / memory2 / FS2生产接线：定向Go验证归档（2026-11-01）

结论
本批独占23生产源（18既有+5新prefix）与8定向测试，准确31源见catalog/source与go/source-lock.json。显式新profile仅允许精确18c五元组+plan2+KernelPrep2+PreSystem3+Topology4+memory2+FS2。旧369、selector0/1/2既有版本、Frame6/7/8形状、原KernelPrep5与发行锁不修改。Root独占FS/System/physical/发行锁与Git/index；本归档中的Root4仅为实际编译依赖快照，不构成本agent的生产文件提交清单。本组件不能宣称完整Source/normal降限/真实服务/端到端通过。

统一版本化合同
初始8WAL family显式字符串initial8-kernel-prep-userns-v2；仅前2槽改为kernel-bootstrap-v2-intent/complete.json，后6槽原名/顺序不变。不扫描后继WAL，不修改历史stage/floor要求。memory ConfirmedReference新增omitempty的可比较值字段original_initial_wal_family；旧空字段编码维持原字节，原数组不改pointer。Host原全5资产runtime、challenge、QMP/MaxVCPUs与Guest profile/build选择都严格绑定；不以任意recognized profile泛化。Preempt_dynamic full测试明确可选，不新增无必要性能限制。

原R1与独立R2
原R1同一Guard槽只接原durable KernelPrep2 opaque和同linuxKernel指针；JSON/current0/新Guard均不能产Source。HandoffRef为全值可比较不可变历史，绑定原R1 PID/start/private nonce、VM/boot/8namespace、原ELF、精确Kernel五元组、原state/lock对象、原procfs RO FD对象与2原WAL完整身份和SHA。Pin仅dup原RO FD，P2 declaration3/transfer2严格原5FD接收getter尚待后继联合图。
新R2工厂只接P2私有原proof第五FD能力，先typedFS/ref，后独立KernelR2，再MemoryR2。未实现getter或缺真实FD必Unknown；不重开sysctl路径，不篡改原R1身份，不要求R1长期存活。原OFD共享offset由ReadAt(offset0)有界canonical零读规避；普通临时文件共享OFD U通过，准确18c proc_sysctl read_iter/ki_pos源码支持的静态论据见design；原procfs实际Pread与真实5FD/后继R2仍须联合S验证。
运行期KernelR2观察只持资源锁与自身mu，验证当前R2/task/namespace、原对象/2WAL、控制值与固定kernel属性，零write/sync；不回FS、不取broker/exec锁，不加载state.json/floor，不重PGEmpty。工厂显式同步原2WAL与目录不属于Observe。close仅自身mu释放原dup/pins/task/ns，幂等且不回FS。MemoryR2通过FS已安装kernelConsumer，方向FS→Kernel再Memory；Kernel不反调FS，避免循环依赖/flock反锁。

准确U与编译
最终916818基于80bab8ae87eb6a3c8377e46b79d9d0b14c696f65+准确owned31+Root4，实际7458 COPY逐文件SHA核对；Go1.24.3，RUN2CPU/2GiB，缓存保留、网络none。14race根36子例全部PASS，0fail/skip；Linux CGO0 tagged native测试程序只编译0；Darwin arm64 resourceguard/vmguard/bootprotocol三包只编译均0；外层build0，77.898秒。raw result只记录test_exit，额外4个compile退出码已逐个从原rc读取，catalog列全；不据外层成功省略编译判据。binary SHA留catalog，未提交/未执行。无VM/PG/原九服务S，S=0/E=0/normal=false。
定向U只覆盖本批修改：五资产精确版本矩阵、原legacy字节、cross tuple/marker/private args、固定family8历史与sync13、typed Ref可比/JSON拒签、新Guard缺源、缺proof/错误FD/canonical零、shared OFD offset、Memory2族引用和Host full plan。没有扩测原Neon无修改业务逻辑。tagged native测试只是后继隔离VM原FD Pread机制入口，尚未运行。

失败历史与原receipt
history/compile-first保留df73f0编译仅检查（U0/S0）。history/focused-first-failed保留617af5真实14类定向U中的失败原日志：3个正例实际发现metadata分派在physical JSON解码前，误落旧policy decoder；另1测试私有临时资源目录未0700。业务接线顺序与测试fixture分别修复，未改原receipt绿化。该attempt原驱动文件名compile.json/compile.rc和result U=0是继承模板标签，实际为Go U断言失败，不是编译失败；需以原事件判读，不虚构PASS。history/focused-second保留ccb917修复后14race/36sub通过29.598秒；最终格式化31源在916818重跑，同样全过并补compile。

待验边界
本批没有P2新decl3/transfer2实际服务源码，故原FD5发送/接收、Origin2/ref SHA/原Gate保护链、R1退出后R2持续与PG populated/normal fence/resize生命周期均待联合真实隔离S。原Actor/Root可放宽userns的未来writer、普通9身份/metadata/devtmpfs/原始设备权限、Compute SUID与完整normal安全证明仍需各自Source，不能以userns=0/18c元数据门代替。MissingSources不清空，持续资格显式Unknown；117/118/119 Helm真实E2E未由本批签署。PG17/18上层沿同一能力/期望实际状态/恢复版本合同，不新增PG版本分支。
