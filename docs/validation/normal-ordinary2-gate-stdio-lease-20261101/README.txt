本批是普通2 Gate/原始角色租约/typed stdio组件，非完整normal、PG或K8s验收。

准确Git基线9ca537f345908b46b1be4d421ad6516e831aefd8，加P2 owned25、P1 pending backend2不可变依赖；8232输入actualCOPY逐项核过。Go真实RUN3CPU/3GiB，21race根91子例、LinuxCLI/Darwin、Guest/Host编译及原Hostseed/fixture导出七阶段均0。

最终真实18c Guest为2f5db5；169.03秒，Guest/R1/Host/QEMU唯一Wait均0，KVM enabled/present，实际notes a496a3c44fc0b374f100ba0331a2e21f02d6e79ac999769a8d3ad4d71b76024e。原Guest2vCPU，512MiB基座→1GiB目标/4GiB最大，原Host6/7/单8→KernelPrep2/Memory2→FS2→真实5FD→同g R2。Docker实际2CPU/3GiB、swap0/networknone/readonlyRootfs，终态OOMKilled=false，见原JSON。

只有一个S根。5个真实run分别是BusyBox touch两代、BusyBox uname、DEV长network及同scope worker、DEV Compute NNP0占位。ordinary真实UID/GID分别60001/60002/60003，所有四项、补充组为空、五cap集合/NNP1/唯一完整BPF在真实第二exec-stop读回。Root factory实际新建null和两pipe，停点pidfd_getfd+KCMP核0..2同OFD，不传FD9；Rootconsole只是Root有界drain sink。15个stop/active或pending-exit/真实Wait检查点，5笔两pipe EOF。wait不是物理释放，EOF不替代全后代/长期持页证明。

同原resource锁内：三WAL真实fsync+FileWitness→原stopped controls/stdio/creator独立pin→候选（不可授权）→真实Detach→active→解锁。Compute独立a.compute在同停点排他reservation；旧PGgetter在candidate拒，Detach后在资源锁尚未释放点返回同原clone，实际执行已断言。短命真实touch/uname可进入pending-exit但不RootAction；唯一Child.Wait+独立CreatorPin POLLIN后才KnownExited，stdio EOF之后才可同role新代。mode2 creator真正retire，不重用M冒退出。

旧1/2默认参数、Compute NNP0、Root/broker/resource/PID1无普通安装器；只原RO能力2选择新plan/GO2/600xx控制。SIG scope最后leader rawexec，worker继承同domain；Source没有LL规则读回，证明来自原RO Gate checked顺序+完整BPF/caps readback，不声称可读LL。Linuxamd64/Go1.24.3以外Unknown。

历史首失败原样保留：预算spec换行/actualCOPY忽略.github为执行器阶段；native未用import、测试大ELF复制触库存、账号在旧runner晚生成均已精确修正新夹具。6b3e50新private resolv0644与严格私有模式不符，ed1e49旧105s不足；f31cfb父dir0700导致touch失败且DEV finish ctx丢观察点；312cf2三ordinary完整通过，独立vmstart仍查旧marker所以无Compute。新fixture不改生产库存界/门，旧run不转签。55bebe是前一snapshot完整subset正控；最终d8d217只在native追加Compute getter两点断言，新ELF2f5db5重跑，不称相同快照。

Root namespace/proof/factory只消费真实typedFS和同g原ExecProof，未从metadata/selfPID/census补来源。固定原Root Actor/4或5FD来源的生命周期由原交接链控制。DEV私有ctx观察者仅tag测试设置，可报错/观察本原对象，生产nil；finish新ctx保留原publication的观察函数，不选择来源/权限。

限制：udev、udev-init、ACPI、logs、clock、daemon没有本批真实业务兼容正控，客户端仅TEST_ONLY待机/跳过，不能算9服务通过。network/Compute是明确DEV占位；Host API预留模型、无PG、无StartCAS/JWT全链、无normal降限或K8s。真正普通2仍待固定stock/真实包/Root动作接线与持续Source闭合。初始PID1/broker stdio未由本factory补签；PG stderr是Rust另建pipe，仍需原Compute与Root pipe工厂联合来源，不能由Compute stderr推导。原五处normal安全拒绝未改。
