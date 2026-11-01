ordinary2 原sysfs catalog：非事件容器与CPU modalias文本形态

只改catalog与其直接测试两源。既有wire/DB/SCM parser及其排序、重复key、控制字符、
数量/长度约束完全保留。不是因kernel属性顺序而放宽：原StaticProperties本来sorted=false。

真实81a2dc Guest在/root PCI host bridge uevent存在而subsystem缺失时失败。核准确Linux
6.12.26 drivers/base/core.c/bus.c/pci/probe.c/kobject_uevent.c：无bus/class的device
仍有uevent文件，但core filter不发事件、show严格返回零字节且无subsystem link。
新catalog仅在原sysfs uevent严格零字节+subsystem ENOENT时记录独立非事件容器，
保原目录与uevent两FD和原对象身份，Fstatat证明目录下uevent仍同dev/inode/type，
持续再核SYSFS、同原对象、目录绑定、空内容与link仍不存在；其他错误/非空拒绝。
不伪造subsystem、不纳入stock触发/投影目标，WalkDir不SkipDir，所有子设备仍全遍历。
事件目标+非事件容器共用4096总上限，close回收原pin。准确kernfs/file.c/dir.c删除节点
会令原FD后读ENODEV的源审查也归档，但持续目录关联不只依赖这一间接性质。

下一cde907真实KVM已越过PCIroot，随后静态属性通用order/length错误。Host118实际
CPU0原uevent尾部双LF留此；准确cpu.c print_cpu_modalias先写LF，cpu_uevent将其直接
作为MODALIAS，core show再写LF。新helper仅在实际subsystem cpu、canonical cpuN路径、
最后MODALIAS=cpu:type:且双LF时去额外一个LF，再走原strict StaticProperties。错路径、
cpu00、child、错误子系统、未知空行、三LF、内部空行、重复key、长值和controls仍拒。
普通非CPU属性不做处理，DB/wire不能借此接受双LF。错误附原path/subsystem方便现场定位。

最终ae39f9精确组合58 race根/301子例、零skip/fail，7编译/导出阶段全0；其中catalog
目标5根/20子例，不能把整组合数当作这两源的新增覆盖。真实18c/KVM ae39f9-native已
从catalog走到programs/run-wait，之后原命令/子类Wait报EFAULT，Host1/Guest1原FAIL保留。
所以本批只证明catalog已越过这两个实际拒绝点；未得到stock complete、projection、Source、
全部9角色、PG、持续force/I、normal安全降限、Linux/K8s E。完整上下文/日志仍在/data
原nonce目录，source-lock与test.json完整原SHA引用保留；不把Host研究或源代码审查当Guest通过。
准确版本的官方源URL与SHA见research/upstream-sources.json，不把上游v6.12冒6.12.26。
