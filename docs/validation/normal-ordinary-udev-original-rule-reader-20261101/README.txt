普通udev v2 原RO规则FD读者接线与socket事实 / 2026-11-01

范围与结论
修复实际接线阻断：System.readClosure只把exec/loader放system.files，原catalog从该map取config .rules会恒拒。现在capture明确接收同Root preparation的私有typed OriginalRuleReader，逐一与同原System完整.rules/config库存对等，以原RO FD读取实际字节；无system.files fallback、无路径重开补签。

严格边界
先短持System.mu复制静态库存、释放后进入Inputs reader，避免Inputs→System反锁。全量path/SHA/kind/size匹配，nil/缺/多/重复拒绝；原FD必须regular/O_RDONLY/CLOEXEC、真实RO mount，ReadAt0与raw及digest一致。成功接管所有dup并deepcopy原raw；任何失败关闭全部已移交dup。该检查只证明准确RO输入，规则语义、真实stock执行及全量sealed projection仍需原prep opaque。
q.prepare在实际原prep完成/引用绑定后typeassert同对象并传capture；Root preTarget将用同p的早期Inputs reader，早期输入不是处理完成Source。15b Reference、System3、5FD均不变。
q.close在设置closed并释放q.mu、停止runtime monitor和关闭catalog后调用原prep私有close一次。外层允许仍持resource flock；prep.close只能释放原本实例FD/内存，不回g/FS/System/resource。原closeOnce和新U核先置closed、无q.mu、幂等。未实现或擅自替Root/P2新prep构造者签发。

准确最终图
build-final-181295：base b39c2e1 + aaa1cc不可变普通11 + 本lane准确7overlay，合16源码。9race根49子项全部PASS，0FAIL/0SKIP；Linux CLI、Darwin、准确native ELF编译0；实际单RUN 3CPU3GiB，50.403514083秒。完整COPY校验、source-lock、实际memory.max/cpu.max均在原run及副本。生产2源相对旧11只改规则读者必要hunk和close hook；monitor/kernel socket实现没有改变。
native-final-2f798d：同build的同一准确ELF，真18c内核、2vCPU/512MiB Guest、2CPU3GiB外层、KVM enabled/present，Guest两根3子项均实际PASS，QEMU0，2.798236922秒。
第一根新建group1/group3 netlink socket：SOCKFS=1397703499、dev8、statx mask6143、MountID9真实存在、cookie1/2、netns-cookie1；通用FS reader和两个生产monitor.open均无错误。因此‘memfd无MountID可推socket恒拒’候选被实际排除；没有修改通用FS reader或新造socket类型。
第二根只在独立Guest自建tmpfs/普通规则文件，转RO后核2个原regular O_RDONLY CLOEXEC FD完整交接、deepcopy及duplicate/wrong-actual-bytes/missing三负控实际拒绝并关闭移交FD。使用模型System/typed DEV reader，仅机制，绝不据此冒原生产System/RO Inputs资格。

保留历史
f691a1/ff804b两个编译通过图、fe0530首次真socket事实均原样保留。74f1d9为native驱动init shell regex未引号，实际unexpected (、测试零执行、init退出的FAIL；其原result/console不改。修正驱动双引号后用新nonce 2f798d，两根真执行。没有以改业务码或篡改raw绿化。

仍未完成
本lane没有实际原Root stock-preparation Source constructor、原70文件执行闭包、原5FD/R2/prepared窗口与所有writer Wait/monitor seal联调；没有完整catalog/projection/未来memory枚举2的生产正路径。旧sticky4c1e21/UUID f8f及其他历史S仍只对原ELF成立，不因本图编译重新转签。无九真实服务、无PG、无safe normal、无fullSource/E、无Host真实API、无Helm/K8s。新rules reader及两项机制不清MissingSources、不移动PG发行锁、不引入上层PG版本分支。Root是唯一Git/index writer；后续Root/P2将以同实例typed能力接线。
