ISO固定物理名组件修复（不修改逻辑模板/JSON/SHA）

当前ISO依赖的标识总30字节包含;1，长逻辑键dsga-prebootstrap-system.json被真实
Runner roundtrip发现截成dsga-prebootstrap-syste.json。保留首次Runner失败原路径/SHA，
本批不修改第三方依赖；Root负责实际Runner六文件写盘与roundtrip组合。

RuntimeISOAssetName只接受resource+原5策略逻辑键；唯一长键映射固定短物理名
dsga-pre-system.json，其余映射自身。短名当逻辑输入、截断别名、大小写、路径、
空白/NUL/版本后缀等均拒，不做通用规范化/截断，不能碰撞或隐式增加资产。
公共常量RuntimeISOPreSystemAssetName由PreSys固定路径与native ISO夹具共同使用。
运行期PreSys reader和正式Bootstrap bridge均复用FixedPrebootstrapSystemPolicyPath。
两处原文件只各改一行，逻辑RuntimeDocument2/模板键/控制来源/许可协议无变更。
新reader必须配新短物理名ISO；没有长名或截断名legacy fallback。

精确Git b177+4源（排除Root/P2活动WIP）787项Go/mod实际COPY逐hash相等。
Go1.24.3 race15根77sub全部PASS/零skip：新映射2根21sub，直接PreSys9/44与桥4/12。
同图导出准确短名ISO fixture并静态编译，源码检查/测试/exporter/build均真实rc0。
准确369 KVM三原组件模式全部PASS，Guest/QEMU0，query-kvm enabled/present；
每模式新ext4状态盘，不复用旧账本；2CPU/256MiB Guest、外层2CPU/2GiB串行有界。
Guest实际挂载只读ISO先断言短文件存在/旧长文件不存在，三个Guest读同policy SHA
279957e8d3b191249fbc8cda67590b712445338c32351b23b29fbf62cd4b6d7f。
随后真实KernelPrepare→Origin→PreSys→正式Bootstrap/重观察链验证同原引用。
四个JSON payload与旧准确fixture逐字节相同，变化仅物理目录项，不改SHA来源语义。

这是映射U及真实内核/ISO/ext4服务组件复测，角色为sleep夹具，没有PG运行、Root
完整出生/FS/I/normal降限或K8s E2E；三个S沿原稳定case ID，覆盖增量0，不重复新增。
正式Runner完整六文件ISO roundtrip由Root另行准确图验证，不把本genisoimage夹具
当作已测过该第三方writer或可部署许可。缓存及历史结果未删；无index/commit/push。
