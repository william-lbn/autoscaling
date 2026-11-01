stock初次处理DB/processed一致性捕获 / 2026-11-01

新2文件API captureNormalOrdinaryStockUdevDatabase(ctx,*RuntimeDirs,*Catalog,MonitorReceipt)
→[]DeviceProjection。只数据捕获，不产Source/Root命令；Root factory另持原resource flock核同g/R2、
3actor、全部原writer真Wait与原monitor连续group1/2 finish。Receipt/Cookie/UID0或map本身无此资格。
函数自己锁Runtime.mu/当前M，调用者不可预持Runtime.mu；必须phase1且原17graph/pin保护仍在。
先原catalog.verify，原ueventFD重读/原object/原kernel字段双向比对（DEVNAME规范化），从真实
devnum/ifindex按官方get_id_filename取data id；存在devnum还核原nodeFD actual Rdev/type，
非请求提供路径/名字。重匹配完整原kernel/processed序列与catalog目标，每target必须matched add。
固定children-max1初次执行下按最后sequence add选目标消息，DB必须与它一致，不任选旧匹配掩冲突。

从原root dirFD打开字面data及派生id，NOFOLLOW/NONBLOCK，Root regular/单link/64KiB上限、
同原runtime mount/dev/FS，前后stat/size/mtime/ctime稳定；不跟symlink/特殊文件/目录alias，
缺与空区别保留，读取发生handoff前。结束再核catalog/runtime，返回独立深copy full processed。

官方DB不等于整个monitor。公开持久E只能在原matched Root processed同名同值，未知E无对应则
Unknown不silentdrop；严格大写键/值/126项/字节界、禁原kernel/身份覆盖与LD/KMOD/UDEV控制、
路径/执行环境保留键。原普通UDEV_DISABLE_PERSISTENT_STORAGE_RULES_FLAG有限既有字段不泛前缀。
私有E按官方不进monitor；不改原pure parser有限E表。新reader先抽E逐核，其余I/S/G/L/W复用原
pure投影：I有则USEC必须同，DEVLINKS/TAGS排序集合及存在性精确；L/W只内部不强求消息同名项。
full Root processed可含非持久附加字段，非空DB不整体相等；不从未知caller map推实际执行来源。
I缺但原Root消息真实正canonical USEC可保，只标固定dynamic1且不超过原MONOTONIC；没有值绝不造常数。

归档前自审明确update_db失败被原C忽略：缺/空DB若Root消息仍有ID_MODEL等info-like字段，不能
认证has_info=false。故仅这两场景附加字段限准确C internal setter literal+原kernel消息项，
未知额外信息Unknown；empty-node DB仍可有真实USEC/DEVUID等非持久值。没有devnum/ifindex的无info
节点可无文件；有devnum/ifindex必须DB存在。不能把未证after-RUN额外持久字段解释成任意非持久。

旧949生产6b5+新test515的精确red fec819两直接case实际FAIL，source/build/CLI/Darwin0，完整保留；
新green21bf9c源c36/test515，Gitb39+KL11/monitor2(74ec锁18998)+IUSR/runtime4+own2，1377COPY
逐hash同，5race根43命名子项全PASS零skip，LinuxCLI/Darwinarm64编译0。初始949 5/41当时绿也
按原义保留，不冒新逻辑已测试。每job独立3CPU3GiB/GOMAX2/GOMEM768/真实RUN assert，原Wait与
原unit终态后预算释放。测试仅纯语义/私有临时树，不向Host/run注入或执行Root命令。

真实目录/captured Root链正向仍要后继完整stock Guest，不制造假positive；catalog配置FD原接线和
netlink匿名socket mountID适配问题已分别向Root/Kernel报，其修复不由此parser伪造原对象。
当前S0/E0/Sourcefalse/normalfalse，未删除任何既有降限拒绝，不改其它6冻源或共享协议/Git。
