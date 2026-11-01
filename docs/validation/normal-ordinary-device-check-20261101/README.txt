ordinary2 固定device-check/Root动作3与5组件 / 2026-11-01

6源码：新Linux客户端/非Linux拒绝/新聚焦U，既有CLI/协议/server三个窄hunk。
RunNormalSystemOrdinaryV2DeviceCheck(ctx)只固定len2命令，旧getuid拒绝之前分派后在函数内
硬核当前M的四UID/GID60005、无补组、NNP1及5cap集合0；Root/SUID/错身份不连接，其余旧CLI
路径不动。UID不是Root授权。服务端还逐packet核原RoleMainLease/creator/phase/fullcontrol与
完整Kernel credential+SO_PEERPIDFD对象，普通60005不能自领角色。

固定/run/neon-dsga/ordinary-udev-init.sock Root0:GID60005/0660，父Root0755。单次16B
NDSGOA02/proto2/action5空body，65s总deadline/ctx取消关闭连接；无设备/path/PID/hash/预算，
无自动重连重发。真实Root PEERCRED+原SO_PEERPIDFD77/PIDFS/stat/poll，不以signal0测Root活。
ACK必须16B NDSGOAOK/2/5+逐packet同Root cred，截断/rights/错包拒；rights接收即CLOEXEC并关。
普通状态/proc/thread-self/status是本次直接依赖，全Gate仍须后继核准固定自身读者，不能泛开/proc。

既有动作1..4字节及路径/语义保留。server只新增udev0:60004/0660/action3和init0:60005/0660
两个固定endpoint。从同g slots借原q，strict同authority/FS/proof/originalFS，缺源/tombstone/异g
Unknown，handler绝不prepare或从UID/JSON补源。action3同resource flock核原udev main再emit原票据。
action5同flock begin→释放锁等原monitor/processed至多60s→同flock finish重核原发起main才ACK；
不得持锁等action3形成死锁。任何wait/finish失败使原q失败Unknown，丢ACK不清旧cold并重造。

两个prepare factory目前没有既有生产caller；P2唯一runtime薄hook须原prepared同g先stock/source
后actions、保存close和两done、错误取消。清理先actions.close取消并Wait所有handler，再close
qUdev/stock pins，不能提前关catalog FD。TLS检查仍严格要求原/run/neon/management已被合法供给，
本批不在请求中补目录。旧privsep目前kind1log/kind2memory，其余EOPNOTSUPP，所以新action5原
catalog全触发正链还需kind3/设备exact规则适配；不把本客户端机制S冒全9服务/原coldSource完成。

准确Go acbdca：Git17cb+仅immutable KL11(74ec)+own6，1389actualCOPY/source全等；12race根28
命名子项全PASS零skip，其中6新根20sub+旧6根8sub直接回归（不重复增覆盖）。Linux正式CLI与
Darwinarm64 package编译0，Go3CPU3GiB/Go2/768/单RUN实际硬限，原唯一Wait已终态。

实际18c/KVM5b11f6：同acbdca CLI ELF，Guest512MiB/2vCPU/max4GiB；Host2CPU/3GiB无网只读。
8case：Root完整ACK1正控，错/半/rights/丢ACK4次各单请求退出Unknown且不重连；错roleUID、
真实NNP0 SUID、错socketmode均零连接拒；Guest0/QEMU唯一Wait0，kernel notes精确a496a3...。
Root responder明确MODEL_ONLY：没有原creator角色租约/真实cold触发，不授Source/normal/E。

首efb367与诊断2846ad两个FAIL全部保留：前6case已到达，但C负例先NNP1再exec，preexec确
ruid60005/euid0/suid0，实际exec后的/proc四UID已全60005/cap0/NNP1，原‘应拒Root’期待不成立。
新nonce仅改C负例NNP0使真实SUID身份不合客户端要求，Go生产/测试/CLI ELF全部不变。
本报告只记录实测前后凭据，不拿新Root JSON伪造生产来源，也不重标旧FAIL为通过。

本批完成的是实际客户端与固定server分派组件，未来正常内存控制资格仍Unknown，E0。
未删normal五拒绝，未改P2runtime/ctor/Gate、其它冻源、共享上层ABI或Git。
