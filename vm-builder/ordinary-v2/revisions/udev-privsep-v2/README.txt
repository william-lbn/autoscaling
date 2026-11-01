eudev ordinary v2 privsep revision — 默认未组合生产资格

本revision只叠原frozen349bb486 patch，不修改首包18/391；UID/GID60004、cap0、无NET_ADMIN/
MKNOD，不改公共libudev Root uid接收规则。新显式fixed模式内部用原fork前匿名seqpacket。
原creator pidfd+SO_PEERPIDFD/PIDFS事实+pair cookie/OFD+每包SCM actor+generation共同校验。
所有EVENT含首事件走同IPC；worker DONE只交本次processed属性，只有main请求固定Root socket。
Root完整ACK前不结清event/idle；EOF/错代次/actor/丢ACK均失败关门，不重试或降回旧Root分支。

准确6.12 netlink禁止cap0向用户port/group发送，6.12 pidfs可用唯一inode；Host6.8旧anoninode共享
inode明确EOPNOTSUPP，不以numeric PID补身份。原Scope SIGNAL保留，main/worker继承同domain。
Core没有/proc PID反查，仅持原pidfd/OFD；未确认worker退出不能原fallback广播/删DB结清。

固定Root endpoint /run/neon-dsga/ordinary-udev.sock Root0:60004/0660，父Root0:0/0755。
请求既定16B NDSGOA02/BE32 2/BE32 action3；body前40B NDSGUE01：schema1,kind16,action16,
index32,seq64,count32,bytes32,reserved0(均BE)，后按ASCII key排序NUL KEY=VALUE，总body<=8192。
main每事件独立连接，fork时无Root client FD；Root真端必须原role lease+peerPIDFD+每包credentials，
并原group1 kernel票据/固定目录/RO规则schema，不以caller seq/sysfs当前census/UID当来源。
kind1仍为原virtio log、kind2为原memoryN，必须由Root核原Geometry最大范围/holes。
kind3现在仅选择其它原静态对象、index固定0；main仍取未被worker COW修改的event->dev，
原action/devpath/subsystem/devnode/devnum/seq逐项匹配后才发送。selector不产生catalog票据。
CPU仅在原device属性序列化前按唯一cpuN/CPU/MODALIAS tuple去掉唯一末LF，原raw不改；
wire接收、SCM、DB和Root ACK仍走原strict canonical，不接受一般控制字符或错tuple。
新源码actual eudev编译和6组46个codec案例已过；真实CPU KVM机制正在独立验证，不能
将此初始实现或Root模型提升为完整普通9服务、真正Root Source、PG或normal资格。

8个Core unit/mechanism在369 Guest零skip通过；Host只codec2与拒旧backend ENV，不包装成8通过。
本组件driver只落实E/P/I=0(ambient随之0)/NNP1；bounding与最终五cap/SIGNAL来源未由本fixture认证，
必须由P2真实installer/停点readback组合，不能把这里的cap0字样理解为五集合已全审。
真实模型S：同原main→同worker PIDFS inode两代add/change→Root group2→未改stock libudev消费。
stock ELF来自已验签Alpine eudev-libs-3.2.14-r1，原subsystem BPF仍安装，hash a8f0c311。
消费者必须在真实udev/control就绪后构造；库在缺control时永久groupNONE，不能改此门求绿。
同UID非main实际Socket请求拒；原消息错代次/rights/错活pidfd/死对象与端点cookie负例均保。
Root模型重放/假index是模型predicate测试，不签真lease/WAL。Root真producer不在本revision。

build-eudev.sh/Dockerfile.rebuild为受控2job源码重建；verify-driver.py只准备可核Context或核
测试收据，不生成Source/ready/normal许可。工具需固定原Base digest，实际命令/nonce/limits/
Source/ELF/全部失败和终态见独立validation catalog。不得在Host跑Guest网络/时间/电源动作。

旧Root分支实际回归与丢ACK无重发负例同final模型binary另run；所有首次BUILD/ENV/机制/S失败
原样保留，不转签旧ELF。生产Gate/stdio/所有静态设备/真正原Root Source组合尚待，E0/PG0/normal0。

后继真实证据：18c隔离KVM的CPU0 add/change两代和丢ACK两稳定机制目标均已通过；
原kernel/sysfs raw逐byte解码、main/worker PIDFS和UID/GID/NNP/五组cap原值、stock已验签
libudev消费均归档。旧e473拒绝为负对照，单次write仅用于完整日志，不泛称console原子。
只证明Main唯一Wait，worker证明两代身份/存活，未独立确认worker唯一Wait；Root仍是模型，
没有实际role lease/WAL，不授Source/PG/normal/端到端资格。详见两个docs/validation阶段。
