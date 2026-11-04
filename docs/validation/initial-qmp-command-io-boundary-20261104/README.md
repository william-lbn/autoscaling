# 初启QMP完整命令I/O期限修复

PG17三节点集成中，新7410的Runner记录private QMP write unconfirmed: i/o timeout并结束QEMU。公开StateClaim没有私有WAL分派字段，不能从null或超时证明命令未发送；SIGKILL也可能来自错误清理，OOM保持Unknown。本批不把机制复现当作该历史现场唯一原因。

源码缺口：原私有QMP连接一次设置绝对2秒期限，初启同一连接随后执行peer整ELF校验、Kubernetes授权读取和WAL落盘，下一条命令可在原总ctx仍有效时已耗尽I/O期限。未修RED沿原真实Unix socket/child/pidfd/WAL模型，仅让一次成功的StateClaim读取延迟2.15秒，即复现写超时：模型qom-set计数0、may-have-sent文件保留、父ctx仍活。原未确认写入不重放根继续通过；原normal三子例因Mac临时路径/private/tmp不在Linux存在而失败，修正为canonical /tmp短路径后才真正执行通过。

修复只在实际Start句柄捕获后的原初启runCtx添加绑定同一私有QMP指针的局部标记。每个完整命令在唯一Write前，设置一次min(当前时间+原2秒,原总ctx截止)；Write、全部reply和异步event共用该窗口，不逐frame/partial续期，不重连、不重发。SetDeadline前后都核ctx，失败保持拒绝/Unknown。peer、UUID、几何、授权复核、WAL落盘和sole qom-set顺序保留；标记不进入ready或持久对象。共享geometry/hardware/origin只在匹配初启标记时走新私有分支；普通qmp.go、默认未标记和post-ready Read/Set保持原行为，公共DTO/Guest/PG内核未改。

Linux修后唯一GREEN 0ecefd：准确6个测试根/33个pass事件、0fail/0skip；包括完整延迟初启唯一qom-set1、父ctx/取消/Setter错误、部分写/缺回复不重放、event不延长期限、外来标记和普通默认路径、原严格身份/帧/大整数边界。首target测试与source通过后由Host Python核准确根数，再用同源cached target构建正式Runner；两个target testsJSON和manifest逐SHA相同，没有重复计数。固定Go SDK无Python时不安装新环境。

实际3Gi/3CPU/swap0，峰值1105133568字节、OOM0；117.630秒，原Wait/terminal0/reconcile0。8个准确文件由原aaf快照加独占改动冻结，Runner ELF SHA1e02773b552832c899b35c3137a668fc9a5b8a6e44f930bd116af50fabe7dd8c，44220600字节。源码、六根名单、资源和产物见receipt.json。首次GREEN c757重复追加两个旧函数导致编译失败、SDK校验命令缺件均原票保留，无失败产物发布。

大证据留118 /data/neon-build/validation/initial-QMP-command-IO-RED-20261104T050040Z-c21727/、initial-QMP-command-IO-GREEN-20261104T051447Z-c7576f/及initial-QMP-command-IO-GREEN-repaired-20261104T052118Z-0ecefd/。本批是明确模型边界组件验证，不是KVM/PG/normal或三节点真实端到端认证。待单Runner镜像/三节点预载和新物理身份冷启验证，失败7410不重置/不复用旧证明。PG17/PG18共用此初启路径，接口不分叉；PG17安全normal硬降、Grow/Regrow/自动策略仍须实际验收。
