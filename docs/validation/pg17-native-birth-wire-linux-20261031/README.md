# PG17 原生出生屏障 wire：Root 侧独立组件

Go1.24.3/Linux6.8.0-147/race/count=1：3个U根、1个真实Unix组件根全部通过，零失败/跳过。实际COPY源码与命令见同目录manifest/summary。该实现未安装listener、没有生产ACK发送者，不能凭通过帧扣R、迁组或授予normal下降。

协议与PG侧共同冻结：Unix SOCK_SEQPACKET、网络字节序，request232B/ACK112B。0..7 magic=NDSGBR01，8..11 version1，12..15 kind=request1/ACK2，16..47原nonce，48..79原SourceSHA，80..111原ProfileSHA。请求112..119原AS；120..123 maxBuffers；124..127 pageBytes；128..135原totalR；136起4项{base,reserved,element}各3个uint64。ACK精确回显全部nonce/摘要，没有缺省/0成功。

Expected必须先来自原Root Pending和可信构建ABI，不能由packet创建。解析重新checked计算四数组pageceil和总R、核原AS/M/页、低47bit对齐/非重叠映射；短/长帧、请求伪装ACK、nonce/摘要/ABI/额度漂移均拒绝。收到的地址仍是待核声明，未读取实际maps或完整seccomp，不形成真实R持有证明。

真实S组件以降权nobody子进程连接独立临时socket，核内核SO_PEERCRED原PID/UID/GID及SO_TYPE；错误peer、stream、短长帧、读超时均拒绝。实际传入1个及64个SCM_RIGHTS（后者超过控制缓冲区）验证拒绝和接收进程FD数不增长；MSG_CMSG_CLOEXEC避免临时FD跨exec。测试自身临时目录/进程被清理，没有生产socket、系统用户、cgroup/节点/网络改动。

未来生产接线必须在同原资源锁下核Pending、start ticks、exe/namespace、实际AS/四maps/完整不可放宽过滤规则，迁同PID至固定PG域后读回并fsync Enrolled，最后才可发送ACK。错误/丢回复保留原Unknown。当前没有这些生产状态转换；没有PG、原出生封存、normal写、VM缩容或K8s验收。此结果只来自宿主Linux，目标6.12.26 Guest还须另验。
