# 固定启动依赖阶段记录（U/B，真实组件尚未运行）

System3 原来把 `udev-init` 设为 WAIT。BusyBox 先执行 WAIT，之后才启动 ONCE/RESPAWN，因此冷启动等待的 udev 还没有启动。本次只将 System3 的该项改为 ONCE，并通过原受控对象确认依赖；旧 v1/v2 表不改。

冷事件首批发出前，锁外有界等待同一 Guard 的原 udev 活跃租约，并通过原 Task 的 pidfd/procfd 检查实际 control socket 已监听、kernel netlink 已订阅 group1。每次使用均检查同原 FD/OFD；已捕获对象被关闭或替换即拒绝。只有原受控槽位尚未发布或同应用尚未 ready 可以等待，其它 Unknown 不重试。现有 R2 权限不增加，无 numeric PID 重新打开或新 wire。

Compute 在原 queue90 内、execMu/pending/stdio 效果之前等待。进入原资源事务后再次核验冷启动最终确认、同一个原 init candidate 的唯一 Wait 正常退出0、原 run terminal 及精确 exited 持久见证。等待时不持 resource flock 或 execMu；锁序保持 broker.mu→resource→producer。timer 只唤醒，不产生 ready 许可。

本阶段实际结果是 18 个定向 race U、114 个子用例、0 skip 与八个构建阶段全部通过。其中新增4个U根/9子用例，原直接耦合回归14根/105子用例；不全部当新增。原始命令、JSON、源码校验和资源计数在 [Go](Go/)；源码及两份独审锁在本目录。eudev 原源码的 control/netlink 事实记录在 [机器记录](eudev-original-ready-source-facts.json)。

11af79 在实际4CPU/4GiB限制内执行67.094秒，峰值内存1,140,408,320字节。四个ELF仅在 `/data` 保留，路径与SHA见 [catalog](catalog.json)，不加入Git。原源码/构建缓存和历史镜像均保留。

**S 当前 NOT_RUN。** 本次导出的旧 fixture seed 仅是构建导出smoke，不准当新运行输入。P2正在准备新C、23项真实payload、PCP1024与Root外层package lock；后继复用本轮四ELF，使用新nonce、精确输入锁和单次真实PID1组件验证。即使该组件通过，官方Compute的 spec:null/Empty NoPG 也不等于真实Postgres激活。

本阶段未实际执行PG，`postgres_versions_actually_tested=[]`，业务归属PG17仅记录在target_backend_versions。没有签发完整Source、安全normal降限、Linux端到端或K8s通过；中央基表和发行锁未改。后续真实存储/PG激活、安全降限及117/118/119 Helm回归仍需完成。旧EINTR误杀和旧启动失败原据在其原归档，未改写为本次绿。
