# Guest资源已生效丢回复后的失租接管

2026-10-29，Mac Darwin arm64。准确测试源码96ca5c2adcef82fd2abd9819f5401d872971e70f，
仅测试接线；[收据](DSGA-已发送Guest失租接管-Mac收据-20261029.json)固定三文件、用例与日志摘要。

完整首次成功及Prepare/Host/VM后，Root已设置原限额但返回503，GuestReady保持
Unconfirmed，PG仍Planned。旧owner因真实续约失败退出，新Elector以同Lease UID
接管，CAS落盘丢回复仍只增加一次epoch。原WAL/预算/Initial保留；新Resource SDK
恢复原Pending，旧Session迟到和新owner重放旧Root均拒绝。新owner只Observe原
Root一次，原命令epoch/PID/限额不变，当前PG floor更新；Record已落盘再丢回复。

后继普通推进不会派发旧PG；原Key请求取消后，按实际Root/未Grow PG/1GiB VM
生产当前预算、Cleanup/Finish与Host结清，不用旧512MiB Initial覆盖效果。同一
SDK原Pending已只读解除；Root setter和QMP各1次，PG Resize总0。新owner新Key
下一Grow Prepare通过，实际PG代次未增长、VM仍1GiB；不重复后继完整Grow矩阵。

新增Mac-race1顶层/1记录67.77秒、包70.407秒，0fail/skip；owner vet和相对
AS d8594166增量lint0，diff通过。正式Elector/Session/SDK/UID-RV账本/预算/Host
读者使用生产实现，Root内核/PG/资格/Kube/QMP为明确peer，不是Linux物理E2E。
PG17/Rust及生产Go未变，不重跑旧矩阵或重编Linux产物；准确23d455fb构建证据
仅在未变范围复用，没有新Linux执行。所有者AGENTS修改保留且不stage。

此阶段关闭Guest已生效丢回复后的真实接管窗口，不代表分区旧进程、异常来源
关闭、早期init/无Wait/跨boot迁移、normal安全降低/私有后台持续上界、Monitor/
自动策略/全部写者/CPU完成，仍需核心代码或Mac组合；PG17整体未完成，之后
PG18→PG16。Linux/K8s/NeonVM/HugeTLB/物理回收和性能仍须目标环境。Goal active，
上游冻结和独立控制面保留；不构建镜像、不部署、不合main，阶段归档后继续。
