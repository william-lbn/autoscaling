# PG已提交丢回复后的真实失租接管

日期：2026-10-29，Mac Darwin arm64；准确测试源码`ebf064ac0d411662bc6f6086a85d4ff1f034adc1`，
沿既有自有分支。仅测试接线，无生产协议、PG17或Rust代码修改。源码、日志和
用例准确映射见[机器收据](DSGA-已发送PG失租接管-Mac收据-20261029.json)。

## 本次闭合的窗口

使用完整首次成功→真实Prepare/Host/Runner/QMP/GuestReady前驱，PG Resize
已经提交但回复丢失，外层原步骤保持Unconfirmed，SDK保留完整原请求。另一
Elector先竞争，旧owner真实续约失败并join退出；生产30/20/2秒期限不变，不手改
Owner/删除Lease。新Session从同Lease UID接管，接管CAS丢回复后只确认一次代次。
原Active、Initial、所有预算/高水位完整保留，旧worker迟到调用和新owner重发旧
Resize均拒绝。

新owner请求原操作取消，PG回原已Committed的TooLate完整回执，不能记NoEffect。
回执Record实际落盘再丢回复，SDK继续持有未知；普通后继循环用原终态Status
只读确认解除该项Pending，再按当前实际PG成本/Root/1GiB VM/完整原资格产生
实际预算及Cleanup/Finish、Host结清。原旧512MiB Initial不能覆盖已发生效果。
最后新owner新Key的下一Shrink Prepare成功，PG Resize和QMP总数各为1。

首次开放只运行必要成功前驱，未重复原初始故障矩阵或后继完整Grow；旧VM窗口
默认行为不变。Host原Lease用既有正式client-go续约夹具保持，不重建同名租约。

## Mac实际验收与边界

新增1顶层/1记录-race73.58秒，包76.167秒，通过且0fail/skip；owner包vet退出0，
相对69f30248增量lint0 issues，diff通过。初次测试用错Prepared字段，已改从
原Runner已发布出生读取Pod UID；该失败不计通过。lint仅删无效赋值和恒nil返回，
不改变业务路径，不为等价语句重跑73秒组合。

正式Elector/Session、SDK、UID/RV WAL及预算/Host读者是真实生产实现；PG、Root
内核、平台资格、Kube服务和QMP仍是明确peer。本次没有新Linux构建或执行，不是
真实PG内存/Neon存储或Linux/K8s物理E2E。已有23d455fb Agent/Root双架构编译
证据只对未变生产源码复用。

该窗口通过不代表Guest已派发、分区旧进程、早期init/无Wait/跨boot迁移、缺源/
异常关闭、安全降normal/持续私有后台、Monitor/自动策略及全部写者/CPU完成。
这些仍是核心代码/组合工作；PG17未完整，后续PG18→PG16。Linux/K8s/NeonVM/
HugeTLB/物理回收和性能仍需目标环境。Goal active，上游冻结、独立控制面保留；
不构建镜像、不部署、不合main，阶段归档后继续。
