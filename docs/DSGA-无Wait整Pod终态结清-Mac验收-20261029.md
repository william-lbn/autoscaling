# Runner 无 Wait 的整 Pod 终态结清：Mac 阶段验收

日期：2026-10-29，Asia/Shanghai。沿用冻结基线与当前自有功能分支。本模块补齐 Runner 已 Start 后崩溃、原 Wait 永远无法生成时的关闭路径；整体 PG17 尚未完成。

## 行为与不变量

新增独立整 Pod 终态来源 `pod-terminal-v1`。版本字段只识别序列化事实，不表示中间产品交付；此来源与原 QEMU Start/Wait 来源不同，不能解码成 Wait，不制造 PID、退出码、launch nonce 或 Runner WAL 摘要。

只有首次选择关闭来源且原 Wait 真实 404 时，可信 Scheduler 才可封存完整原 Pod 终态。Pod UID、VM owner、Node 与原 Birth 必须一致，RestartPolicy 为 Never、Runner restartCount 为 0，全部 app/init/sidecar 均有完整 terminated/容器 ID/起止时间；仍运行、残留 sidecar、Pod 404、网络/权限错误及坏 Wait 均不能取得许可。目标 Node UID 另由原节点 owner/fence 和 CAS 重新核验。

封存前再 GET 原 Pod；确定名称的 immutable、VM owner 和 finalizer 保护来源 CREATE 后必须真实 GET 确认。回复未知只按原名称恢复。关闭来源类型、UID 和原文摘要写入原 Pod 的关闭意图后固定；迟到 Wait 不替换，原来源丢失或换 UID 必须保留关闭未知。正式顺序仍为 Node fence → 完整终态/原 Birth/关闭来源 → Pod UID/RV 意图 CAS → 来源重读 → Node CAS → 再确认 → VM 关闭锚点 → 移除 Pod 保护。

活动 ReadPublished 和 Controller 在读取旧 State/QMP 前检查封存来源；原 Pod GC 后也返回停止，不能重新授权旧发布。GC 仅在正式全 Node 结清与 VM 关闭锚点核验后回收新来源。所有原未知 resize、原预算及 Runner WAL 保留，不把整个 Pod 停止写成原内存动作 Completed。

## 实际验证

四个受影响 Go 包全量 `-race` 通过：vmguard 52 顶层/224 含父子记录、hostscheduler 18/50、hostguard 24/81、controllers 17/68，共 111 顶层/423 记录，0 fail/skip，含 2 个隔离子进程 helper。另加来源 CREATE 落库后丢回复及 GET 不可用的恢复测试 1 顶层/1 记录通过。

真实 Mac 隔离 Launcher 调用生产 RunQEMU，测试二进制替身作为 QEMU 子进程核验真实父 PID 后 SIGKILL Launcher：原 fsync process-launch.json 存在，process-exit.json 和 API Wait 均不存在，原 WAL 字节不变。仍 Running 的 Pod 夹具不能生成来源，只有完整终态后才成功。Kube/CRI/容器终态是明确协议夹具；此测试不运行真实 QEMU/KVM 或认证真实容器停止。

Scheduler 覆盖缺 Wait 成功、Node CAS 丢回复、迟到 Wait、来源换 UID/丢失与坏 Wait；原权限错误拒绝矩阵及整 Pod 结清回归保留。Controller 覆盖封存来源先于旧发布、坏/失保护来源拒绝、Pod GC 后屏障保留及完成后的正式 GC。

正式自动 Grow→Shrink 全控制组合 169.30 秒通过，正常 Finish 与 Host 结清，QMP peer 实际值读回 1 GiB，扩缩各一次；未放宽 5 秒最大采样间隔或领域安全条件。三包 vet、最终增量 lint 0 issues、33 个共享消费者仅编译通过。所有采用测试合计去重 113 顶层/429 条含父子记录，0 fail/skip（含 2 个 helper），不能称为 429 次独立 E2E。准确日志摘要以同名机器收据为准。首次消费者编译误写不存在的 neonvm/runner 路径，保留失败日志；纠正为实际共享包后重新编译，不将首次失败记为成功。

## 余项与环境边界

本批 PG/Rust 源码未改，不重复已验证的准确 PG17 安装。受控重新开放与有界重复生命周期、normal 安全降限、私有后台持续预算及活性、分区/跨 boot、Monitor/WSS/总配额/RW-RO/可信 CPU 和全部写者仍须继续实施核对，不能归为仅待 Linux 测试。

Linux/K8s/KVM/NeonVM/CRI/cgroup/HugeTLB、真实物理返还和性能另验；本阶段只有 Mac 执行，不构建镜像、不部署、不合 main、不改独立控制面，不追新上游。所有者 autoscaling AGENTS.md 改动保留不暂存。PG17 全面实现、Mac 验证及远程归档后才暂停；Goal 当前保持 active，不进入 PG18/PG16。

勘误：前一 Neon 12eb66fa9 提交正文的“41 扩展阶段”应为 39；原 index 日志及机器收据为 39，本次不改历史或强推。
