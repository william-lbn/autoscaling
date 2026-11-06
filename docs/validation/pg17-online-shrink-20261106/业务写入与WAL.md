# PG17 业务写入跨内存收拢回归

## 三节点实际结果

代码基线 `fe297d848d0468b474c1de1420f2bddf94639a57`，自有 Helm 49。Compute `neon/cp-d8d85ed796068ab5-cqz6x` 在 worker-117。用建库时的正式应用身份 `app_owner` 经 TLS Proxy，在单独的测试 schema 创建有 WAL 的普通表，写 512 行，并持续提交递增且唯一的事务标记。每次提交后读取插入位置，再分别向三个原 Safekeeper 的同一 tenant/timeline 确认 flush_lsn 和 commit_lsn 达标，独立 Proxy 连接核对已提交内容。测试不会重放结果未知的事务。

- 原自动 Grow 90/4 通过。原后端 414 在扩容在途时成功提交 21 笔业务事务，重新连接的应用读到相同的行数和摘要。PG 达到 2048 个缓冲区，normal 硬限 1127743488 字节，QMP 为 8 GiB。
- 原自动 Shrink 90/5 完成物理缩容，PG 为 256 个缓冲区，normal 硬限 1077411840 字节，QMP 为 7 GiB。21:55:53.036 UTC，PG 对原后端的普通 SQL 返回 `dynamic shared buffer admission is closed for this boot`。业务回归为失败。
- 完成后独立后端 438 核实共有 41 笔提交，三台 Safekeeper 各自确认过这 41 笔。第 42 笔未入库。原 512 行仍可读且校验和一致。日志采集覆盖 27 个实际容器流。
- 118 原件位于 `/data/neon-k8s-pg17/reports/PG17-持久SQL与真实缩容准入拒绝-20261106T215553Z/result.PUBLIC.json`，SHA256 为 `e20ea9adeed3ff0482e24610df74585ff1c0f61c944ae7f489a4c000b27f4dce`。组内原始 WAL、PG、Guest 和 Host 回执由该文件索引。物理动作四项通过，PG17 业务缩容仍待修复及原场景复验。

## 单服务对照和修复约束

Neon 仓库原分支提交 `ecb12557200406d94f34df202351f4792ca3321e` 包含真 PG17 用例。持有实际尾页 pin 使原内核进入排水，普通应用在另一个连接提交事务。释放该页后原缩容成功；固定页持续占用时原内核会拒绝该次缩容、保持原容量。两个定向用例通过，用时 15.7 秒。

现场失败来自正常 Quiesce 使用全库准入关闭，随后远程步骤继续要求准入关闭。后续修复仅限具备持续硬预算的已绑定实例。写前同 boot、Owner、准入和容量代次，原请求的四数组物理释放，真实 P/A/N 与 System 约束、VM/QMP 及原 Scheduler 的结算按顺序核实。旧模式、未知和部分完成保留原拒绝及恢复规则。候选证据验证通过后仍要在 Linux 的真实 KVM 和三节点用业务事务验证，才能进入 PG18。

## 在线缩容候选的 Linux 验证（2026-11-06 UTC）

同一 PG boot 同时协商持续硬预算、Root normal 限额、数组权限和在线缩容能力后，准备阶段固定原开放准入版本。PG 真正排空尾页，Root 校验原操作、连续代次和四份物理释放，再在锁内核验私有域、数组域及父 normal 实际硬限。Scheduler 继续持有节点预算，最终由 QMP 和数据读回验证结果。旧协议仍验证真实关闭。接管时对只读准备保留原账本和 Unknown，避免制造一个从未派发的关门命令。

第一轮真实 KVM 在 W28 确认准入版本 2 仍开放且前台有工作。`quiesce` 已记录，`resize_pg` 尚未派发。原严格 JSON 读取器只识别 `omitempty` 为可缺省，新写入的只读命令正确省略 `close`，字段当时仅声明 `omitzero`。修订后用 W28 三份原始回执逐字通过原解码器，v1 仍拒绝缺少关闭命令。原失败保存在 `/data/neon-build/validation/PG17-在线缩容真实PG与KVM-20261106T2300Z/`。

28 个定向根用例、含子项 173 个通过，0 跳过，`go test -race` 退出 0。覆盖原 W28、跨 Owner 和硬限交接、四数组释放、丢回执和 Unknown。编译产出原生 Host/Guest、正式 Root、Agent 和资格读取器，来源各有 SHA。118 的原索引为 `/data/neon-k8s-pg17/reports/PG17-持久SQL与真实缩容准入拒绝-20261106T215553Z/W28原回执与生产审计单测.PUBLIC.json`，SHA256 为 `fc78e2d1e40fefcf5a6cd3efd88e79b6a9ec9dd18c30aa23832be31865287dec`。

截至本阶段提交，三节点原四项物理动作已通过。修复后的三节点应用持续提交、重连和三个 Safekeeper 的 WAL 仍待真实验证。隔离 KVM 的 Host API 使用模型。PG18 按相同合同排在 PG17 产品验收之后。
