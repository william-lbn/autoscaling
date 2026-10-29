# 关闭接续持久WAL、生产恢复与管理选择：Mac阶段验收

日期：2026-10-29。完成范围为PG17外围关闭接管的持久业务链；不是PG17整体
封版或生产认证。准确源码摘要、原失败及最终日志见同名机器收据。

## 问题及最终行为

旧owner的关闭可能已持久但根本未送达PG；提高PG issuer floor只隔离旧owner，
不会自动关闭准入。新owner不能修改旧请求epoch后重放，也不能用NotFound或
当前closed布尔抹掉旧未知。本批在既有关闭接续协议/SDK基础上接入外层生产者。

Journal v11保留原ServiceClosure全部请求、owner、写前序列、预算与Unconfirmed，
独立保存最新Continuation和直接前驱完整身份。更高owner须显式选择最近SHA256；
同owner不可反复更换revision，重复接管不可跳过前驱。最终新关闭确认后，原始
仍未知，最近选择Settled；不把旧请求改成成功或NoEffect，不释放预算/缩VM。

正式管理`continue_service_closure`只接受最新选择摘要，不接收原生命令、预算或
revision。PGWorker先认证当前Session/同VM和PG boot，先原生Fenced再读同容量
全局revision，保存完整三份选择，经原UID/RV写前CAS完整确认后才派发。同boot
未消费的revision可复用，旧关闭已应用则用下一项；floor或PG不可达继续安全等待。

写前CAS、setter或Record丢回复都保留WAL；Worker重建恢复AdmissionPending、
AdmissionClose、AdmissionContinuation。普通Advance仅查询最近选择；当前owner
可经`replay_service_closure`明确重放原选择，不自动接续/重开。旧回执、旧管理
selector、未来floor、已失租Session均不得完成或派发新选择。来源恢复本身不重开。

## 验证和失败证据

| 范围 | 实际结果 | 边界 |
|---|---|---|
| Journal新增及受影响关闭 | -race 9顶层/63含子记录，0fail/skip，包21.788秒 | 模型、strict codec及typed HTTP UID/RV CAS；原生回执为明确合同夹具 |
| 管理协议受影响 | -race 3顶层/21含子记录，0fail/skip，包2.232秒 | 严格selector与JSON，不证明网络管理认证 |
| 新生产关闭接管两分支 | 最终-race 1顶层/3含子记录，0fail/skip；测试115.75秒、包120.648秒 | 原生client-go Session/SDK/Worker/executeControl；PG/Root/Kube/平台为peer |
| 旧v10 root-drift及实例绑定 | 首轮分别5.83/0.41秒通过，合2顶层/3含子记录 | 同次首轮新组合失败，不能称整次命令通过；仅修新测试断言后未重复这两项 |
| 三包vet及最终owner vet | exit0 | 实际静态检查 |
| 增量lint | 最终0 issues | 首轮一处gofumpt失败已保留，格式修复后通过 |
| DSGA/Agent/Controller消费者 | 只编译exit0，所有测试被过滤或无测试文件 | 不是这些消费者的业务回归或Linux证明 |

新`unknown-windows`在原关闭未发送、准入仍开时，经真实失租接管；后继写前CAS
成功丢回复零派发，重建SDK完整恢复三份选择，只读未找到不自动重发，同owner
不得重选。显式原后继setter送达前失败，再真实失租、拒旧selector、接续最新选择，
setter生效丢回复；重建只读完整证明，Record写成功丢回复后重读Settled。最终
owner=4，原/最终revision均2，门禁关闭、原预算/未知保留、无Resize/历史/Active。

`original-applied`先让旧关闭生效但丢回复，再真实失租接管，先fence后用revision3
关闭；最终owner=3，原revision2，不覆盖原关闭。两分支接管CAS也实际丢过回复，
恢复只确认同次决定，不增加多余epoch。所有生产租约30/20/2秒和请求期限不变。

首轮两个新分支在已失租旧Session调用被拒绝后失败：测试只接受LeadershipLost，
executeControl先耦合Session ctx，实际更早返回context canceled。只修测试接受两种
拒绝，保留零派发/完整状态检查，不放宽生产取消边界；首轮失败日志完整保留。
之前审批502/超时拒绝均发生在进程创建前，不当测试执行。所有验证句柄均终态。

## 兼容和恢复约定

v10无接续仍完整兼容。v11要求完整Continuation；删除它或携带它降v10均拒绝，
旧读者不能忽略新门禁。继续4KiB完成空间、深拷贝回执和有界元数据；只保留最近
及直接前驱，不无界增长。原ServiceClosure存在时管理Begin/首次重放等仍被栅栏
拒绝。只读闭合不授予新服务许可，受控重开须下一独立业务协议。

本批不修改PG内核/Rust协议或两份PG锁，冻结b078不动；不重复前阶段真实PG/SDK
原生门禁。不新建独立控制面或多版本分支，不构建镜像/部署/合main/生产发布。

原Goal已恢复active，原heartbeat已通过应用工具更新：PG17全面代码、Mac可执行
验证及远程归档后暂停，不自动进入PG18/PG16。候选开发恢复/收据保留原时点历史，
本验收及最新执行记录覆盖其“未运行/未通过”状态，不能用历史收据比对最终源码。

## 仍须业务及目标环境验收

本批只闭合空闲、首次开放已确认、外层完整日志的新owner关闭接续。仍须实现
在途/首次开放未知异常关闭、坏/缺日志独立关闭、受控重开；normal安全降限、
私有后台持续上界与活性、分区/出生不完整/Runner无Wait/外层跨boot迁移、Monitor
缓存WSS/组配额/RW-RO/全部最终写入口/可信CPU，以及各自Mac验证和远程归档。

管理HTTP及Submit入站本批未新增实测；直接运行正式executeControl处理器，不能
称新协议完整网络E2E。真实Go→Compute/SQL/PG证据沿用协议SDK前阶段，不称本批
原生生产接管。Linux/K8s/etcd/NeonVM/KVM/HugeTLB/物理回收/迁移/长稳性能仍须
目标环境验收；缺失业务不归入Linux待测。PG17全面完成后才暂停本轮。
