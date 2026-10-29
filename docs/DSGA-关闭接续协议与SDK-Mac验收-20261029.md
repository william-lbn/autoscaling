# 关闭接续协议、SDK与Compute路由：Mac阶段验收

日期：2026-10-29，Asia/Shanghai。本阶段为PG17核心的低层接续能力，**尚非完整
Agent接管恢复或整体内存扩缩容验收**。源码和证据摘要见同目录
`DSGA-关闭接续协议与SDK-Mac收据-20261029.json`。继续既有分支，上游冻结不变。

## 问题与实际实现

旧owner关闭尚未送达时，新owner不能改写其epoch重放；只提升PG issuer floor
也不会关闭准入。本批在两仓增加同构的完整接续协议、严格解码/摘要校验、Go与
Rust SDK状态保存和恢复、Compute认证路由及OpenAPI/严格宿主门禁接入。

每份选择包含不变的Original、最近PreviousClose、新Close。三者必须同VM/
Compute/PG boot、容量generation和预算ID；前驱关闭，后继epoch严格提高，
global admission revision不回退。未送达前驱允许复用尚未消费的revision，
已应用前驱通常须next revision；必须由PG实际检查连续性，SDK不猜测补成功。
高位uint64全部规范十进制字符串；比较不执行加法，不因换owner重置revision。

Rust在同一Mutex内串行保留完整选择，网络期间不持锁；先取得原生Fenced回执
才向PG发当前owner关闭。floor推进、Pending、NotFound、ResultExpired或当前
closed布尔均不构成关闭成功。只有完整原生关闭回执才确认。同预算已开始的
旧工作、Host和VM约束不释放，也不生成VM缩容许可。

超时、future取消和未确认回复保留选择。显式接续须匹配最初意图与直接前驱；
最近选择可以原样重放。链按严格递增epoch、固定容量和预算传递封住更早owner，
保存最初与直接前驱而不无界增长。完整恢复接口不能覆盖另一未知；旧普通
setter/close-original/status不能越过新的接续状态。

原生fence明确返回Stale且本次setter尚未发送时，只恢复本次调用前的SDK状态。
若此前已有未知则逐字保留；空SDK不会被迟到owner永久占住。此分支只说明本次
setter未发送，不能宣称最初命令从未应用，外层WAL与预算仍须保留。

新增POST端点为`/dsg/v1/admission/continue-close`及其`/status`。旧两份意图
只读授权，新setter必须恰为当前签名epoch；独立认证/无缓存/16KiB限制沿用。
新端点不在普通轮询中自动产生选择，旧部署不支持时明确失败，不回退legacy。

## 实际验证与局限

- Go API：5顶层、43含子记录，`-race`，0fail/skip；26共享向量为其中一项。
  Go SDK：3顶层/3记录，`-race`，0fail/skip，覆盖真实本地TLS、两次丢回复、
  再次接管、完整恢复、旧选择/坏回执拒绝和本地派发互斥。TLS服务为协议peer。
- Rust合同：原准入3项及新增接续3项通过，0fail/ignored；向量含4有效/22无效。
  Go/Rust fixture逐字一致，完整文件SHA为
  `3e324f153ed82b1c8df77868804929d889c0c1875a2a99482d70617458a9e8a1`。
- 真实Mac PG17准确二进制为`b078736a4cfdfccce69914feefb5b5e56560c1e8`。
  新SDK原生组合1项1.31秒，覆盖未送达/已应用/冲突、重复owner、旧回执不能
  清新选择、Stale回退不丢旧未知、四个JSONB/SHA摘要和真实Postmaster重启拒绝
  改绑。首次WAL/丢外层回复采用显式恢复输入，未运行真实Agent/Lease写前CAS。
- Compute真实Router受影响3项通过（其中2项真实PG），0fail/ignored、24filtered；
  原resize阻塞时观察通道关闭1项通过、11filtered。明确采用定向范围，不宣称
  整包或整个产品E2E已通过。先前/中间同场景通过不累计。
- 两包Go vet、增量lint 0issues通过；DSGA/Agent消费者仅编译通过，无用例执行。
  Rust两包lib及四个受影响测试目标Clippy `-D warnings`通过，依赖历史
  remote_storage的27warnings保留。两仓diff、宿主脚本语法及Neon版本锁验证通过。
- 本批未改PG内核，不重编或重复PG旧矩阵；没有新Linux交叉编译或Linux执行。
  Go运行后仅格式化、显式零值及局部变量改名，按所有者要求不重跑相同行为。

首轮四个向量摘要失败来自生成器误用紧凑字典序，修为PG JSONB键长/字节序与
空格分隔，并由真实PG独立核全部有效摘要。原生测试首轮额外SQL参数被推断为
jsonb而Rust传String的夹具错误，修为text→jsonb后通过。首轮失败日志保留。
一次自动审批502阻止TLS进程创建，稍后正常审批已恢复；不是用绕开权限替代。

## 下一阶段必须完成的代码

本批没有改变外层Journal v10，也没有把新选择接入生产PGWorker、Session恢复
配置或本地管理入口。因此“新owner原关闭未送达”的**完整生产生命周期仍未
闭合**，不能仅凭本批SDK测试移到Linux待验。下一步须：

1. 独立持久接续选择与原Owner/Sequence/摘要、写前UID/RV CAS及有界完成空间。
   CAS失败或丢回复不发送；恢复只查询最近完整选择；原始关闭永不覆盖。
2. 当前Session在原串行Run内推进，显式新owner接续/同owner原重放分别认证；
   恢复Go SDK三份选择。反复接管保留前驱、所有原预算和历史；不自动重开。
3. Mac组合覆盖CAS前后/PG前后丢回复、重建、真实失租接管和下一轮受控恢复，
   不用手填模型终态替代真实生产调用。

其他仍须代码：安全降低normal及私有/后台持续约束和活性、在途/首次开放未知/
坏缺日志异常关闭及受控重开、分区/出生不完整/Runner无Wait/外层跨boot迁移、
Monitor/WSS/组配额/RW-RO、全部最终写者和可信CPU。PG17完整后再PG18、PG16
独立内核/扩展/发行及各版回归；独立控制面继续由另一任务完成后统一E2E。
Linux/K8s/NeonVM/HugeTLB/物理回收/性能另行目标验收。默认关闭；不构建镜像、
部署、合main或发布生产。持续目标active，本阶段归档不等于整体完成。
