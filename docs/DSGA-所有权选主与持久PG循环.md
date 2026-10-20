# 所有权选主与持久PG循环

日期：2026-10-21。按V3继续实施，Goal active、默认关闭。本批在生产包
`pkg/dsga/owner`实现真实client-go Lease循环、journal owner接管以及长期PGWorker。
它们已进入真实Compute/SQL/PG门禁；**Agent实际入口、凭据发放、全部预算与最终
Runner执行链仍未完成**，不能把这些包或一次门禁当作完整R03/生产发布验收。

## 领导权与最终执行权分开

锁定的client-go为v0.31.7。其选主本身不提供fencing，本批不改变这个前提：Lease
用于选择可信协调者，journal单调epoch用于持久身份，PG/Runner仍须分别拒绝旧
epoch并串行处理已发动作。读取Lease或调用Check之后也可能换主；两对象不是
原子读取，任何超时/失租均不是取消成功或预算可撤销证明。

每次New生成128位随机进程身份，不接受Pod名或请求自报holder。Run仅可使用一次。
Lease名称来自完整namespace/name/VM UID/incarnation的领域分离224位摘要，完整
绑定又通过annotation和VM controller ownerReference校验，不能因摘要或名称相同
就采用另一对象。首次确认的Lease UID在同一Elector生命周期内固定；删除重建、
缺失/错误UID/RV、外来VM、缺失holder/期限/时间、负计数或未支持的alpha选主字段
均拒绝，不静默改绑。名称复用不能重置journal。

Lease使用真实typed client GET/POST/PUT；更新携带原UID/RV。写成丢回复/坏对象
不给领导权确认，后续须重新GET及条件更新。LeaseTransitions仅为client-go进度，
不转换成DSGA epoch；int32耗尽在请求发送前拒绝，不能先写坏对象再报错。

生产默认LeaseDuration=30s、RenewDeadline=20s、RetryPeriod=2s，支持有界配置并
遵守client-go jitter条件。选主按本地观察续约变化处理，不拿远端墙钟判定最终
执行许可；时钟速率、API延迟、转换频率与规模仍须目标环境测量，没有宣称SLO。

## 持久接管与退出

选主回调先核验当前Lease，再读取完整原journal，CAS Takeover增加其uint64 epoch。
原请求、最初owner、全部已派发命令、未知动作和预算引用均保留；新owner不能
继续派发旧操作。CAS/回复不确定时有界重读，仅确认本随机holder已持久保存的
同一代次，不重复增加epoch，也不初始化空日志兜底。epoch耗尽直接拒绝。

Session只能在真实Lease和持久接管均确认后产生。Check再读当前Lease/journal，
固定UID/incarnation/holder/epoch；Actor只是值副本，不是独立许可。Check和接管
API调用均以RenewDeadline设自己的期限：即使Lease仍正常续约，VM读取单独卡住
也会停止等待。长期Session保留领导权context，不能继承短期接管context而误过期。

client-go的OnStarted/OnStopped是异步生命周期，停止可能早于启动回调登记。
本批用短登记锁封闭注册窗口，锁内不做网络/等待；停止后迟到回调无业务副作用。
失租/取消后等待已登记业务回调真正退出，不提前返回并让上层重新开一套执行者。
工作错误或panic保留原状态，错误打印不回显工作回调的私密内容。
ReleaseOnCancel=false：不靠提前放Lease伪装旧SQL/VM动作已被撤销。后继仍须等待
选主及执行fence、按原意图对账，不能删除未知动作/预算。

## PGWorker实际运行规则

NewPGWorker从可信Compute配置构造长期同一Client，epoch采用Session确认的值；
有活动PG派发时恢复其完整Pending，拒绝调用方提供不同原意图、其他VM或静默
替换PG boot。每次取凭据前重新Check；取凭据另受Compute请求Timeout约束，
因为HTTP Client.Timeout本身不覆盖Do之前的凭据回调。提供器必须遵守context。

Advance和Replay都把调用方context与Session取消绑定，即使调用方给background，
失租仍会取消真正的在途HTTP；取消后命令保持Unconfirmed，由后继对账。
Run实际执行定时/唤醒协调循环，网络或持久化未知后重读原状态，保持原Client，
不自动POST重试或重建空客户端。其他领域的ExternalStep只等待，不跳过预算/VM
步骤；closed Wake/Replay通道置nil，避免CPU/API忙轮询。Observer须有界返回。

Replay通道只接受可信协调入口的显式Key，参数仍取自原journal；普通定时/错误
不投递重放。其余原意图/终态/取消/删除边界沿用pgexec。旧SDK未知原意图即使
外层已关闭，也不能自动清空并派发另一条PG请求；在Start新步骤前明确拒绝，避免
先制造新的Unconfirmed。跨owner终态关闭、旧SQL SDK栅栏的受控解除仍须后续协议。

## Mac实际验证

受影响race最终组合包含DSGA/API/Agent/Scheduler plugin/Controller/VM API，原219
事件加本模块35事件（15个顶层、20个子测试），共254个case pass事件；不把父/子
或fuzz种子当独立顶层场景。12有测试包与9无测试包分列，0fail/用例skip。
native标签全受影响lint另验；go.mod/go.sum不变。

用例覆盖接管写成丢回复只得一个新epoch、双竞争者2→3递增及保留旧未知预算、
失续约取消/实际join、同名Lease重建/错误绑定/坏schema、更新丢回复/计数溢出、
工作失败/panic/预取消、journal owner提前变化，以及实际PG循环的丢回复查询、
显式重放、旧boot/epoch/Pending拒绝、长调用方context下的HTTP取消、closed通道、
禁止旧pending制造新步骤、VM读与凭据等待各自有界。

真实跨仓门禁新增：第二个独立journal由生产Lease选主接管至epoch2，使用真实
签名epoch2 JWT，PGWorker实际2048→4096 grow，第二次提交后丢回复；循环查询
并持久落账，只有显式重放才第二次POST。完整历史/数据/实际PG重启拒静默重绑
继续执行。1 Rust外层/2独立Go race进程、零skip/ignored/filtered；本批初次候选
8.48s通过，最终源码及准确提交结果另在Neon主仓记录。

最终源码门禁又通过8.49s，Go正常4.05s/重启后0.01s，日志
neon-owner-native-final.log。准确Go提交推送后再复验，结果记入Neon主仓。

Kubernetes仅为本机HTTP/RFC6902/UID/RV语义夹具，非PG预算/Guest/VM仍是明确
test_only body。没有运行真实apiserver/etcd/RBAC、Host/Guest/VM或物理释放。
不能用测试中的假非PG回执调用生产预算适配器。既有envtest缺etcd的失败沿用
单列，不称为Mac限制；本轮不准备K8s部署。

最终反向分别移除Lease UID固定、真实join、journal owner核验、Session取消链接、
旧SDK意图屏障、int32溢出检查、独立VM读取期限、凭据等待期限，均要求实际断言
失败而非编译错误；finally逐字节恢复3份生产源码，再执行完整受影响组合。
HTTP取消夹具早期未读完body、以及清理context误用Session短ctx的修正是测试问题，
不计生产故障注入证据；旧失败日志保留。

日志在工作区build：autoscaling-owner-{negative-final.log,full-final.jsonl,
full-final.stderr,lint-final.log}；原生候选为neon-owner-native-first.log，准确依赖
复验另列。Mac沿用Xcode14.4 SDK/clang/CGO、Go1.25.13，未改全局工具链。

## 后续接线与发布门槛

下一步接Agent实际受管VM入口、凭据broker/Control-plane Driver与受控跨owner/
boot恢复，再接全部硬预算/quiesce、Monitor和最终Runner私有QMP/资源承诺链。
本批没有给旧Agent监控/缩容路径添加“默认成功”，没有自动打标或启用功能。
PG其余版本、Driver/Helm完整产品实现及真实Neon存储E2E仍需继续，不能归为Linux
待测。Linux/K8s/KVM/HugeTLB/物理回收/迁移/性能/长稳后验；不构建镜像、不准备
部署、不合主分支、不发布生产。
