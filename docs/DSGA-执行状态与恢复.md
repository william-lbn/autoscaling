# DSGA Autoscaling 执行状态与恢复

更新：2026-10-21。分支`codex/dsga-autoscaling-20261019`，只推自有fork，不合main。
本文件是当前入口；初始维护快照仍见[基线](DSGA-上游基线与开发约束.md)。

## 最新接续：当前预算长期持有与独立核心范围（2026-10-26）

当前完整预算与Finalize Completed同CAS保存，独立于Active/历史淘汰；取消/接管
不清除。Model v3要求原Guest/PG及完整原WAL/回执，生产准入核同一长期预算。
旧v2只在原Active完整重新核验后显式升级，未知准入先查询；不从历史补写。
Mac去重38顶层/111含子项race、readonly vet及实际Agent双架构仅编译通过，
真实旧565读者拒绝新格式；见[本批准确证据与未完成项](DSGA-当前预算长期持有-Mac验收-20261026.md)。
只推进Neon/PG17/autoscaling核心，独立控制面完成后再移植/统一E2E；本任务
不追入其新main、不改其业务/SDK或分支。以下旧记录保留历史时点，不代替最新
验收。整体核心未完成，默认关闭；继续可信预算/最终VM/首次开放等代码。

## 最新接续：所有权选主与持久PG循环

生产owner包已实现固定VM/Lease身份的client-go选主、journal CAS单调epoch接管、
Session真实授权检查/有界API与凭据等待、停止登记屏障及实际工作join，以及长期
同一Client的PG协调循环。真实epoch2签名→PG循环→4096 grow/丢回复恢复已运行；
完整结果、反向/夹具失败与边界见[本模块](DSGA-所有权选主与持久PG循环.md)。
仍未接Agent实际入口/凭据发放、完整预算及最终VM执行链，不能关闭R03或Goal。
PG6b7/ABI6和锁不变，上游再次fetch未新增；只推自有功能分支。当前提交/远程
以Git/refs核验，AGENTS.md所有者7行修改保留，不混入本业务。

## 最新接续：PG持久派发与显式原意图恢复

pgexec已将真实Store/Compute客户端接成写前确认→实际PG→严格完整事实落账，
独立Provider阶段与显式同owner/boot原意图重放均实现；没有自动重发未知POST、
清SDK/外层预算或跳过其他领域。完整race219事件/11测试包/9无测试包，0fail/用例
skip；真实Router/JWT/SQL/准确PG链路已通过，仍非完整协调器/VM端到端。详见
[本模块与失败证据](DSGA-PG持久派发与原意图恢复.md)。扩展envtest缺etcd的失败
单列保留，不把它称为Mac限制或通过。当前业务提交/远程以Git/refs核验，所有者
AGENTS.md修改保留。本批不改PG或锁，不构建镜像/准备K8s/合主分支，Goal继续。

## 最新接续：ABI6与PG接管隔离

PG17依赖6b7c3655ab74062e117eef7400953cd000679a49已先推送核验。当前Go批次升级
ABI6、强制Snapshot floor、新增第六正式Fence路由消费者，保留原未知意图与独立
observer池；Fenced不是VM许可。详见[契约与实测](DSGA-PG接管隔离与ABI6.md)。
完整race185个case pass事件/0fail或用例skip，10测试包与9无测试包分列；普通及
native标签lint0issues。真实Compute/SQL/PG候选链路1外层/2 Go race进程通过，
准确提交复验随后记录。恢复以实际Git/远程refs为准；AGENTS.md已有7行所有者
边界修改保留，不混入业务提交。可信owner/持久循环及全部预算/VM链仍需继续。

## 当前已实施

- Agent的真实VM写入路径固定watch中的UID/Runner身份，先GET，再用UID/RV条件
  Patch；检查源资源快照、精确单位/范围/溢出、返回值和不确定结果。保留官方
  同Revision分批批准及0兼容模式，不把传播Revision当DSGA不可变操作ID。
- Go协议请求/查询类型在`pkg/api/dsga`，完整身份、无损uint64、严格重复/未知/
  缺失/大小写字段、规范ID、期限与随机操作ID。与Neon6e63共享高位向量。
- 持久执行日志在`pkg/dsga/journal`：完整原意图/命令、条件写前记录、未知动作/
  接管恢复、取消清理、单调高水位、16条且受字节约束的历史。使用真实VM clientset
  的UID/RV/模式条件Patch，写入不确定时无派发票据。受管标记一旦存在，旧Agent/
  Controller/QMP内存入口拒绝回退；显式新VM初始化和Webhook标记保护。详见
  [本批边界与恢复规则](DSGA-持久执行日志与恢复.md)。

- 正式Go HTTP消费者已完成五路由、双连接池/在途栅栏、完整有类型事实和原生
  context摘要核验，详见[HTTP客户端](DSGA-Go正式HTTP客户端.md)。原始null标量
  明确拒绝；完整审计body提高为正式HTTP的32KiB，命令独立保留8KiB。

- Go→真实Compute→SQL→PG原生门禁已完成，含丢回复、原意图重放、真实PG重启和
  数据校验；详见[真实联调](DSGA-Go真实Compute联调.md)。没有执行VM/预算动作。

这些模块不构成完整DSGA自动伸缩。尚未接入可信owner分配/
日志启动与协调循环、组件实际对账、全部预算/quiesce、Monitor新协议、Scheduler承诺和最终
Controller/Runner串行fencing；当前Controller仍有直接QMP路径，Core旧模式以
spec传播状态调度，不能用API成功或旧Ok替代实际VM完成。这些是代码剩余项。
完整目标持续active，默认关闭，不构建镜像/部署K8s/合主分支/发布。

## 基线与已验证事实

本批fresh fetch官方main仍`a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f`，
已用祖先检查确认完整包含。自有起点ef6cd154…保留既有CI与维护提交，没有挑选
部分bug或改写主分支。新提交以Git HEAD及GitHub refs为准，恢复不只看旧文档SHA。

- 最终受影响组合：`go test -race -count=1 -json ./pkg/agent/... ./pkg/api/...
  ./pkg/plugin/... ./pkg/neonvm/controllers ./neonvm/apis/neonvm/v1`，一行执行。
  132个pass事件、0fail/用例skip（含父/子测试和fuzz种子），8个有测试包通过；
  9个包无测试文件，不计业务通过。Agent31、既有相关83、协议18分别记账。
- 请求fuzz实际1275872次输入，15秒配置/约16秒fuzz墙钟，命令19.280s通过。
- 真实clientset/HTTP/RFC6902竞态反向：移除UID/RV test操作，两项确定性失败：
  Revision6被旧5覆盖，同名重建对象8slots被错误写成4。finally逐字节恢复源码，
  随后最终完整组合与lint通过，无故障源码留存。
- golangci-lint2.4.0按仓库v2配置本地检查Agent/API所有包，0issues；gci及仓库
  gofumpt0.7检查通过。go.mod/go.sum未变，没有升级项目依赖。

日志位于工作区根`build/`：autoscaling-dsga-cas-{full-final.jsonl,
full-final.stderr,lint-final.log,negative.log}、
autoscaling-dsga-contract-fuzz.log。最终测试62209/lint88750已终态0，负向73158
结束且恢复完成，不重新poll或启动旧句柄。没有远程Linux CI执行证据。

### 持久执行日志批次的最终门禁

再次fresh fetch官方Autoscaling main仍a4bce09e…，已确认是当前功能头祖先。
本批未变go.mod/go.sum，无PG/Neon gitlink或版本锁变更。

- 恢复后的最终完整组合：`go test -race -count=1 -json ./pkg/dsga/... ./pkg/api/...
  ./pkg/agent/... ./pkg/plugin/... ./pkg/neonvm/controllers ./neonvm/apis/neonvm/v1`
  一行执行，161个pass事件，0fail/用例skip，9个有测试包通过；9个无测试文件包
  单列，不算业务通过。journal为17顶层测试/2子测试，另fuzz父用例/4种子，共24
  pass；Ginkgo既有TestAPIs内部0 spec，未将其包装为额外集群验收。
- 恢复解码fuzz采用原始封装和重算checksum的原始模型两条输入路径，最终20秒
  配置、35835次实际输入、22.873s命令通过。初版大种子仅6次的低覆盖运行日志
  保留，没有以其替代最终结果。模糊测试是有限探索，不等于全部输入证明。
- 定向移除RV条件/UID条件各确定性失败一次，UID项还断言持久对象确被误覆盖；
  源码finally恢复，前后SHA256同为f2332387373ed7c4a824bc3b6edc6664d67e68026c339e28197be70dd76ea30a
  （后续仅补中文说明，实际发布内容以提交为准），最终完整race在恢复版本通过。
- 本地golangci-lint2.4.0，DSGA/API/Agent/Controller/VM API范围0issues；格式与
  diff检查通过。日志autoscaling-dsga-journal-{full-final.jsonl,full-final.stderr,
  lint-final.log,fuzz-final.log,negative.log}；33634/2260最终测试均终态0。

这是本地HTTP/RFC6902持久协议测试，无真实apiserver/etcd、PG/VM动作或领导权
认证证据。新Store未自动接入运行循环，无自动打标/启用；完整业务继续实现。

### 正式HTTP消费者批次

完整受影响race最终183pass事件、0fail/用例skip；10有测试包通过，9无测试包
单列。v2lint包含plugin全范围，最终0issues。日志autoscaling-dsga-http-
{verified-final.jsonl,verified-final.stderr,lint-release.log}。请求/响应向量与Neon
逐字节共享，原生context向量由PG486338生成，未改PG/三处锁。Neon main再次
fresh fetch仍fa504217且包含于功能分支。没有全量重编未修改的PG/C代码。

既有Scheduler闭区间随机测试误判实际触发，原失败保留在release-final.jsonl，
单独修复后定向race100轮通过，再执行最终完整组合。Rust原有摘要漏核对也已
反向确认：移除新比较后full_context用例确定性失败，finally恢复后SDK8/HTTP11
真实门禁通过；最初SDK路径错误是链接配置失败，未算业务反向证据。

Mac Go1.25.13 arm64，显式Xcode14.4 SDK/clang/CGO编译与链接参数；初始默认
CommandLineTools27.0 SDK链接错误日志保留，是宿主配置问题，切SDK后完成检查。
原宿主golangci1.63太旧，独立工具在工作区build/tools，不修改全局安装。

## 继续顺序与不可破坏项

最新跨语言门禁：Neon严格脚本编译本仓显式tag的Go race程序，1个Rust外层/2次
独立Go真实用例通过，0skip/ignored/filtered；Go tag lint0issues。普通183事件
沿用ee7f7df批次未改的生产代码验证，不把新标签用例重复计入该数字。本批仅增加
测试和文档，未改生产路径/PG锁。相关官方main fresh fetch仍为上述SHA且均是祖先。

继续真实持久协调循环/唯一执行者与完整资源承诺：不能将账本丢失、lease到期、PG换boot
或HTTP超时视作旧动作取消；明确原操作全部效果后才允许冲突新目标。先接Go
事实消费者，再将核验后的事实写入恢复链；最终Runner必须封住旧QMP写入路径，
API CAS不能撤销已发QMP。完整PG预算/准入与其余组件仍按V3共同落实。

真实apiserver/etcd、KVM/NeonVM/Guest/Host物理回收、HugeTLB、迁移、K8s和性能
后验；本次HTTP测试模拟API CAS，不能将其记作目标环境认证。Mac可执行的业务
测试仍需随各模块继续补全，不把缺代码归入Linux待测。

本轮`AGENTS.md`已有项目所有者的7行环境边界变更，保持原样，不混入业务提交。
不创建子代理。Neon PG实际目录/锁/旧stash约定继续遵守根工作区记录，不apply
任何已apply过的stash；本批未改PG、Neon gitlink或两份版本锁。
