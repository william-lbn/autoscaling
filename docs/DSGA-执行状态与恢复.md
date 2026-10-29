# DSGA Autoscaling 执行状态与恢复

更新：2026-10-26。分支`codex/dsga-autoscaling-20261019`，只推自有fork，不合main。
本文件是当前入口；初始维护快照仍见[基线](DSGA-上游基线与开发约束.md)。

## 最新验证：出生失败关闭与整Pod结清已完成Mac阶段（2026-10-29）

Initialize在返回Runner前失败，现先持原flock封存不可撤销屏障；同nonce补齐
State/Binding及v3关闭来源也始终nil/error。已确认State UID独立fsync，后续
Binding失败不丢身份；来源替换/缺失、坏WAL和已有launch拒绝。迟到Tick/Start
永久拒绝，外层预算不改。整Pod终态与Node CAS未知恢复复用正式消费链。

两包Mac -race采用去重58顶层/221含父子记录pass（新增4顶层14记录），0skip；
Scheduler首轮旧v8断言失败单列，改为未知v9后只复测受影响两项通过。vet/最终
增量lint通过。详细真实失败、源码/日志及编译边界见[阶段验收](DSGA-出生失败关闭-Mac验收-20261029.md)
和[机器收据](DSGA-出生失败关闭-Mac收据-20261029.json)。PG17/Rust未改，不重复
原生大矩阵；仅同调用可封存的出生失败完成，崩溃无屏障/无Wait/跨boot与其他
核心缺口继续。Goal active，PG17全面完成后才暂停，不自动进入18/16；所有者
AGENTS修改保留不stage，不追新上游、不改独立控制面、不建镜像/部署/合main。

## 最新验证：最终开放异常关闭已通过Mac阶段（2026-10-29）

新增v13保护完整Finalize已永久持有、前驱均Completed且最终Resume已Start的
终段。原Active完整SHA固定全部命令/回执/未知，关闭WAL和完整原生proof独立；
不普通Record/取消/Finish原操作、不释放预算、不缩VM。来源在setter后审计前
漂移也同轮关闭；确认后SDK重建不恢复已被覆盖的旧开放pending，原未知保留。

新模型按用例去重4顶层24记录、旧v10/11/v12模型13顶层91记录、生产9模式
分批-race通过；真实Session失租接续/SDK重建/CAS丢回复及来源恢复不自动重开
已验。最终正常结案1顶层3记录、受影响首次2模式通过；vet/增量lint0、消费者
仅编译exit0。原正常链3种永久故障移至各自关闭生命周期，没有删除覆盖。
首轮Store沙箱监听失败和旧基线真实失败保留，不能整体涂绿或用过滤冒充全量。
准确源码/日志/采用及边界见[阶段验收](DSGA-最终开放异常关闭-Mac验收-20261029.md)
和[机器收据](DSGA-最终开放异常关闭-Mac收据-20261029.json)。所有测试句柄已终态。

本批PG/Rust业务未改，不重编准确325a；PG gitlink及两份PG锁不变。代码/中文
文档随后按AS依赖→Neon锁顺序中文提交推核，远程以实际SHA为准。所有者AS
AGENTS原改动不stage、旧stash不动。仍缺Resume尚Planned/其他在途及坏缺日志
关闭、受控重开/重复生命周期、normal/持续约束、其他出生/无Wait/跨boot迁移、
Monitor/CPU等核心代码，不归Linux待验。Goal active；PG17全面完成后才暂停，
本轮不进入18/16、不改独立控制面、不建镜像/部署/合main。

## 最新验证：首次开放未知异常关闭已通过Mac阶段

PG17自有修复`325aeb42a6080d86902d51892b2518a70ec2512b`在冻结b078之上，只允许
关闭跨过未投递revision，开放仍精确next；官方冻结点/清单不变，没有追新。
准确325a干净源码已编译安装，Neon扩展安装通过，完整二进制SHA匹配。新增SQL
与Rust原生SDK在原b078失败、325a通过；新SQL及旧准入4项、Rust新用例1项、
旧接续1项、SDK2项、认证HTTP3项均通过，0skip/ignored。没有重复完整未变PG矩阵。

AS v12独立原首次开放/关闭WAL及完整proof、生产首次安全检查和SDK重建通过。
原InitialHeld/预算/开放Unconfirmed保留；关闭不证明旧工作未执行或可释放/缩VM。
新Journal按用例去重4顶层28记录、受影响v10/11九顶层63记录通过；生产8个模式
分两次-race通过，含原开放未送达/已应用、CAS/setter丢回复、资格丢失两窗口、
真实Session失租接续与setter后来源漂移。正常首次→Grow和旧空闲Root受影响
组合也通过；vet/最终增量lint0，DSGA/Agent/Controller消费者仅编译退出0。

日志及源码摘要、首轮失败、准确采用及环境边界见[本阶段验收](DSGA-首次未知异常关闭-Mac验收-20261029.md)
和[机器收据](DSGA-首次未知异常关闭-Mac收据-20261029.json)。Go组合PG/Root/Kube/
平台仍是peer；原生PG链独立验证，新管理HTTP/Submit入站本批未新增E2E。
验证句柄59913/60160均终态0，进程核对无相关残留，不重复已通过未变测试。

本阶段源码、验收和机器收据随本次中文模块提交归档。按依赖顺序先推PG/AS，
核完整远程SHA后才更新Neon两份PG锁/gitlink及AS锁；远程结果由实际Git核验及
Neon最新锁记录，不以本地commit代替。所有者AS AGENTS改动保留不stage。PG17仍缺在途/坏缺日志异常关闭、受控重开与重复生命周期、normal/
持续约束、其他出生/无Wait/跨boot迁移、Monitor/CPU等核心代码，不能转列Linux
待验。Goal active；PG17全面代码/Mac验证/远程归档完成后才暂停，不进入18/16。

## 最新验证：关闭接续持久WAL与PGWorker/管理处理器已通过Mac阶段

原Goal恢复active；原neon heartbeat已通过应用工具更新为PG17全面完成后暂停，
不进入PG18/PG16，未创建重复任务。此前paused及审批失败段为历史，不覆盖本段。

本地v11独立Continuation、完整三份选择恢复、显式更高owner接续与原选择重放已
验证。新组合最终-race1顶层/3记录通过，0fail/skip，115.75秒；真实Session失租/
重复接管、写前CAS/PG前后/Record丢回复、SDK重建与旧Session零派发。PG/Root/
Kube/平台为显式peer，executeControl为正式处理器但未走新管理HTTP/Submit入站。
原Journal9顶层63记录、协议3顶层21记录采用不重跑；首轮受影响旧v10/实例绑定
两顶层3记录通过单列，首轮整条命令因新测试只接受LeadershipLost而实际先返回
context canceled失败，修新测试断言后两新分支通过。三包vet及最终owner vet、
增量lint0、共享消费者只编译exit0。详细源码/日志/未执行见[阶段验收](DSGA-关闭接续持久WAL-Mac验收-20261029.md)
及[机器收据](DSGA-关闭接续持久WAL-Mac收据-20261029.json)。

验证进程已终态，PG17 b078冻结/独立控制面/环境边界不变。候选记录保留历史；
接下来中文模块提交、先推AS核完整SHA再更新Neon锁/文档并推核，远程以实际Git
核验为准。在途/首次开放未知异常、坏缺日志关闭/受控重开、normal/持续约束、
生命周期与Monitor/CPU仍有代码工作；不是PG17全面完成，不将其归为Linux待验。

## 本次心跳读取到Goal已暂停（2026-10-29）

get_goal实际返回status=paused，工具未说明暂停原因。PG17尚未全面完成，不能
将该状态解释为验收或触发了“PG17完成后暂停”的业务条件。本心跳未启动开发、
构建或测试；候选源码摘要仍与既有收据一致，owner组合日志仍不存在。

旧neon heartbeat仍为ACTIVE且提示自动进入PG18/PG16，已调用应用工具尝试改为
PAUSED并更新为最新PG17停止点，但自动审批服务返回usage limit、明确未执行。
不得绕过审批改写应用配置。后续心跳先读Goal真实状态；paused时不自行复工，
等待所有者明确恢复。源码/候选/既有日志保留，恢复后仍须先完成新owner生产组合。

## 最新本地候选：关闭接续持久WAL及生产入口（未提交）

按所有者最新指示，先完成PG17全部核心代码、Mac可执行验证与远程归档后暂停，
不进入PG18/PG16；当前未满足该条件，Goal active。根AGENTS已记录，原heartbeat
更新遇审批服务断连/超时，工具未执行；恢复必须按最新停止点。

本地新增v11独立Continuation、原UID/RV写前CAS、完整PGWorker三份选择恢复与
显式continue_service_closure管理入口。原关闭/预算/未知不改；当前选择只读
轮询，更高owner先原生Fenced再观察revision，写前确认后派发，不自动重开。
Journal 9顶层/63记录-race及管理协议3顶层/21记录-race通过，0fail/skip；三包
vet终态0，首轮gofumpt修正后最终增量lint0。新生产组合已编写但四次启动均在
创建进程前被自动审批服务502/超时拒绝，没有owner日志/进程，不能算通过或归
Linux后验。详见[当前候选恢复](DSGA-关闭接续持久WAL-开发恢复-20261029.md)及
[候选收据](DSGA-关闭接续持久WAL-候选收据-20261029.json)。

两仓尚未提交/推送；AS HEAD仍ca15c2bd、Neon仍d9701afb及原依赖锁；PG17三处
b078冻结不变，无遗留测试进程，不重跑已通过未变功能。下一步先执行新owner
组合与受影响v10/Worker绑定，真失败先修，通过后再按依赖顺序中文提交推核。
所有者AS AGENTS改动保留；不追上游/独立控制面，不建子代理/镜像/部署/合main。
以下既有已归档阶段仍有效，不将本候选写成已验收或PG17全面完成。

## 最新接续：冻结基线上的Runner持久消费与私有QMP（2026-10-26）

按所有者17:29:30冻结维护截止，本轮不再fetch/merge新上游。实际Runner出生/
固定Kube消费/每VM持久floor与完整未知/私有QMP单次set/实际读回/原UID不可变
回执已接到启动；受管Controller读取该来源，移除旧TCP/manual内存控制入口。
Mac去重20顶层/110含子项通过，0最终失败/跳过，readonly vet与Runner/Controller
双Linux架构仅编译通过；准确边界、失败原日志及仍缺核心代码见
[本模块验收](DSGA-Runner持久执行与私有QMP-Mac验收-20261026.md)。Agent typed
派发/VM源长期结算、可信Prepare/Host生产者、首次准入/安全降限/受控恢复仍须
收尾，不能将本模块写成整体封版。控制面/其他PG暂缓，默认关闭，Goal继续。

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
