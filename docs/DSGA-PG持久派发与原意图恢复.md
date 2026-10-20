# PG持久派发、事实对账与显式原意图恢复

日期：2026-10-21。按V3继续实施，整体Goal active、默认关闭。本模块在
`pkg/dsga/pgexec`使用真实journal Store及正式Compute HTTPS Client处理ResizePG/
ReleasePG，不再依靠调用者手填PG Completed。它仍需接入可信owner协调循环；
没有实现预算/Host/Guest/VM适配器，不把该模块作为完整R03或生产发布验收。

## 实际调用与不变量

构造时固定Store、已认证配置的Client、每进程owner及完整PG boot，owner epoch
必须与Client固定epoch相同。`Advance(ctx)`每次重新读取持久状态，核验VM/owner，
只推进一个PG步骤；遇到其他领域或清理步骤返回ExternalStep。它不是跳过预算
或自动生成资源许可的快捷路径，也不会自行创建日志、选主或循环忙重试。

正常PG派发顺序：读取完整原意图→严格PG Fence→journal Start确认持久写前记录→
发送相同完整命令→校验原生回执→journal Record条件提交。写前记录带完整请求、
目标Compute、owner及命令摘要；API错误、写成丢回复或CAS失败均没有新派发。
源VM UID/resourceVersion条件由Store执行，PG完整Key/context幂等及floor另作隔离。
网络期间不持本地锁；同owner并发Start也只有一个CAS成功并取得派发票据。

已Unconfirmed的PG步骤在普通Advance中**只查询或业务取消，不自动重发Resize**。
旧owner/取消中的原请求保持原epoch，当前owner用独立observer查询/取消；不会
改成新epoch后继续执行旧动作。查询结果必须再用完整Request重算context摘要，
不能只检查Key而借用不同VM目标、预算或期限的回执。当前observed不能比刚取得
的同boot Fence generation倒退；更高floor也立即停止当前执行者的写后推进。

返回进度：Idle表示没有活动意图；ExternalStep交其他领域适配器；AwaitingFact
表示仍不确定，须保留意图/成本并有界退避；Recorded只表示本PG步骤事实落账。
PG COMMITTED仅作为逻辑ResizePG完成，不把Provider/Guest/VM后续步骤视为完成。
Denied或本次执行明确Busy/Rejected记Rejected，仍需外层预算/准入清理。
NotFound/Expired/Stale/Conflict不能构造NoEffect或取消完成。

## Provider核验与失败恢复

ReleasePG独立取得Fence并查询事实。只有当前同boot/完整Key/context/容量generation、
同一目标且PG执行者已退出时，才处理当前四数组结果；历史回执不能与regrow后的
分配拼接。四数组必须全部无未知/部分分配，释放身份/generation一致，Provider
证明完整请求区间物理释放；原生确认的零长度Noop不要求无意义的系统调用。

旧执行仍在途、换成其他操作或历史/当前无法对齐时保持Unconfirmed。PG执行者
已退出且仍是本操作，但Provider unsupported/失败/保留checkpoint工作区/缺少
完整物理证明时记Rejected，完整保存原成本和未知区间。重复查询不会自行重试
已退出的Provider；后续应清理/保留预算并以新revision重新规划，不能永久轮询后
误报成功。Rejected不表示没有副作用，绝不直接恢复准入或释放Host承诺。

审计同时保存完整Fence和ControlResponse，没有凭据，不截断回执。超过既定32KiB
Observation或整体journal预算时写入拒绝并保留Unconfirmed，不删除活动意图腾空间。

## 显式重放及SDK栅栏

`ReplayOriginal(ctx)`必须由可信协调器显式选择，Advance不会调用。它只重放本
owner、同boot、没有Abort且已持久Start的**完全相同命令**，当前VM仍须处可派发
生命周期。用于写前提交丢回复/发出前中断，或原生终态落账后解除SDK本地pending。
它不重放VM/QMP，不撤预算，不把超时当作完成。新owner、VM删除、未Start、原
命令变更均在任何Fence/Resize之前拒绝。

已Settled的步骤还必须有保存的完整原生Committed/Denied终态；Busy或无原生
结果的Rejected不能重发，否则过去拒绝的命令可能在环境变化后成为全新执行。
新回执不得与已持久终态矛盾，不重写原终态的历史observed。正常查询/Fenced
仍不会清Client.Pending；只有显式同原意图幂等重放确认后由Client解除本地栅栏。
即使重放成功，外层完整操作仍须逐项完成预算/准入/VM对账。

## Mac验证与证据边界

- 完整受影响race组合：219个case pass事件（含父/子、fuzz种子），0fail/用例
  skip；11个有测试包通过，9个无测试文件包单列。新PG适配器8个顶层/26个子测试
  共34事件；不是219个独立顶层场景。
- 覆盖写前提交丢回复无派发、实际发送丢回复仅查原意图、记录丢回复重读恢复、
  预算摘要串用、owner切换/更高floor、旧boot/错误命令/目标、Provider全数组及
  partial/unsupported/checkpoint/regrow、旧owner取消、显式恢复与无结果终态拒重放。
- 真实Go→TLS→生产Compute Router/JWT→双SQL→准确PG17 6b7c3655/Neon1.8：
  1 Rust外层/2 Go race进程通过7.08s，零skip/ignored/filtered。正常流程的首次
  1024 grow由pgexec确认写前记录后执行，提交后丢回复，再构造适配器查询并持久
  落账；只显式ReplayOriginal才第二次POST。后续shrink/regrow、600行超过bootstrap
  数据校验、真实PG重启拒静默重绑保留。准确依赖提交后另复验并记在Neon主仓。
- VM API仅为本机HTTP/RFC6902存储夹具，非PG领域使用明确test_only body。
  没有运行真实apiserver/etcd、owner租约、预算、VM/QMP或物理释放。
- 一次扩大到Controller子目录的尝试因缺`/usr/local/kubebuilder/bin/etcd`启动失败，
  继而触发既有AfterSuite清理panic，实际0/2 spec。失败保留在full.jsonl，不能
  记成通过或称Mac天然不支持；本模块未改该套件，本轮也不准备K8s部署。
  受影响组合使用原范围`./pkg/neonvm/controllers`，与envtest套件分列。

受影响命令（一行执行，显式Xcode14.4 clang/CGO sysroot）：

```sh
go test -race -count=1 -json ./pkg/dsga/... ./pkg/api/... ./pkg/agent/... ./pkg/plugin/... ./pkg/neonvm/controllers ./neonvm/apis/neonvm/v1
```

本地golangci-lint2.4的native标签及上述全部组件检查，脚本/格式/diff另验。日志
位于工作区build：autoscaling-pgexec-publish.{jsonl,stderr}、
autoscaling-pgexec-lint-publish.log、neon-pgexec-native-release.log。
最终定向反向：故意漏完整意图核验、漏物理释放证明、在Start前提前派发、跳过
已拒绝步骤的原生终态重放资格检查，四项均实际断言失败，没有以编译错误计数。
原始源码finally逐字节恢复，前后SHA256均
`b8355b965253bb97bd0fa936128320d783eefe4cf8c79c3e2130e2993047b42b`；随后完整受
影响race重验。最终反向日志autoscaling-pgexec-negative-final.log，早期3项日志
negative.log保留。不把显式重放测试夹具从Unconfirmed改为Settled的修正算生产
故障注入证据。普通/native完整标签lint0issues，生产依赖/go.mod/go.sum未变。

## 继续实施

可信owner/租约与实际持久协调循环、Monitor、全部硬预算/quiesce及清理、Scheduler/
Controller/最终Runner执行隔离、受控跨boot恢复、其余PG、Driver/Helm和真实Neon
存储E2E仍需继续；不是只待Linux测试。Linux/K8s/KVM/HugeTLB/物理回收/迁移/
性能与长稳后验。本轮不构建镜像、不准备部署、不合主分支、不发布生产。
