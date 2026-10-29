# Cleanup恢复准入异常关闭：Mac阶段验收（2026-10-29）

本模块补齐取消恢复Cleanup已写前持久之后的来源异常关闭；整体PG17尚未完成，
持续目标保持active。产品交付目标为完整内存自动扩缩容，日志格式18只是明确持久
结构和旧读者拒绝边界，与PG18及中间产品交付无关。

## 行为与不变量

- 原实际容量取消、完整目标取消、容量未变取消分别复用原生产预算审计；旧容量
  必须引用旧完整预算ID，不能误用新Active目标。容量固定原代，禁止跨容量接续。
- 原Cleanup未知/完成、发送开放/只读均可安全关闭。先核原来源，异常后原生fence、
  同boot/固定容量/revision检查，再原VM UID/resourceVersion写前CAS确认后派发。
- 写前回复丢失零派发；关闭回复丢失只查原请求。同owner显式原重放或真实失租后
  更高owner接续均保留原选择，关闭不能产生Finish、Host释放或VM反向写。
- 完整原Active、原Cleanup命令/回执/未知、历史和永久预算头逐字保留。来源恢复
  不自动重开；SDK重建在关闭已确认后不再恢复被覆盖的旧开放pending。
- 公开恢复入口及最终Admission Executor均拒绝仍持有ServiceClosure的恢复开放。
- 修复旧v2实际容量Cleanup尚无永久头的遗漏：安全预审不能因无头跳过；仅合法
  格式18独立关闭可暂不具备永久头，仍强制完整原Cleanup/资源身份校验。移除关闭
  或Cleanup不能借例外恢复普通服务，不能投影补造Initial/Finalized预算。

## 实际验证

Mac arm64，Go 1.25.13，`GOPROXY=off GOSUMDB=off`，工作区GOCACHE及Xcode
MacOSX14.4 SDK/clang。业务Go验证全部启用`-race -mod=readonly -json -count=1`。
监听场景经用户明确批准、受审提升执行。所有测试句柄已收取进程终态。

| 采用范围 | 去重顶层 | 含父子通过记录 |
|---|---:|---:|
| Journal新关闭模型/HTTP CAS Store及旧关闭受影响回归 | 49 | 321 |
| Admission Executor全包 | 5 | 32 |
| 生产Agent/Runner来源异常与正常取消恢复受影响场景 | 3 | 33 |
| 合计 | 57 | 386 |

采用记录0 fail、0 skip；父子记录不是386次产品E2E。Journal新模块单独5顶层/
49记录，旧回归44顶层/272记录。生产新8完整目标模式及实际/旧容量各4模式、
受影响正常取消14模式均有成功证据，精确(package,test)和日志SHA256见
[机器收据](DSGA-Cleanup恢复准入异常关闭-Mac收据-20261029.json)。失败日志及父
记录保留，**capacity-matrix、owner-matrix、normal-cancellation原批仍为失败**；
这里只采用其中无失败的成功子树及后续定向成功替换，不能说这三个整包通过。

关键入口：

```sh
go test -race -mod=readonly -json ./pkg/dsga/journal -run '^TestCleanupServiceClosure' -count=1
go test -race -mod=readonly -json ./pkg/dsga/admissionexec -count=1
go test -race -mod=readonly -json ./pkg/dsga/owner -run '^TestProductionCleanupSourceFailureClosesAndPreservesOriginal' -count=1
go test -race -mod=readonly -json ./pkg/dsga/owner -run '^TestProductionAgentRunnerGrowClosesBudgetAndAdmissionOnOriginalWAL/partial-safety-' -count=1
go test -race -mod=readonly -json ./pkg/dsga/owner -run '^TestProductionAgentRunnerShrinkRechecksGateAndOriginalObserve/restore-prior-safety-' -count=1
```

实际分批只复测新增/修改/受影响场景，未重复未变全量；最终日志选取不等于重新
运行上面的整组命令。`go vet`三个受影响包退出0；工作区`build/tools/golangci-lint`
增量检查0 issues/退出0；`git diff --check`通过。共享消费者19包pass、14包无测试
文件，命令`-run '^$'`仅编译，不计业务通过。PG/Rust未改，本批没有重新执行原生
PG、Linux交叉编译、镜像或部署。

## 失败历史与修正

- 首轮网络监听受沙箱拒绝；批准后执行。早期PG peer将原Cleanup开放误作Resume，
  改为严格检查实际持久原WAL，新增关闭peer单独核完整关闭请求，不放宽旧setter。
- 两个实际容量unknown场景暴露无永久头路径遗漏，修生产逻辑和模型后单独通过。
- transfer和普通abort-committed早期未创建Cleanup；增加健康来源前置断言后分别
  单独通过，真实Lease失租接管关闭已验；不声称已查明早期失败的全部根因。
- abort-replies明确在Finish之后被旧45秒组合deadline耗尽。仅测试总期限改90秒、
  abort组合维持真实Host Lease续约；生产请求/租约不变。定向两场景退出0。
- 旧abort-root-drift预期恢复来源后继续原开放，与新安全合同不符。新预期为同轮
  独立关闭、原Cleanup仍Unknown、恢复来源后ErrServiceSafety且零setter，已通过。
- 旧容量restore-prior、replies、negative实际均通过。原总包失败历史保留。
- 系统lint由Go1.23编译不匹配；采用工作区新版。首轮10项lint与猜错cmd路径均保留，
  修正后最终lint/vet/准确路径仅编译均通过，不把首次失败涂绿。

## 版本与验收边界

AS本批基线`7a333101c6fff6240d54ca06604c5f9f7da4b6cb`，Neon基线
`67897d8ea1a117b66929d48bd1f591c4f5e3af9b`；本批源码哈希由机器收据固定，
中文模块提交/自有分支推送与Neon准确发行锁后续共同证明远程归档。PG17实际
源码/gitlink/两份锁保持`325aeb42a6080d86902d51892b2518a70ec2512b`。

Go组合中的PG/Root/QMP/Kube为明确peer，owner/SDK/CAS/Lease/生产恢复代码在
Mac实际执行；不代替Linux/K8s/KVM/NeonVM/HugeTLB/物理回收与性能验收。
尚须代码：坏缺WAL独立关闭、受控重开与有界重复生命周期、安全降normal、
后台/私有持续硬上界及活性、出生崩溃无屏障/无Wait/分区/跨boot迁移、Monitor/
WSS/总配额/RW-RO/可信CPU及全部最终写者。缺失业务不能转列Linux待验证。

不追冻结后上游、不改独立控制面、不构建镜像、不部署、不合main；所有者AS
AGENTS原修改不stage。PG17全面完成/Mac验证/远程归档后才暂停，不自动进入18/16。
