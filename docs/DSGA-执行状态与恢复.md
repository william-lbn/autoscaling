# DSGA Autoscaling 执行状态与恢复

更新：2026-10-20。分支`codex/dsga-autoscaling-20261019`，只推自有fork，不合main。
本文件是当前入口；初始维护快照仍见[基线](DSGA-上游基线与开发约束.md)。

## 当前已实施

- Agent的真实VM写入路径固定watch中的UID/Runner身份，先GET，再用UID/RV条件
  Patch；检查源资源快照、精确单位/范围/溢出、返回值和不确定结果。保留官方
  同Revision分批批准及0兼容模式，不把传播Revision当DSGA不可变操作ID。
- Go协议请求/查询类型在`pkg/api/dsga`，完整身份、无损uint64、严格重复/未知/
  缺失/大小写字段、规范ID、期限与随机操作ID。与Neon6e63共享高位向量。

这两项不构成完整DSGA自动伸缩。尚未实现Go实际HTTP消费者、持久操作账本/
唯一issuer/跨进程对账、全部预算/quiesce、Monitor新协议、Scheduler承诺和最终
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

Mac Go1.25.13 arm64，显式Xcode14.4 SDK/clang/CGO编译与链接参数；初始默认
CommandLineTools27.0 SDK链接错误日志保留，是宿主配置问题，切SDK后完成检查。
原宿主golangci1.63太旧，独立工具在工作区build/tools，不修改全局安装。

## 继续顺序与不可破坏项

继续持久意图/唯一执行者与完整资源承诺：不能将账本丢失、lease到期、PG换boot
或HTTP超时视作旧动作取消；明确原操作全部效果后才允许冲突新目标。先接Go
事实消费者，再将核验后的事实写入恢复链；最终Runner必须封住旧QMP写入路径，
API CAS不能撤销已发QMP。完整PG预算/准入与其余组件仍按V3共同落实。

真实apiserver/etcd、KVM/NeonVM/Guest/Host物理回收、HugeTLB、迁移、K8s和性能
后验；本次HTTP测试模拟API CAS，不能将其记作目标环境认证。Mac可执行的业务
测试仍需随各模块继续补全，不把缺代码归入Linux待测。

本轮`AGENTS.md`已有项目所有者的7行环境边界变更，保持原样，不混入业务提交。
不创建子代理。Neon PG实际目录/锁/旧stash约定继续遵守根工作区记录，不apply
任何已apply过的stash；本批未改PG、Neon gitlink或两份版本锁。
