# Agent原VM派发与来源结算：Mac验收

本模块在2026-10-26 17:29:30北京时间的固定上游基线上，补齐实际Agent的
typed ResizeVM/ObserveVM、最终预算和准入对原Runner持久来源的消费。沿自有
`codex/dsga-autoscaling-20261019`前向演进；代码提交为
`844e99c76e785aada27eef92df780e002b7d8303`，前置预算v4提交为`2bc22973`。
PG17仍为`b078736a4cfdfccce69914feefb5b5e56560c1e8`，不再追新增上游。
源码、实际用例全名、日志和编译产物哈希见[机器收据](DSGA-Agent原VM派发与来源结算-Mac收据-20261027.json)。

这项关闭Agent与Runner之间的缺口。**整体核心仍有文末所列未实现功能，不能
将其包装为仅待Linux测试，不能宣布完整内存伸缩版本已经封版。**

## 实际实现和不变量

`pkg/dsga/vmexec`接入已有生产PGWorker推进循环。Planned ResizeVM先读取
原Runner出生、当前floor/Lease UID及真实before，再GET完整请求固定名称的原
Host对象；核当前认证PG、Root、原Prepare及原持有来源。Grow必须已完成原
ReserveHost；Shrink必须原PG释放、GuestReady和关闭门禁仍有效，重新采样
DSM/连接/前台工作状态。后台、DSM和2PC允许非零，由持续聚合约束覆盖。

Agent随后仅以原VM UID/RV CAS保存完整typed命令；Runner由自己的Kube客户端
拉取并执行。Agent不增加第二个QMP写入口、不改spec.Use、不临时创建预算CM。
Host持续持有不少于`max(before,target)+overhead`，并绑定真实原Pod/Node UID。
Start回复丢失时，服务器可能已保存原意图，Runner可按原意图继续；Agent恢复
只查原命令，不能重建一个新目标或重发QMP。

Unconfirmed ResizeVM先查原Runner回执，再访问Host/Root/PG；来源失联不应
阻止原执行域的只读查询，但也不能因此写外层Completed。仅当原Birth/State/
Receipt UID、原完整命令和双摘要、当前无pending、实际requested和实际容量
均到原目标，并且外部持有与原生事实仍有效，才写VMSettlement。

ObserveVM复核同一原ResizeVM回执，保存相同typed命令和逐字节相同VM事实，
不产生新的Runner内存写。原Observe Start回复丢失后仍只读同一原Resize来源。
原not_sent是明确未发送结果，但本模块不会自动重放；生产显式重放传输仍列待办。

`pkg/dsga/vmguard.Reader`固定原VM/incarnation与Session确认的Lease UID。
Finalize和准入均必须读取原不可变回执，并交叉核对原Prepare/Guest/Host摘要
及完整typed VM命令。Finalize Start之后和准入执行后再次核来源；删除、同名
新UID、错误floor、未发布或不一致的完整结果均不完成。

## 持久预算与兼容

RunnerSources独立保存原Birth/State/Receipt引用及原Runner命令、外层VM命令
摘要，进入VMSettlement和Finalized.Sources。当前预算连同原Finalize完整命令/
回执在同一VM CAS原子升为**Model v4**，不随请求结束、16条历史淘汰或接管
消失；不能用数值相同的新来源替换，也不能降回无Runner来源预算。

旧v1/v2/v3继续严格读取。生产构造器必须安装原Runner读者；旧领域构造器仅
保留历史路径，不能消费带新Runner来源的数据。实际已发布c7源码读者验证：
原v3可读，v4以及删掉Runner字段的v4投影均拒绝。Model版本不能删字段回退。
PG原生ABI未变；控制面保留d84分支/SDK565，本轮不修改。后续移植先升级所有
会读取账本/最终预算的读者到支持v4的准确依赖，再启用新来源及统一E2E。

## 实际Mac结果

Mac arm64、Go1.25.13、race、readonly模块及已有缓存。去重**53顶层/164个
含子项完整测试名通过，最终0失败、0跳过**。早先smoke/v4单项由最终完整
受影响范围替换，不重复累计；未变PG、Provider及HTTP测试未重跑。

| 范围 | 顶层/含子项 | 实际断言 |
|---|---:|---|
| journal/budget受影响全包 | 49/119 | v4原子持有、深拷贝、历史淘汰/接管不丢原来源、投影/降级拒绝，保留旧预算行为 |
| 生产预算/准入 | 1/31 | 30场景，含原Birth/State重建、原Receipt在Finalize前/Start后/开放前丢失，外层不误完成/开放 |
| 实际Agent+Runner扩容组合 | 1/5 | 正常、QMP回复丢失、VM Start/Record回复丢失，最终预算及恢复准入；唯一内存写一次 |
| 实际Agent+Runner缩容组合 | 1/5 | 正常、写前门禁重开、VM后Root漂移、Observe Start回复丢失；复核原Observe，至多一次内存写 |
| 实际c7旧读者 | 1/4 | v3接受、v4拒绝、删Runner字段投影仍拒绝 |

增量golangci-lint 2.4.0使用仓库配置、Linux目标，最终**0 issues**，未禁用
规则；初22项及后3项显式零值/格式/错误字符串报告分别保留。8包Mac readonly
vet退出0。实际Agent、Runner、Controller的Linux amd64/arm64均编译成功
（buildvcs=false），未执行ELF，不构建镜像。后续仅显式零值与格式补正，业务
语义未变；收据哈希固定交付树，不追溯宣称早先测试拥有相同格式化后的测试源码。

owner的Go日志完整结束为package PASS、30子场景及顶层全部通过。运行期间
本地包装脚本的后续compile路径曾被修正，Go完成后zsh读取变化脚本产生解析
错误，包装器退出1；此退出与Go通过分别记录，未冒充包装器退出0。最终脚本
已通过`zsh -n`。为遵守不重复未变业务要求，没有因此重跑全部30场景。

可复现的主要命令（使用收据中的准确提交和匹配Go环境）：

```sh
go test -mod=readonly -p=2 -race -count=1 -timeout=240s -json ./pkg/dsga/journal ./pkg/dsga/budget
go test -mod=readonly -p=2 -race -count=1 -timeout=600s -json ./pkg/dsga/owner -run '^TestProductionBudgetFinalizationUsesOriginalClaimsAndRecoversOnlyReadback$'
go test -mod=readonly -p=2 -race -count=1 -timeout=180s -json ./pkg/dsga/owner -run '^TestProductionAgentRunner(GrowClosesBudgetAndAdmissionOnOriginalWAL|ShrinkRechecksGateAndOriginalObserve)$'
```

模块完整执行脚本、旧源码合同测试及所有本地日志路径/哈希在机器收据中。
协议兼容不能通过“模拟一个旧DTO”替代实际历史源码编译。

## 边界与仍须实现的核心

组合执行真实Session、VM UID/RV WAL、Agent、Runner、私有Unix socket QMP
协议、文件锁/fsync、预算及准入代码。**Kube API/Core UID-RV、QEMU/KVM、
可信Prepare/Host及PG/Root内核是明确协议夹具**；缩容原PG释放/排空前驱也为
明确夹具。本结果不认证真实Host容量、内核物理热拔或完整Neon端到端。

仍须核心代码：完整可信Prepare与剩余系统/过渡/安全资格生产者；真实Host/
Scheduler持有与显式释放；安全normal下降和持续后台/私有约束；首次预算及
首次正常SQL开放；公开策略/操作生命周期；生产Runner显式not_sent重放、
取消未知结清和受控跨boot。持续目标保持active，不能将这些缺口移到Linux待测。

Linux后验另列：真实apiserver CAS/RBAC/来源保护，QEMU/runas/私有socket/KVM，
virtio-mem异步或部分热拔及连接故障，cgroup/普通页/HugeTLB真实物理回收，
Host持有/迁移互斥，性能与长稳。独立控制面完成后再移植、做统一端到端。
其他PG版本暂缓；不构建镜像、不准备/执行部署、不合主分支、不发布生产。
