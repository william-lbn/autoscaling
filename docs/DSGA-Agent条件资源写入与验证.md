# DSGA Agent 条件资源写入

日期：2026-10-20。按 V3 R03 补强现有 `execNeonVMInterface → Runner.doNeonVMRequest`
真实路径，不另建控制服务。**这是API写入保护，不是完整持久owner或最终VM执行权**。
仍未启用生产DSGA自动缩容；完整预算、Monitor/PG联动及Runner fencing须继续实现。

## 本批行为

创建Runner时从原watch事件固定VM UID；名称不能替代UID，派发前不得用新GET到
的UID覆盖原身份。`VmInfo`新增可省略UID，VM读取真实metadata，Pod读取匹配的
VirtualMachine控制者引用，不能把Pod UID/名字标签当成VM UID。仅策略fixture可
缺UID；实际写入拒绝空UID。

每次资源写入先GET，校验原UID/命名空间/名称、Runner Pod名/IP、存活阶段、删除
状态、当前slot大小与资源界限。内存必须是当前slot的精确整数倍，不再向下取整；
防止int32数量、int64总字节溢出。GET与Patch共用原超时，不持Runner/Core锁跨网络。

动作的原资源快照必须仍与当前spec一致，或目标本身已是当前spec；然后JSON Patch
先test UID/resourceVersion，再更新资源。GET后的对象重建/并发变更因此不能被旧
请求覆盖。CAS失败不在本函数内部重新GET并强套旧目标，交回上层重新对账。
可选targetRevision使用RFC6902 add，兼容字段原本缺失的合法旧VM。

官方Revision是进度标记，**不能直接当不可变operation ID**：同一策略目标分批
获得Scheduler批准时，同一Revision可以继续合法变更资源。本实现保留此行为，
用动作源快照和RV阻止旧批次倒退；拒绝低于已记录Revision和同值改flags。
Revision=0旧模式继续可用，但仍有UID/RV条件；DSGA新协议须使用独立完整key。

响应必须仍对应原VM/Runner、精确目标和Revision；不能把不同的服务端返回值
当成功。请求可能已发送后的HTTP/传输错误标为unconfirmed，下一次先读取对账。
同Revision且目标已经写入时只确认现有spec，不重新派发、不改原时间记录。

## 两个实际负向证据

测试通过真正生成的clientset发HTTP，服务端按RFC6902应用Patch。临时只移除两条
test操作后，两项确定性失败；finally已逐字节恢复生产源码：

- 新Revision6已写入后，旧请求晚到，实际被覆盖回Revision5。
- GET后对象以同名新UID出现，即使HTTP返回核验报未知，资源仍已误写成4 slots，
  应保留的8 slots被改变。仅在响应后查UID不能补救已发生的写入。

日志：工作区`build/autoscaling-dsga-cas-negative.log`。这是API竞态与对象写入
的反向证据，不是QEMU/Guest物理内存测试。模拟端点不认证真实apiserver admission、
etcd持久性或多Controller最终执行协议。

## Mac验证与剩余范围

使用Go1.25.13 darwin/arm64、Xcode14.4 SDK与clang。默认CommandLineTools27.0
SDK曾导致链接器无法读取新格式Security.tbd，已显式选择既有14.4 SDK后通过，
没有修改全局工具链。宿主旧golangci-lint1.63不支持仓库v2配置，独立安装v2.4.0
到工作区build/tools；不升级项目依赖。格式采用仓库gofumpt0.7及原gci分组。

受影响Agent/core/Executor、Scheduler plugin、Controller及VM API在Mac执行race
回归；新HTTP用例覆盖空/改变身份、重建、迁移/删除、上下限/单位/溢出、同Revision
分批批准、响应丢失/被改变、并发晚到、旧Revision0和从Pod读取正确UID。测试实际
数量和最新结论见同目录执行状态记录，不用无测试文件包或skip虚增通过数。

本批最终Agent共31个pass事件（10个顶层测试及21个子测试），受影响既有Core/
Scheduler/Controller/VM API另83个pass事件；该模块范围合计114，无失败/用例skip。
另一个Go请求契约模块18个事件单独记录。完整组合命令132个pass事件、8个有测试
包通过、9个无测试文件包不计业务通过；race与本地v2 lint均通过。日志为工作区
`build/autoscaling-dsga-cas-full-final.{jsonl,stderr}`和
`build/autoscaling-dsga-cas-lint-final.log`，不是整个仓库所有Linux功能测试。

复现使用`go test -race -count=1 ./pkg/agent/... ./pkg/api/... ./pkg/plugin/...
./pkg/neonvm/controllers ./neonvm/apis/neonvm/v1`，需在一行执行；Mac显式设置
SDKROOT/CC/CGO_CFLAGS/CGO_LDFLAGS到上述14.4安装。完整项目的Linux Runner、
真实apiserver/etcd、QMP、迁移、物理回收、K8s及性能仍由后续目标环境验收。

当前Core仍以spec传播状态进行旧模式调度；本批没有把它认证为实际VM资源。
后续DSGA必须单独持久保存完整操作、唯一issuer、资源承诺和未确认效果，在最终
Runner串行执行并核验实际QMP/Guest后才释放承诺。旧Controller直接QMP路径亦须
封堵；API CAS不能撤销已发QMP。上述为代码剩余项，不归入Linux待测。
