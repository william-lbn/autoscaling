# 首次预算的Host持续核验与零派发取消：阶段验收

代码`1e7826557b79592f4dac853913e404f7f9054f25`，沿用原自有分支及冻结上游。
本批接通InitialHeld在Host/Scheduler的持续读取，验证零派发取消及历史淘汰后
原初始预算仍完整。**尚未实现初始部分效果取消、峰值结清和来源回收**；这几项
不能因本阶段通过而移入仅待Linux测试。VM仍v9、Node仍v7，默认关闭。

## 代码职责与约束

Scheduler无Active但有InitialHeld时进入Publisher核验，避免旧路径只识别普通/
实际预算而直接跳过。固定原初始预算、资格CM UID/内容、Runner出生/身份摘要/
Pod/Node、当前owner和原Runner实际容量；来源与VM快照前后复核。已有Last须
原域确认完成或不可重发取消，未知及相同数值不能代替原回执。

原初始placement早于Resize，缺少Host操作CM或VM命令是正常领域形状；不能为
复用SettlementFor伪造一次Resize成功。本分支只观察，不降低placement、不清
任何峰值或来源，也不授PG开放/Guest/QMP权限。PG/root持续认证仍由Agent负责，
Host读者不把自身通过扩展为完整服务预算许可。Node选主floor可正常推进，容量
行、原Holds和格式不变。普通/实际预算继续原结清链，未放宽检查。

## 采用验证与加速结果

本批-race采用6顶层、8条含父记录pass，0skip；不是8项产品E2E。逐项源码/日志
摘要及映射见同名JSON收据，父记录不重复算场景。

- 新首次生产→Host Publisher→零派发取消→实际Scheduler循环4.06秒：同名换资格
  UID拒绝；取消无setter/Resize；初始2GiB保守placement在实际VM512MiB时保持；
  没有伪造Settlement/Host CM或删除原资格。
- 新纯Model/codec场景5.65秒：超过历史上限的零派发取消、原结果过期，完整
  Initial持有不变；后继新请求可开始，存在Unconfirmed时仍不能Finish。
- 既有实际容量预算Host结清9.72秒及普通预算实际Scheduler/GC26.27秒定向通过；
  既有Scheduler完整容量恢复/首次placement与legacy隔离两项通过。
- 四包lint为0 issues、vet通过。准确源码Agent/Scheduler/Runner/Controller各
  Linux amd64/arm64共8个ELF核架构/VCS SHA/摘要，仅编译未执行。Go/模块与提交
  相同，保留所有者AGENTS.md导致vcs.modified=true；未构建镜像。

首次合并筛选未匹配owner子场景，该包“no tests”不计业务通过；随后按准确
Go子测试名补执行。旧普通Scheduler组合发现fake LIST只过滤label、未执行
spec.nodeName，错误返回waiting Pod；局部夹具补真实字段过滤，生产严格节点
校验不变。修复后内部业务断言通过，但25秒测试总期限抢先取消Session；仅该
含实际2秒ticker/2.2秒GC观察的场景使用既有DefaultOptions和45秒总期限，业务
deadline/单轮10秒未改。最终26.27秒通过，原失败日志保留且不记为通过。

已通过且未改的PG/Rust、预算模型全包和前批首次到Grow故障链不重复执行。
PG/root/资格/Kube/QMP物理效果仍显式夹具，未认证真实Linux或实际VM owner切换。

## 剩余代码与后续平台验收

仍须代码：初始部分效果取消/峰值结清/来源生命周期，缺原来源或root不一致的
受控恢复，安全降normal与后台/私有活性，真实接管/无Wait/Start/init/跨boot/
迁移恢复，全写者/CPU和可信请求/取消/重放入口及Monitor/自动策略。

Linux后验：平台资格真实数值/签发链、cgroup/HugeTLB/物理回收、NeonVM/KVM/
QEMU热插拔、K8s执行权/网络分区/持久化与Node对账、Neon存储负载与长稳SLO。
控制面移植后再统一产品E2E。默认关闭；不追新上游、不部署、不合main、不发布。
