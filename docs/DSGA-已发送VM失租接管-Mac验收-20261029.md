# 已发送VM失租接管与实际容量恢复

日期：2026-10-29，Mac Darwin arm64。准确测试源码`a6a2e7616c07f82a33d30fcc11d0722784d1b577`，
沿原`codex/dsga-autoscaling-20261019`。本批仅测试及夹具接线，生产协议、PG17、
Rust不变；整体PG17核心仍未完成。前一未发送窗口收据保留原源码，不改写历史。

## 新关闭的窗口

首次服务实际开放并完成原ResizeVM写前日志后，正式Runner跨过may_have_sent
屏障，发送一次QMP；peer实际变为1GiB，但丢回复，外层仍Unconfirmed。指定旧
holder续约失败，等待生产30/20/2秒Lease失租，再由同Lease UID的第二个正式
Elector接管，接管CAS落账丢回复后读回。原Active、Initial及持有完整跨代保存。

新Session只读回原Runner结果，落原动作Completed；不重发QMP、不制造NoEffect。
取消使用原Prepare的Outside/资格生成capacity v1实际预算，不能用旧512MiB
Initial（v2）覆盖已到1GiB的VM。外部CM的UID由真实budgetclaims.Reader认证，
先证实原来源可读，再拒绝同名换UID；静态Model不承担网络事实认证。

原Cleanup Start/Record丢回复、资格暂缺/释放拒绝与SDK重建后，按实际1GiB结清
永久预算及Host。后继同容量Grow不再写QMP，只扩PG，再完成下一次Shrink Prepare。
组合共QMP写1次、PG resize1次、准入2次；新owner不能重放旧epoch命令。
原未发送窗口仍保持512MiB/NoEffect语义，本批未重复其175秒组合。

## 实际验证及未通过记录

最终Mac -race新增组合113.40秒、owner包116.312秒，1顶层/1记录，0fail/skip；
owner包go vet退出0，相对5c8a1bd1增量lint 0 issues，diff检查通过。
最终JSON、源码与日志摘要、复现命令见[机器收据](DSGA-已发送VM失租接管-Mac收据-20261029.json)。
首轮45.29秒失败保留：测试错误地要求纯Model认证外部CM UID，修为Reader认证；
没有放宽生产来源检查，也不把首轮失败计入最终成功。

PG/root/Kube/资格/QMP仍为明确协议或领域夹具；正式Go选主、SDK、WAL及条件
持久化实际运行。业务顺序调用使用注册回调返回的Session，不是假称全进程产品
E2E。没有新增Linux执行/交叉编译或PG/Rust矩阵；未变证据按准确源码范围复用。

## 剩余工作

该窗口完成不能代表PG/Guest已派发、分区旧进程、早期init/无Wait、跨boot/迁移
等所有恢复闭合。安全降normal、持续私有/后台上界、可信生产入口与自动策略等
仍须代码及组合，不是仅待Linux。PG17完整归档后PG18→PG16，逐版适配和验证。
Linux/K8s/KVM/NeonVM/HugeTLB/物理回收及性能后验；功能默认关闭。维持上游
冻结，不改独立控制面，不构建镜像、不部署、不合main、不发布。Goal保持active。
