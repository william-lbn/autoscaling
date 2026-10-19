# DSGA：上游基线与开发约束

日期：2026-10-19。功能分支：`codex/dsga-autoscaling-20261019`。

本分支直接从刚重新 fetch 的自有 `origin/main` 建立：
`c0052f5f2d38fce6c70e448f3f1ee2ee239a0a93`。官方 `upstream/main` 为
`a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f`；自有分支多 3 个 CI 提交、落后 0。
官方 migration state machine 修复及更早的全部 main 提交均已包含，保留自有发行 CI。

## 后续功能修改范围

- Agent 沿用现有 Revision，增加 PG boot/操作身份、过期许可拒绝与重连对账。
- 扩容先确认 VM/Guest 实际增加，再请求 PG 扩容；缩容先取得当前操作的内核排水/
  回收结果，再调整 VM，最后按实际完成结果释放资源承诺。
- Monitor 的普通 cgroup/LFC 成功回执不能冒充新 DSGA 物理回收证明。
- VM 当前采用 virtio-mem，需验证实际设备状态；HugeTLB 归还 Guest 池不等于
  Host 已回收。迁移与资源变更必须互斥并验证身份。

本提交仅建立可审查的基线和范围，尚未修改 Agent/NeonVM 业务逻辑，不能认为
端到端动态 Buffer 功能已实现。

## 质量和合并规则

始终在自有 fork 的新分支开发；每轮先同步全部相关官方修复，冲突单独记录。
小批修改、关键逻辑详细中文注释、Mac 可执行业务逻辑验证后模块化提交。
远程功能分支用于后续 CI/环境/性能测试；所有必要测试通过且项目所有者确认前，
不合并自有 main，不发布生产。Linux/KVM/HugeTLB 真实环境验收另行安排。
