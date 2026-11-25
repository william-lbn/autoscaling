# Host 首次建账与部署版本推进

标准 Helm 站点使用显式 `initialize_mode: kubernetes-current-snapshot-v1`。它解决部署生成配置后 Node 心跳、标签或状态正常更新造成 resourceVersion 过期、首次建账永久不能完成的问题。

## 固定边界与单次快照

Node 名称及 UID、原 Lease 名称/命名空间/UID、内存 Capacity/Allocatable、Profile 哈希、系统保留预算仍是预先批准的固定边界。模式必须提供匹配的 KubernetesBudget，并与 `initialize_at_resource_version` 互斥。旧配置保持原冻结 RV 语义，不会被自动升级为新模式。

每次 Owner 协调先权威读取原账本。只有账本、Origin 和保护终结器全部不存在，才进入首次建账。随后读取当前原 Node，检查固定预算策略，再读取完整分页的已分配 Pod 集，按普通 Pod 硬限和受管 VM 的完整资源入账。该次读取的 Node RV 仅作为该次初始化 CAS 的条件；读后又有变化则本次失败，下一轮重新读取全部来源。

## 冲突、丢回复与重新启动

Store 的 Node UID/RV JSONPatch 测试和当前原 Lease 授权不变。写入未知时本轮不重放，下一轮先读取：若已提交，继续同一 Origin；若原对象仍完全未初始化，重新核验来源后只能由条件 CAS 产生一次有效建账。残缺、损坏、带保护标记但无账本的状态不允许重建。已有账本、容量持有和 VM 身份不因升级或超时清空。

部分安装修复必须另存升级意图及原收据，在原 Node/Lease/Origin 范围内观察，不能改写旧冻结输入或伪造成功。新镜像先在声明的仓库核实摘要，再通过原 Helm release 升级；最终还需全新清理重装及 PG17/PG18 业务验收。

## 验证边界

回归包含部署后 RV 推进、普通 Pod 全额入账、旧模式拒绝过期 RV、Node/Lease/容量漂移零写、未知写后保持 Origin、重启保持账本、CAS 冲突后重新读取新 Pod，以及配置互斥和预算缺失拒绝。单元测试不替代现场冷安装及真实物理内存验收。
