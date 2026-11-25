# Neon 虚拟化与资源自动伸缩

本维护分支提供 NeonVM、Scheduler、节点 Agent、资源安全协议和资格工具，服务于通用内网 Helm 产品。它与 Neon 托管服务的上游开发环境不同。完整系统部署统一使用 [Neon Helm 标准入口](https://github.com/william-lbn/neon-helm/blob/codex/dsga-neon-helm-v3-20261019/README.md)，不直接用本仓库的开发 YAML 或旧 `make deploy` 组装产品。

## 组件职责

| 组件 | 职责 |
| --- | --- |
| NeonVM Controller | 管理 VM、Runner 生命周期及经协议允许的资源调整 |
| Runner | 在 Kubernetes Pod 中承载 QEMU 与 Guest |
| 设备插件 | 向 Kubernetes 声明实际 KVM 设备 |
| Scheduler | 根据原 Node/Lease 身份及 Host 账本管理可调度容量 |
| Agent | 读取当前实例与业务信号，协调自动伸缩和安全状态 |
| Root 与资源协议 | 验证原实例、Owner、预算和实际资源变化，未知状态关闭准入 |
| Qualifier | 基于真实原始观察发布逐实例资格，不能用配置存在代替 |

详细边界见[当前架构](ARCHITECTURE.md)。DSGA 实现位于 `pkg/dsga/`；[正常内存配额说明](pkg/dsga/resourceguard/normal_quota_budget.zh.md)描述 Guest 内存边界。

## 部署和验证边界

节点地址、名称、UID 和 Lease 不进入复用发行模板。安装入口按显式标签筛选当时合格的 Linux/amd64 KVM 节点，再从当前集群读取身份与容量。现有 5 个 worker 已完成 PG17/PG18 自动内存、公开业务生命周期验收；1..16 Host 是当前合同上限，不代表各种规模与硬件均已实测。

可信 Host 集合与普通放置筛选分开：开放放置条件不授予全局 Node 写权限。Agent 不得获得 Node PATCH；Scheduler 只对本次可信 Node 名称拥有所需权限。新增 Node 连续接入暂缓，不能把 Kubernetes Node Ready 直接当成完整 Host 与资格注册。

内存验收必须观察同一 Compute 的 Guest 物理增长与回缩、原数据、WAL 结清。改 VM spec、Runner 资源或 cgroup 数字都不能代替原生证明。Root 读取未确认时关闭准入仍正确；安全恢复必须验证原关闭原因和原预算，不能健康读一次就自动开放。

## 开发与维护

构建、原生 SDK 检查和必要镜像打包在 Linux 构建机执行。依赖使用已提交的公开维护 SHA，不保留本地路径替换。运行与改动相关的 Go 测试、Chart 合同及真实集成；私有 fixture 未提供的跳过必须单独记录。

维护分支为 `codex/dsga-autoscaling-20261019`。提交和关键解释使用中文，保留原作者源码注释、许可证及机器协议名称。上游原 README 的完整历史见 [a5e925b 快照](https://github.com/william-lbn/autoscaling/blob/a5e925b5f9180e083e9c86dd7be4ee3f55b014cb/README.md)；该快照是参考资料，不是当前产品安装入口。

Host 部署初始化、固定身份与未知写恢复规则见 [Host 首次建账](docs/Host首次建账.md)。
