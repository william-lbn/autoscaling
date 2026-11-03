# Scheduler 完整 Pod 硬预算预检

本轮真实 Compute Pod 有 11Gi 内存硬限，而 VM 内存为 7–8Gi。此前 Filter 只做本地模型筛选，直到 Reserve 才按完整硬限拒绝容量不足的节点，导致本次 Pod 多轮重试。现在在 Filter 的本地互斥锁之前重读原 Pod、VM、Node、Lease 和完整账本，使用与 Reserve 相同的模型进行只读试算。

预检不保护 Pod、不抬高水位、不写 Node/Lease/ConfigMap，不修改评分或强制指定节点。Reserve 仍独立重读并执行最终 UID/RV CAS；未知来源、未知回复和已有持有继续保留原拒绝语义。VM 从 8Gi 降到 7Gi 后，存活 Pod 的 11Gi 硬预算继续保留，只有正式退出才释放。

定向验证：7 个测试根、30 个 PASS 事件，race/skip/OOM 均为 0；实际耗时 81.504 秒，原 unit 自然退出 0。包括真实 Framework.Filter/Reserve、三节点容量样本、来源变化、旧 Node RV、完整 11Gi 水位及 Unknown 负例。测试中的 HTTP/节点预算属于组件 fixture，不等同于真实集群资格。

证据目录：/data/neon-build/validation/scheduler-host-filter-focused-20261103T192037Z-790e0e。源码快照实际校验 11673 个文件；独审见同目录 independent-host-filter-review-P2.json。正式 Scheduler ELF 为 4a63df4d3d4152b8c3c8e5dfb6308c27c52f3f744d52d891df44787a03d9579a。

尚未验收：本修复镜像的实际部署与候选筛选；PG17 Guest 初始化、SQL、资格以及 Grow/安全 normal Shrink/Regrow/自动策略真实端到端。没有降低安全门、预算或用组件结果替代端到端。
