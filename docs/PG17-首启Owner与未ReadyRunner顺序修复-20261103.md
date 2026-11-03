# PG17 首启 Owner 与未 Ready Runner 的顺序修复

首启时，Runner 在 QEMU 启动前必须取得真实 Owner Lease 和 journal 接管证明；旧 Agent 又等待 VM.status.node、PodIP 和 Runner Ready 才开始选主，形成循环依赖。此修复只接通该顺序，不改变上层控制协议、PG 内存预算、Guest 启动政策或正常伸缩动作。

Agent 从原 VM watch 缓存挑选已经正式解除 hold 的受管 VM，按实际 Pod 的唯一 VM owner UID、namespace/name、spec.nodeName、status.PodIP 和首次 Pod UID 固定选择。未 Ready 状态仅进入现有 per-VM Runner；原 pods 映射和锁承担去重。真实 OwnerElector 创建 Lease 并通过原 journal CAS 接管，不手工创建 Lease，不手填 Owner。

同一个 Elector/Session 在 Guest 启动期间继续续租，只读等待 Controller 正式发布 Running/Node/IP。启动总等待为固定五分钟，单 API 读取仍受原请求上限约束；取消、对象身份替换及 Pod 终态严格结束。之后沿原凭据和资源恢复入口继续。正式 Ready 事件只更新原 Runner，正常 Running 交回原 watch，不继续每秒 GET 全部运行中 Pod。

Runner 只在 initial 模式、写本次 process-launch 之前等待 Owner，固定绝对期限三十秒。等待同时要求本地与远端原历史 pristine、无 launch/exit、原 VM/Pod/Node/Birth/StateClaim 完整。只有首次 Owner Lease 404，或合法原 Lease 已创建而 journal 仍是原 bootstrap owner 的窄窗口可以等待；403、非法 Lease、已选 UID 替换/消失、既有真实 Owner 缺 Lease、终态和不完整来源立即拒绝。收敛后仍调用原完整 authorize，普通动作从未获得这种重试。

等待失败仍须保存本次明确未调用Start的真实关闭事实：以独立有界上下文重新核原local pristine、无既有launch/exit，再写本次唯一canonical launch并走原NoStart v2持久化/发布。任何旧launch、退出或冲突都不覆盖，也不以新事实结清未知旧执行；成功路径仍只允许一次Start。

以下是可复跑的定向单元/顺序模型，使用生产 Agent Runner、OwnerElector、client-go 和原校验器，API 由明确的 UID/RV 测试 tracker 提供；它们不代表真实 Kubernetes、Guest 或平台内存资格通过。

```sh
go test -race -count=1 ./pkg/agent -run '^TestDSGAInitial'
go test -race -count=1 ./pkg/dsga/vmguard -run '^TestInitialOwnerWait'
```

Agent 顺序用例使用本轮 CPU 2、内存 7/8Gi 的 Spec，以及最初 Pending、status.node/IP/CPU/memory 实际值均未发布的形状：先真实选主和 journal 接管，再并发处理 Ready 与 ensure，核同一 Runner/Owner 和正常态不再额外 GET Pod。TLS 正控保持独立 CA 与非 CA 的平台 wildcard 叶证书。Runner 用例覆盖 404 到原 Owner 收敛、403、非法来源、UID 替换、终态、已有 launch/退出、非 bootstrap Owner 下的 404 和取消。

构建/测试基准为不可变 Git eb9bf8cb1f36facdc8c1f1301bcb920bb18f36d2 加本组明确文件；并行模块的未提交改动不进入快照。各次失败原票保留在 Linux /data/neon-build/validation，不能把编译成功、单元模型或一次阶段通过写作 PG17 全链验收。
