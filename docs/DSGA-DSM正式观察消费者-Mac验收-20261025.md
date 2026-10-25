# DSM1 正式观察消费者与 Mac 验证

沿既有 `codex/dsga-autoscaling-20261019` 增量实现；本批 fresh fetch 官方main
`a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f` 已完整包含。PG17依赖原生
`5462f1de54a97b9a9235c69d64d31cb0825dcea5`，Neon正式SQL升级至1.15。

新增 `DSMResponse` 和 `Client.DSMState`，只走固定认证GET `/dsg/v1/work/dsm`。
所有字段强制存在、未知/重复/null拒绝；字节量使用规范Uint64字符串。逐项验证
Native DSM1、current_boot_only、本实例/boot同点Prepared1/Connection1/Work2、
实际页幂/对齐/最小占用、pending与unknown互斥子集、固定控制成本、GUC的KB范围、
已执行正数额度与当前保留量。ledger无效拒绝消费，不能据失败清原预算。

默认0明确未限制，允许如实观察，不把它补成有限额度。UNKNOWN仍属于原保留量，
foregound_idle可与非零DSM/2PC并存；不能丢字段制造排空。main-region DSM已在main
成本中，控制段单独，不能重复计账。这是本boot普通页payload，不是PTE/THP/Host、
跨boot清理、完整Quiesce或VM许可。独立读不清Pending，不重放resize/准入。

Mac Go1.25.13：新协议1与SDK/TLS1两项race通过，相关两包vet通过。实际带tag的
race二进制 `build/dsga-dsm-formal-native-go.test` 构建后，Rust严格门禁创建独占
准确PG/Neon1.15/生产Router及一次性JWT，由匿名stdin传入原请求与token；Go经
真实本机TLS反代、HTTP、正式SQL读到实际2MiB固定DSM及2PC槽1，预算4MiB、
同点Memory不变，原Pending保留，零POST；内层TestNativeDSMState与外层Rust各1
通过，0skip/ignored。上游blob/S3或用户集群未参与；不是response body mock。

二进制在commit前构建，收据绑定实际源哈希/产物哈希，不宣称其VCS标签为发布提交。
不重复已通过Native DSM8/core225/相关隔离5或未改Agent等包全套。日志：工作区
`build/dsga-dsm-formal-go.jsonl`、`...-go-vet.log`、`...-native-go-build.log`、
`...-native-go-http.log`。原生/Neon的失败夹具和替换事实另见Neon中文验收。

尚需完整预算生产者、后台/私有约束与最终Host/VM闭环代码；不能把观察合同计作
整体完成。Linux/K8s/NeonVM/HugeTLB物理/性能另验。默认关闭全部VM许可，不构建
镜像、不准备/执行部署、不合main，沿既有各服务分支继续演进。
