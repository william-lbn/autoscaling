# Work2启动包前覆盖的消费者

沿既有 `codex/dsga-autoscaling-20261019` 演进。Work2明确包含Postmaster fork前
登记至PGPROC交接，旧Work1无法证明这个范围，因此严格拒绝Work1及未知版本。
Buffer ABI6、Work字段布局和写接口不变；不得仅凭foreground_idle发Quiesce/VM许可。

Mac两项受影响Go协议/SDK race通过，新增旧版本拒绝及重复键校验；vet通过。
准确PG17 `9db3b91d3caff9f674786252f0104de75ed8e709` 及配套真实Neon C/Rust
扩展下，内层Go race→本机TLS→生产Router/JWT→SQL→PG门禁通过。Rust夹具
真正完成GSS请求并收到N，故意不发startup包；Go读到Work2及恰好一个startup。
Native快照未变、Pending保持、零写POST。日志在工作区build目录
`dsga-prestart-go-work2.jsonl`、`dsga-prestart-go-http-native.log`、
`dsga-prestart-go-vet.log`，不重复旧全套。

TLS是本机终结，不模拟PGbody；不等于Linux/K8s端到端。仍缺禁止后续新fork的
持续门禁、后台/DSM全排空与可信预算/Host/最终VM/公开worker及受控恢复代码。
本轮不构建镜像、不准备或执行部署、不合主分支，保留所有者AGENTS修改。
