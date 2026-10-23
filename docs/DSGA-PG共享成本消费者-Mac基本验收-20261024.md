# PG固定主段与数组成本消费者：Mac基本验收

本批Go协议和Client.SharedCost消费Neon1.11的GET /dsg/v1/budget/shared；对应原生PG17共享成本API1，BufferABI6不变。startup_shared_upper_bytes已经包括bootstrap四数组，运行期成本须采用固定主段+当前登记数组上界，不能直接把启动总量再加当前数组。值来自同一PG快照，64位规范字符串、严格JSON、完整boot绑定及checked加法；失败/未发布/未知/未物理回收占用继续由PG上界保留。

此范围只包含固定PG主段及DSG数组，DSM/私有/LFC/OS/Guest池另计。成功读回不清任何pending，不发VM许可，也不是完整预算约束已生效证明。HTTPS/独立观察连接、no-store、不重定向及有界JSON沿用既有消费者。

Mac新增协议去重/溢出/严格解码及TLS消费者保留未知两项race通过，0fail/skip；受影响包vet通过。Neon侧原生SQL两项、Rust契约一项和生产Router→准确PG/Neon1.11一项另有基本验收。本仓TLS服务是明确的协议夹具，不是实际K8s或PG。日志工作区build/dsga-shared-cost-go-mac。

复现：go test -race -count=1 ./pkg/api/dsga ./pkg/dsga/compute -run '^TestSharedCost'。Linux K8s/RBAC/证书/NeonVM/HugeTLB/物理/性能后验；完整预算生产者、启动准入及最终VM链继续实现。仅PG17，不构建镜像、不准备部署、不合并main。
