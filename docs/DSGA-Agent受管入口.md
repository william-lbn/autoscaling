# DSGA Agent受管入口

2026-10-21。`Runner.Run`在创建任何旧monitor/executor前GET固定VM UID。
只有明确读到同一未受管VM才走官方legacy流程；GET失败、同名重建、删除、损坏/
缺半边标记均失败关闭。受管VM缺可信DSGA配置或Agent Pod UID也不回落旧流程。

配置`dsga`默认为nil。显式启用时需要`gatewayURL`、`managementCAFile`、
`projectedTokenFile`，`requestTimeoutSeconds`为1..30，
`computeTimeoutSeconds`为90..120，`pollIntervalSeconds`为1..60。
专用投影令牌audience为`neon-dsga-credential-broker`；`K8S_POD_UID`来自
Downward API，不能用Pod名/IP替代。这里不提供/执行部署，平台接线后续另验。

入口核对原VM的Node/Runner Pod/IP，使用完整journal Store、真实Lease Elector及
Pod绑定holder；取得短凭据后创建同一owner生命周期内的长期PGWorker。已有意图
恢复Pending，未知POST只观察，只有已认证的显式选择可以重放原意图。

状态提供phase/epoch/实例及owner协调心跳；不把心跳当PG、预算、VM成功或物理
回收证据。新入口没有旧executor时State仍能安全读取。异常退出记录errored并按
现有退避重启；取消不释放仍在运行的owner worker。锁只保护本地状态，不跨网络。

当前循环只执行journal中已由外层确认到达的PG步骤；其他领域保持ExternalStep，
没有自动Begin/预算伪造/VM资源PATCH替代。完整规划器、Monitor、全预算与最终
Runner仍须继续接入。该入口尚不能宣称完整伸缩或生产验收通过。

Mac受影响全量race303事件、13测试包通过/9无测试包，0用例fail/skip，lint0问题，
原生PG链路9.24秒通过。详见[凭据及首次发现验证](DSGA-专用凭据客户端与首次发现.md)。
新增测试覆盖缺配置/标记损坏/孤立journal不启动legacy、UID变化/API失败不能
收养新VM、无旧executor状态安全、owner协调心跳新鲜/过期；不冒充真实K8s验收。
