# 原 Owner 自动内存策略回放

从冻结的输入开始，用仓库中的原 `policy.Engine` 返回每次样本的候选、判定窗口和原因。

```sh
go run ./ci/dsga-policy-replay < owner-trace.json > replay.json
```

输入包含 `protocol_version` 1、`policy_revision`、完整的 `configuration` 和按求值时间排列的 `events`。配置使用 Owner 记录的自动策略格式，包含有资格的目标、阈值和时间参数。时间使用 RFC 3339。样本沿用 Owner 的 `decision.sample` 字段。

| `kind` | 时间和字段 |
|---|---|
| `evaluate` | `at` 对应实际求值时间，`sample` 为完整原始采样。采样时间保存在 `sample.at`。 |
| `accepted` | `at` 对应 Owner 确认原请求已被原日志接受或重放的时刻。 |
| `discarded` | `at` 对应原请求确认未被接受并清除局部待决意图的时刻。 |
| `reset` | `at` 对应采样中断或观察失败后的窗口重置。 |

选择目标后，后续样本需要由原确认事件解除待决。轨迹右端可以保留待决标记。需要完整求值轨迹复现驻留和冷却。已有的结构化决策日志只包含选中目标的样本，可用来核对对应事件的身份和结论。回放进程只读 stdin 并写本地结果，预算、资源和操作结算仍由运行中的 Owner 处理。
