# PG17 normal 首次拒绝的定向诊断

真实整链中首次 normal-apply 返回 409，后续 normal-observe 无法确认，尚未形成 normal-quota WAL。本改动为原 Root 失败路径补充固定数字阶段和 errno，帮助定位实际首次拒绝。

诊断格式仅为 DSGA_NORMAL_RPC_FAILURE stage=<数字> errno=<数字>，通过既有 Root 日志输出。不输出原错误正文、请求、命令或来源；返回原错误对象。原错误分支、判断顺序、短路行为、状态持有和恢复选择保持不变。逆向移除诊断标注后，consumer 与原版本字节一致。

定向验证运行 2b6227：单测、Root CLI、Guest 和 CLI help 各阶段均为零退出；新增测试 TestNormalQuotaFailureKeepsOriginalAndOnlyScalarFields 覆盖错误身份和固定标量输出。原失败和完整运行资料留在 /data/neon-build/validation。

本提交仅完善定位手段，不表示首次 409 已修复，不表示安全 normal 降限或 Linux/K8s 端到端通过。DEV Guest 观察器和其他诊断源不包含在本提交中。
