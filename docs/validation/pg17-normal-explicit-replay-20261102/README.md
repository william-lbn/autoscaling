# normal 显式恢复补齐首次交付

控制器落账原请求后若 Root 从未收到信封，旧 Replay 会直接拒绝，无法交付这项原请求。本次只在显式 Replay 的同一 Root 原锁内选择首次交付：没有本命令信封和配额前缀，且原身份、同 epoch、同 Postmaster 出生、连续 revision、无另一未确认命令均满足。已有逐字匹配信封的 entry-only、partial 和 complete 保持原重放路径。

完整 PG、数组、预算授权和信封持久化仍先于任何硬限效应。普通 Advance 只 Observe；普通 Apply 和独立 Recover 保持原语义。不增加公共接口，不根据 HTTP 409 猜测未开始。

5c8662 定向 race 验证：2 根、18 子例通过，零失败、零跳过，46.214 秒。测试覆盖 Root 私有选择器及原 handoff 的模型内核，不代表真实 HTTP、PG、cgroup、normal 降限或端到端通过。大日志与私有输入留在 /data/neon-build/validation。
