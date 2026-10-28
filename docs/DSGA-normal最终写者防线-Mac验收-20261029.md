# normal最终写者防线与历史未知恢复

日期：2026-10-29，Mac Darwin arm64。准确业务代码`480c0259bddfad23ee0f7e55206888035c3f5454`，
沿既有自有功能分支。PG17未改，不重复未变模块矩阵。准确源码/逐用例/日志摘要见
[机器收据](DSGA-normal最终写者防线-Mac收据-20261029.json)。

## 修复的问题与实际边界

上层resourceexec已经拒绝运行期降低normal，但Root Guard原先仍可凭一次低
memory.current采样写小memory.max。生产Bootstrap后的运行命令现在在同一VM
文件锁内比较原Held/Bootstrap和当前实际memory.max，任何新WAL及内核写前
拒绝真正的normal降低；即使目标高于Held而低于外部调大的实际值，也不能写。
ErrUnsafeNormalDecrease明确此安全拒绝，不代表已实现安全降限或后台持续约束。

已确认原命令的相同重试继续只读。升级前持久未知降限命令保留原WAL和ErrUnknown，
禁止Replay再次写；原Observe仍可确认已经发生的实际效果，后续增加按确认后的
当前Held继续，不用旧Bootstrap抹掉历史。无Bootstrap的旧独立协议保持兼容，
不能因此获得生产出生权限。占用采样、普通SQL准入关闭都不是全部写者排空证明。

## 实际Mac验收

受影响resourceguard整包-race：47顶层/96含父子记录，0fail/skip，包7.867秒；
同包vet退出0，相对378c9102增量lint0 issues，diff通过。新增低占用/启动Held/
运行Held/外部实际漂移前拒绝，逐字节验证WAL不变且内核零写；旧pending降限
重放拒绝但只读观察/幂等及后继增加通过。两项既有来源/未知用例改用normal增加
制造部分写，保留全部断言；没有删恢复断言来绕过新防线。

文件锁/fsync及本地TLS/UDS是真实Mac操作，内核为明确夹具。未新增Linux构建或
执行，不能据此证明真实cgroup写入、全部写者持续上界、排空活性或物理回收。
特权外部写者仍须目标环境隔离和验收。安全降normal仍是代码工作；PG17核心
其他缺口继续完成，完整归档后PG18→PG16；Linux/K8s/NeonVM/HugeTLB/性能后验。
上游冻结、独立控制面和镜像/部署/main边界不变，Goal active。
