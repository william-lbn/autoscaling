# 自动缩VM从Prepare至正常结案的Mac全链组合

日期：2026-10-29。测试准确提交`72d8a4208267d935b29c5ea0a2e7d1aeb237bffe`；正式源码与前阶段
`ef3b331309de703c62a273ba8e64d5a060972340`相同。本批只改两个测试文件，未增加生产代码，
未重复已通过的策略/Agent/Journal或PG/Rust矩阵。逐记录、源文件和日志摘要见
[机器收据](DSGA-自动缩VM全链-Mac收据-20261029.json)。

## 本次实际闭合范围

前阶段只验证自动Shrink至Prepare，后续仍Planned。本组合增加明确中间平台资格，
首次自动Grow到2GiB；生产Finish与Host结清后重建策略，重新累计无公共客户端低压
窗口，自动Begin目标1GiB。两规格normal硬上界都为512MiB，不降低normal，不按
当前占用补资格，也不把协议peer数字当目标Linux资格认证。

原Prepare为正式生产者生成，不注入完成。继续正式Host持有、Quiesce关门/排空、
ResizePG/ReleasePG、GuestReady、ResizeVM、Runner原QMP及实际读回、ObserveVM、
FinalizeBudget、ResumeAdmission，最后由生产执行器正常Finish并由Publisher结清
Host。PG协议peer在SDK重建后继续同一实例、原WAL和完整请求；无手工Store.Finish。

核原ObserveVM的ActualMemoryBytes和QMPObservedMemoryBytes均1GiB，最终永久预算
和成功历史固定同一Shrink请求；两个历史均Succeeded、Active为空、准入已开放。
QMP整组仅扩/缩各一次；Shrink PG Resize一次，关闭与重开各一次。后台/DSM/2PC
在Quiesce peer中保留非零，由原聚合硬额度覆盖，不伪造全部后台为空。

## 实际验证与局限

最终Mac -race：1顶层/5含父子记录，0fail/skip，组合120.28秒。owner vet、
相对f631f2c3增量lint、diff检查通过。首轮测试引用错误字段导致编译失败，修正后
第二轮119.86秒通过；因接近原两分钟测试上限，仅为本新组合增加三分钟总测试
余量和阶段日志，再采用本收据的最终结果，不改生产请求期限、轮询或租约。
不累计重复组合，不把5条父子记录当5个独立E2E。

```sh
go test -race -count=1 ./pkg/dsga/owner -run '^TestProductionAutomaticMemoryPolicyUsesOriginalOwnerAndBudgetChain$'
go vet ./pkg/dsga/owner
```

本批只有测试代码，正式源码与ef3b3313相同，已核Git差异；Agent双Linux架构交叉
编译证据复用前阶段，不重复编译或运行。真实Linux PG/Root、K8s API/RBAC、QEMU
热插拔、HugeTLB、物理内存回收、长稳和性能未在Mac认证。PG/Root/Kube/QMP及
Provider释放事实仍是显式peer；本次全链只指该Mac业务组合。

## 仍须推进

- 安全降低normal、私有/后台持续上界和活性；本例保持normal不变不能代替它。
- Root/资格/日志异常及约束越界受控关闭；采样拒绝仅冻结新建议。
- 分区旧进程、出生不完整、Runner无Wait、外层跨boot/迁移。
- Monitor缓存/WSS/组配额/RW-RO完整策略、全部最终写者、可信CPU。
- PG17核心完成归档后，PG18→PG16各自内核/扩展/发行及版本专属验证。
- Linux/K8s/NeonVM/HugeTLB/物理回收/性能，独立控制面后续统一E2E。

上游PG17冻结、实际HEAD/gitlink/两份锁b078736a保持；原自有功能分支继续演进，
所有者AS AGENTS修改保留不stage。原Goal active、heartbeat沿用，不创建子代理/
重复自动任务。默认关闭，不构建镜像/部署/合main或发布生产，整体仍未完成。
