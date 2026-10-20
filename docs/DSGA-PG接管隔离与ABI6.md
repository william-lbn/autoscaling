# PG接管隔离与ABI6消费者

日期：2026-10-21。此模块是V3跨owner恢复的一部分，整体Goal仍active，默认关闭。
对应Neon PG17提交`6b7c3655ab74062e117eef7400953cd000679a49`、Neon扩展1.8、
Buffer ABI6；旧ABI5/扩展1.7客户端不能混用。上游main重新fetch仍为
`a4bce09e4d6bd7932ea634a1ed79afe26a9cfd1f`，已完整包含，无须空合并。

## 契约与不变量

正式第六路由为`POST /dsg/v1/issuer/fence`，请求只含protocol_version=1、完整
instance和十进制字符串issuer_epoch。必须等于本客户端已认证配置的epoch；Compute
另核验签名JWT中的当前epoch及VM/Compute绑定。调用者自报的更大数值不是租约认证。

PG在同一boot内维护独立单调issuer_floor_epoch。接管与新操作登记在同一状态锁下
比较/推进floor，封闭“旧请求先过检查、新owner接管、旧请求才登记”的窗口。普通
grow/shrink也提升floor；低epoch精确已保留结果仍可历史重放，未知旧请求拒绝执行。
floor不会因SQL回滚、扩展降级、取消或历史淘汰下降；只有实际新PG boot重置。

回执包含requested_instance、requested_epoch、disposition和独立observed：

| disposition | 可采用的事实 | 禁止推导 |
|---|---|---|
| fenced | 同一boot/floor，PG resize串行执行者及同步Provider已退出 | 物理释放成功、业务完成、VM许可、原预算可删除 |
| pending | floor已经建立，旧执行仍在途；可能仍为COMMITTED且Provider未返回 | 接管完成或旧动作无副作用 |
| stale | 当前boot/实例不符，或已有更高floor | 可改绑、可回退epoch、可重建空状态 |

PG已提交不代表Provider执行者退出。Fenced也不排空普通SQL、不认证Host/Runner、
不解除VM未知QMP或预算承诺；vm_shrink_permit始终false。

Go `Client.Fence`使用独立observer HTTP连接池，旧执行池阻塞时仍可访问。发送后
超时/错误/无法验证事实按MayHaveExecuted保留不确定性，不能假定floor未推进。
该方法成功也不清客户端保存的原resize意图。外层持久日志、预算及旧副作用仍须
对账；禁止靠Fence创建新空Client绕过未知请求。完整boot绑定继续严格，未实现的
显式受控重绑不会自动触发。

Snapshot强制包含issuer_floor_epoch；操作/释放epoch不得超过floor。Go/Rust共享
响应向量逐字节一致。严格必填/未知字段/重复键/null/uint64校验继续沿用，不能
在缺字段时补零；本批未改变请求协议版本和完整意图摘要算法。

## Mac实际验证

- 完整受影响race组合：185个case pass事件（含父/子与fuzz种子），0fail/用例skip；
  10个有测试包通过，9个无测试文件包单列。包括DSGA/API/Agent/Controller/VM API/
  Scheduler plugin；不是185个彼此独立顶层场景。
- 新用例验证成功Fenced仍保留原未知意图、错误epoch本地拒绝且不发HTTP、执行池
  阻塞时observer仍可取得Pending。事实层拒绝伪造Fenced、floor不符及缺字段。
- 本地golangci-lint2.4.0完整受影响组合与`dsga_native_e2e`标签检查均0issues。
- 真实Go→TLS→生产Compute Router/JWT→双SQL→准确PG6b7/完整Neon1.8：1个Rust
  外层、2次独立Go race进程通过，零skip/ignored/filtered；候选联调4.19s。新流程
  先Fence1，实际grow/shrink/regrow及丢回复恢复后，用真实epoch2签名Fence2并核对
  floor/容量和旧Client pending不变。PG实际重启后仍拒绝静默重绑。

日志在工作区build：autoscaling-issuer-full.{jsonl,stderr}、
autoscaling-issuer-lint.log、neon-issuer-exact-go-native.log。带标签lint另见
autoscaling-issuer-tag-lint.log；依赖提交后再次从准确SHA执行真实门禁，记录另附。
常规race命令沿用`DSGA-执行状态与恢复.md`的全组合；真实门禁由Neon
`ci/test-dsg-go-client.sh PG_INSTALL_DIR AUTOSCALING_SOURCE_DIR`驱动。
Mac使用Go1.25.13 darwin/arm64、Xcode14.4 SDK/clang与CGO sysroot，没有修改全局工具链。

## 剩余业务与验收边界

可信唯一owner/租约分配、持久协调循环、Monitor、显式Compute重绑、完整硬预算/
quiesce、Scheduler/Controller/最终Runner私有QMP隔离及其余PG/Driver/Helm、真实
Neon存储E2E仍需继续编码和集成，不能归入Linux待测。PG floor只是必要的一层。
Linux/K8s/KVM/HugeTLB/真实物理回收/迁移/性能认证后验。本轮不构建镜像、不准备
部署、不合并主分支、不发布生产。
