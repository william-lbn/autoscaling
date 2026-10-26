# 原VM明确未发送后的显式重放：Mac验收

沿自有`codex/dsga-autoscaling-20261019`，在29edec63基础上完成生产Session/
Agent循环→同VM持久选择→Runner私有QMP的原命令重放传输。代码提交
`9116929577cf0d8aa2fcabd8f563d7267711012e`；详细源码、用例和日志哈希见
[机器收据](DSGA-原VM显式重放-Mac收据-20261027.json)。上游继续冻结于
2026-10-26 17:29:30；PG17仍b078，独立控制面仍d84，本轮未改它们。

## 选择、执行和恢复

实际`PGWorker.ReplayVMOriginal`先检查当前Session，经`vmexec`读取原typed
ResizeVM与真实原not_sent回执；无pending、同owner/epoch/PG/Guest/原before
成立后，重新核认证PG、Root、Host、原Prepare和Shrink的持续门禁/Guest事实。
原期限不可延长，原命令、Key、步骤和完整摘要不可修改。

Agent只在同一VM以UID/RV条件写入`dsga-vm-original-replay-v1`注解：schema1、
原Key/owner、Birth/State/原not_sent Receipt真实UID、原双摘要和新128位nonce。
整个注解含外层空白限2048字节，拒绝重复键、未知字段和零nonce。复用既有VM
写权限，不新增公开QMP入口，不授予Agent预算CM的创建/写入权限。

重复显式调用先复用已提交的同一选择；完全相同选择只GET确认同RV，不再PATCH。
写回复丢失仍返回UnconfirmedWrite，保留服务器可能已经保存的原选择。普通
轮询只对账，绝不因超时/错误/not_sent生成或投递新的选择。

Runner自行拉取同VM选择，在原文件锁内同时核当前floor、原完整命令、本地
LastPublished回执UID和新nonce。新not_sent尚未发布、原UID不同、原before
漂移、来源失联或选择被回灌都不发新写；不能只用“nonce与上次不同”循环重试。
授权再次核验后，仍先持久prepared与may_have_sent，再只尝试一次私有QMP。
可能已发送继续只读原目标；取消/接管不能把旧命令换成新epoch重新执行。

`PGLoopOptions.VMReplay`与PG/Root/准入的显式选择分开，关闭通道后置nil，
不忙轮询。实际Session/循环入口已经接生产执行器；默认通道nil，公开策略/
操作协调入口的授权与接线仍属于后续核心生命周期，不能说本模块已经完成该业务。

## 兼容与持有

选择是有界提示，最终许可仍来自原执行域核验；不改变Model v4、Runner持久
record v1、PG ABI或当前预算结构。实际已发布29源码Runner收到合法新提示后
仍保原not_sent并零内存写，未把新提示当旧自动重放许可。启用前必须先升级
实际Runner；旧读者兼容不代表其具备新重放功能。

成功后最终VM/预算绑定新的完成回执，原命令/Key和其他原来源保持。Host持续
持有max(before,target)+overhead；本模块不释放预算、不清原未知、不实现
取消结清、跨boot收养或旧领域对象回收。独立控制面的Model v4读者移植要求
继续保留，本轮不修改其SDK/分支，不用新核心强行兼容旧读者。

## 实际Mac验证

Mac arm64、Go1.25.13、race、readonly缓存依赖。按完整测试名去重**8顶层/
33含子项最终通过，0失败、0跳过**；受影响项复核替换旧项，不累计。

| 范围 | 顶层/含子项 | 实际断言 |
|---|---:|---|
| VM条件选择、严格wire | 2/10 | 真实HTTP/UID-RV CAS，丢回复原选择保留；错误owner/Key/摘要/取消/已结清拒绝；原WAL不改、重复选择不改RV；注解整体有界 |
| Runner原重放状态机 | 2/13 | 完整同命令，原回执UID、新nonce、已发布及before门槛；再一次not_sent后旧选择不能循环消费 |
| 实际Agent循环扩容 | 1/6 | 正常与选择丢回复闭合到最终准入；Host在选择前后丢失阻断；可能已发送拒绝；原命令/nonce保持，成功内存写一次 |
| 实际缩容重放 | 1/2 | 原PG释放/Guest/门禁复核后重放，原Observe/最终预算/准入闭合，内存写一次 |
| 关闭VM选择通道 | 1/1 | 关闭后不忙循环 |
| 实际旧29 Runner | 1/1 | 忽略新提示，原not_sent不变、零内存写 |

初轮模型包有1个严格wire失败：json.Unmarshal在调用自定义解码前裁外层空白，
已用固定原始注解解析器补边界，仅替换该项。首轮组合夹具错误假设显式通道
总先于就绪ticker；改为仅投递一次且容许继续等待后通过，未放宽生产状态机。
初轮lint的predeclared变量已修。原失败日志保留；同原选择只读幂等化后，仅
替换1个CAS子场景及3个受影响组合子场景；其他已通过用例复用原准确记录。

最终增量golangci-lint 2.4.0使用仓库规则、Linux目标，0 issues，未禁规则；
4包Mac readonly vet通过。准确源码Agent/Runner各Linux amd64/arm64编译成功
（buildvcs=false），四产物哈希固定，未执行ELF。早先四份编译产物另保留，
未用它们冒充幂等化后准确源码。未重复未变PG、Provider、HTTP或core225测试。

运行命令与本地完整日志路径在机器收据和`build/run-dsga-vm-replay-20261027.sh`、
`build/run-dsga-vm-replay-quality-20261027.sh`中；最后准确构建脚本另列收据。
初轮模型包的整体退出1如实保留，最终按全名覆盖该失败项，不能说初轮整包绿色。

## 证明边界与剩余核心

组合运行实际Session、Agent、同VM条件日志、Runner、文件锁/fsync、Unix QMP
协议与最终预算/准入。**Kube API/Core、QEMU/KVM、Prepare/Host及PG/Root
内核事实为明确协议夹具**；缩容原PG释放/排空前驱也是夹具。单元状态机的
回执发布另有明确夹具，不以其代替真实Kube原回执验证。

仍须核心：可信完整Prepare与剩余系统/过渡/安全资格，真实Host/Scheduler
持有及显式释放，安全normal下降及持续私有/后台约束，首次预算/开放、实际
策略/Monitor/Agent Begin/公开生命周期接线，取消未知结清和受控跨boot。
整体未封版，Goal继续active；不能将上述代码缺口归为Linux待测试。

Linux后验独立：实际apiserver CAS/RBAC/领域来源保护，真实QEMU/KVM/私有
socket权限与异步/部分热拔，内核普通页/HugeTLB/真实物理回收与Host/迁移互斥，
性能和长稳。其他PG暂缓；不构建镜像、不准备或执行部署、不合主分支。
