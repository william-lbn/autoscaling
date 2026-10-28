# CPU完成读回与取消屏障后的PG再出生

日期：2026-10-29，Mac Darwin arm64。CPU准确源码
`5181b364958928316385f0675529a1894c5d8d69`；取消后出生修复准确源码
`a009521f1d84af3920aebc8d21e652bcf14650a8`。沿自有原功能分支，不追新、不改
独立控制面。5文件源码摘要、逐测试及原失败/最终采用和6个交叉ELF摘要见
[机器收据](DSGA-CPU结果与取消后出生-Mac收据-20261029.json)。

## CPU原语的真实错误

原CPUScaler读取OfflineCPUs失败却返回nil，daemon可能回200且Runner记录目标，
实际未执行扩容。现保留原读取错误；sysfs扩缩写后再读取在线数量，读回失败或
不匹配均保持未确认。不自动回滚部分成功写，后继协调从实际状态继续差量。0或
负数目标在任何IO前拒绝，CPU0保护不变。此层仍由调用者串行，且不授予认证或
调度权限；受管Guest旧无认证PUT仍拒绝，可信CPU全链仍为核心缺口。

受影响cpuscaling整包Mac -race6顶层/15含父子记录，0fail/skip，包2.222秒。
新增验证在线/离线读失败、写后读失败、部分写保留及差量恢复、双向写被接受但
实际未变时拒绝、无效目标零IO，并覆盖原成功扩缩/no-op回归。vet0、相对95946509
增量lint最终0。首轮仅新增测试字面量gofumpt1项，修格式后不重复未变业务。

## 取消历史与当前出生的生命周期

复查前一批取消屏障时发现自身缺陷：record.validate要求历史Cancelled与当前
Birth永久同PID/starttime，而原合法启动允许在空组且旧约束确认后推进Birth。
所以启动save/exec可成功，下一Inspect却无法读取该日志。新增回归先在旧实现
真实复现Bootstrap、已Held及PID复用三类失败，不能仅靠编译或启动返回成功签收。

现保留历史完整取消命令、旧PID/starttime和资源revision水位，当前Birth可以
按原空组/已确认约束门槛推进。仅历史结构校验不再要求它永远等于当前出生；
安装取消屏障及Cancellation.ValidateFor仍严格核原出生，不能用新进程补出旧
命令的当前NoEffect。启动保存前校验修改后的完整record，失败不保存、不exec。
Root v2格式不变，旧版回滚仍拒绝，不删屏障、不重置revision、不清未知。

Mac受影响取消/出生/Bootstrap/系统域及SDK组14顶层/33含父子记录、0fail/skip，
包4.045秒；vet及相对5181b364增量lint0。三类新回归均核重建Guard后完整读盘、
原Held/Original/取消正文不变、旧Apply/Replay/当前取消回执拒绝且零setter、
新出生后继命令按持久水位成功。首次修复组因普通沙箱禁止httptest listen而panic，
授权后原范围完整通过；日志保留，不把沙箱失败计通过。未重复前一长租约组合。

两模块合计20顶层/48含父子记录，不能与上一阶段累加冒充完整产品E2E。CPU
sysfs和Root kernel/exec/空组为明确peer，Root文件锁、原子rename/fsync及HTTP
SDK为真实Mac实现。这不是外层跨PG boot/迁移完整恢复证明。

## 准确构建与剩余边界

CPU daemon按5181b364编译Linux amd64/arm64两个ELF，Agent/Guard按a009521f
编译两架构四个ELF，全部exit0，核VCS/ELF架构/SHA256，均未执行。构建时
只有所有者AGENTS未提交，modified=true及离线缓存stat权限警告如实保留。未改
PG/Rust/Controller/Runner不重跑或重编，PG17三处b078保持。

受影响复现命令：

```sh
go test -race -count=1 -json ./pkg/neonvm/cpuscaling
go test -race -count=1 -json ./pkg/dsga/resourceguard -run '^(TestCancellationBarrierSurvivesLaterPGBirth|TestResourceCancellation.*|Test.*Birth.*|TestBoundedBootstrapHoldsActualLimitsBeforePGBirth)$'
go vet ./pkg/neonvm/cpuscaling ./pkg/dsga/resourceguard
```

可信CPU写通道及Scheduler授予、Monitor自动策略、全部最终写者仍未闭合；本
原语修复不能替代它们。安全降normal/持续后台私有上界、异常来源关闭、分区旧
进程/出生不完整/无Wait/外层跨boot迁移仍须实现，随后PG18→PG16独立适配验证。
真实Linux/K8s/KVM/NeonVM/HugeTLB/物理回收与性能后验。默认关闭，不构建镜像、
不准备或执行部署、不合main、不发布，阶段归档后持续Goal仍active。
