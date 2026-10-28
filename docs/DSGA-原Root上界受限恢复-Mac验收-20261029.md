# 原Root已持有上界的受限恢复：Mac阶段验收

本批沿既有Autoscaling/Neon功能分支实现，PG17保持冻结b078；不追新上游、不改
独立控制面、不构建镜像、不准备或执行K8s部署。准确代码、逐文件摘要、用例
映射及日志hash见[机器收据](DSGA-原Root上界受限恢复-Mac收据-20261029.json)。
准确Autoscaling代码`81a4a2b3255fa032dc6743a69a2e7e7fb9baf608`，Neon路由代码
`6d71ad239dfa038adbcd26e659a1ee6ae665f23b`。本阶段完成明确的一条恢复路径，
不把所有Root/缺源/跨boot恢复写成已完成。

## 修复范围和原来源

原Root完成Bootstrap/资源命令后，实际memory.max或某个整HugeTLB池被外部调小，
原查询会拒绝漂移，但普通Apply/ReplayOriginal不会修复已完成命令。新增显式
`POST /dsg/v1/resource/repair-held`，只能选择Root原持有上界，不能传新限额、
PID、路径、boot或预算参数。Neon正式Rust Router使用原专用短期签名和冻结
VM/Compute绑定，固定UDS透传原字节；Root再次验签、严格JSON和当前floor。

`source_sha256`以独立域前缀绑定完整原Bootstrap、Held、Birth。实际采样和
可变floor不进入摘要，后者每次在锁内重新核验。新owner可用自己的执行代次
选择同一来源，但老命令的epoch/revision/预算/PG出生一字不改。未知Original、
未完成Bootstrap、原Birth不符、缺/坏日志、换Guest/cgroup均不生成修复许可。
未运行过资源命令的原Bootstrap也可选择；无Birth时必须证实组未populated且
有限pids.current为0，不能收养没有记录的进程或把僵尸任务当空组。

## 写前和写后不变量

锁序仍是原资源文件锁→原日志/floor/出生→真实当前上界→原日志fsync→内核写→
完整读回。一次恢复只把更紧或相等的实际memory.max/整池恢复到原持有目标：

- 目标完整上界仍必须放得进当前Guest；全部hstate同序、完整、无溢出且实际
  整池核算一致。任何实际normal或池大于原目标都在首个写前拒绝，不能按RSS
  或当前未使用页推导安全降限。
- swap.max/current必须仍为0，无surplus/overcommit；任务上限/占用和系统
  额度/归组/PID1/root剩余用户态必须仍正确。本入口不修复这些不可变域漂移，
  不迁进程、不杀PG，不借一次恢复降低normal或释放物理页/Host承诺。
- 当前完整约束已匹配时只读，不重复内核写。需要修复时，先再次fsync原完整
  持有记录；真实rename/持久化失败不触发内核写。内核部分失败或读回失败
  返回Unknown并保留同一已承诺上界。
- 修复没有新资源目标或新分配，因此不改原Held/Original/Applied事实和协议
  版本。Applied只记录原操作曾完成，不能单独说明这次修复成功；当前完整
  Evidence仍须逐字段匹配。正常预算消费者继续以ConfirmedLimits核实际值，
  不因原Applied=true提前结清或开放。
- 调用者取消不会在同步内核调用中途释放文件锁；新floor推进必须等待它结束。
  Linux所有生产写者仍须root独占，任意外部管理员并发写不由文件锁自动排除。

## SDK、Owner与回复未知

SDK在发送前固定来源摘要及独立目标slice，修复未知有单独本地栅栏，包含没有
运行期Command的Bootstrap。错误/超时不自动重发POST，`ObserveHeldRepair`
只读原State/实际约束及原Postmaster出生；来源或floor漂移不清未知。新命令
发送在修复未知期间阻断。

若旧GuestReady回复丢失，而Root已完整落账后又漂移，可修复完全相同的Held；
SDK保留旧Command Pending，直到它自己的原Observe/外层Record确认，修复本身
不推进GuestReady/PG/VM WAL。不同Command未知继续拒绝。修复未引入新逻辑目标，
重建SDK从同一持久Root来源和实际值恢复；不能重建日志或自动生成修复选择。

ResourceWorker显式入口核真实原Session/Lease、VM可派发和持久Guest绑定，
Root锁内最终核floor；写后重新只读核原效果和执行权。它不创建预算、不改
外层操作或Node，不把一次修复回执变成以后请求的永久许可。Monitor/策略的
可信选择和完整产品入口仍在后续代码范围，普通轮询不自动投递该动作。

## 实际采用的验证

- Mac Go `-race`新增及受影响范围去重22顶层/56含父子记录、2包，0fail/skip。
  最新修复范围13顶层/39记录，原未变HTTP/Owner范围9顶层/17记录采用已有
  最终结果；不把中间成功重复计数。准确每项耗时和过滤规则写入机器收据。
- 覆盖Bootstrap/运行期的健康只读及漂移恢复、相同原命令Pending、不同未知
  拒绝、同值换来源拒绝、原PID/Guest漂移、无记录进程/僵尸、swap/HugeTLB/
  pids/系统域不符、真实Mac持久化失败、部分写、Guard重建、丢回复只读恢复、
  新owner签名和跨进程文件锁，以及RPC期间调用者修改slice的隔离。
- 两包vet、相对17d7c744增量lint0；不宣称全仓lint清零。Neon正式Rust路由
  认证1项、固定UDS原字节/签名/关闭连接1项通过，0fail/ignored，目标rustfmt
  通过。compute_tools lib/test的clippy退出0，改动文件无诊断；原remote_storage
  27条依赖警告仍保留，不声称全依赖无警告。UDS原文夹具不假装有效Root预算，完整请求由Go生产handler另验。
- resourceguard完整测试包Linux amd64/arm64两个ELF在Mac交叉编译成功，
  核实际架构、Go构建信息和摘要，**未运行**。不是镜像或8服务产物重建，不能
  记为Linux业务/内核测试通过。未变PG17/Runner/QMP矩阵不重复执行。

真实Mac代码包括文件锁/fsync、Go SDK/Owner、Ed25519、TLS以及Rust Router/UDS。
内核、PG进程、Kube/Lease或UDS peer有明确夹具；没有真实Linux cgroup/KVM/
NeonVM物理操作，没有包含独立控制面的产品E2E。

初轮新增有限pids夹具未给出启动前pids.max观测，先在Bootstrap被拒；改用既有
有限任务夹具，未放宽生产预检。内部修复标签误用空格被ID校验拒，正向用例
抓到后修正合法标签。unused import、增量lint/格式失败和旧SDK27/链接器不兼容
构建失败留档，显式Xcode14.4 SDK后Rust通过。所有失败均不计为成功。

## 剩余核心工作与Linux边界

本批只关闭已持有来源下可逐维单调恢复的Root漂移。缺原Outside/资格/Birth/
Root日志、实际高于原上界、swap/任务/系统归组异常以及跨boot来源闭合仍有
受控恢复代码工作，不能包装成仅待Linux测试。安全降低normal与后台/私有
持续上界、真正owner切换的完整业务组合、Start/init失败/无Wait/迁移、Monitor
自动策略/可信请求取消重放和全部写者/CPU仍须完成。PG17核心完整归档后继续
PG18→PG16，各版内核/Neon扩展/发行与专属回归独立。

Linux目标环境须验真实cgroup/HugeTLB读写及部分恢复、进程UID/exe/归组、
root独占写者与权限、内核同步写/取消/持久化、真实K8s和NeonVM/物理回收、
存储/性能与长稳。Mac通过和Linux编译不能保证这些正确。功能默认关闭，
持续Goal保持active，阶段归档不等于整体完成，不合主分支或发布。
