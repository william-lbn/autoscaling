# PG建立前Root恢复与正式阶段交接

日期：2026-10-29，Mac Darwin arm64。公共核心11dee1f1、Agent准确源码
`23d455fb9ed95cdca35474bd670fbc75d8416550`，沿原自有功能分支。PG17三处b078
不变。准确15文件、逐用例采用映射、日志及ELF摘要见
[机器收据](DSGA-启动期Root恢复交接-Mac收据-20261029.json)。

## 修复的实际业务窗口

原入口必须先建立PGWorker；首次Root实际约束漂移会更早失败，管理入口不可用。
Agent现在先创建同一Session的专用ResourceRecoveryWorker，再运行只Root恢复
门槛。首次原Bootstrap已确认、完整来源可选且没有孤立运行命令时，允许先把
漂移中的原Guest身份写入VM账本再fence；这不是约束完成、预算确认或开放准入。
缺/坏日志、未知Bootstrap、孤立Original仍拒绝，不能借此收养旧效果或换boot。

管理端点发现返回CoordinationBinding，PG boot为0明确只表示PG端点尚未建立。
该类型仅用于本地管理，原生PG Instance解码仍严格要求非零。0 boot只准
resource_state/repair_held_resources/observe_held_resources；不能Begin、取消或
重放PG。正式PG端点也不把0当通配身份，仍固定实际非零boot及完整原操作。

恢复与正式PG循环共用原SDK执行槽，不能并行启动。普通轮询只读，约束未确认
保持原owner及端点；修复丢回复保留PendingHeldRepair，即使实际已经恢复也须
显式Observe。确认后先结束/注销旧端点，再建立真实PG客户端及正式管理端点。
整个恢复不修改PG绑定、预算、GuestReady、VM或Host终态。旧端点不可复用。
Run/Session/调用者三方取消贯穿同一调用，Run先停也须取消并join管理HTTP。

私有UDS仍默认关闭；若没有配置管理入口，漂移时保持只读等待，不会自动修复或
回落legacy。配置时复用原UID/目录/socket权限边界，不增加租户或TCP接口。

## Mac增量验收与准确采用

四包去重20顶层/52含父子记录，0fail/skip；四包vet退出0，相对8f371309增量
lint0 issues，diff通过。second与final2逐用例采用最新日志，相同子树不累加。
后续仅补新取消/UDS场景和受影响解码，没有重跑未变PG/Rust/其他领域大矩阵。

已验证：初始漂移严格构造仍拒绝而专用构造只绑定/fence；错误摘要零修复；生效
丢回复仍保持Pending且不交给PG；显式Observe后正式PG能力协商并产生实际boot；
旧0 boot不能进新端点；重复执行槽/旧端点拒绝；未知Bootstrap/孤立命令零绑定/
零fence；取消保留原WAL。实际阻塞TLS修复HTTP中只取消Run，Session和调用者
仍活着，确认HTTP结束并保留未知。真实私有UDS→HTTP→正式恢复端点的发现/
状态/修复/结束组合通过；不是完整预算/存储/物理内存产品E2E。

首轮暴露测试字段名错误及管理0 boot嵌套解码问题，后者用管理专用类型修复，
未放宽原PG身份；后续测试传输类型错误改用真实TLS阻塞夹具。增量lint完整
结构初始化问题修正为明确零字段。失败日志保留，不计通过或隐藏为环境问题。

## Linux编译与未完成代码

本次准确Agent源码同时包含前一normal最终写者防线。Agent及Root Guard各
amd64/arm64共4个ELF仅交叉编译，架构、VCS revision、SHA256均核对；没有执行。
构建唯一未提交文件为保留的所有者AGENTS.md，故modified=true；离线模块stat
缓存写入有权限警告，但4次构建退出0且产物可核验，不能称无警告或Linux已通过。

本次关闭“已有完整原来源的初始Root约束漂移”这一启动恢复窗口，不覆盖缺/坏
来源、未确认Bootstrap/孤立命令的受控关闭；实际高于原上界或swap/任务/系统
异常仍需安全处置，不能自动写低限额。安全降normal及私有/后台持续上界、其他
接管/启动/无Wait/跨boot迁移、Monitor/自动策略和全部写者/CPU仍有代码/组合
缺口。PG17核心未完整，完成后再PG18→PG16。Linux/K8s/KVM/NeonVM/HugeTLB/
物理回收和性能仍须目标环境，现有Mac逻辑证据不能替代认证。

Goal active，沿用原自动接续；上游继续冻结、独立控制面不动；不构建镜像、
不准备部署、不合main、不发布，阶段归档后继续核心代码。
