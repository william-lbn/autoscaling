# 新开放、再关闭与接续 SDK 只读恢复：Mac 阶段验收

日期：2026-10-29。继新开放持久账本后，本批接通准确原未知的 SDK 重建及
Advance/Run 只读对账。生产新开放选择/派发、来源前后审计、异常再关闭发送、
业务门禁解除和有界生命周期归档仍须代码；不是受控重开业务完成，Goal active。

## 实现与不变量

NewPGWorker 在原关闭已确认后，按新开放自己的完整 WAL 恢复 AdmissionPending，
不能退回旧首次开放；再关闭未知同时恢复原新开放和最新关闭，接续还恢复完整
原始/前驱/当前选择。已确认再关闭保留原历史未知，但不恢复成可迟到开放的 SDK
栅栏。外部提供的错误旧初开 pending 与新 WAL 不一致时拒绝。

Advance 和原生过期 Run 都只查询最新原选择，并在当前 Session/原 UID/RV 下落账。
NotFound、较新 open、409 或错误回执不结清原未知；落账丢回复按真实持久状态
重建。普通轮询零 setter，不续租、不自动 rearm、不改变 Run 过期 latch，也不清
外层关闭/预算/首次未知。旧关闭管理重放不能退回绕过新 WAL。网络不持本地锁，
落账前复核 Session，生产串行性继续由原 Run goroutine 和 Store CAS 保证。

## 实际验证

Mac -race 按 package/test 去重 6 顶层、58 含父子记录，0 fail/skip。新增两根共
14 记录：SDK 十二模式及过期 Run。SDK 覆盖新开未找到/较新开/已应用/落账丢回复/
已确认，再关闭未找到/已应用/落账丢回复/已确认，接续未找到/已应用/落账丢回复。
真实 Elector 在先前 WAL 之后合法接管，SDK 读取旧 owner 原请求；首次未知不改，
终态重建不复活 pending，未决重建保持准确原请求，所有模式零 setter。

过期 Run 仍提供串行状态管理和原 WAL 查询，拒绝 Begin，零 renew/rearm/setter，
整个账本不改；取消后 join 真实循环。该例仅启用原生期限分支，来源 Reader 明确
未配置，验证已有关闭 WAL 先于来源 Reader 访问，并不是生产来源审计器测试。

受影响首次 SDK 未知、准确实例绑定、既有原生过期受限管理及正常原生 rearm
通过。完整两新根 51.926 秒通过后，补旧初开 pending 替换拒绝、取消/join 清理与
一处 lint 改动，仅定向替换两项变化场景及过期 Run（12.045 秒）；其余已过场景
不重复。原正常 rearm 50.826 秒、其余受影响三根 26.837 秒均正常退出。
最终 owner vet/增量 lint（0 issues）通过，33 消费包仅编译退出 0。

Go 固定工作区 GOCACHE，GOPROXY=off/GOSUMDB=off，-mod=readonly -race
-count=1 -json。准确命令、日志 SHA256、采用替换与源码摘要见机器收据。
第一批夹具误写 continue-closure/status，三项接续失败，整批失败；改用真实 SDK
continue-close/status 后完整两根通过。原失败和 tagged switch lint 日志保留，
没有放宽生产校验或把失败批整体涂绿。

## 认证范围与下一步

本批执行真实 Session/SDK/TLS/typed HTTP/RFC6902 CAS/Run；PG 回执及 Root 仍为
明确领域 peer，不代替真实 PG/生产来源/Runner/VM/Linux 端到端认证。PG/Rust/
HTTP/Go wire 未改，PG17 实际仍 79367196a74eef82e67cac917877d2e6271d3e70，
不重编或重跑未变原生与自动 Grow→Shrink。

继续实现正式新选择/派发、完整预算与来源前后审计、异常再关闭、Run latch/业务
门禁解除、SDK 接续和有界重复生命周期。normal 安全降限、私有/后台持续预算及
活性、分区/跨 boot、Monitor/WSS/配额/RW-RO/可信 CPU/全部写者等仍需代码。
Linux/K8s/KVM/NeonVM/CRI/cgroup/HugeTLB/真实物理与性能单列未验，不建镜像、
不部署、不合 main、不改独立控制面。所有者 AS AGENTS 不 stage。PG17 整体未
完成，全面代码/Mac 验证/远程归档后才暂停，不自动进入 PG18/16。
