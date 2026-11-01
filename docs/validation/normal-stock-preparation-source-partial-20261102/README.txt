PG17 原stock preparation与Source接线阶段归档（持续目标未完成）

本阶段代码19个明确owner源中14项相对aeb1de442c...变更、5项已HEAD同字节。原AS分支不变。Root独占Git/发行锁。
最新完整组合e5a558=Git dd23075+Source19+A2，实际85 race根405sub/零skip，CLI/Builder/Darwin/Guest/Host/两export八阶段全0。这些是组件/编译组合证据。
fc39a5只改DEV观察，复用原85/405/八阶段与同字节Host/CLI/Builder，重新编译真实Guest/seed；真实stock四命令唯一Wait、原monitor/DB/seal、完整WAL+readback后的生产ref/R1alive观察已达到，198目标、phase3。
但是fc仍原Ready25秒超时，R1确退后ref未达到；真实S最终FAIL，不称完整Source或normal通过。
9dbb65仅独立shadow快照的私有阶段诊断，仓库19源未变：complete入口21.009s，完整生产finalref/原readback已返回23.390s，q.Source第一freshref开始23.391s，原25s窗口先到，后面capturecatalog/install/finalref尚未开始。
partial trace只能说明complete写入前19次inspector累计5.029s、整个R2含采样rchar约1.6985GB，不是准确ELF字节/磁盘I/O，也不是完整startup计数。stage=complete本身位于WAL写入之前，不能冒已持久通过。
所有S保原Host/Guest FAIL，不把U/进程0冒S，未跑PG/普通九服务/真实normal硬降限。normal=0，Linux E=0，K8s E=0。HostAPI明确MODEL，真实KVM/18c/2vCPU只证明组件现场，不是生产认证。
当前缺口是原安全25s窗口内完成真实Source/COMMIT与R1退出继承，另future enum2、真实九服务/PG/normal连续安全、镜像与Helm三节点回归仍待后继。
没有prune/丢缓存/扩大窗口/改宿主网络；冻结PG17 gitlink/发行锁未移动。

本Git归档只含小代码/日志/原终态/资源证明。完整源上下文、所有历史失败、镜像、原磁盘与全manifest仍在各/data/neon-build/validation原nonce目录，下面摘要保存它们的原SHA。docs/doc历史档不作为未来编译上下文，但外部证据必须保留。
