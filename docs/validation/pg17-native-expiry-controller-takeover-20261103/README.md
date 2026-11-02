# PG17 原生租约过期与更高 owner 接管定向归档

真实单机组件整轮通过，耗时413.665秒，总429.934秒；旧Run/Elector先有序退出并join，原30秒租约自然过期后，新owner从epoch2升至3。原生deadline和TTL不被观察续期，旧Session拒绝；只按原关闭WAL和命令SHA显式执行rearm、reopen、CompleteServiceReopen，再开始新的Shrink/Regrow请求。

三事务全部Succeeded，Active和ServiceClosure为空，ServiceLifecycle持久保留。独立复核原Root127文件、N/P/A硬限读回、四数组物理释放、QMP8→7→8及512行数据校验。完整原件留/data，摘要和源码匹配见两份JSON。

没有延长原请求：新请求仍120秒、原生TTL仍30秒。为容纳额外真实过期及恢复成本，仅DEV整体观察窗增加；ad8原390秒观察先于仍有效请求截止而关闭Guest的失败保留。当前通过快照的Go内部watchdog仍420秒且未触发，后续510秒一致性修正单独记录，不重复本已通过整轮。

六源全是非默认DEV测试，无生产协议、权限门或setter放宽。范围严格为模型Host API与真实PG/Root/QMP的有序交接、自然过期；不能据此宣称controller crash、Root死亡后收养存活PG、生产Source或三节点K8s端到端已通过。
