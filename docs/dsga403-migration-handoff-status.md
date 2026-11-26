# DSGA 跨 Host 交接当前状态

当前先修正既有拒绝位置：Admission 使用直接读取的原 VM，Controller 在 finalizer、VM 状态、目标 Pod 或 QMP 副作用前再次读取。不可变 DSGABootstrap 或任一受管标记的存在都要求受控协议，不能翻一个 annotation 进入 legacy。直接读取未知、原 Controller VM UID 不同均拒绝继续。

新受管迁移不进入旧流程；已经存在的迁移只增加等待原受控交接的解释性条件，保留原 UID、阶段、source/target、finalizer 和原预算。不会先标失败再自动清理、恢复源或切换到同名新 VM。普通 legacy VM 的原迁移流程保留。

这项改动不等于跨 Host 迁移能力。待补的完整链仍包括：原 Owner 冻结计划与兼容 profile，原 Scheduler 两端独立承诺，受控 pre-copy，原 Source 停止并持久禁止恢复，原 Target 取得执行许可，原 Guest/PG boot 与数据/WAL保持，Runner/Node/QEMU/normal-origin真实来源重绑定，以及最后按原退出事实结清旧 Host。目标可能执行后不得恢复旧源快照；未知保留双端责任。

Host HugeTLB 与 Guest PG HugeTLB 独立。Host 大页 profile 须扩原 Node CAS 的独立池维度和总物理覆盖，固定 QEMU backend/实际映射与相应收费，不能直接套普通页 Pod memory floor，也不能把全局 HugePages_Free 变化当本实例释放。
