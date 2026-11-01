PG17 原broker同次新Observation去重，方案A；未增加生命周期缓存。

原verifyLocked签名保持，私有core把唯一一次完整inspectOriginalBrokerTask移到全部原声明/权限/state+lock/WAL四前缀/namespace+root FD检查之后，仍精确比较原transfer.Task。Filesystem getter消费刚完成的同调用栈Observation，不再立刻重复inspect；仍持原resource→proof锁，末原Task.verify、原pidfd/procfd dup与newTask实际验证保留。未改其它FS内容hash、协议、timeout、Guest几何或Root stderr六源。

新增DEV ctx固定聚合 normalBrokerInspectionDEVTraceKey{} / normalBrokerInspectionDEVTrace，无可替换函数或授权回调。仅计真实broker inspection次数/单调耗时/整个R2 /proc/self/io rchar差及缺样次数；包括采样自身和其它goroutine，绝非exact ELF io.Copy n，不参与Source/预算/Ready。production Background无trace。P2后继在原R2 ctx装一次，最后仅打印一次snapshot，不能用诊断数据补来源。

246536：准确Gitdd23075+仅两源，1451Go/mod逐sha；3race根12sub、CLI/Darwin编译均0。closed-proof/foreign-System与真实kernel task闭、错start/boot、其它活task逐错误断言确达对应拒绝且trace.calls=0；WAL夹具先确认原四持久prefix通过，再实际删除/改字节/namespace绑定变化拒绝。WAL夹具是原语法模型+真实文件机制，不冒完整Core真实namespaceFD/Source已测。DEV计数实读32KiB并确认仅背景不开计数。未加AST形状测试，顺序/唯一inspection由Root与独审看真实diff，完整positive及累计收益必须原18c S来证。

edf024第一次只有Docker模板;;语法失败，Go未执行；原result/日志保留，新nonce修driver后才246536绿。原只读审查曾算最小898.2MiBELF读、预计省255.3MiB，这是确定调用图的静态预测，不是已有运行计数，更不承诺省秒数。真实P2当前d41到handoff22.7s但25sReady仍超时，下一同2vCPU/18c新nonce才判断效果，原25/30窗口不改。Source、normal安全降限、Linux/K8s E均未通过。
