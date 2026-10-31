PG17 数组helper完成前缀持久性 v2

这是默认无生产消费者的私有组件修复，不是完整PG/normal安全降限。原Root真实helper退出后，四JSON可见仍不代表完成前缀已持久。本批新增Root进程私有见证与显式恢复；普通Observe/gate零写，Root重启/partial保持Unknown。详见 source-proof.json、production-diff.patch.txt 及独审交接。

最终U：24根119子例（新增5根18子例+直接旧回归19根101子例），race/source/test/build全0，零skip。最终369 KVM：1根4case（原3+新Root显式恢复1），Guest0/QEMU0，4份报告SHA完整。新S case不重复算新S根；同步失败只为U确定性IO模型；没有真实磁盘故障/PG/E2E。E0，所有normal/Source/physicalfree声明false。

精确code为原12源+新durability_test共13源，实际改4源。原79阶段清单保留为历史；其current code hash已由本批source-proof/joint manifest明确替代，不要拿旧v1 manifest冒充新代码。原静态C ELF/369tuple与收费主体无变化。原12独审发现的唯一阻断已按Root选定合同实现；修复作者不自行签独立通过，Root须复核后统一提交。

未接生产launcher/所有cap写者/WAL/正式Source；不因本组件通过删除原五拒绝。后继必须明确把本门禁接所有写者并用原Source授权，显式Recover不能伪装普通Observe或HTTP/config bool。
