QMP真实最大内存几何组件，2026-10-31

固定私有只读命令，同QMP连接复用原Runner UUID；准确QEMU8.1.5原schema：/machine.memory的size/max-size/slots、唯一vm0 virtio-mem、query-memdev的pc.ram和vmem0、QOM后端size相互核验。max不能用requested或当前plugged代替。只允许Guest node0/Host默认policy/无host-nodes的新显式候选几何。

组件允许base小于业务min用于未来earlyboot，旧Observation/Command与生产最小内存检查没有放宽。原进程/Host/ISO/Guest/持久首次grow接线仍未完成；无qom-set，无normal或VM下降许可。不是端到端，不存在完成Guest/PG运行的暗示。

实际U：新3根/75sub，直接相关旧3根/16sub，合计97个测试节点PASS，race/count1、0skip。首次旧夹具因缺/private/tmp失败，隔离Linux测试容器补目录后实际重跑，没改业务断言。后续仅原生测试输出缓冲改文件消除潜在竞态，生产/普通U字节未变，引用同次真实U结果而未重复计数。

实际S：新静态二进制在只读/无网络容器运行8个QEMU8.1.5 KVM场景，逐例query-kvm enabled/present为true；包括earlybase、requested与actual/max不同、legacybase、错base/max/UUID、后端不足和额外后端。全部PASS0.66s，QEMU逐个被原父Wait。CPU暂停且无Guest，准确定位为QMP组件。独立复核无提交阻断，已修测试日志并发读取风险。

最终native snapshot实际COPY1458文件全匹配，其中699 Go/mod/sum。新旧源码/配方/nonce/二进制/命令与实际日志均有原始收据；大二进制在/data，仓库只留小档案。

官方源只作schema核对（Linux显式代理下载并核hash）：https://github.com/qemu/qemu/blob/v8.1.5/qapi/machine.json 及 hw/virtio/virtio-mem.c，实际max_size=memory_region_size(memdev->mr)。官方原文和首轮原始观察存/data/neon-build/research/qemu-memory-geometry-20261031与/data/neon-build/validation/qmp-original-geometry-probe-20261031T0005Z。
