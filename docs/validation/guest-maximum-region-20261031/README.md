# Guest不可变virtio-mem最大区域来源组件

本批不需新内核。准确Linux6.12.26驱动将probe缓存的addr/region_size全部保留为同名顶层iomem资源；真实369 KVM在requested=plugged=0时仍报告全768MiB区域。Guest的block_size/node未独立导出，明确不填伪值。后继Mmax须用原Runner实际QMP base+此区域长度，与同实例machine maximum、device max/address、两个backend交叉；Kpresent不是base容量上界。

只读私有观察结果不发Source/normal/完整I许可。生产跨包原Runner/Host reservation/Guest Origin/不可变设备生命周期尚未接线。原normal五拒绝保留。

- U：5根47子用例，race/count=1，零失败/零skip；精确命令见source-proof.json。
- S：NEON-S-MMAX-001，准确369/notes7cf、实际KVM、Guest Go root PASS及显式exit0、QEMU0。QMP只读9命令；设备addr4GiB/region768MiB，base256MiB，保守几何和1GiB。测试装置UUID/boot不冒充生产原身份。
- 原失败：接口探针误用不存在的memory/online路径；首Go-Guest宿主评估器误把Uint64字符串和QMP整数直接比较。各原result/console/exitcode保留；独立重评估与最后新nonce完整PASS分列。

静态二进制和大CPIO保留Linux缓存/validation，不入仓库；精确SHA与actual COPY全集见source-proof、go/与native/。不宣称真实PG、memory.max下降或K8s回归。
