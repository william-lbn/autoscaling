# DSGA normal-split-v1 专用内核配置

从冻结B20配置只增加`CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y`。旧默认配置、VOLUNTARY/DYNAMIC和无DEBUG_FS保持原样；没有应用静态PREEMPT_NONE草稿。专用0007默认关闭，仅新profile明确开启。

构建选择`AMD64_KERNEL_CONFIG=profiles/normal-split-v1/linux-config-amd64-6.12.26`；新准确release/artifact/resolved config/notes已在`config-source.json`逐项锁定，17次PID KVM、真实双域counter压力和同kernel四项IRQ寄存器边界已完成。后两项只属kernel组件，不是PG、Root出生封存或完整normal回归；旧0ab/552/3b历史tuple保持原样。

两处完整ancestor取消循环IRQsave/restore只闭counter取消段调度/IRQ重入，不关闭force，不证明FS/Root/I/Movable。HugeTLB本地撤账保持原语义；所有未闭来源继续拒绝normal/VM降限。IF关闭两项明确通过硬件调试器在pushf保存前注入，IF开启两项是自然入口；没有写Guest内核文本或counter，没有换TestOnly内核冒充生产图。

完整范围、原失败和源码收据见仓库`docs/PG17-normal-split-IRQ取消段-源码与验证合同-20261031.md`及`docs/validation/normal-split-kernel-20261031/`。本批未发布正式kernel镜像，未授予缩容许可。
