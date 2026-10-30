# PG17-S-KERNEL-001：真实KVM内核物理组件观察

结果：**PASS_COMPONENT_ONLY**。2026-10-30在worker-118实际以KVM启动原vmlinuz，唯一根测试 `TestPG17NativePhysicalKernelComponent` 通过，Guest正常poweroff，Docker/QEMU exit=0。组件测试不是PG17内存伸缩E2E，也不产生缩容授权。

## 精确事实

- kernel artifact SHA256：`0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0`。
- 实际release：`6.12.26-william-bd33f7c78026`；实际 `/sys/kernel/notes` SHA256：`b52a6321c44be2b8d138e9ca990ec86413c3744a189a118ed69aa295cb8ef03d`。
- 2 possible CPUs、1GiB Guest RAM、pid_max=512、4KiB base page；无Guest swap、THP/KSM sysfs。
- 原命令行明确 `kernelcore=256M memhp_default_state=online_movable cgroup.memory=nokmem,nosocket,nobpf`；auto_online_blocks实际为online_movable。
- DMA32 present=66462页、Normal present=0；**K=272228352B**。kernelcore字面256M不能替代实际present求和。
- Movable present=195551页（800976896B）；CPU已填充charge stock组件上界524288B，**不是完整在途I**。
- 原测试Guest bootID：`d874c52c-fcb2-438d-b8cd-3e5db1ea24b7`；真实空cgroup设备号23/inode28，memory.max仍为max。
- VMUID与ComputeInstanceID均明确test-harness身份，没有伪造Kube原UID。观察重复读取Match通过；`VMShrinkPermit=false`。
- 只读128MiB memory blocks：0/1为DMA32，2的valid_zones=none，3..7为Movable。**没有offline/online写入或热插拔操作，不能据此声称物理热移除通过。**

## 执行边界

旧Runner镜像仅作为锁digest的QEMU8.1.5/BusyBox/musl来源，直接 `--entrypoint /usr/bin/qemu-system-x86_64`，没有运行旧Runner业务。镜像digest：`sha256:f170364ea02fc54d08523226e8616b1ef1c4097a6a2cc5b3a66db7756ef5df9e`。QEMU二进制SHA：`8d8d8ee83471749d0b882ce34344638e0ac0fa2390cd822b98d9b6314e2f9cb5`。

QEMU强制 `-accel kvm -cpu host`，没有TCG回退；kernel console也明确Hypervisor detected: KVM。Docker network=none、Guest -nic none，没有修改宿主网络、Pod、节点或集群。容器最多2CPU/2GiB、无额外cap、只读rootfs，120秒TERM/10秒KILL保护。实际2.67秒结束并清理本测试容器。

最小initramfs只含缓存BusyBox/musl、静态Go测试及init。init仅挂proc/sys/dev/cgroup2、设置Guest pid_max、激活memory controller并创建空测试cgroup；没有PG、normal限额写、VM resize、Host/Outside资格或OriginalBirth封存。notes/release相等不是完整来源认证，更不是生产授权。

## 源码与工具

新增测试：`pkg/dsga/resourceguard/physical_kernel_profile_native_test.go`，tag `linux && dsga_native_kernel_profile`，仅一个根测试。三个显式env必须精确匹配；缺省直接失败，不能在普通宿主上默默跳过或读取后宣称成功。使用真实Guest bootID和原空cgroup dev/inode调用包内readPhysicalKernelComponent。冻结的P2三个实现/单测文件未改。

编译：现有Go1.24.3工具镜像，CGO_ENABLED=0、linux/amd64，`go test -mod=readonly -trimpath -c -tags dsga_native_kernel_profile -o /out/physical-kernel-native.test ./pkg/dsga/resourceguard`。实际RUN禁网，nonce使编译步骤实际执行；保留编译缓存。实际COPY 677份Go/mod文件，完整逐文件manifest SHA256：`28e35b169e6c6e24395f5efc39d5648640f0b59cdaef6b2799ddf6031401f2cb`。静态测试ELF SHA256：`41c99db95061bc9a6145ee500643694731d916fd9f42d307e7358e73588f66ac`。

首次gofmt单文件rw挂载无法创建旁边临时文件，改用只读容器stdout格式化；首次Buildx入口未source项目env，exit125，修正后禁网编译exit0。Ubuntu官方busybox下载遇当时指定代理SSL失败，保留stderr；无需新下载，直接复用本地锁镜像BusyBox+musl。本代理没有重启代理或安装系统包。准备失败不算内核执行失败，原始记录保留。

## 证据

仓库保留小型真实收据：result.json、source-proof.json、kvm-command.json、guest-init.sh和原始console.log。完整构建命令、原始构建日志、Go版本、全部COPY manifest、BusyBox提取命令、CPIO条目及hash、initramfs、kernel notes/zoneinfo/meminfo、容器退出与清理证据位于：

`/data/neon-build/validation/pg17-s-kernel-001-20261030T154132Z`

无PG/压力/安全normal降限/VM缩容/K8s E2E通过声明。后续更改内核、cmdline、编译源码或Guest产物需要重新获取匹配的真实组件结果；本结果不能追溯补签原出生前资格。
