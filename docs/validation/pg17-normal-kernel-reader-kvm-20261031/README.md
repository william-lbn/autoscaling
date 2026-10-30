# PG17-S-KERNEL-002：准确normal-v1新reader真实KVM

PASS_COMPONENT_ONLY：单根、零跳过，Guest/QEMU exit0。直接调用新readPhysicalKernelComponent并Validate、两读Match和严格JSON往返；旧native文件不改。

源为Git7ed不可变对象+P2 owned-files九个冻结SHA覆盖+本次独占native测试；693份Go/mod实际COPY逐SHA匹配，明确排除main活动prebootstrap。CGO0/Go1.24.3、编译RUN禁网、固定工具digest。独立snapshot和全部source manifest在外部目录。

使用真实552ffabd... normal-v1内核，release6.12.26-william-normal-v1-b20ac4f85d74，config935e4377...、notes3d22f157...；KVM only、2CPU/1GiB、无网卡，精确完整值见result。实际源：LSM五项+ABI6，initial PID namespace device4/inode4026531836，PG空cgroup真实device23/inode28，memory.high=max；allocator min_free2059kB/scale10/boost15000/fraction0/proactive20，未调参。真实swap/noTHP/noKSM/online_movable/页及zone原值全部由reader检查；K272228352B、stock524288B，VMShrinkPermit=false。

这些默认allocator值不是完整normal profile资格。Identity明确test-harness，GuestBootID真实8399cd77-e96a-4dd6-9ab7-b1432068abbc；没有Root Enrolled、PG、normal写、VM resize或E2E。securityfs/cgroup挂载、pid512与空PG placeholder仅在隔离Guest setup。

准备脚本首次字面量替换发生Python语法错误，尚未执行Guest；原失败脚本保留，改用callback字面量后成功。实际Guest Go timeout60秒，原通用wrapper元数据继承90，result明确纠正并保留原始记录。全部结果采用真实Guest退出与Go根结果，未按QEMU关机0冒认通过。

外部完整证据：/data/neon-build/validation/pg17-normal-kernel-reader-20261030T173615Z。测试容器已清理，未改Host网络/既有VM、冻结业务源或Kernel配置。
