# PG17-S-WIRE-001：6.12 KVM目标平台，首次失败保留

状态：**FAIL_COMPONENT_FIXTURE**，不能按QEMU正常关机exit0认定通过。Guest测试exit=1；父端FD断言rights子例expected10、actual9。原始console完整保留，不重跑旧源码取绿、不改为<=、不把计数减少认作通过。

来自GitHub已push AS 1f57705491a5407bbea3496460b0d96e5c58cc64不可变git archive的独立只读snapshot，明确排除P2未提交physical_kernel_origin源码。实际COPY 684份Go/mod文件逐SHA与snapshot匹配，manifest SHA985059d3e5a6c0874f411bcbba2eed7989aac35b406ec76a772bc655c204a8d5。固定工具镜像Go1.24.3、CGO0、linux/amd64，编译RUN禁网；此Guest不是race运行，Host之前race记录单独保留。

实际KVM、CPU host、2CPU、1GiB、Guest -nic none、Docker network none；/tmp由Guest root建为0755，父子Unix socket只位于独立临时目录。旧Runner镜像仅直接取QEMU8.1.5，不运行旧Runner业务。kernel SHA0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0，QEMU SHA8d8d8ee83471749d0b882ce34344638e0ac0fa2390cd822b98d9b6314e2f9cb5。静态测试ELF SHA5a9a47cd5c86b59b54af35213da21ac3637e8004ccd9438b4f9680f22043fa0f。

实际5根：S-WIRE根FAIL；三个既有U-WIRE根PASS；同一产物的既有PG17-S-KERNEL-001物理观察根PASS。41子例中rights失败，其余40通过，覆盖ID不增加。有效peer子例真实UID/GID65534、真实child PID的SO_PEERCRED核验成功；64个FD截断等其余实际Unix子例通过，但整个wire组件仍失败。失败子例在严格FD比较处中断，因此不能声称其后正常Wait路径已验证。

主执行已确认fixture竞态：os/exec把strings.Builder stdout/stderr转为管道，child发送后立即退出，异步复制goroutine可能在前后计数间关闭管道，导致10→9；数量相等还可能抵消真实泄漏。修复计划仅改native测试：明确父子同步屏障让child/stdio/pidfd在FD核验期间存活，之后通知退出并严格Wait；不是sleep/<=。生产三个wire源不改，待新提交独立snapshot重编重跑。本目录保留原失败，不能被后续成功覆盖。

运行内核release真实6.12.26-william-bd33f7c78026，notes SHAb52a6321c44be2b8d138e9ca990ec86413c3744a189a118ed69aa295cb8ef03d。K272228352B、Movable800976896B、stock524288B，VMShrinkPermit=false；只是原物理观察案例额外平台记录。

没有PG、生产listener/ACK发送者、Root Pending/Enrolled、原maps/filter封存、normal写、VM伸缩或K8s/E2E。没有Host网络/配置/已有VM改动，本测试容器已清理。完整原始命令、CPIO构建算法/条目、二进制、源archive/COPY清单、console/notes、评定脚本均在：

/data/neon-build/validation/pg17-native-birth-wire-kvm-20261030T163112Z

evaluate-result.py将Guest失败传播为exit1；run-kvm.py仅记录QEMU退出，二者边界明确。不可从无条件诊断行PG17_NATIVE_BIRTH_WIRE判定成功，必须检查Go根/子例和Guest exit。
