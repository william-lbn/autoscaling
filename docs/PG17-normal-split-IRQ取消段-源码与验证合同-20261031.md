# PG17 normal-split IRQ取消段：源码与定向验证（2026-10-31）

默认关闭的0007只在新normal-split配置明确开启。准确新kernel构建和直接受影响的KVM组件验证已完成；没有修改普通page-counter API、HugeTLB本地迁账语义或PG/Root/VM许可消费者。静态PREEMPT_NONE草稿没有应用。

## 改动与持续边界

`CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER`依赖`X86_64 && MEMCG && !PREEMPT_RT`，默认n。新配置从B20只增此项y，保留VOLUNTARY/DYNAMIC、无DEBUG_FS/noTHP。实际resolved与旧3b的语义差异也仅此一项；旧默认639在同新Kconfig执行olddefconfig后明确保持此项n。

两处完整child→ancestor取消循环外各一次local_irq_save/restore：普通uncharge，以及try_charge失败回滚。失败点若高于被观察normal，中间祖先会晚于叶子扣账，所以失败循环不能遗漏。入口已关IRQ时恢复仍关闭；不逐层重新开IRQ，不增加锁、不睡眠、不改变nr_pages或保护计数。正常无underflow分支只有算术/atomic/READ_ONCE；坏counter触发WARN不属于支持不变量。

实际完整缓存C/H树（86,703文件）核查发现合法外部`hugetlb_cgroup_move_parent`只做本地cancel并保留祖先HugeTLB账，调用方持hugetlb_lock且IRQoff；它不属于普通memory counter，不能误换成hierarchical uncharge或直接static化API。0007保持该调用与header原样，普通两循环被完整保护。

IRQoff禁止正常task抢占和maskable IRQ在正差额窗口重入；x86 NMI仍需原代码源与单层/latch条件。普通4KiB/noTHP且没有HugeTLB混入normal counter时，单操作D=max(128,31×1)=128页。新增保护可把窗口并发收紧到CPU当前context加NMI的2C；首组件压力仍保守使用4C（C2时4MiB）。这不关闭force，也不覆盖counter先扣后物理free、迁移双页、PCP/Mmax或Root/FS来源。

## 准确构建锁

| 来源 | 值 |
|---|---|
| profile ID | `neon-linux-6.12.26-dsga-normal-split-v1` |
| release | `6.12.26-william-normal-split-v1-e210f5e3ee94` |
| artifact | `3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0` |
| resolved config | `11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be` |
| 实际notes=ELF notes | `7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828` |
| 新输入config | `de98f51a2ac4c7d3d3b8472993fd716b62584a3a0607257301a7ecd612316855` |
| 0007 patch | `e210f5e3ee9436089be24b318aa6e10019f4014303f26e5964a994a0f717cb88` |
| 7patch canonical（key含patches/） | `de01c7bbe6fce8bc06e26c442463e138142080d9c5ede4a44681ea3fbf8ec742` |
| 实际patched page_counter.c | `d93ef617a216fe05d88e75da40546776a1c16569c6332bf30c2e12a564f81e79` |
| 实际patched init/Kconfig | `16f90ca98b6b9378df80b2c971dd1df45e3c199bcd30f27b5743eb197f0c8c7c` |

构建81.107秒、8jobs，旧APT/下载/6patch/准确3b make层CACHED；新stage只apply0007、配置和增量make/relink。没有把新patch放入旧stage的COPY集合导致全树重编。最终compiled receipt在同新stage核全7patch、实际两源码/Huge/PID、config和bzImage。显式代理健康退出0，没有prune、daemon重启或宿主网络改动。

## 精确验证与范围

| 分类/编号 | 实际结果 |
|---|---|
| PG17-U-KCFG-007 | 默认off的Kconfig解析、显式on的新kernel编译均通过；不是另一个默认kernel的运行证明 |
| PG17-S-PIDCAP-002 | 同新kernel17次KVM，5正常+12预期PID0早期拒绝全部通过；每次实际QMP KVM enabled/present |
| PG17-S-IRQCOUNTER-001 | 真实双叶cgroup压力通过；8和6个worker各256循环，共3584worker循环；先A24→12MiB再同父52→40MiB，private固定24MiB，全部strict reap；parent/private/arrays的events.local high/max/oom/kill均0 |
| PG17-S-IRQCOUNTER-002 | 同准确kernel、Unix GDB硬件断点，2site×IF0/1共4项真实寄存器边界通过；uncharge3祖先、rollback2祖先实际各扣nr_pages |
| PG17-E-normal/Root/VM | 未执行；没有PG、完整Root原出生封存、完整force/I资格或K8s部署 |

IRQ四次自然入口均IF1：IF1两项原样运行，IF0两项明确在实际pushf保存前仅注入CPU IF0，真实CLI/整个取消段/返回逐指令观察，返回后恢复原caller IF。不是自然IRQoff caller覆盖，也不是通过mock/另一TestOnly内核替代。未调用Guest函数、未改函数参数/counter/内核文本；原RSP独立核没有M/X内存写或Z0软件断点包。实际KVM QEMU8.1.5支持NOIRQ→KVM_GUESTDBG_BLOCKIRQ，使用0x3，不宣称其支持TCG的NOTIMER。

IRQ专用工作负载故意令另一parent max0，引发4个子进程memcg OOM并逐个reap，用于到达两祖先失败回滚；它与前一压力case的零OOM验收严格分开。pressure的counter峰值29,319,168/22,351,872B，不是RSS或物理回收量。两次kernel/IRQ寄存器case均实读/sys/kernel/notes并与准确artifact notes相等。

## 原失败与归档

保留224809 runner模板语法失败（代理/make前），IRQ230535的KVM不支持NOTIMER(E22，Guest init前)，IRQ230738的GDB有符号kernel指针解码错误（IF注入前）。修正只在测试工具与新nonce，没有为让测试通过改kernel。所有原日志/JSON仍在/data验证目录，未覆盖或删除。

小型源码、配置、原始控制台、QMP/退出、4项寄存器前后counter与raw RSP见[验证归档](validation/normal-split-kernel-20261031/)。大二进制/符号/initrd与缓存保留在`/data/neon-build/validation/normal-split-kernel-20261030T224843Z`及收据引用路径。本批未发布正式kernel镜像；所有未闭来源仍AllSourcesClosed=false，VM许可=false，五处normal拒绝未动。Root统一提交/推送。
