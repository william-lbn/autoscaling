stock udev kmod31-r2输入原语 / 2026-11-01

新2文件，pinNormalOrdinaryStockUdevKmodInputs(ctx,*Inputs,*RuntimeDirs)→private对象；
recheck/snapshot深副本/close。只从原Inputs/System3取完整准确Kernel五元组并核实际uname；
模块根固定/lib/modules/<原release>，非调用者路径/预算/ready bool。两个原opaque已close则拒。
固定static3根/etc/modprobe.d、/usr/local/lib/modprobe.d、/lib/modprobe.d以及整个模块根，
必须真实RO目录为空或真不存在且钉最近实际RO父；任何regular/特殊/目录项/hidden/config/
softdep/indices/payload均Unknown，不按.conf glob漏过。最终lookup根alias拒；只保原System
默认/lib→/usr/lib祖先同构布局，不开放其它alias或任意模块支持。原输入路径每次复核原pin/
实际state/devino/mountID/namespace以及RO祖先，不从省略metadata推缺失。

/run/modprobe.d仅原RuntimeDirs.run独立dup FD下真实ENOENT；反复核原RootRoot0755，存在任何
对象均Unknown，不mkdir/chown/delete。输入Root supervisor env复用同normalOrdinaryStockUdevEnvironment()
准确新slice三项PATH/LANG/LC_ALL；严格核顺序/完整值/无重复额外变量，无KMOD_LOG；不取本进程
os.Getenv冒childEnv，也不加MODPROBE_OPTIONS/shell。Root start只用同固定函数且不再改Env。

/proc/cmdline固定FD是原procfs regular/Root只读文件mode、64KiB有界内容SHA/bytes，重核原FD
并重开同路径，不输出cmdline原文。/sys/module/compression只允许精确18c全tuple的编译预期缺失：
config966e6151 MODULE_COMPRESS未设（没有显式NONE=y）；Kconfig DECOMPRESS depends COMPRESS，
Makefile仅MODULE_DECOMPRESS编decompress.o，属性只该TU的SYSFS late_initcall创建；实际生成宏一致。
仅exact18c + 原Runtime namespace/17graph + 原sysfs/module父FD + 真实ENOENT才接受；出现节点、
其它errno、其它kernel都Unknown，不把libkmod宽松open错误fallback当none。原sysfs保RW热增语义，
不机械套静态ext4的RO条件；静态四根仍严格RO。完整准确来源锁与835c4198证明见外部proof副本。

首3ac771：Git97d+Root339616 immutable10/deleted1+InputsIUSR001/Runtime冻结4+仅Kmod新2，
1364实际Go输入COPY逐hash一致；新5race根34子例，原Inputs6/41+Runtime5/27直接回归，
总16根102命名子项全PASS零skip，LinuxCLI/Darwinarm64编译0。3CPU3GiB单RUN、Go2/768MiB，
原Wait与原unit终态后预算释放。模型仅私有temp树类型/空目录/alias/全部资源拒绝、固定Env/Kernel
谓词/缓存副本，不制造真实RO构造正控或借Kernel环境probe算S。本地生成器一次SyntaxError在
远程执行前发生，属于ENV并如实记录，不是Go业务失败；首实际Go图无fail。

关键历史边界：339616当时MF仍在/etc/neon-dsga持久RW孔，真实RO Inputs入口不能通过。
Root已发现并迁common路径至/neonvm/config/dsga-ordinary-stock-udev-manifest.json，本包业务仅
引用共同constant无literal读者绕过；原cohort不翻签新路径，不放宽RO。后继精确组合应取新Root快照。

当前S0/E0/Sourcefalse/normalfalse，生产constructor未完整接线。Root factory还须消费真实KernelPrep2
模块禁用opaque、同g原R2/3actor/全部writer真Wait及实际stock处理和seal；这些不是本引用JSON或
一个Bool能产生的能力，不删除原normal拒绝。
