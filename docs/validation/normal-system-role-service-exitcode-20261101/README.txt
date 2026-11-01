精确退出码窄修：最终补档（supersedes旧164的当前源码资格，不覆盖其历史）

最终准确图为b73+原Root8+本12源，只有listener、新U和native3文件不同于b44。实际5636 COPY逐项匹配；28个Go race根/191子例无失败无跳过。新增1根/55子例含真实子进程exit0/2/7/255、SIGTERM，及逐项Unknown/Wait/creator/持久/外来error拒绝。旧164全部文档、其stage/freeze/manifest及所有4e/b44等原日志保持不变。

真实369/KVM：一个TestNativeNormalSystemRoleServiceInitial S根，七个稳定检查点001..007；不能算7个独立S。Guest/QEMU/initialRoot均0，57.830秒。原四个检查点全部由本新Go ELF重新执行；005/006/007新增实际target exit2/exit7/SIGTERM→143通过原单一Wait、durable exited、broker reply和真实固定PID1 client API返回码。目标compute实际保持12秒跨旧10秒窗口，并等待三个退出码检查点后退出0；原FD9同OFD/FLOCK锁仍保持到真实Wait。005..007并发顺序可异，检查每个恰出现一次。

生产修复只读原child.cmd.ProcessState.Sys的syscall.WaitStatus。只有原done、真实Waited、creator租约done且无creator error，原run terminal/durable且非Unknown，才能映Exited原ExitStatus(0..255)或Signaled的128+signal。非nilWait只能是原cmd.ProcessState同一对象的原*exec.ExitError；finish的Join树有界检查，唯一允许的叶子就是该原WaitError。持久失败/Unknown/额外错误/creator失败/非原Wait/缺ProcessState/无原完成证据都仍Unknown，绝不用errors.As吞掉Join中其它失败。

CLI边界：真实CLI ELF a30d7eb1d8945376d7f45ba36d693823d18c9215041ed4d0ca62118775959bec只静态编译，未在Guest执行。Guest Go ELF a0fdfba9aee005aa783fde0781d50dccebd6aee49458bb248a629ad76e1d21ac由TestMain路由真实生产Start/RunBroker/RunClient/Root initial/FS函数，客户端返回码通过该真实进程os.Exit路径；不能称最终生产CLI全链已执行。其它服务/compute为明确静态替身；新增logs/clock/acpi仅用作退出码探针，不是真vector/chrony/acpi服务兼容验证。BusyBox init/vminit/musl真实，13row空环境PPID1、原4FD/FS/probe-ready-COMMIT等原边界同旧档案。

Source/ordinary9 controls、typed stdio、真实compute/PG、Host/完整I/安全normal降限和K8s E仍未完成。Root8未修改且独列reviewed hashes。Git/index/提交由Root统一，本最终stage union=旧历史文档+新补档+当前12源码，不擅自stage Root8。
