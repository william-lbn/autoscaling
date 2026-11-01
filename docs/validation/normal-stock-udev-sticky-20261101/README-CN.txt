普通临时tmpfs sticky/DAC机制S归档

最终4c1e21 PASS_MECHANISM_ONLY，实际18c内核/原notes、KVM enabled/present，Guest512MiB/2vCPU，容器2CPU3GiB，原QEMU/Guest/专用child实际退出0，5.136552秒；父Go测试根1+专用独立worker1，原预算Wait terminal。无PG、无Source、normal=false/E0。

只在本Guest新建/run/stock-sticky-own-*/udev普通临时对象：父Root:60004/01771，原rules.d Root:Root0755。专用Go worker AllThreads清所有5capset、附组置空、四UID/GID60004、NNP1，并对当前5个M实读status。control socket、queue文件、data目录三正控成功；原rules rename/remove/实际rawRenameat替换/write、父chmod/ruleschmod六项EPERM/EACCES；父Root后核原rules同inode/原mode与owner均未变，父Root身份不漂。

首5d8f0c原FAIL保留：第三项os.Rename命中Go1.24.3 os/file_unix.go24..42的EEXIST前置限制，没有进入实际VFS。不能据EEXIST签sticky通过；后继只窄改测试为unix.Renameat直达内核，仍仅EPERM/EACCES合格。原前两负控/实际caps事实与未执行后项严格分开。

本S不触碰生产/run/udev、原Root/PG控制对象，不执行被拦截metadata反例；旧17挂载图/宿主设置完全未改。真实stock临时Root准备→有限DB移交→普通UID60004实际九服务仍须后继联合验证。ELF/initramfs等大二进制保Linux原路径，由inputs/manifest SHA定位，不放入Git档案。
