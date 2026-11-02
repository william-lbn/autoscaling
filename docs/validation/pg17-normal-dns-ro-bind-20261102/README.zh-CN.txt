只修正常normal镜像构建的真实DHCP DNS写入：原Alpine hook在RO /etc创建resolv.conf.$$并rename，与vminit已保留的原单文件RW bind孔不兼容。prepare-root只按确切行计数改DNS输出group→原resolv inode，不改原IP/route、legacy、网络配置或Root门；DNS写入失败明确exit1。

实际4d98a5：显式/bin/sh entrypoint执行完整生产prepare-root，实际hook由181f37e2变8a3296af。隔离Docker根真实read-only且仅resolv单文件RW：原rename对照不能更新，新hook保同inode写出search+两DNS；把同孔改RO则真实exit1且原内容不变。3机制case均PASS，共1.974秒。预留native2CPU/3GiB，各容器实际1CPU/256MiB，network none/仅隔离NET_ADMIN；没有宿主网络变化。

先前242a73被缓存stock image默认/bin/true吞命令，虽然进程0但SHA不变/无DNS，已原样保FAIL，不当通过。此前reserve大小写unit拒也无实际执行。原始stdout/mountinfo/cpu资源/生产输入SHA保/data目录。这里只是实际Linux文件系统/脚本机制，非Guest18c、PG、normal许可、K8s或E。
