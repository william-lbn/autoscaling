本批：PG版本中立声明3 / Origin2 / transfer2，原四FD顺序不变，仅追加原KernelPrep2只读OFD。
运行路径是准确18c Kernel2原准备→原Host6/7/单8→同g原Memory确认→真实FS2→R2严格5FD接收→FS/KernelR2/MemoryR2绑定→真实probe/ready/COMMIT。
R1活时和正常退出后都在同g/resource锁下实读原typed Kernel/Memory消费者。不是从JSON、当前PID或请求bool重建Source。
声明1/2仍严格旧四FD；新字段包括null/empty在旧版拒绝；新Origin2/reference递归精确键/必填/定长数组，跨版本/错build/错Root/错namespace/多或少FD均拒。
U最终10根84sub race全过/零skip，Linux CLI、Host/Guest native与库存导出全0。真实S只记一个Host/Guest配对根，001..010是同根检查点，不算10根S。
真实512M基座→1GiB目标/max4GiB，2CPU、无NIC；原minfree按512M硬上界调用生产helper得到4096KiB，先写RO policy再KernelPrep封存；运行后没有补调控制。
同源第二遍补实际native容器cgroup：3GiB/no swap/2CPU/pids128，只读Root无NIC，终态Exit0/OOMKilledfalse。第一遍c2仅参数声明，不能冒称当时已独立实读。
Darwin包编译先0但CLI因旧公开Gate缺!linux stub失败，原日志保留。新增!linux stub只Unknown，最终Darwin包/CLI0；Linux三个ELF与实际S二进制逐字相等，见portable证据。没有把新增文件说成已实跑S图。
原失败完整留在/data外部，history给逐hash索引；大ELF差异日志只存位置/sha，不删除或覆盖。256M页分配告警与2048→2593真实漂移保留，不把其当PASS或隐藏。
Host API/预留仍模型；其他服务有明确standin，udhcpc无NIC错误不是DHCP兼容证明；普通2安装器/Root动作/真实PG未接本批。Linux生产CLI仅编译，Guest由TestMain调用生产函数。完整Source、safe normal资格与K8s E仍false。
Root array_service_kernel_linux.go及Kernel31属于其它owner，单独reviewed hashes，不随P2 stage冒独占。后继Gate2/typed stdio/role lease在独立新prefix中，未混入本freeze。
