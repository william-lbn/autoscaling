# PG17 出生前 exec 环境净化：Linux 精确验收

新代码在资源出生意图持久化与内核写入前净化 exec 环境：删除全部 LD_*、GLIBC_TUNABLES、GCONV_PATH/LOCPATH/NLSPATH/GETCONF_DIR、MALLOC_*，删除调用者全部 NEON_DSGA_*，只从原 root Source 重新加入 AS。拒绝重复键、畸形/NUL 与超过 256KiB 的输入，错误不回显凭据；原存储和控制面 token、PGDATA、正常 locale/业务变量原样保留。原环境数组不被修改。

该入口覆盖受管正常启动与精确 --sync-safekeepers；不依赖先前 SUID loader 的清理，也不因直接 root 调用重新暴露 PG main 之前的环境注入。固定 ELF、库目录及运行时扩展完整可信来源仍须另行封闭。动态 loader 的来源说明见 [GNU libc 官方手册](https://sourceware.org/glibc/manual/latest/html_node/Dynamic-Linker-Hardening.html)。

三个新增 U 根与一个真实 loader S 组件通过。真实 C DSO constructor 在 baseline 中于 main 前创建标记；同一动态 ELF 在生产净化环境中正常运行且标记不存在，业务 token 保留。测试没有把不存在的注入库当成功对照。其余采用的是直接关联的既有 AS/source/WAL/unknown/normal 拒绝回归，全部通过，无 skip。精确根数、子例数与编号见 summary.json/adopted-tests.json；既有根不计入新增覆盖数。

实际 COPY 的全部 680 份 Go/mod 文件在测试容器内计算 SHA，归档时逐项与工作树核对。真实执行用 nonce 与 count=1，复用编译缓存、不复用测试结果；Go1.24.3、race。两个原始运行及 metadata/日志保留在 Linux，完整路径和 hash 见 raw-receipts.json。

这不是原生 PG 出生屏障、同 PID 迁组、完整 seccomp/文件域资格、安全 normal 降限或 KVM/K8s E2E 验收。所有五处 normal 降限拒绝仍保留；该批改动不产生 VMShrinkPermit 或封版声明。
