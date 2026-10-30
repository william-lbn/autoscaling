# normal-v1准确内核：独立TEST_ONLY构建与真实KVM能力实证

状态 **PASS_CAPABILITY_COMPONENT_ONLY**。本批只给neonvm-kernel/Dockerfile增加可选AMD64_KERNEL_CONFIG；默认linux-config-amd64-${KERNEL_VERSION}，ARM/依赖下载/普通make行为不变。实际frontend默认配置导出639ea751...、指定profiles/normal-v1/...导出B20...均PASS_SOURCE_ONLY。P2配置文件由其独占，未由本执行者修改。

normal-v1按P2最终三项：STATIC_USERMODEHELPER=y、STATIC_USERMODEHELPER_PATH=""、CONFIG_LSM加入landlock（base虽编译Landlock，未列LSM不能推定激活）。未修改旧physical常量/guard以取绿。

## 精确构建

- source base AS7ed9356424474f64489dec1f92a9eb864aaead71，kernel源码/patch来自该固定Git对象；仅叠本批Dockerfile参数与P2冻结profile/来源文件。独立只读tar保原Git文件mode，避免影响既有依赖/source缓存；没有活动Go/PG树构建。
- 独立VERSION_SUFFIX=-william-normal-v1-b20ac4f85d74；8jobs，真实make与导出exit0，396.55秒。无正式镜像发布，仅TEST_ONLY raw artifact。
- release：6.12.26-william-normal-v1-b20ac4f85d74。
- artifact SHA256：552ffabd096736e38d2d4b6c119533e4a26c2b9a47f6e04a8e1795a084f8d85d。
- build ELF .notes及真实Guest /sys/kernel/notes字节相等，SHA256：3d22f15739bcd10735099d344c1c306f6bd7eccfbed8b3e4f1295e5893036d6b。
- **实际resolved .config** SHA256：935e43773879d6a4f5c27c2ef966314de7e0e463a4a9bebefa3dc5dcf283ce5b。
- **输入source config** SHA256：b20ac4f85d74b6dfca4f207cbee3cd72f6b34307fc1a4fee13519eba244ed1f5。两者不同，不能把输入B20叫作实际编译config。差异仅release注释头和CONFIG_CC_VERSION_TEXT归一为Ubuntu cross-gcc13.3.0~24.04.1；GCC_VERSION130300及所有策略设置相同。
- 5patch canonical SHA256 d3b3b463402e6df0058d3a5fc3b6f944d0efda3183c00652fe6e50fb2d38a268；算法为sorted compact JSON相对patch路径→文件SHA的摘要，完整文件表见runtime-proof。

## 同initrd真实KVM对控

两个Guest完全相同initramfs b38c151fd8185699d2b8942aae8491d70920aff7054de8723f90e6d427d0494c、相同cmdline，独立bootID；固定QEMU8.1.5/f170364e...镜像仅直接entrypoint取QEMU，不运行旧Runner。强制KVM/CPU host，2CPU/1GiB，无TCG，Docker network none、Guest -nic none。150秒外部/90秒Go保护，本测试容器均清理。没有Host网络/配置/已有VM/K8s改动。

**PG17-S-UMH-001**：向两个Guest相同/sys/kernel/uevent_helper写入可执行/umh-helper.sh，真实读回一致；向固定/sys/devices/virtual/mem/null/uevent写add，两者trigger exit0。marker脚本只认精确ACTION=add/SUBSYSTEM=mem/DEVPATH=/devices/virtual/mem/null。
旧0ab3内核正控真实执行marker；新552ff内核同输入等待5秒无marker。正常PID1/bin/sh exec均成功。证据是对控和绑定准确构建，不是单看未出现文件；只测试了这一个实际UMH路径，编译字面量空路径的全局语义另由源码/config绑定。

**PG17-S-LANDLOCK-001**：旧内核实际LSM lockdown,capability,yama,apparmor；Landlock query ABI=-1/errno95，probe/Guest exit2，明确记为unsupported，未伪写整体PASS。新内核实际LSM lockdown,capability,landlock,yama,apparmor，未用boot lsm=覆盖；ABI6。独立单线程静态C探针先证两文件都可读，再NNP=1+仅允许目录规则：允许读取成功，外目录读取errno13/EACCES；probe/Guest exit0。这个单进程READ_FILE能力测试不是完整生产PG过滤器。

既有**PG17-S-WIRE-001**在两kernel均10子例PASS：真实UID/GID65534、SO_PEERCRED/类型/短长帧、1/64FD清理和超时；使用7ed同步fixture严格Wait及FD相等，无覆盖ID增量。

新kernel原始物理观察：4KiB页、possible CPUs=0-1、online_movable；K272228352B、Movable800976896B。此pair未额外运行新producer的swap/THP/KSM及完整runtime guard，不能由编译config/能力收据代替；P2新reader必须实际执行全部检查。原旧physical观察接口只认旧release，**没有在新variant上改常量或伪装release**。

## 证据与边界

runtime-proof.json逐项保留旧不支持exit2、新exit0、两原始console/boot/notes/LSM/zoneinfo及同输入绑定。lsm sysfs无结尾换行，原console连接下一BOOT_ID标记；解析器按明确marker切分，原文不改。landlock-probe.c/guest-init/marker脚本及编译镜像ID/hash公开在收据里，不以stdout诊断行或QEMU关机0代替Guest结果。

没有PG、Root Pending/Enrolled、原A/R及全部filter绑定、完整持续收费/在途包络、normal限额写、VM伸缩或K8s/E2E。任何生产降限仍须后续完整合同与实际验证，本结果只提供准确内核能力事实。

完整外部档案（包含不可变source tar、实际Dockerfile/命令、build.log、raw artifact、实际resolved config和ELF notes、编译器/源tar SHA、全部KVM数据）：
/data/neon-build/validation/pg17-normal-v1-kernel-20261030T165552Z

本目录仅小型可review收据/源码/原console，不提交内核二进制。旧0ab3产物与旧失败全部保留。
