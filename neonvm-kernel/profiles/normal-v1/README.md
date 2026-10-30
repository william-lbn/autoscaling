# normal-v1 独立受限内核配置（2026-10-31）

本目录仅提供 PG17 normal 真降限候选 profile 的编译配置；不是出生资格、运行实证或降限许可。旧默认 config、旧 artifact 与现有五处 normal 降限拒绝保持原样。

## 三项准确变化

原 `neonvm-kernel/linux-config-amd64-6.12.26` SHA256 为 `639ea751eb4ae7743e2695368286d1aeb0417f6680d6ac24ae3b051948a94b96`。本目录同名配置 SHA256 为 `b20ac4f85d74b6dfca4f207cbee3cd72f6b34307fc1a4fee13519eba244ed1f5`。逐字节差异为：将 `# CONFIG_STATIC_USERMODEHELPER is not set` 替换为下列前两项，并将原 CONFIG_LSM 名单加入 landlock：

```text
CONFIG_STATIC_USERMODEHELPER=y
CONFIG_STATIC_USERMODEHELPER_PATH=""
CONFIG_LSM="landlock,yama,loadpin,integrity,apparmor"
```

原 CONFIG_LSM 为 `yama,loadpin,integrity,apparmor`；准确 `security/landlock/setup.c:75` 的 DEFINE_LSM 没有指定 order，默认 MUTABLE，并非始终启用的 FIRST。原 CONFIG_SECURITY_LANDLOCK=y 本身未激活 Landlock。原启动必须排除 lsm= 覆盖/重复漂移，实际核 securityfs LSM 列表、Landlock ABI 与 allow/deny；不把编译选中当执行资格。

路径放在 profiles 子目录，避免旧 `echo-version.sh` 根目录 `linux-config-*` glob 选错版本。amd64 专用，不扩大 arm64 支持范围。

## 可选构建入口

由构建负责人为原 Dockerfile 的 `build_amd64` 段增加可选 `AMD64_KERNEL_CONFIG`，默认值仍是 `linux-config-amd64-${KERNEL_VERSION}`；只有显式选择以下路径才编此 variant：

```text
--build-arg KERNEL_VERSION=6.12.26
--build-arg AMD64_KERNEL_CONFIG=profiles/normal-v1/linux-config-amd64-6.12.26
--build-arg VERSION_SUFFIX=-william-normal-v1-<本次冻结源码短SHA>
--target kernel_amd64
```

旧构建默认语义不变。必须导出实际生成 `.config`、准确完整 release、patch/source SHA、bzImage SHA/size、ELF notes SHA、构建命令与日志，并重新锁到新 artifact。`make olddefconfig` 后若出现额外变化，必须审核而非继续声称只有三项。旧 `0ab3f714...` artifact 的 source/config 事实不包含关闭 UMH，禁止借旧 Observation/Origin 常量补签。

## 源码依据与真实负测

准确 Linux 6.12.26 `security/Kconfig:183..216` 明确空路径关闭全部 UMH。`kernel/umh.c:369..372` 将路径固定为编译字面量，`:423..424` 在 `queue_work(:434)` 前直接结束，因此运行时修改 modprobe 或 uevent helper 路径也不能重新产生 UMH 用户进程。`init/main.c:1378..1390` 的 PID1 启动直接调用 kernel_execve；普通用户 exec 同样不经 UMH，仍须真实启动验证。

实际 KVM 建议使用相同 initrd 在旧/新内核做成对控制：root 将 `/sys/kernel/uevent_helper` 设置为实际可执行 marker 程序，向同一 `/sys/devices/virtual/mem/null/uevent` 写 add。旧内核必须实际出现 marker，才可把新内核同输入 trigger 成功且 marker 不出现作为禁 UMH 实证；旧正控未触发不得算 PASS。并验证正常 PID1/shell exec 仍成功。KVM 禁止 TCG、独立记录两内核与 initrd/脚本 hash。此负测不能替代生产设备 allowlist、模块/加载路径、Root/System 归组与原启动顺序。

## 最早 Root 残余的封存合同

新 VM 最初只暴露原固定基线 base RAM；`kernelcore >= boot totalpages`。准确 `mm/mm_init.c:449..454` 此时没有 ZONE_MOVABLE；Runner `main.go:443..459` 的既有几何以 minSlots×slot 为 base，virtio-mem requested-size=0，但当前代码尚无下述原门禁。

第一次任何 Movable online/hotadd/QMP requested-size 增长之前，必须完成原只读 source/Origin、独立非授权 PreBootstrapSystemPlacement（包括 PID1/原 sysinit/helper 的 System 迁组）及持久确认，并冻结未来 Root/UMH/设备/FS/namespace 写入口。正式 Bootstrap 的 normal+wholePool+Outside≤当前 GuestMemTotal 检查不能在约两份 K 的 Outside 大于 base 时放宽；必须等原 Host 预留的首次 Movable 增长完成后再执行。Runner 必须持原来源认证该确认及原零初增状态，不得凭某次 QMP=0、当前空 PG 组或普通 ready 文件追认历史。失败保原 Pending/Host 预留，不先扩内存再补事实。预归组只安装原 System cap、迁移原用户和保留独立阶段 WAL，不生成正式 Bootstrap.Applied、PG birth、RootHeld 或许可；PG/vmstart 始终拒绝。首次增长后真实 Guest usable 达到正式原预算才允许完整 Bootstrap。前阶段 base 真实启动存活与后继无 Root 新收费仍须单独验收，不能把 System cap 当作 base 已容纳完整预算。

早期 root 用户 folio 数量最多为原 base 对应的物理页数，迁组不转移旧收费。后继迁移可以改变位置，因此 Outside 要分别保留 `K_kernel + K_initial_root`，其中 `K_initial_root=pageCeil(originalBaseBytes)`；不得一份 K 同时覆盖当前 opaque kernel 与迁出的旧 root 页，migration 双份另入 Transition。静态 UMH 关闭仅排除一类未来 root 用户 mm 生产者；还需从实际允许 Guest 代码/设备/FS 证明没有其他 kernel/root 后台新用户 folio 收费绕过 System。当前 1GiB/kernelcore256M 旧 KVM probe 不具有此历史，不能补签。

## 验收状态

配置差异、原配置未变与 SHA 已核；新 variant 编译、实际生成 config、KVM UMH 正/负控制和生产最早拓扑封存均由后继收据记录。此文件创建时均未执行，不授予 normal 降限。
