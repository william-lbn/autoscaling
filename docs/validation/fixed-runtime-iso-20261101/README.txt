固定纯数据运行时 ISO：五源组件验收归档

从原整组 raw assets 重新派生 opaque boot 要求，Doc2 固定模式只写六个原始 policy asset；实际 ISO 解包逐字节匹配且不含 command/args/env/mounts/sysctl/swap 脚本。v1 和 unmanaged 保留原路径。

新模式在 Runner Birth 前拒绝未实现的动态 TLS、磁盘、InitScript、命令、环境、SSH、sysctl、swap 输入；创建 ISO 前再次拒绝，实际 sentinel 文件在拒绝后不被 O_TRUNC 破坏。此批尚未实现动态 TLS/业务配置数据合同，不能称生产全部可启动；后续固定数据输入不得退回任意 root shell。

Git3b99eb7 加本批五源，3063 个非 .github 快照文件（包含 embeds）实际逐 hash 校验；字段 all_go_mod_inputs 为历史命名，不代表只含 Go 文件。实际硬限 2CPU/2GiB、无额外 swap，11 根60子例通过，新增2根16子例，直接相关回归9根；静态 Runner 编译退出0。原始源码锁、命令、限额、事件流、终态和 P2 独立复审原文均保留，二进制只归档 SHA。

该证据是组件和静态编译，未执行生产 Runner/Guest/QEMU、Source 或安全 normal 降限。S新增0/E通过0；不与独立 DEV 挂载图合并冒充完整 Provision。
