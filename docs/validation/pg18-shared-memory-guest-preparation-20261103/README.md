# PG17/PG18 共用真实内存验证主链

只提取已有 PG17 DEV 主体供版本17/18包装复用，不修改生产预算、Root权限或公共协议。原PG17显式opt-in保持；PG18须另选固定opt-in。就绪后从原真实管理SQL读取server_version_num并严格校验major，固定同步日志标签不能当版本证据。

编译和入口路由1根3子通过，Guest ELF固定于receipt.json；447/ddd两图产物逐字一致，只计一次编译组件，不增加功能通过数。当前尚未跑PG18实际SQL/normal/VM周期，Source、端到端与K8s均未验收。

完整原件位于 /data/neon-build/validation/pg18-trusted-control-guest-preparation-20261103 及对应447/ddd nonce；精确逆变换证明除四处已声明改动外主体完全还原旧PG17源，SHA f4d4e4cb16028a317dc2be2a0396175021b0b342ce7d828021ae7c0d455702a6。大日志、二进制与运行私有输入不入Git。
