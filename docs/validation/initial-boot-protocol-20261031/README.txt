Host/Guest共用早期启动线合同（版本1，非授权）

共用RunnerIdentity、InitialPrebootChallenge、InitialPrebootReceipt与完整绑定校验。
字段/tag/order保留Host私有原草案；旧vmguard.Identity及v1账本不迁移。PG版本不进入
合同。显式base与业务min/max/block检查，不把静态上界当实际base。额外检查min-base
在同block网格。Nonce沿原Host64位小写hex语法；格式不能证明随机来源或资格。

严格原control解码器拒任意层重复键/未知/大小写别名/缺字段、非字符串或前导零
Uint64、尾随与超过8192字节。匹配先Validate双方再完整struct相等，跨source、
Host阶段、nonce、Runner、ISO、Secret、kernel与base均拒。没有verified或permit。

准确Git1a25125+仅2源，810Go/mod实际COPY逐hash，禁网race3根36子场景全部过，
无skip/fail，source/test/build0，总构建4.303秒，独审原文保留。新增稳定ID见catalog。
只测线格式和绑定，未运行Guest/实际Host/通道，不认证输入摘要真假，S0/E0。
实际生产者必须核原API持有、QEMU pidfd/peer、固定ISO/WAL/当前kernel与原nonce；
不能把本包Match成功当准备完成、首次grow或normal安全资格。
