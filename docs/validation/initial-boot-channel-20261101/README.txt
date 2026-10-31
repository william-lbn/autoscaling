固定早期启动线与原QEMU私有通道（组件归档）

共享12字节DSGB/version/kind/长度头，读取前先拒未知类型/版本或0/>8192长度；
严格typed JSON。写前校验，短写/错误不隐式重试。InitialMovable完整保留历史
Preboot，当前Root身份独立；expected/4096精确比页数，不对任意pages做乘法。
InitialCompletion匹配准确原Movable receipt摘要；它只结束初次启动握手，不签normal。

Host固定原StateDir/initial-boot.sock、原pidfd/proc/start、Unix peer UID/PID和原exe
dev/inode/SHA前后复核。单次三分钟有界会话，调用ctx更短时从严；取消/错源/错帧/
错phase关闭流，不unlink旧socket或自动重发。不消费Wait，不杀进程或退预算。
调用者必须先关闭/join channel后销毁原process，不能重建channel绕过WAL Unknown。

精确Gitad96425+本批六源，828Go/mod实际COPY逐hash、禁网race13根64sub全过，
新增10根28sub，旧preboot3根36sub仅直接回归。真实Linux子进程/Unix peer但Guest
回执是显式模型，非QEMU/PG/KVM证书。原失败423d74的100ms测试在原exe hash期间
过期，子进程未收challenge便EOF，严格Wait断言抓到失败；最终只改_test用私有pipe
确认完整challenge后才cancel，生产de83字节不变。历史失败和前置通过cohort全保留。

P1独审原文与全部命令/源码锁/资源范围/退出码保留。actual_source是COPY候选，
不宣称每个Go文件都编译。Host旧ad KernelBuild字段为reference摘要的跨轨差异由
后继改成唯一artifact SHA；本批模型不能认证那条真实链。后续还须生产Host/Guest、
8MiB virtio-mem、sysinit/完整Source/normal/端到端。S0/E0，原五拒绝保持。
