# Guest 启动阶段诊断

真实11b测试VM已启动QEMU，但Guest初始化失败，控制台只显示后继resource guard的“文件不存在”。原Pod的运行时ISO已被正常GC，不能据此推断旧脚本存在或某一步挂载成功。本批只补充定位能力，不声明修复了启动根因。

vminit在失败时报告固定阶段和原数字退出码；不打印Runtime/Source正文、ISO内容或环境凭据。原挂载、目录及父链检查、旧脚本拒绝、RW临时视图清理和退出码均保留。initial-boot-enter仅表明到达原exec边界，不代表Provision、Root资格、PG或资源许可通过。

1个测试根、8个子场景共9个PASS事件，race/skip/OOM均为0，实际40.138秒。覆盖原成功到exec、两类PG路径别名、resolver别名、bind失败以及securityfs已挂载、ISO挂载失败、禁止旧Runtime脚本。外部mount mock不是真实Guest挂载证明。首轮两个新mock路径匹配错误失败保留；仅修最长/runtime前缀映射后复验，没有放松断言。

同批按原六个-X基础镜像参数构建唯一正式Builder，实际ELF为80d101c26a016bc169eb5c0d6e68c131734d2ebb75673e1df4262393f1c6249a，CLI help退出0。Go元数据未显示ldflags，参数依据成功执行的原固定Dockerfile命令，不冒称元数据中存在该字段。

原始证据：/data/neon-build/validation/pg17-vminit-stage-diagnostics-repaired-20261103T194325Z-816ba3。下一步以新Builder重新采集System/Native绑定、生成新Guest并读取实际磁盘；隔离单机早期启动只定位初始化，不授予Root/PG许可。真实Guest、SQL、资格和四项内存端到端尚未通过。
