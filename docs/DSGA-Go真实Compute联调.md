# Go 客户端与真实Compute/PG联调

2026-10-20。新增`pkg/dsga/compute/native_e2e_test.go`，使用显式
`dsga_native_e2e`构建标签。常规183事件测试不包含本用例，不把未编入的用例算
成通过或skip。严格门禁编译Go race测试程序，真实执行两次，分别覆盖正常阶段
与PG重启之后；两次都无skip。不是人工响应夹具。

宿主驱动在自有Neon功能分支`ci/test-dsg-go-client.sh`：

```bash
bash ../neon/ci/test-dsg-go-client.sh /absolute/path/to/pg17-install "$PWD"
```

两仓须检出本系列功能分支；脚本接受明确源码路径，记录Go SHA，检查准确PG17
版本锁/二进制，编译`go test -race -tags dsga_native_e2e -c`。Mac本次使用已有
Xcode14.4 SDK/clang以及Go1.25.13；需要时在命令环境显式指定SDKROOT、CC及CGO
sysroot编译/链接参数，不修改全局工具链。Neon Rust依赖缓存使用工作区build/cargo。

实际链路：Go生产Client→测试TLS终结点→真实Compute外部Router/专用JWT中间件
→长期owned双SQL客户端→准确486338 PG及完整Neon1.7扩展。TLS仅由本机测试证书
终结，上游固定本次新建loopback服务，禁用环境代理；没有将生产Client改成可
接受HTTP或跳过证书。一次性JWT由Rust内存生成，经匿名stdin传入，不放argv/
env/日志/账本，不读取用户密钥或用户数据库。

实际验证：错误旧scope的有效签名JWT被拒绝；1024→256→2048 Buffer增长/收缩/
再增长；完整含引号/反斜线/HTML字节的预算ID摘要；独立历史result1024与当前
observed2048；实际TooLate取消、同Key改预算Conflict，不产生VM许可。

第一次PG已完成的resize回复被TLS终结点改为503，仅此处注入丢回复。Go保留原
意图，独立查询查回真实Committed，冲突新意图没有发送，完整原意图显式重放得到
Replayed并清除客户端栅栏。没有透明POST重试；查询成功本身不清未知意图。

随后Rust真实重启同一PG，保留原Compute OnceCell，确认它返回503。另一个Go
进程亦不能重新绑定旧上下文，不因重建Go对象视作空闲。测试在Go结束后直接SQL
校验600行数据的count/sum/MD5；数据体积大于2MiB bootstrap，重启后再次校验
相同值。不是从HTTP body或人工数据估算正确性。

最终新门禁：1个Rust外层集成用例、2次独立Go race用例，0failed/ignored/filtered/
skip；外层4.54s，Go正常0.18s/重启后0.01s。Go按构建标签lint范围0issues。
日志位于工作区build/neon-dsg-go-native-strict-final.log与
autoscaling-dsga-native-lint-final.log。重构共享夹具后，Neon原有SDK8/HTTP11及
两包普通34/30、全targets本包Clippy均另通过；具体日志在Neon当前状态页。

此测试手工创建ComputeNode运行态/启动私有PG，没有运行整个compute_ctl启动流程、
Monitor/Agent持久循环、Scheduler、Runner或Pageserver/Safekeeper，不构成完整
Neon存储或VM内存端到端。预算ID仍只是引用，没有预算证明，物理回收不在本门禁
认证范围。后续继续实现可信owner、完整硬预算/quiesce、PG接管fence和最终VM
执行链；Linux/VM/K8s/性能后验，默认关闭、不构建镜像、不部署、不合主分支。
