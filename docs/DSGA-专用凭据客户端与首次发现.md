# DSGA Agent专用凭据客户端与初始PG绑定

2026-10-21，V3增量模块。默认关闭，不是完整在线扩缩容交付。

`pkg/dsga/credential`消费中心专用TLS监听器的短令牌，不读取/持有签名私钥。
可信配置固定HTTPS根URL、管理CA文件和专用audience投影SA令牌文件；SNI固定为
`<VM名>-management.<VM namespace>.svc`。正常TLS链/主机名验证后，还要求叶证书
包含管理域wildcard，拒绝已经分发给Guest的旧per-Compute叶证书。管理域私钥
只挂中心，CA及SA投影文件按平台可信方式提供；代码不生成这些部署资源。

真实owner Session提供生命周期、完整Ownership及Check。中心仍独立TokenReview、
核对SA/Pod/Node、Lease和完整journal；客户端接口不是远端认证捷径。每次Token/
Identity读取先检查同一owner，网络/读文件不持缓存锁。并发刷新只有一个请求，
等待者可以独立取消；所有调用有有限期限且随owner取消。失败不使用旧缓存，不
跟随重定向、不采用环境HTTP代理、不打印Bearer。SA文件每次刷新重读，CA每次
新TLS握手重读，支持投影目录原子换代。响应限制16KiB、严格字段/身份/有效期及
no-store。刷新提前10–15秒随机散开，不承诺尚未实测的集群规模或QPS。

首次获得可信Compute实例后固定到本Client；刷新回复出现另一实例时永久失败关闭。
令牌过期/刷新不会重造长期Compute SDK或清除未知原意图。SDK的执行与观察连接池
独立，POST不透明重放。中心和原生Compute各自验签，客户端不另造签名权威。

`compute.DiscoverInitial`只允许未指定boot且无Pending的初始协商。生产
`Session.NewPGWorker`先读取同一完整journal：有HWM就恢复其原完整实例，即使
Active为空或历史被淘汰也一样；只有HWM/Active/History/ExpiredResults/Pending
全空才允许首次发现。返回Client前固定boot，之后不自动改绑。已有journal的CIID
或实际PG boot变化必须拒绝并等待后续受控恢复，不能借发现抹掉历史。

## 实际Mac验证

Go1.25.13，受影响范围：`./pkg/dsga/... ./pkg/api/... ./pkg/agent/...
./pkg/plugin/... ./pkg/neonvm/controllers ./neonvm/apis/neonvm/v1`。

- `go test -p=2 -race -count=1 -timeout=150s -json`：303个用例pass事件
  （含父/子用例），13个有测试包通过，9个无测试文件包单列；0用例fail/skip。
- 带`dsga_native_e2e`标签的同范围golangci-lint：0问题。
- 主仓`ci/test-dsg-go-client.sh`使用实际PG17 `c8482ee`安装：1个Rust外层/
  2次独立Go race进程、真实TLS→Compute Router→SQL→PG，9.24秒，通过且
  零skip/ignored/filtered；执行了epoch2 owner持久循环及响应丢失后的显式重放。
  该门禁仍使用原原生测试TLS桥，不宣称已覆盖新增中心网关/SA凭据整链。

日志位于工作区`build/autoscaling-agent-credential-{final.jsonl,final.stderr,
lint-final.log,native.log}`。本包TLS服务端/Authority、选主API的本机协议夹具均
不冒充真实K8s、RBAC或etcd；新增中心专用监听器的跨仓联调另行验证。

Linux K8s投影/证书轮转与网络隔离、所有预算/Guest/Host/VM领域、物理回收及目标
性能仍未验收；其中尚缺业务代码必须继续实施，不能只写成待Linux测试。
