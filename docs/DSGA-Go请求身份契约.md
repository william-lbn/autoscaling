# DSGA Go 请求身份契约

日期：2026-10-20。`pkg/api/dsga`实现与Neon正式SQL/HTTP协议1相同的请求与查询
类型，供后续持久意图、Go客户端及最终fencing复用；**不是旧Monitor协议升级或VM许可**。
尚无完整Go HTTP消费者、持久owner/历史和完整资源承诺，不据类型关闭R03/R09。

测试向量从自有Neon提交`6e63d8d57e3df8ce98c9770c8d322d7c296ca686`的
`libs/compute_api/testdata/dsg_request_v1.json`逐字节复制，固定SHA256：
`91a4adf021a7078a3a11edb0a4084ac0165d5d83fc44ac5a212be69eb968d006`。
协议变化须同步两仓和C/Rust/Go消费者，不把副本解释为会自动追踪上游。

## 数值、字段与边界

- 所有uint64使用规范十进制JSON字符串，不允许number、前导零、正负号、空白、
  小数/指数、Unicode数字或溢出。覆盖2^53、2^63与uint64最大值，禁止float中转。
- 完整key绑定VM UID、Compute实例、PG boot、issuer epoch、Revision及128位随机
  操作ID；不能拿名称、PID或旧VM传播Revision代替。ID按网络字节序拆分两半，
  只拒绝两半同时0；生成使用crypto/rand，不采用浮点随机或顺序Revision。
- 请求全字段不可变，包含目标Buffer/VM bytes、expected generation、预算引用与
  deadline。预算引用不是预算证明。所有身份正值与ASCII可见ID范围与C/Rust一致。
- 原始token扫描先拒绝任意层重复键（含转义后同名），再以RawMessage核对必填/
  精确字段名，从原字节做有类型解码；不先map归一化。Go默认忽略缺失字段、
  大小写折叠或最后一个重复值的行为均不接受。失败不部分覆盖原不可变意图。
- 普通字段即使可为null也须出现；只有显式omitempty可缺省。公共严格对象解码
  限64KiB/32层/2048值节点，禁止UTF8错误、尾随JSON和递归误用UnmarshalJSON。
  有interface的内部扩展解码也使用json.Number保留精度。

网络入口必须调用`DecodeRequest`或在原始body层限制16KiB。标准encoding/json
调用UnmarshalJSON前会剥去外层空白，不能靠类型内部长度检查限制整个body；
已专门测试这一边界。服务端结构合法仍须校验实例/能力、完整预算与唯一issuer。

期限为uint64字符串Unix毫秒；结构解析不根据当前时钟拒绝已过期的原意图重放。
先查历史/幂等，再为新动作计算相对等待，使用有界减法且截到PG60s；超时不会
撤销已发送动作或资源承诺。

## 实际证据

本包race单元测试与fuzz种子通过；15秒fuzz实际1275872次输入、约16秒fuzz墙钟
通过（命令总19.280s），包含畸形/重复/深层/丢失字段与高位往返不变量，不是物理
或跨服务执行证据。后续严格对象误用保护及json.Number补强再执行对应回归。

日志工作区`build/autoscaling-dsga-contract-fuzz.log`、
`build/autoscaling-dsga-cas-contract-final.jsonl`及最新全量/race/lint记录。
没有升级go.mod/go.sum；不构建镜像或部署。Go请求类型仍须继续接入实际客户端、
持久操作存储、Monitor/Agent及VM链，不能把缺代码包装为Linux待测。
