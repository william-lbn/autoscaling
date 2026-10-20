# DSGA 远程凭据主体核验

更新：2026-10-21。此批实现远程签发器所需的生产身份核验核心，并让现有选主
Session复用同一条完整journal核验路径。**不是完整凭据服务或Agent入口交付**；
尚未装配中心HTTP签发入口、Compute目标/私钥查找、Agent投影令牌消费及Driver。
功能继续默认关闭。没有创建镜像、准备部署或修改主分支。

## 身份与读取顺序

`owner.Options.PodUID`采用Agent的Downward API Pod UID；`New`仍为每个进程生成
128位随机数，holder编码为`dsga-<32位小写hex>.<PodUID>`。同Pod内重启或两个进程
不能共用holder。UID限制90字节及无分隔歧义的字符，使完整holder不超过journal
现有128字节上限。空PodUID保留已有嵌入式调用兼容，但不能申请远程生产凭据；
生产Agent接线必须显式提供，不得在缺字段时退回旧路径。

`Session.Ownership()`只返回待核对值；复制、自报epoch或取得该结构不产生权限。
`VerifyCurrent`使用签发器自己的Kubernetes客户端执行：固定绑定Lease GET →
完整原journal GET/受限解码 → 同一Lease再次GET。检查VM UID/incarnation、Lease
UID/归属/holder及原journal已持久化Owner。允许同holder续约的RV变化；journal
读取期间同名Lease被重建，即使holder不变也拒绝。Session.Check已经调用此实现。
Snapshot.VirtualMachine返回同一次journal读取的VM深拷贝，不能修改CAS原快照。

多对象GET不是原子许可，最后读取后仍可能换主。Session检查另有RenewDeadline；
远程签发器采用自己的总期限。它们都不替代原生PG issuer floor及最终Runner
fencing，不清除已派发未知意图，不撤销预算，也不自动接管/初始化日志。

## PodVerifier生产规则

签发器必须显式配置唯一允许的Agent ServiceAccount namespace/name，并使用自身
RBAC身份的typed Kubernetes客户端。Agent的Bearer仅放在TokenReview.spec.token，
请求audience固定`neon-dsga-credential-broker`，不把它当签发器访问apiserver的
Authorization。拒绝非Authenticated、错误状态、非专用返回audience、其他SA
主体、缺失/多值Pod绑定；不本地解析未验证JWT。

随后GET真实ServiceAccount和Pod，核对TokenReview的SA UID/Pod UID及实际对象
namespace/name/resourceVersion；删除/同名重建、非Running、SA或Node不匹配均
拒绝。Pod UID必须与完整随机holder后缀相同。可选node-name声明一旦返回就须
匹配真实Pod；Node UID不是Pod字段，最终节点资源操作另行核验，不能按名称猜测。

再用VerifyCurrent核验完整VM/Lease/journal；VM须Running/Scaling、Runner存在且
VM.status.node等于已认证Agent Pod.spec.nodeName。续约新鲜度采用签发器时钟与
显式clockSkew保守收紧：拒绝零时间、超出容忍的未来续约，以及加上clockSkew后
已进入Lease到期边界的记录。不能由远端墙钟证明最终执行权。跨节点时钟偏差及
实际API延迟仍须目标环境验收；短于容忍窗口的Lease会保守拒绝，不静默放宽。

整个TokenReview/SA/Pod/Lease/VM检查链有独立有限期限，Bearer最大16KiB且只能
为可见ASCII。认证错误统一返回无秘密的ErrCredentialsDenied，不回显Bearer、
TokenReview私密错误或Kubernetes原始body。没有正向授权缓存，没有在失败后
初始化/Takeover或释放资源。HTTP入口还须拒绝重复Authorization、限制请求体、
采用验证TLS且禁止重定向；本模块不是HTTP路由，也不代替这些入口责任。

## 中心签发与Agent尚待接线

签发器还须从可信VM业务归属找到owned Compute配置/凭据，核对DynamicSharedBuffers
VMUID/ComputeInstanceID及JWK与私钥相符，固定实际TLS目标，不接受请求方URL。
密钥保留中心Secret，Agent仅持有专用短期令牌。签名紧接本次核验并绑定当前
compute_id/VMUID/ComputeInstanceID/issuer_epoch、专用audience/scope与有限期限；
读取密钥/配置后须重新确认同一owner，不长期缓存Snapshot当许可。PodVerifier
本身不持有签名私钥，不签JWT，不把自己的成功解释为完整目标绑定已完成。

创建Driver还缺Runner启动前的显式初始化窗口：实际VM UID要在创建后才能绑定
Compute配置和journal，现有Controller会自行进入Pending并分配Runner。不能给
已经运行的VM事后打标，也不能把这项未实现归入Linux待验。受控首次PG boot
发现/跨owner或boot的SDK关闭、完整预算/Monitor/Runner和其他PG版本继续实施。

## Mac验证与边界

受影响全量race最终278个case pass事件，12个测试包、9个无测试包，0case fail/
skip；owner20个顶层及39个子测试。事件包含父/子，不冒充278个独立顶层场景。
native标签受影响lint0issues。旧8项生产反向沿用，新增3项分别移除二次Lease
读取、TokenReview Pod UID比较及专用audience检查，均被真实业务断言拒绝；
finally逐字节恢复生产源码后重新执行完整race和lint。go.mod/go.sum不变。

真实Go→TLS→生产Compute Router/JWT→SQL→PG17候选门禁8.26s通过：1 Rust外层、
2 Go race进程，owner epoch2/持久循环/原意图恢复阶段标记完整，零skip/ignored/
filtered。该证据验证已有PG链路兼容，**没有执行真实Kubernetes TokenReview**；
准确提交再编记录到Neon主仓。PG当时准确锁6b7c3655；随后发现社区新维护补丁，
将独立合并、重编、验证并更新锁，不把旧证据冒充新PG提交证据。

主体测试使用本机HTTP的TokenReview/Pod/SA协议夹具及真实生成客户端；Lease/
journal使用现有UID/RV/RFC6902夹具。它们不等于apiserver真实认证/RBAC、etcd、
跨节点时钟或Linux物理回收验收。最初超时夹具未先读请求体导致httptest关闭等待，
已修正并终止该旧进程；通过结论仅来自修正后独立完成的日志。

日志：工作区build/autoscaling-pod-full-final.jsonl、autoscaling-pod-lint-final.log、
autoscaling-pod-negative.log及neon-pod-native-candidate.log。Linux/KVM/HugeTLB/
真实物理释放与最终性能验收按V3后验；缺失签发入口/Agent等仍属于代码工作。
