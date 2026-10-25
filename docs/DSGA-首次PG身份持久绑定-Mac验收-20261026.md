# 首次PG身份必须先持久固定，再交执行器使用

本模块实现首次准入所需的持久PG身份基础，不是首次开放业务或完整预算生产者。
沿原codex/dsga-autoscaling-20261019增量开发，默认关闭，不合main、不构建
镜像、不准备/执行部署；PG17及Neon1.15未改。

## 已证实缺口和代码不变量

改产品代码前，实际Owner/SDK/TLS在没有Resize/HWM时，先接受boot11，又在
重建worker时接受boot12，新负向测试按期失败。根因是只有首个Resize才把PG
写进HWM，早于Resize的首次准入生命周期没有独立持久身份。

新journal.Model增加必需的PGInstance：首次能力协商只读取得真实VM/Compute/
boot后，以同一原VM UID/RV CAS固定，确认前不返回可用执行器。CAS丢回复返回
UnconfirmedWrite，关闭本地client但保留服务器可能已提交的身份；重建只读原
绑定，不重新发现。资源worker参数先于能力/凭据检查，错误构造不能污染绑定。
显式指定另一个boot也在取凭据/HTTP之前拒绝，不允许绕过持久约束。

PG绑定不创建Active/HWM、不修改PG准入/floor、资源预算或VM。Clone深拷贝，
owner接管、历史淘汰、编码恢复和新操作均保留身份。已有v1操作只沿原HWM恢复，
不从当前PG补字段。旧原历史仍可读，但新请求不能先改HWM再收养另一boot。
旧未知时保留Busy；通用Cleanup成功不等于跨boot的PG/资源/VM关闭证明，同boot
新owner仍可继续操作。受控跨boot必须独立实现，不能靠清字段或重造日志替代。

## 格式和升级约定

创建阶段继续Model v1；首次绑定与升为Model v2在一次CAS完成。v2必须携带
PGInstance，v1不允许混入该字段；外层压缩信封仍v1。这里的Model v2与此前
Resume审计v2是不同对象，不改变PG Buffer6等接口、WAL、原生协议或配置。
旧Model本已有严格JSON解码：实测完整v2先以unknown pg_instance拒绝；投影
掉字段并正确重建checksum/压缩后仍因版本拒绝；原准确v1账本照常读取。

先前只看到外层json.Unmarshal，误以为旧解码会忽略字段；首次兼容验证已证明
Model自定义严格解码会拒绝，注释和断言已纠正，初轮错误断言日志保留。v2用于
明确新必需语义，不把这一误判记录为旧产品缺陷。实际旧model/store源码通过
Go只读overlay使用，未reset工作树；原v1/v2输入来自真实typed Store/CAS。

上线顺序必须所有账本读者（控制面、Agent、Controller/Runner相关消费者）先
升级，再启用v2生产。旧二进制会失败关闭，不允许删pg_instance、改回v1或
重新Initialize来回退；须先对账并按受控恢复协议处理。此处没有执行部署。

## 实际Mac验证

去重36顶层/71含子项race通过，0测试skip：首次绑定模型/历史/编码、typed
Store真实HTTP/RFC6902与UID/RV、写后丢回复/损坏回包、实际Owner/SDK/TLS
首次发现/显式boot/取凭据前拒绝、旧HWM及准入恢复。Kube与PG能力为明确
协议夹具，不是apiserver/etcd或真实PG crash/完整集群认证。

初窄组8顶层/29含子项通过；增加参数拒绝、旧HWM约束后，对受影响journal
组件做一次全量检查：53通过/1失败，旧测试曾把通用Cleanup当成跨boot许可。
保留Busy语义，修订该测试并只替换失败/补旧历史断言，原53项不重跑；去重时
旧名称由新名称替换。只复核前批准入丢回复/重建两场景，未重跑其余15场景。
这些候选、全量、替换的计数不相加；原失败和改码前负向均留存。

最终readonly vet journal/owner/bootstrap通过；实际Agent Linux amd64/arm64
最终业务源码交叉编译通过，仅编译不执行。只因语义/注释最终定稿重建制品，
不构建镜像。旧读者验证三条断言通过，PG/DSM/Neon HTTP与发行test_compute5
均未改变，不重复验证。完整源码、制品、配方与原始日志见
[机器收据](DSGA-首次PG身份持久绑定-Mac收据-20261026.json)。

本批fresh fetch的Neon fa504217、AS a4bce09e、NeonPG17 56692dfb、社区
PG17 bd807012及控制面mainf7df93a全部missing0，无需空维护提交。
完整可信Prepare/Finalize/SystemOutsidePG、安全降normal/后台私有上界、
首次预算和开放、Host最终VM/公开worker/受控跨boot仍须代码。Linux内核/
HugeTLB/物理释放、目标集群E2E/性能另验；整体Goal active/incomplete，其他PG暂缓。
