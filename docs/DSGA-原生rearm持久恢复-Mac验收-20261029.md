# PG17 原生 rearm 原请求查询与持久恢复：Mac 阶段验收

日期：2026-10-29。仅在原自有功能分支增量实施；冻结上游不变。PG17 代码
`79367196a74eef82e67cac917877d2e6271d3e70` 已先推自有 fork 并核对完整远程 SHA。
本批恢复的是关闭后的原生期限；业务重新开放和有界重复生命周期仍须后续实现，
不能将本阶段当成 PG17 全面完成。持续目标保持 active。

## 行为与恢复边界

PG 在原 status_lock 内原子保存成功 rearm 的完整原 CAS 和应用期限，仍放在原已
计费 cache line 中，不新增共享预算字节或改变 Buffer ABI。只读查询同时返回原
请求是否保留、历史应用期限和独立当前快照；renew、过期和更高 floor 不擦旧事实，
下一次成功 rearm 覆盖单槽。未命中只表示未知，历史命中也不保证当前期限有效。
新 boot 清槽，原 boot 身份仍须匹配。查询不续租、不 fence、不开放、不授权缩 VM。

Neon 扩展 1.17 增加独立只读 SQL，Rust 合同、认证 Router 和 Go SDK 校验完整
原请求及历史/当前分离。更高签名 owner 可查原请求，不能借查询执行 setter，
初始化 scope、未来 epoch、重复字段和不完整请求拒绝。扩展降级只撤 SQL，不撤
内核原事实或期限。旧 SQL/HTTP 协议保持原约束，编号用于准确拒绝旧格式，不是
另交付中间产品。

外层 Journal 格式 19 固定原关闭类别（仅无 Active 的 10/11/12）、完整关闭摘要、
完整永久预算/来源/关闭水位、原观察和原 rearm CAS。只保留一次选择。写前 UID/RV
CAS 确认才返回派发票据，丢回复零派发；前后重审原 Root、资格、Runner、出生和
预算，来源漂移保留原未知。恢复默认只查询原回执；显式重放要求原 owner、摘要、
同 RV 和完整来源，仍发原 CAS，不换 deadline。完整当前快照深拷贝，未知 rearm
另预留 MaxObservationBytes+4096 落账空间，不淘汰资源未知、关闭或永久预算。
Run 过期 latch 不由 rearm 解除，原首次开放未知、关闭和预算全部保留。

## 实际采用的验证

- 准确 PG17 SHA 私有安装与配套 Neon 扩展：原生 10 项，34.130 秒，0 fail/skip。
  覆盖完整 CAS、并发只应用一次、单槽覆盖、续租/过期/更高 floor 保留、查询不续租、
  降级保留、新 boot、权限与严格 JSON。准确链日志 `build/dsga-rearm-status-native-exact.log`。
- 正式 Router→SDK→SQL→准确 PG17：1 项，3.88 秒；认证/重复字段 1 项，Rust 合同
  2 项，均 0 fail/ignored。对应 `http-exact`、`http-auth-fixed`、`rust-contract-fixed` 日志。
- Go -race 按 package/test 去重 110 顶层、540 含父子/种子记录，采用 0 fail/skip；
  不是 540 次独立 E2E。Journal 首批总期限仅采用完整 36 根子树，余 63 根独立通过；
  最终 codec 余量改变后仅替换受影响三根。准确来源、每个日志 SHA 和采用根见收据。
- 生产正常期限恢复及 lost-start、setter-before、setter-after、lost-record、
  source-unavailable 五恢复模式通过；追加 source-after-selection 证明真实 WAL
  选择之后来源漂移零派发。真实 Session、Store、管理处理器与 Runner Tick 参与；
  PG/Root/Kube/平台仍为明确 peer，不能当成整个产品真实环境 E2E。
- 四包 vet、最终增量 lint（0 issues）、目标 Clippy --no-deps -- -D warnings、修改
  Rust 文件格式通过；冻结 remote_storage 依赖有 27 条既有 warning。33 消费包仅编译。
  Neon 默认 CI 232 项中 13 pass、219 环境 skip、0 fail，跳过不能算原生业务通过。

Go 使用固定本地 Go、GOPROXY=off/GOSUMDB=off、-mod=readonly -race -count=1；
Journal 分批上限 15m。Rust 固定 1.88.0、Xcode MacOSX14.4 SDK，--locked --offline。
准确原生重建/安装入口为根 `build/run-dsga-rearm-status-exact.sh` 和
`build/run-dsga-rearm-status-exact-neon-build.sh`，安装在
`build/pg17-rearm-status-exact-install`；candidate 安装不代替准确 SHA 安装。
日志完整路径/摘要、源码逐文件 SHA256、失败历史见同目录机器收据。

## 保留的真实失败

首轮生产夹具未由 Runner Tick 确认新 owner floor，随后原 4m 总期限到达；修夹具
通过真实 Tick 确认，没有放宽生产校验。Journal 原整批超时没有涂绿。初次 Rust
未固定 SDKROOT 链接失败，lint 五项、HTTP 夹具错误预期 403（正式认证为 401）均
修正并重测。全仓 rustfmt 存在无关既有差异，恢复子文件递归格式副作用后仅校验
修改文件。原失败日志保留，具体列表及采用替换规则见机器收据。

## 继续实施与环境边界

后续仍须实现受控业务重新开放、有界重复生命周期、在途资源各自恢复、安全降
normal、私有/后台持续预算和活性、分区/跨 boot、Monitor/WSS/总配额/RW-RO/
可信 CPU/全部写者。它们是代码任务，不能全部列为 Linux 待验。当前仍保持默认
关闭，不清任何原未知或永久预算，不重复未变自动 Grow→Shrink 场景，也不宣称
本批执行了新的自动伸缩 E2E。

Linux/K8s/KVM/NeonVM/CRI/cgroup/HugeTLB、真实物理回收及性能单独未验；
不构建镜像、不部署、不合 main、不改独立控制面。所有者 AS AGENTS 改动不 stage。
PG17 全面实现、Mac 可执行验收和远程归档后才暂停，不自动进入 PG18/16。
