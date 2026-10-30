# PG17 Linux接续：Prepare完成后的Node请求屏障设计

本文件是2026-10-30 Mac阶段收口时保存的后续设计，**尚未实现、尚未验收**。
项目所有者已要求停止新增功能，先归档当前代码，后续验证转Linux；本文件不
授权自动继续实现、构建镜像、部署或合并主分支。

最后一份完整代码基线为Autoscaling自有功能分支
`codex/dsga-autoscaling-20261019`的
`00eda3d5e5fc6ecb76e6e01fcbbf569e85609bda`（短SHA `00eda3d5`）。
本次仅新增设计说明和文本草案，不改变该基线的运行行为。PG17冻结基线保持
不变，不主动fetch/合入后续上游，不自动进入PG18/PG16。

## 已有实现与本设计的边界

已归档的早期类别16恢复处理原Prepare为Planned或Unknown、其余资源步骤均
未派发的情形：保存独立RetainedRecovery，以原完整永久预算ID显式重开，原
Unknown逐字保留，完成CAS保存永久覆盖槽，并提供迟到Outside的保护与有界
回收。该结果不包含本文件描述的新Node屏障。

本设计补充的是另一条真实边界：Prepare已经Completed，但本次Host来源没有
得到完整认证，后续Guest/VM/PG等步骤尚未派发。不能根据Host CM的404、
ReserveHost为Planned、当前内存等于before，推导没有在途Node Reserve、
Host CREATE或BindClaim。现Node最高格式为v8，尚无这条独立提前封存水位。
因此该分支目前仍应拒绝恢复，不能把缺失业务归为“仅待Linux验收”。

唯一形成的代码草案已移出生产包，保存在
[PG17-Linux接续草案/host_barrier.go.txt](PG17-Linux接续草案/host_barrier.go.txt)。
原未跟踪文件为`pkg/dsga/budget/host_barrier.go`，原文SHA256为
`e2620195cd1220addcb44b4c6ba4d72525d9f540df1072507c4f3397053d7cb5`。
此前只运行过budget包的compile-only命令`go test -run '^$'`，用时1.739秒，
输出`[no tests to run]`，记录位于工作区`build/dsga-host-barrier-budget-compile.log`。
该记录不代表功能实现、业务测试或目标环境验收通过。本次文档归档不运行测试。

## Node v9与共享DTO约定

拟在原Node账本的每个Managed Placement增加可选
`Barrier *budget.HostReservationBarrier`，并明确升级Node格式v9。每个原
Pod/VM只保存最新的完整原请求水位；更高水位必须属于同一Instance且严格
前进，不递归嵌套旧Proof。旧v1～v8读者必须拒绝v9，不能丢字段后继续写入。
旧格式无新字段时保留原序列化形状。

共享类型放在budget包，避免journal与hostguard互相导入。草案包含：

| 字段组 | 约定字段与含义 |
| --- | --- |
| 原请求与分配域 | `ProtocolVersion=1`、`Namespace`、`VMName`、`Incarnation`、`RunnerPodUID`、完整`Request`、`BeforeMemoryBytes`、`HostOverheadBytes` |
| 原WAL与VM闭门 | `PreparedSHA256`、`OperationSHA256`、`VMOwner`、`VMSequence`、`VMResourceVersion`、`ClosedSHA256`、`ClosedSequence` |
| 固定Node/Lease | `NodeName`、`NodeUID`、`LeaseNamespace`、`LeaseName`、`LeaseUID`、`NodeOrigin` |
| Node封存来源 | `HostOwner`、`NodeSequence`、`NodeResourceVersion` |
| 实际原Hold | 可空`Held *HostBarrierHold`，含`SourceID`及可空的准确`Claim *ObjectRef` |

`BarrierOwner{HolderID, Epoch}`分别描述VM域和Host域owner；它们属于不同
Lease，不能相互替代或比较epoch大小。`NodeSequence`表示封存后的Node账本
序列；`NodeResourceVersion`表示该CAS使用的**写前原Node RV**，写后RV不能
预知，不得伪造。Node原UID、Lease原UID、origin和当前floor仍须从可信客户端
独立认证。

`PreparedSHA256`必须绑定原Prepare Observation完整原始字节，不把重新
marshal的对象当作同一原文；`OperationSHA256`固定VM原闭门保护的完整
Operation；`ClosedSHA256/ClosedSequence`绑定当前被选中并已确认的原生
关闭。VM序列与Node序列属于不同域，不能混用。具体原文摘要域继续复用现有
生产计算函数，不能另创看似相同的摘要。

`Held=nil`仅可由封存CAS所用真实Node快照确认为没有该完整请求的Hold。
`Held!=nil && Claim=nil`表示已经Reserve但尚未发布；Claim非空表示原UID已
固定。三者必须区分。Node侧重新计算`SourceID(origin, full Plan)`，不能由
Host CM自报替代；不得把不存在的Host来源补成一份空HostReservation。

## 拟定API与职责

以下是本轮沟通定案的接口形态，**生产代码中均未实现**：

```go
// hostguard本地输入；VM闭门事实由可信Publisher从原VM快照取得。
type PreparedClosure struct {
    Plan              Plan
    PreparedSHA256    string
    OperationSHA256   string
    VMOwner           journal.Owner
    VMSequence        control.Uint64
    VMResourceVersion string
    ClosedSHA256      string
    ClosedSequence    control.Uint64
}

func (s *Store) SealPrepared(ctx context.Context, n *Snapshot,
    input PreparedClosure) (*Snapshot, budget.HostReservationBarrier, error)

func (n *Snapshot) PreparedBarrier(plan Plan) (budget.HostReservationBarrier, error)

func (s *Store) HoldPlacementFor(ctx context.Context, n *Snapshot,
    p Placement, key control.Key) (*Snapshot, Placement, error)

func (s *Store) RetireSealedHolds(ctx context.Context, n *Snapshot,
    barrier budget.HostReservationBarrier) (*Snapshot, []Hold, error)
```

- `SealPrepared`只在原Node owner/floor与同Node UID/RV CAS内封存。VM原
  Prepare、原Operation、已确认关闭及其VM UID/RV由生产Publisher认证；
  Store从自己的真实Node快照填入Node binding、origin、owner、写前RV、
  写后序列及实际Hold/Claim，不能接受调用方自报这些Node来源。
- `PreparedBarrier`只读取精确原Plan对应的完整水位。封存回复丢失时必须
  重新GET原Node并只读确认；没有写确认就没有新票据，不换新Key、旧CM UID
  或闭门选择重试。水位被更高Key覆盖时仍不能倒退或重新Reserve旧请求。
- Publisher生产适配拟名`SealClosedHostRequest`。Scheduler仅可对已确认
  类别16关闭、Prepare Completed且其余资源步骤全部Planned的原操作发起
  封存。封存只减少旧请求的执行资格，不增加任何资源/开放权限；Agent只读
  消费完整Node水位，恢复服务仍须显式管理选择。
- Journal后续须扩展独立证明，将完整Prepare、准确Node屏障、原VM闭门、
  当前旧永久预算及各域真实来源交叉绑定。此前Planned/Unknown分支继续用原
  合同，不能用新屏障放宽其它未知或改写原Abort。

共享DTO草案的`Validate/ValidateFor/Clone`只描述结构、算术、身份及深拷贝
要求，不认证网络来源、不产生派发、开放、资源释放或物理空闲证明。

## 只封不放与原预算只读

封存CAS不删除或降低任何现有Hold、Claim、placement，也不制造Host完成、
原VM NoEffect或Host物理内存返还。原placement继续按真实保守额度持有，
不得因为新目标更小、Host来源缺失或某次读失败而压低。

`Reserve`、`BindClaim`与执行用途的`InspectClaim`必须拒绝同原Pod/VM中
不晚于sealed Key的请求；已有Claim的幂等Bind也不能重新获得执行资格。
同epoch/revision换OperationID不是同一请求，不得绕过高水位。迟到CREATE
可能留下原protected CM，但最终Bind必须被Node串行屏障拒绝；不收养同名
新UID、不通过创建替代来源修复缺失。

显式`InspectRetainedClaim`仍可为**准确旧永久预算**认证原Host持有，保留
原CM UID、完整Plan和当前预算水位匹配要求；该只读分支不能交给Runner或
Allocator作为新派发票据。新普通/实际预算替换后，旧恢复Proof仍仅供审计，
当前来源授权必须取新永久头，不能用旧slot重新授权。

## Placement、后继Key与收敛

现`HoldPlacement/reservePlacement`没有请求Key，必须覆盖这条真实写入
旁路，而非只在Reserve/Bind增加检查：

1. 保留旧`HoldPlacement`入口；有Barrier或已结清Through的同Managed行，
   无Key调用只允许不高于原Memory的纯保留，不能增加或清除新Barrier字段。
2. 新`HoldPlacementFor`用于可信Scheduler从当前原VM Active取得的完整Key。
   新Key须属于同Instance，且严格晚于Barrier及已有closedThrough；旧Key即使
   目标数值相同也拒绝。新目标数值本身不是授权。
3. 当前Barrier/Settled由Node模型深拷贝保留，调用方输入不能构造、替换或清
   空它们。迟到旧Node快照写入仍由原UID/RV CAS拒绝；后继真实新请求可以在
   更高Key下重新申请完整容量。

恢复完成后的退还另走独立路径：

- 普通Finalized/Capacity预算继续使用完整旧预算、当前Runner及永久Through
  认证后的既有Settlement，才可收敛已关闭的新峰值Hold。Seal不冒充该结清。
- Initial没有Resize Request或Host目标CM，不能伪造Settlement。拟新增
  `RetireSealedHolds`，仅在可信调用方刚认证无Active/无当前关闭、完整
  RetainedCoverage和准确当前Initial/Runner/Node来源之后，按**最新已认证
  Barrier覆盖的前缀**退休同VM/incarnation/Pod/Instance且不晚于该Key的原
  Holds；逐项保留/检查原Plan、SourceID、Claim UID，placement原值不动。
- 前缀退休必须幂等；只清最新Key一个Hold会漏掉上次清理丢回复或未执行后
  被更高水位覆盖的旧Hold。任何不完整来源、另一Node/Pod/PG boot、较新
  Key、来源替换或失租均拒绝。Store不得把裸Barrier结构当作完整资源来源
  验收，生产适配须明确完成独立来源审计及最终VM/Node/Lease屏障。
- 迟到Host/Outside CM的垃圾回收必须在完整关闭/完成与当前保留引用集合
  认证后进行，使用原UID/RV/正文摘要精确删除。永久slot、原Unknown以及
  `Source.Finalization`等raw payload须显式解码，不能因JSON中的base64漏掉
  引用，也不能因历史淘汰提前删除原来源。

## Linux上的三组真实竞态计划

以下均**尚未执行**。复用真实Store/SDK/Runner/Lease与同一Node UID/RV
RFC6902串行写入；Node HTTP peer与对象存储必须共用一个真实状态，阻塞点在
互斥区外，避免以fake Core重入或两个不一致Node视图伪造竞态。

| 场景 | 必须验证的顺序与结果 |
| --- | --- |
| Initial：Barrier先赢 | Prepare真实Completed、已确认闭门后，Node Seal先落账；此前阻塞的旧Reserve释放后因原Node RV冲突拒绝，fresh snapshot的同Key Reserve也拒绝。原Initial来源、placement、预算头与原WAL不变，无Host成功/NoEffect补写。 |
| Initial：Reserve先赢，CREATE晚到 | 原Reserve已落账，Host CREATE在途或回复丢失；Seal固定真实Held及Claim状态并故意丢封存回复。重建后只读恢复原准确Barrier，释放旧CREATE后，旧/fresh Bind均不得生效。完整旧预算恢复与Complete后才做Initial前缀退休/精确GC，原placement保持。 |
| 普通永久预算：原Claim已绑定 | 原Node Claim已发布但外层Host仍Planned；Seal保留准确Claim UID并禁执行，显式旧预算只读仍成功。独立恢复不改原Host步骤，完整Complete推进旧Through，既有Settlement才清封存峰值；后继更高Key正常申请，旧Key/同位置换ID/同名新UID持续拒绝。 |

第三组后继应使用真实不同的PG目标，例如Buffers从1024到2048，避免
以PG no-op冒充容量代次前进。测试peer只覆盖Node的真实HTTP/RFC6902；其余
Kube对象以同一Tracker作为canonical状态，不能持fake reactor的锁跨域等待。
Host CREATE的迟到payload用独立goroutine及明确Unknown响应表达，此部分是
显式对象存储/故障夹具，不宣称真实Kube网络、apiserver或RBAC验收。

每组还应精确覆盖：Seal/Bind CAS丢回复、旧RV、Node/Lease/CM同名新UID、
owner接管、已保存Proof后的来源漂移、历史淘汰、连续恢复与slot替换、普通新
预算替换仅保留旧Proof审计，以及新旧Key placement写入。只执行新增、修改及
直接受影响场景，不重复未修改功能的全矩阵。

## 后续实施与验收顺序

1. 恢复实施前复核本设计与冻结分支；共享DTO、Node v9模型/Store/所有分配
   写者的最终屏障先完整实现，再接Publisher、Scheduler与固定来源读者。
2. Journal独立证明、原预算恢复/显式重开/Complete与Initial前缀退休必须
   一并闭合，不能把单包编译或某个屏障正例作为端到端完成。
3. 在Linux运行上述真实竞态与受影响业务验证，明确记录平台、命令、SHA、
   成功/失败/未执行项。现有Mac通过记录只说明当时对应代码，不替代Linux。
4. Linux K8s/NeonVM/KVM/HugeTLB、真实物理内存归还及性能验收仍须目标环境
   单独完成；没有授权不构建镜像、不部署、不合主分支。PG17完成与归档后
   按所有者要求停止自动接续，PG18/PG16等待另行恢复。
