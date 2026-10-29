package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// Binding来自已认证的创建/观察流程，在Store生命周期内固定，不能按同名VM的新
// UID或HTTP失败后的临时发现自动替换。Incarnation只可在新VM未启动时显式创建。
type Binding struct {
	Namespace   string
	Name        string
	VMUID       types.UID
	Incarnation string
}

// Store只做恢复事实的条件持久化。调用者负责身份认证和领域回执验证；本包不持有
// 任何PG/QMP/Scheduler凭据，不发送实际资源动作，也不把保存成功解释为动作完成。
type Store struct {
	client  vmclient.VirtualMachineInterface
	binding Binding
	cache   validatedModelCache
}

// 只复用同一Store中逐字相同账本的确定性解码结果；绝不缓存GET、VM元数据、
// Lease、来源或派发许可。完整生命周期原文变大后，每次取凭据重复解压/严格
// 校验同一份历史会占满串行轮次，使有限的新鲜采样窗口无法累计。
// 缓存只有一项，空间仍受原编码/模型界限约束。锁仅保护本地查找及深拷贝，
// 网络和解码都在锁外；并发旧解码后完成最多替换性能缓存，不能推进任何水位。
type validatedModelCache struct {
	mu      sync.Mutex
	encoded string
	model   Model
}

func (store *Store) decodeModel(data string) (Model, error) {
	store.cache.mu.Lock()
	if data != "" && data == store.cache.encoded {
		model := store.cache.model.Clone()
		store.cache.mu.Unlock()
		return model, nil
	}
	store.cache.mu.Unlock()
	model, err := decode(data, string(store.binding.VMUID), store.binding.Incarnation)
	if err != nil {
		// 不缓存错误，也不把已通过的旧模型用于不同/缺失/损坏的原文。
		return Model{}, err
	}
	store.cache.mu.Lock()
	store.cache.encoded, store.cache.model = data, model.Clone()
	store.cache.mu.Unlock()
	return model, nil
}

// Binding返回固定值副本，供选主/凭据适配器核对同一VM incarnation；它不是
// 当前owner或派发许可，不能用最新GET替换其UID并把同名重建当成原实例。
func (store *Store) Binding() Binding { return store.binding }

// Snapshot不可由调用方修改元数据/原始模型。State返回深拷贝，不跨网络持本地锁；
// 唯一线性化点是apiserver对固定UID/resourceVersion的原子JSON Patch。
type Snapshot struct {
	binding Binding
	vm      *vmv1.VirtualMachine
	model   Model
}

func (snapshot *Snapshot) State() Model            { return snapshot.model.Clone() }
func (snapshot *Snapshot) ResourceVersion() string { return snapshot.vm.ResourceVersion }

// VirtualMachine返回与完整journal同一次GET解码的VM深拷贝。凭据签发器须用
// 它核对Node/生命周期/业务归属，不能另取不受保护的owner镜像注解；返回值修改
// 不影响CAS快照或内部模型。跨对象读取仍不是最终执行许可。
func (snapshot *Snapshot) VirtualMachine() *vmv1.VirtualMachine { return snapshot.vm.DeepCopy() }

// Dispatchable只是已读取快照的生命周期条件，不是派发票据或租约。PG显式原意图
// 重放也须拒绝正在删除/没有Runner的VM；网络竞态仍靠UID/RV及最终执行fence封闭。
func (snapshot *Snapshot) Dispatchable() bool {
	if snapshot == nil || snapshot.vm == nil {
		return false
	}
	held, err := snapshot.vm.DSGABootstrapHeld()
	return err == nil && !held && snapshot.vm.DeletionTimestamp == nil &&
		(snapshot.vm.Status.Phase == vmv1.VmRunning || snapshot.vm.Status.Phase == vmv1.VmScaling) &&
		snapshot.vm.Status.PodName != "" && snapshot.vm.Status.PodIP != ""
}

// Dispatch仅在写前日志被确认持久提交后返回。它仍不是VM许可；实际执行适配器须
// 再验证来源认证、当前epoch、原意图/完整命令和预算，最终Runner再次fence/串行化。
type Dispatch struct {
	Binding         Binding
	Owner           Owner
	Key             control.Key
	RequestSHA256   string
	JournalSequence control.Uint64
	Step            Step
}

type UnconfirmedWrite struct{ Err error }

func (err *UnconfirmedWrite) Error() string {
	return fmt.Sprintf("journal write is unconfirmed; reload before dispatch: %v", err.Err)
}
func (err *UnconfirmedWrite) Unwrap() error { return err.Err }

func NewStore(client vmclient.VirtualMachineInterface, binding Binding) (*Store, error) {
	if client == nil || binding.Namespace == "" || binding.Name == "" || !validIncarnation(binding.Incarnation) {
		return nil, fmt.Errorf("journal store requires fixed VM/incarnation binding")
	}
	if err := control.ValidateID(string(binding.VMUID), "VM"); err != nil {
		return nil, err
	}
	var store Store
	store.client, store.binding = client, binding
	return &store, nil
}

func (store *Store) readVM(ctx context.Context) (*vmv1.VirtualMachine, error) {
	vm, err := store.client.Get(ctx, store.binding.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if vm == nil || vm.UID != store.binding.VMUID || vm.Name != store.binding.Name || vm.Namespace != store.binding.Namespace || vm.ResourceVersion == "" {
		return nil, fmt.Errorf("journal VM identity changed")
	}
	return vm, nil
}

func (store *Store) fromVM(vm *vmv1.VirtualMachine) (*Snapshot, error) {
	return inspectVM(vm, store.binding, store.decodeModel)
}

// Inspect以唯一有界解码器核验已读取的完整VM/journal，供Controller/准入复用。
// 它不访问网络、不认证调用方、不造票据；快照深拷贝，外部修改不能改变CAS事实。
func Inspect(vm *vmv1.VirtualMachine, binding Binding) (*Snapshot, error) {
	return inspectVM(vm, binding, func(data string) (Model, error) {
		return decode(data, string(binding.VMUID), binding.Incarnation)
	})
}

// 所有当前VM身份、bootstrap和Compute绑定都在每次GET之后重新核验。
// decodeModel仅可复用确定性完整模型；外部Inspect仍独立执行严格解码。
func inspectVM(vm *vmv1.VirtualMachine, binding Binding, decodeModel func(string) (Model, error)) (*Snapshot, error) {
	if vm == nil || vm.UID != binding.VMUID || vm.Name != binding.Name || vm.Namespace != binding.Namespace || vm.ResourceVersion == "" {
		return nil, fmt.Errorf("journal response VM identity changed")
	}
	if vm.Annotations[control.ModeAnnotation] != "1:"+binding.Incarnation {
		return nil, fmt.Errorf("journal incarnation marker missing or changed")
	}
	data, present := vm.Annotations[control.JournalAnnotation]
	if !present {
		return nil, fmt.Errorf("journal history is missing; automatic reset is forbidden")
	}
	if _, err := vm.DSGABootstrapHeld(); err != nil {
		return nil, err
	}
	if vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != binding.Incarnation {
		return nil, fmt.Errorf("journal bootstrap binding differs")
	}
	model, err := decodeModel(data)
	if err != nil {
		return nil, err
	}
	if model.HighWater != nil && model.HighWater.Instance.ComputeInstanceID != vm.Spec.DSGABootstrap.ComputeInstanceID {
		return nil, fmt.Errorf("journal retained Compute identity differs from bootstrap")
	}
	if model.ResourceIdentity != nil && model.ResourceIdentity.ComputeInstanceID != vm.Spec.DSGABootstrap.ComputeInstanceID {
		return nil, fmt.Errorf("journal resource binding differs from original bootstrap")
	}
	return &Snapshot{binding: binding, vm: vm.DeepCopy(), model: model}, nil
}

func (store *Store) Load(ctx context.Context) (*Snapshot, error) {
	vm, err := store.readVM(ctx)
	if err != nil {
		return nil, err
	}
	return store.fromVM(vm)
}

// Initialize是显式的新VM初始化，不是Load失败后的兜底。只允许未创建Runner、未
// 重启且仍处初始阶段的VM，并绑定创建流程提供的准确RV。控制器必须在启用/启动
// 受管Runner前完成此步；运行VM不能在线打标绕开既有QMP连接或重置丢失历史。
func (store *Store) Initialize(ctx context.Context, bootstrapVersion, holder string) (*Snapshot, error) {
	vm, err := store.readVM(ctx)
	if err != nil {
		return nil, err
	}
	held, holdErr := vm.DSGABootstrapHeld()
	if bootstrapVersion == "" || vm.ResourceVersion != bootstrapVersion || control.Managed(vm.Annotations) ||
		vm.DeletionTimestamp != nil || !held || holdErr != nil || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Ready || vm.Spec.DSGABootstrap.Incarnation != store.binding.Incarnation || !vm.DSGAUndispatched() {
		return nil, fmt.Errorf("journal initialization is not a new undispatched VM")
	}
	model, err := New(string(store.binding.VMUID), store.binding.Incarnation, holder)
	if err != nil {
		return nil, err
	}
	annotations := maps.Clone(vm.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[control.ModeAnnotation] = "1:" + store.binding.Incarnation
	model, encoded, err := encode(model, annotationBudget(annotations))
	if err != nil {
		return nil, err
	}
	annotations[control.JournalAnnotation] = encoded
	operations := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: store.binding.VMUID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion},
		{Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations},
	}
	return store.write(ctx, operations, model, encoded)
}

// ReleaseBootstrap只能由已持久化/读回安全Compute配置的可信Driver调用。Config
// UID不是自报预算许可：生产Webhook和Controller仍独立读取owned Secret及完整
// journal核验。此处只负责同UID/RV与原held记录的原子one-shot释放，不写新身份。
// 丢回复返回UnconfirmedWrite；同一原绑定重读已ready可确认原事实，不重置日志。
func (store *Store) ReleaseBootstrap(ctx context.Context, bootstrapVersion string, configSecretUID types.UID) (*Snapshot, error) {
	snapshot, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	vm, model := snapshot.vm, snapshot.model
	if control.ValidateID(string(configSecretUID), "config Secret") != nil {
		return nil, fmt.Errorf("invalid bootstrap config UID")
	}
	bootstrap := vm.Spec.DSGABootstrap
	if bootstrap.Ready {
		if bootstrap.ConfigSecretUID != string(configSecretUID) {
			return nil, fmt.Errorf("bootstrap already released with another config UID")
		}
		return snapshot, nil
	}
	if bootstrapVersion == "" || vm.ResourceVersion != bootstrapVersion || vm.DeletionTimestamp != nil || !vm.DSGAUndispatched() || !PristineBootstrap(model) {
		return nil, fmt.Errorf("bootstrap release requires the original held pristine journal")
	}
	ready := *bootstrap
	ready.Ready, ready.VMUID, ready.ConfigSecretUID = true, string(vm.UID), string(configSecretUID)
	operations := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: store.binding.VMUID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: vm.ResourceVersion},
		{Op: patch.OpTest, Path: "/spec/dsgaBootstrap", Value: bootstrap},
		{Op: patch.OpReplace, Path: "/spec/dsgaBootstrap", Value: ready},
	}
	result, err := store.write(ctx, operations, model, vm.Annotations[control.JournalAnnotation])
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(result.vm.Spec.DSGABootstrap, &ready) {
		return nil, &UnconfirmedWrite{Err: fmt.Errorf("bootstrap release response differs")}
	}
	return result, nil
}

// PristineBootstrap只供初次启动屏障，不能用来证明运行期没有未知效果。
// Sequence/owner代次也必须是初始化值，不能把淘汰历史后的空Active误当新日志。
func PristineBootstrap(model Model) bool {
	return model.ProtocolVersion == 1 && model.Sequence == 1 && model.Owner.Epoch == 1 && model.HighWater == nil && model.Active == nil && len(model.History) == 0 && model.ExpiredResults == 0 && model.ResourceIdentity == nil && model.PGInstance == nil && model.Validate() == nil
}

func annotationBudget(annotations map[string]string) int {
	budget := 256*1024 - len(control.JournalAnnotation)
	for key, value := range annotations {
		if key != control.JournalAnnotation {
			budget -= len(key) + len(value)
		}
	}
	return min(budget, MaxEncodedBytes)
}

func (store *Store) write(ctx context.Context, operations []patch.Operation, expected Model, encoded string) (*Snapshot, error) {
	payload, err := json.Marshal(operations)
	if err != nil {
		return nil, err
	}
	vm, err := store.client.Patch(ctx, store.binding.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	if err != nil {
		// 包括响应丢失、代理错误和条件失败，一律不返回票据，也不透明重试。调用方
		// 重读原意图后区分未提交/已提交未知，不能因错误类型猜测动作已被撤销。
		return nil, &UnconfirmedWrite{Err: err}
	}
	snapshot, err := store.fromVM(vm)
	if err != nil {
		return nil, &UnconfirmedWrite{Err: err}
	}
	if vm.Annotations[control.JournalAnnotation] != encoded || snapshot.model.Sequence != expected.Sequence {
		return nil, &UnconfirmedWrite{Err: fmt.Errorf("journal response differs from committed intent")}
	}
	return snapshot, nil
}

func (store *Store) apply(ctx context.Context, snapshot *Snapshot, next Model) (*Snapshot, error) {
	if snapshot == nil || snapshot.vm == nil || snapshot.vm.Spec.DSGABootstrap == nil || snapshot.binding != store.binding {
		return nil, fmt.Errorf("snapshot belongs to another journal")
	}
	if err := next.Validate(); err != nil {
		return nil, err
	}
	if next.VMUID != snapshot.model.VMUID || next.Incarnation != snapshot.model.Incarnation {
		return nil, fmt.Errorf("journal binding cannot change")
	}
	if next.Sequence == snapshot.model.Sequence {
		before, _ := json.Marshal(snapshot.model)
		after, _ := json.Marshal(next)
		if string(before) != string(after) {
			return nil, fmt.Errorf("journal changed without advancing its sequence")
		}
		// 幂等事实亦不能把旧Snapshot冒充当前owner。新读若发现RV变化，交调用方
		// 重新核验领导权/原意图，不在这里自动采用新对象并重试旧动作。
		fresh, err := store.Load(ctx)
		if err != nil {
			return nil, err
		}
		if fresh.vm.ResourceVersion != snapshot.vm.ResourceVersion {
			return nil, fmt.Errorf("journal snapshot is stale")
		}
		return fresh, nil
	}
	if next.Sequence != snapshot.model.Sequence+1 || next.Sequence <= snapshot.model.Sequence {
		return nil, fmt.Errorf("journal sequence is not the next value")
	}
	next, encoded, err := encode(next, annotationBudget(snapshot.vm.Annotations))
	if err != nil {
		return nil, err
	}
	operations := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: store.binding.VMUID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: snapshot.vm.ResourceVersion},
		{Op: patch.OpTest, Path: "/metadata/annotations/" + patch.PathEscape(control.ModeAnnotation), Value: "1:" + store.binding.Incarnation},
		{Op: patch.OpReplace, Path: "/metadata/annotations/" + patch.PathEscape(control.JournalAnnotation), Value: encoded},
	}
	return store.write(ctx, operations, next, encoded)
}

func (store *Store) checked(snapshot *Snapshot) error {
	if snapshot == nil || snapshot.vm == nil || snapshot.vm.Spec.DSGABootstrap == nil || snapshot.binding != store.binding {
		return fmt.Errorf("invalid journal snapshot")
	}
	return nil
}

func (store *Store) Takeover(ctx context.Context, snapshot *Snapshot, previous Owner, nextHolder string) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.vm.Spec.DSGABootstrap.Ready {
		return nil, fmt.Errorf("bootstrap is held; owner takeover forbidden")
	}
	next, err := snapshot.model.Takeover(previous, nextHolder)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

// BindResource沿用完整VM UID/RV CAS，先持久原Guest绑定再向root推进floor。
// 写回复丢失没有派发许可；重读只确认同一原身份，不能改取新Guest自动重试。
func (store *Store) BindResource(ctx context.Context, snapshot *Snapshot, actor Owner, id resourceguard.Identity) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() || snapshot.vm.Spec.DSGABootstrap == nil || !snapshot.vm.Spec.DSGABootstrap.Ready || snapshot.vm.Spec.DSGABootstrap.ComputeInstanceID != id.ComputeInstanceID {
		return nil, fmt.Errorf("resource binding requires original ready running VM")
	}
	next, err := snapshot.model.BindResource(actor, id)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

// BindPG只把已认证的首次只读发现固定到原VM，不能充当预算/首次开放许可。
// UID/RV CAS与全部操作共享同一账本；写后丢回复返回UnconfirmedWrite而非
// 可用worker。调用方只能重读原绑定，绝不能换当前boot重试或先派发PG动作。
func (store *Store) BindPG(ctx context.Context, snapshot *Snapshot, actor Owner, instance control.Instance) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() || snapshot.vm.Spec.DSGABootstrap.ComputeInstanceID != instance.ComputeInstanceID {
		return nil, fmt.Errorf("initial PG binding requires original ready running VM")
	}
	next, err := snapshot.model.BindPG(actor, instance)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

func (store *Store) Begin(ctx context.Context, snapshot *Snapshot, actor Owner, request control.Request, now uint64) (*Snapshot, Disposition, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, Stale, err
	}
	if snapshot.vm.DeletionTimestamp != nil {
		return nil, Stale, fmt.Errorf("cannot begin an operation on a deleting VM")
	}
	if !snapshot.vm.Spec.DSGABootstrap.Ready || request.Key.Instance.ComputeInstanceID != snapshot.vm.Spec.DSGABootstrap.ComputeInstanceID {
		return nil, Stale, fmt.Errorf("operation requires the ready original Compute bootstrap")
	}
	next, disposition, err := snapshot.model.Begin(actor, request, now)
	if err != nil {
		return nil, disposition, err
	}
	result, err := store.apply(ctx, snapshot, next)
	return result, disposition, err
}

// FenceUnacceptedBegin只关闭未被接收且已经过期的原Begin投递窗口。
// Begin和本屏障共用VM UID/RV CAS：若原Begin先提交，本CAS失败，调用者须
// 读回原WAL；若屏障先提交，全部延迟旧RV的Begin均失败。屏障只推进日志
// 序列，不新增操作/效果/预算/NoEffect，不影响其他域的执行未知或历史。
// 回复丢失仍是UnconfirmedWrite；调用者保留最后发送的原RV，重读新RV及
// 完整Lookup方可关闭本地投递栅栏，不能仅凭期限或404证明未接收。
func (store *Store) FenceUnacceptedBegin(ctx context.Context, snapshot *Snapshot, actor Owner, request control.Request, now uint64) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	m := snapshot.model
	if actor != m.Owner || request.Validate() != nil || request.Key.IssuerEpoch != actor.Epoch || m.PGInstance == nil || request.Key.Instance != *m.PGInstance || m.Active != nil || m.Lookup(request) != Stale || now < uint64(request.DeadlineUnixMillis) {
		return nil, fmt.Errorf("Begin fence requires original owner and expired unaccepted bound request")
	}
	next, err := m.next(actor)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

func (store *Store) Start(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key, action Action, target string, command []byte) (*Snapshot, *Dispatch, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, nil, fmt.Errorf("VM cannot dispatch in this lifecycle")
	}
	next, err := snapshot.model.Start(actor, key, action, target, command)
	if err != nil {
		return nil, nil, err
	}
	return store.dispatch(ctx, snapshot, next, actor, key, action)
}

func (store *Store) dispatch(ctx context.Context, previous *Snapshot, next Model, actor Owner, key control.Key, action Action) (*Snapshot, *Dispatch, error) {
	result, err := store.apply(ctx, previous, next)
	if err != nil {
		return nil, nil, err
	}
	var dispatched *Step
	if action == AbortReconcile {
		dispatched = result.model.Active.Cleanup
	} else {
		for i := range result.model.Active.Steps {
			if result.model.Active.Steps[i].Action == action {
				dispatched = &result.model.Active.Steps[i]
				break
			}
		}
	}
	if dispatched == nil || dispatched.Phase != Unconfirmed {
		return nil, nil, &UnconfirmedWrite{Err: fmt.Errorf("dispatch record is missing")}
	}
	return result, &Dispatch{Binding: store.binding, Owner: actor, Key: key, RequestSHA256: result.model.Active.RequestSHA256, JournalSequence: result.model.Sequence, Step: cloneStep(*dispatched)}, nil
}

func (store *Store) Record(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key, observation Observation) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.Record(actor, key, observation)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

// 首次预算/开放与Resize共享同一个VM UID/RV线性化点。保存成功不是外部
// 开放回执；尤其Start失败不返回票据，调用者不得继续发送native setter。
func (store *Store) BindInitialBudget(ctx context.Context, snapshot *Snapshot, actor Owner, initial budget.Initial) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, fmt.Errorf("VM cannot publish initial budget in this lifecycle")
	}
	next, err := snapshot.model.BindInitialBudget(actor, initial)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

// InitialDispatch不包含伪造的Resize Key；命令摘要及StartedSequence固定这次
// 原首次开放。只能在Start的UID/RV CAS回复完整确认后取得本票据。
type InitialDispatch struct {
	Binding         Binding
	Owner           Owner
	JournalSequence control.Uint64
	Admission       InitialAdmission
}

func (store *Store) StartInitialAdmission(ctx context.Context, snapshot *Snapshot, actor Owner, command budget.InitialAdmissionCommand) (*Snapshot, *InitialDispatch, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, nil, fmt.Errorf("VM cannot start first admission in this lifecycle")
	}
	next, err := snapshot.model.StartInitialAdmission(actor, command)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, snapshot, next)
	if err != nil {
		return nil, nil, err
	}
	if result.model.InitialHeld == nil || result.model.InitialHeld.Admission == nil || result.model.InitialHeld.Admission.Phase != Unconfirmed {
		return nil, nil, &UnconfirmedWrite{Err: fmt.Errorf("first admission dispatch record is missing")}
	}
	original := *cloneInitial(result.model.InitialHeld).Admission
	return result, &InitialDispatch{Binding: store.binding, Owner: actor, JournalSequence: result.model.Sequence, Admission: original}, nil
}

func (store *Store) RecordInitialAdmission(ctx context.Context, snapshot *Snapshot, actor Owner, receipt budget.InitialAdmissionReceipt) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.RecordInitialAdmission(actor, receipt)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

// RetainFinalizedBudget是经执行器完整重新核验后的显式v2升级CAS，绝不是Load
// 解码时的自动补写。只改同一原Active的长期预算保存，不改动旧原命令和回执。
func (store *Store) RetainFinalizedBudget(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.RetainFinalizedBudget(actor, key)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

func (store *Store) RequestAbort(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.RequestAbort(actor, key)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}

func (store *Store) StartCleanup(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key, target string, command []byte) (*Snapshot, *Dispatch, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, nil, err
	}
	next, err := snapshot.model.StartCleanup(actor, key, target, command)
	if err != nil {
		return nil, nil, err
	}
	return store.dispatch(ctx, snapshot, next, actor, key, AbortReconcile)
}

func (store *Store) Finish(ctx context.Context, snapshot *Snapshot, actor Owner, key control.Key) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.Finish(actor, key)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, snapshot, next)
}
