package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
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
}

// Snapshot不可由调用方修改元数据/原始模型。State返回深拷贝，不跨网络持本地锁；
// 唯一线性化点是apiserver对固定UID/resourceVersion的原子JSON Patch。
type Snapshot struct {
	binding Binding
	vm      *vmv1.VirtualMachine
	model   Model
}

func (snapshot *Snapshot) State() Model            { return snapshot.model.Clone() }
func (snapshot *Snapshot) ResourceVersion() string { return snapshot.vm.ResourceVersion }

// Dispatchable只是已读取快照的生命周期条件，不是派发票据或租约。PG显式原意图
// 重放也须拒绝正在删除/没有Runner的VM；网络竞态仍靠UID/RV及最终执行fence封闭。
func (snapshot *Snapshot) Dispatchable() bool {
	return snapshot != nil && snapshot.vm != nil && snapshot.vm.DeletionTimestamp == nil &&
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
	return &Store{client: client, binding: binding}, nil
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
	if vm == nil || vm.UID != store.binding.VMUID || vm.Name != store.binding.Name || vm.Namespace != store.binding.Namespace || vm.ResourceVersion == "" {
		return nil, fmt.Errorf("journal response VM identity changed")
	}
	if vm.Annotations[control.ModeAnnotation] != "1:"+store.binding.Incarnation {
		return nil, fmt.Errorf("journal incarnation marker missing or changed")
	}
	data, present := vm.Annotations[control.JournalAnnotation]
	if !present {
		return nil, fmt.Errorf("journal history is missing; automatic reset is forbidden")
	}
	model, err := decode(data, string(store.binding.VMUID), store.binding.Incarnation)
	if err != nil {
		return nil, err
	}
	return &Snapshot{binding: store.binding, vm: vm.DeepCopy(), model: model}, nil
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
	if bootstrapVersion == "" || vm.ResourceVersion != bootstrapVersion || control.Managed(vm.Annotations) ||
		vm.DeletionTimestamp != nil || vm.Status.RestartCount != 0 || vm.Status.PodName != "" || vm.Status.PodIP != "" || vm.Status.Node != "" ||
		(vm.Status.Phase != "" && vm.Status.Phase != vmv1.VmPending) {
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
	if snapshot == nil || snapshot.vm == nil || snapshot.binding != store.binding {
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
	if snapshot == nil || snapshot.vm == nil || snapshot.binding != store.binding {
		return fmt.Errorf("invalid journal snapshot")
	}
	return nil
}

func (store *Store) Takeover(ctx context.Context, snapshot *Snapshot, previous Owner, nextHolder string) (*Snapshot, error) {
	if err := store.checked(snapshot); err != nil {
		return nil, err
	}
	next, err := snapshot.model.Takeover(previous, nextHolder)
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
	next, disposition, err := snapshot.model.Begin(actor, request, now)
	if err != nil {
		return nil, disposition, err
	}
	result, err := store.apply(ctx, snapshot, next)
	return result, disposition, err
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
