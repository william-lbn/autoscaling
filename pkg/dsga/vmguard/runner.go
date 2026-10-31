package vmguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"time"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const (
	FixedStateDir    = "/vm/dsga-runner/active"
	VirtioBlockBytes = 8 << 20
)

// Config由Controller原VM快照和Downward API提供，不接受远程请求决定路径、
// VM UID或Kube端点。Min/Max用真实VM重新核验，防止旧flag启动不同几何QEMU。
type Config struct {
	StateDir         string
	PodName          string
	PodUID           string
	NodeName         string
	MinimumBytes     control.Uint64
	MaximumBytes     control.Uint64
	InitialBootstrap *InitialBootstrapConfig
}

type Runner struct {
	engine  *engine
	store   *journal.Store
	core    coreclient.CoreV1Interface
	coord   coordclient.CoordinationV1Interface
	birth   Birth
	pod     string
	node    string
	claims  *budgetclaims.Reader
	initial initialLifecycle
}

// Initialize只用于一次明确的原Runner出生，必须在QEMU启动之前。目录由启动
// 入口显式安装，不在丢日志后补建；本地birth-intent先fsync，再创建受保护CM，
// 最后持久固定真实UID。任何已有birth/state都拒绝自动重启/重新发放首次身份。
// 出生选择与nonce一起持久；同调用失败或无defer的直接崩溃都只允许后继
// 在原flock内封存不可撤销屏障，再补齐同nonce的关闭来源，
// 始终返回错误，不把完整关闭来源转换为可启动Runner。既有运行WAL不走此分支。
func Initialize(ctx context.Context, store *journal.Store, coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, c Config) (result *Runner, initErr error) {
	if store == nil || coreClient == nil || coord == nil || !filepath.IsAbs(c.StateDir) || control.ValidateID(c.PodName, "Runner Pod name") != nil || control.ValidateID(c.PodUID, "Runner Pod UID") != nil || control.ValidateID(c.NodeName, "Node") != nil {
		return nil, fmt.Errorf("original Runner requires fixed Kubernetes clients, directory and Downward API identity")
	}
	mode, modeErr := initialModeForConfig(c)
	if modeErr != nil {
		return nil, modeErr
	}
	if c.InitialBootstrap != nil {
		v := *c.InitialBootstrap
		v.RuntimeJSON = append([]byte(nil), v.RuntimeJSON...)
		c.InitialBootstrap = &v
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s, err := lockState(callCtx, c.StateDir)
	if err != nil {
		return nil, err
	}
	var failed *failedBirth
	defer func() {
		// 此调用尚未返回Runner，因此QEMU不可能取得本调用的启动权。
		// 先在原flock内封存失败，再释放锁补齐仅供关闭的原来源；无论
		// 补发是否成功，返回值始终nil/error，不能把失败恢复成正常出生。
		sealed := false
		if initErr != nil && failed != nil {
			sealErr := sealFailedBirth(s, *failed)
			initErr = errors.Join(initErr, sealErr)
			sealed = sealErr == nil
		}
		s.close()
		if sealed {
			initErr = errors.Join(initErr, closeFailedBirth(store, coreClient, coord, c, *failed))
		}
	}()
	if raw, err := s.readProcessFile("birth-abort.json"); err != nil {
		return nil, err
	} else if raw != nil {
		var original failedBirth
		if json.Unmarshal(raw, &original) != nil || original.validate(store.Binding(), c) != nil {
			return nil, fmt.Errorf("original failed Runner birth is malformed or replaced")
		}
		failed = &original
		return nil, ErrStopped
	}
	if raw, err := s.readProcessFile("birth-intent.json"); err != nil {
		return nil, err
	} else if raw != nil {
		var original failedBirth
		if json.Unmarshal(raw, &original) != nil || original.validate(store.Binding(), c) != nil || original.StateClaim != nil || original.BindingClaim != nil {
			return nil, fmt.Errorf("interrupted Runner birth lacks its complete original selection; controlled recovery required")
		}
		// 先检查原WAL与launch，不以“没有当前进程”猜NoStart。持有同一
		// flock的旧RunQEMU必须先写launch；它若先获得许可，此处拒绝，
		// 它若后到，则会看到defer封存的abort并被拒绝。绝不返回Runner。
		prior, err := s.load()
		if err != nil || prior != nil && !pristineBirthRecord(prior, original) {
			return nil, fmt.Errorf("interrupted Runner birth has unreadable or nonpristine execution history; controlled recovery required")
		}
		launch, err := s.readProcessFile("process-launch.json")
		if err != nil || launch != nil {
			return nil, fmt.Errorf("interrupted Runner birth already has an existing or unknown launch; controlled recovery required")
		}
		failed = &original
		return nil, ErrStopped
	}
	old, err := s.load()
	if err != nil || old != nil {
		return nil, fmt.Errorf("Runner journal already exists or is unreadable; controlled recovery required")
	}
	snapshot, err := store.Load(callCtx)
	if err != nil {
		return nil, err
	}
	model, vm := snapshot.State(), snapshot.VirtualMachine()
	held, holdErr := vm.DSGABootstrapHeld()
	if held || holdErr != nil || vm.Spec.DSGABootstrap == nil || !vm.Spec.DSGABootstrap.Ready || vm.DeletionTimestamp != nil || model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 || model.ResourceIdentity != nil || model.PGInstance != nil || model.BudgetHeld != nil {
		return nil, fmt.Errorf("Runner birth requires original ready pristine VM journal")
	}
	if vm.Spec.CpuScalingMode == nil || *vm.Spec.CpuScalingMode != vmv1.CpuScalingModeSysfs {
		return nil, fmt.Errorf("managed Runner requires sysfs CPU mode; raw public QMP is disabled")
	}
	slot, exact := vm.Spec.Guest.MemorySlotSize.AsInt64()
	minSlots, maxSlots := uint64(vm.Spec.Guest.MemorySlots.Min), uint64(vm.Spec.Guest.MemorySlots.Max)
	if !exact || slot <= 0 || minSlots == 0 || maxSlots < minSlots || maxSlots > uint64(math.MaxInt64)/uint64(slot) || c.MinimumBytes != control.Uint64(minSlots*uint64(slot)) || c.MaximumBytes != control.Uint64(maxSlots*uint64(slot)) {
		return nil, fmt.Errorf("Runner flags differ from original VM memory geometry")
	}
	pod, err := coreClient.Pods(store.Binding().Namespace).Get(callCtx, c.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := originalPod(pod, store.Binding(), c.PodName, c.PodUID, c.NodeName); err != nil {
		return nil, err
	}
	if vm.Spec.ServiceAccountName == "" || vm.Spec.ServiceAccountName == "default" || pod.Spec.ServiceAccountName != vm.Spec.ServiceAccountName || pod.Spec.AutomountServiceAccountToken == nil || !*pod.Spec.AutomountServiceAccountToken {
		return nil, fmt.Errorf("managed Runner requires explicit original service account and API token mount")
	}
	if vm.Status.PodName != c.PodName || vm.Status.Node != "" && vm.Status.Node != c.NodeName || vm.Status.RestartCount != 0 {
		return nil, fmt.Errorf("Runner birth VM status belongs to another Pod/Node/restart")
	}
	node, err := coreClient.Nodes().Get(callCtx, c.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node == nil || node.Name != c.NodeName || node.UID == "" || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return nil, fmt.Errorf("original Runner Node unavailable")
	}
	// 必须早于birth-intent/来源创建/QEMU。PG尚未启动不代表首次内存免费；
	// Scheduler Reserve响应未知时保留容量但不许绑定，最终Runner再以自己
	// 的API读取核原持有，防止绕过Scheduler直接指定nodeName启动。
	use := uint64(vm.Spec.Guest.MemorySlots.Use)
	if use < minSlots || use > maxSlots {
		return nil, fmt.Errorf("initial Runner use is outside original VM geometry")
	}
	if err := hostguard.InspectInitialVM(node, store.Binding(), c.PodName, c.PodUID, control.Uint64(use*uint64(slot))); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := Identity{VMUID: string(store.Binding().VMUID), Incarnation: store.Binding().Incarnation, RunnerPodUID: c.PodUID, RunnerBootID: hex.EncodeToString(nonce[:]), NodeUID: string(node.UID), MinimumBytes: c.MinimumBytes, MaximumBytes: c.MaximumBytes, BlockBytes: VirtioBlockBytes}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if mode != nil {
		if e := mode.validate(id); e != nil {
			return nil, e
		}
	}
	original := failedBirth{ProtocolVersion: initialBirthVersion(mode), InitialMode: cloneInitialMode(mode), Identity: id, Owner: model.Owner, PodName: c.PodName, NodeName: c.NodeName, StateClaim: nil, BindingClaim: nil}
	if err := s.birthIntent(original); err != nil {
		return nil, err
	}
	failed = &original
	initial := Publication{ProtocolVersion: initialBirthVersion(mode), Identity: id, BindingClaim: nil, State: nil, Actual: nil}
	raw, err := json.Marshal(initial)
	if err != nil {
		return nil, err
	}
	state, err := createClaim(callCtx, coreClient, store.Binding(), newClaim(store.Binding(), stateName(store.Binding()), stateKind, false, raw), stateKind)
	if err != nil {
		return nil, err
	}
	stateRef := budget.ObjectRef{Name: state.Name, UID: string(state.UID)}
	failed.StateClaim = &stateRef
	raw, err = json.Marshal(stateRef)
	if err != nil {
		return nil, err
	}
	// GET已确认的UID必须在下一次CREATE前独立fsync。直接崩溃后不能把
	// 它降成未知CREATE，并收养同名但新UID的替代来源。
	if err := s.writeOnce("birth-close-state.json", raw); err != nil {
		return nil, err
	}
	birth := Birth{ProtocolVersion: initialBirthVersion(mode), InitialMode: cloneInitialMode(mode), Identity: id, StateClaim: budget.ObjectRef{Name: state.Name, UID: string(state.UID)}}
	raw, err = json.Marshal(birth)
	if err != nil {
		return nil, err
	}
	birthCM, err := createClaim(callCtx, coreClient, store.Binding(), newClaim(store.Binding(), BindingName(store.Binding()), bindingKind, true, raw), bindingKind)
	if err != nil {
		return nil, err
	}
	ref := budget.ObjectRef{Name: birthCM.Name, UID: string(birthCM.UID)}
	failed.BindingClaim = &ref
	raw, err = json.Marshal(failedBirthSources{State: stateRef, Binding: ref})
	if err != nil {
		return nil, err
	}
	if err := s.writeOnce("birth-close-sources.json", raw); err != nil {
		return nil, err
	}
	if err := s.save(&record{ProtocolVersion: 1, Identity: id, BindingClaim: ref, Sequence: 1, Floor: model.Owner, LeaseUID: "", HighWater: nil, Pending: nil, Last: nil, LastPublished: nil}); err != nil {
		return nil, err
	}
	return boundRunner(store, coreClient, coord, c, birth, ref)
}

func boundRunner(store *journal.Store, coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, c Config, birth Birth, ref budget.ObjectRef) (*Runner, error) {
	claims, err := budgetclaims.New(coreClient, store.Binding())
	if err != nil {
		return nil, err
	}
	r := &Runner{store: store, core: coreClient, coord: coord, birth: birth, pod: c.PodName, node: c.NodeName, claims: claims, engine: nil}
	r.engine = &engine{dir: c.StateDir, id: birth.Identity, binding: ref, native: &nativeQMP{id: birth.Identity, path: filepath.Join(c.StateDir, "qmp.sock"), timeout: 2 * time.Second}, now: time.Now, read: nil}
	r.engine.read = r.authorize
	r.engine.initialMode = cloneInitialMode(birth.InitialMode)
	if birth.InitialMode != nil {
		lifecycle, e := newInitialLifecycle(r, c.InitialBootstrap)
		if e != nil {
			return nil, e
		}
		r.initial = lifecycle
		r.engine.native = lifecycle
		r.engine.initialGate = lifecycle.gate
	}
	return r, nil
}

func originalPod(pod *core.Pod, binding journal.Binding, name, uid, node string) error {
	if err := originalPodIdentity(pod, binding, name, uid, node); err != nil {
		return err
	}
	if pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed {
		return ErrStopped
	}
	return nil
}

// 终态关闭只复用原身份校验；活动启动/执行另由originalPod拒绝不可撤回终态。
func originalPodIdentity(pod *core.Pod, binding journal.Binding, name, uid, node string) error {
	if pod == nil || pod.Namespace != binding.Namespace || pod.Name != name || string(pod.UID) != uid || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Spec.NodeName != node {
		return fmt.Errorf("original live Runner Pod identity differs")
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != binding.Name || owner.UID != binding.VMUID {
		return fmt.Errorf("Runner Pod belongs to another VM")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount != 0 {
			return fmt.Errorf("original Runner container has restarted; controlled recovery required")
		}
	}
	return nil
}

func (r *Runner) QMPArguments() []string {
	uuid, _ := r.birth.Identity.QEMUUUID()
	return []string{"-uuid", uuid, "-qmp", "unix:" + filepath.Join(r.engine.dir, "qmp.sock") + ",server,wait=off"}
}

// Tick没有通用外部命令入口，仅拉取固定原VM账本。派发失败仍尽力发布已保留的
// 原结果和当前实际值；所有网络/私有QMP调用在有期限ctx内，取消不清WAL/预算。
func (r *Runner) Tick(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := r.engine.advance(callCtx)
	if errors.Is(err, ErrStopped) {
		return err
	}
	return errors.Join(err, r.publish(callCtx))
}

func (r *Runner) Run(ctx context.Context, report func(error)) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		if err := r.Tick(ctx); err != nil {
			if errors.Is(err, ErrStopped) {
				return
			}
			if ctx.Err() == nil && report != nil {
				report(err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

// BootChannelArguments只在显式新mode提供固定私有virtio端口，不接受HTTP路径。
func (r *Runner) BootChannelArguments() []string {
	if r == nil || r.initial == nil {
		return nil
	}
	return append([]string(nil), r.initial.arguments()...)
}
