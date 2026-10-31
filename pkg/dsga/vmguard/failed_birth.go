package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// failedBirth记录完整的原出生选择以及同一次Initialize已确认的来源。
// 原nonce已先fsync，已确认的CM UID逐字保留；未知CREATE只能用原nonce/
// 原文取得首次确认。它不是已有运行实例缺日志、无Wait或跨boot的恢复许可。
// 原始无ref选择先原子fsync为birth-intent，直接崩溃后也只允许转为NoStart
// 关闭；已确认ref再独立fsync。本地屏障不可撤销，补齐来源不能返回为可执行Runner。
type failedBirth struct {
	ProtocolVersion uint32                `json:"protocol_version"`
	Identity        Identity              `json:"identity"`
	Owner           journal.Owner         `json:"owner"`
	PodName         string                `json:"pod_name"`
	NodeName        string                `json:"node_name"`
	StateClaim      *budget.ObjectRef     `json:"state_claim"`
	BindingClaim    *budget.ObjectRef     `json:"binding_claim"`
	InitialMode     *InitialBootstrapMode `json:"initial_mode,omitempty"`
}

func (f *failedBirth) UnmarshalJSON(raw []byte) error {
	type plain failedBirth
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	if e := rejectLegacyInitialField(raw, next.ProtocolVersion, "initial_mode"); e != nil {
		return e
	}
	*f = failedBirth(next)
	return nil
}

func (f failedBirth) validate(b journal.Binding, c Config) error {
	if !initialVersionMatches(f.ProtocolVersion, f.InitialMode, f.Identity) || f.Identity.Validate() != nil || f.Owner.Validate() != nil || f.Identity.VMUID != string(b.VMUID) || f.Identity.Incarnation != b.Incarnation || f.Identity.RunnerPodUID != c.PodUID || f.Identity.MinimumBytes != c.MinimumBytes || f.Identity.MaximumBytes != c.MaximumBytes || f.PodName != c.PodName || f.NodeName != c.NodeName || control.ValidateID(f.PodName, "original failed Runner Pod") != nil || control.ValidateID(f.NodeName, "original failed Runner Node") != nil {
		return fmt.Errorf("failed Runner birth lost original identity or geometry")
	}
	expected, e := initialModeForConfig(c)
	if e != nil {
		return e
	}
	if !sameInitialMode(f.InitialMode, expected) {
		return fmt.Errorf("failed birth original initial mode changed")
	}
	if f.StateClaim != nil && (f.StateClaim.Validate() != nil || f.StateClaim.Name != stateName(b)) || f.BindingClaim != nil && (f.BindingClaim.Validate() != nil || f.BindingClaim.Name != BindingName(b) || f.StateClaim == nil) {
		return fmt.Errorf("failed Runner birth changed confirmed source references")
	}
	return nil
}

// 同原目录flock内核对原出生意图和launch缺失，再以不替换link+fsync封存。
// API尚未可达也先阻止任何后继Start；文件不完整/符号链接/launch未知均拒绝。
// 不能靠“此时没有state.json”推导没有运行过，更不能补写新的birth-intent。
func sealFailedBirth(s *lockedState, f failedBirth) error {
	raw, err := s.readProcessFile("birth-intent.json")
	var original Identity
	var selected failedBirth
	if err != nil || raw == nil {
		return fmt.Errorf("failed birth lacks its exact original nonce")
	}
	if json.Unmarshal(raw, &selected) == nil {
		// 原选择没有已确认ref；它们只能来自后续独立检查点或原失败屏障。
		// 不接受另一个owner/Pod/Node借相同nonce封存本出生。
		if selected.ProtocolVersion != f.ProtocolVersion || !sameInitialMode(selected.InitialMode, f.InitialMode) || selected.Identity != f.Identity || selected.Owner != f.Owner || selected.PodName != f.PodName || selected.NodeName != f.NodeName || selected.StateClaim != nil || selected.BindingClaim != nil {
			return fmt.Errorf("failed birth differs from original durable selection")
		}
	} else if json.Unmarshal(raw, &original) != nil || original != f.Identity {
		// 旧完整失败屏障仍可补发原关闭；仅有旧Identity的孤立出生不能从
		// 当前VM/Lease猜回原owner，因此Initialize不会为它创建新屏障。
		return fmt.Errorf("failed birth lacks its exact original nonce")
	}
	launch, err := s.readProcessFile("process-launch.json")
	if err != nil || launch != nil {
		return fmt.Errorf("failed birth cannot close an existing or unknown launch")
	}
	raw, err = json.Marshal(f)
	if err != nil {
		return err
	}
	return s.writeOnce("birth-abort.json", raw)
}

type failedBirthSources struct {
	State   budget.ObjectRef `json:"state"`
	Binding budget.ObjectRef `json:"binding"`
}

// 仅出生记录的初始态可转NoStart；任何floor接管、派发、结果或发布都说明
// 已进入执行生命周期。不能将它们归零后补关闭，更不能覆盖缺损的已有记录。
func pristineBirthRecord(prior *record, original failedBirth) bool {
	return prior.validate() == nil && prior.ProtocolVersion == 1 && prior.Sequence == 1 && prior.Identity == original.Identity && prior.Floor == original.Owner && prior.LeaseUID == "" && prior.HighWater == nil && prior.Pending == nil && prior.Last == nil && prior.LastPublished == nil
}

// 该恢复只从封存失败的原目录进入；补齐来源期间持同一VM flock，网络均有
// 总期限，不持全局锁。此前未知的CREATE不重生nonce，已知UID绝不换同名对象。
// State的UID先独立fsync，不能因后续Binding失败而丢失已确认身份；两端UID
// 再一起fsync为关闭检查点，然后才允许保存仅关闭用的初始state。
// 缺/坏的已有state、非pristine状态或其他出生一律拒绝，不用空日志兜底。
func recoverFailedBirth(ctx context.Context, store *journal.Store, coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, c Config, original failedBirth) (*Runner, error) {
	if original.validate(store.Binding(), c) != nil {
		return nil, fmt.Errorf("invalid failed birth recovery selection")
	}
	s, err := lockState(ctx, c.StateDir)
	if err != nil {
		return nil, err
	}
	defer s.close()
	if err := sealFailedBirth(s, original); err != nil {
		return nil, err
	}
	stateRef, bindingRef := original.StateClaim, original.BindingClaim
	if raw, err := s.readProcessFile("birth-close-state.json"); err != nil {
		return nil, err
	} else if raw != nil {
		var saved budget.ObjectRef
		if json.Unmarshal(raw, &saved) != nil || saved.Validate() != nil || saved.Name != stateName(store.Binding()) || stateRef != nil && *stateRef != saved {
			return nil, fmt.Errorf("failed birth confirmed state checkpoint differs")
		}
		stateRef = &saved
	}
	var sources *failedBirthSources
	if raw, err := s.readProcessFile("birth-close-sources.json"); err != nil {
		return nil, err
	} else if raw != nil {
		var saved failedBirthSources
		if control.UnmarshalObject(raw, &saved) != nil || saved.State.Validate() != nil || saved.Binding.Validate() != nil || saved.State.Name != stateName(store.Binding()) || saved.Binding.Name != BindingName(store.Binding()) || stateRef == nil || *stateRef != saved.State || bindingRef != nil && *bindingRef != saved.Binding {
			return nil, fmt.Errorf("failed birth closing sources are malformed or replaced")
		}
		sources = &saved
	}
	prior, err := s.load()
	if err != nil {
		return nil, err
	}
	if prior != nil && !pristineBirthRecord(prior, original) {
		return nil, fmt.Errorf("failed birth cannot replace an existing execution journal")
	}
	if sources != nil {
		stateRef, bindingRef = &sources.State, &sources.Binding
	}
	if prior != nil {
		// 正式state只能后于两个真实UID的检查点。缺State检查点时不能
		// 把已存在记录当作首次未知CREATE，再创建/收养另一个State来源。
		if stateRef == nil {
			return nil, fmt.Errorf("failed birth existing journal lost its confirmed state UID")
		}
		if bindingRef != nil && *bindingRef != prior.BindingClaim {
			return nil, fmt.Errorf("failed birth changed original local binding UID")
		}
		bindingRef = &prior.BindingClaim
	}
	b := store.Binding()
	initial := Publication{ProtocolVersion: original.ProtocolVersion, Identity: original.Identity, BindingClaim: nil, State: nil, Actual: nil}
	raw, err := json.Marshal(initial)
	if err != nil {
		return nil, err
	}
	state, err := failedBirthClaim(ctx, coreClient, b, newClaim(b, stateName(b), stateKind, false, raw), stateKind, stateRef)
	if err != nil {
		return nil, err
	}
	confirmedState := budget.ObjectRef{Name: state.Name, UID: string(state.UID)}
	raw, err = json.Marshal(confirmedState)
	if err != nil {
		return nil, err
	}
	if err := s.writeOnce("birth-close-state.json", raw); err != nil {
		return nil, err
	}
	birth := Birth{ProtocolVersion: original.ProtocolVersion, InitialMode: cloneInitialMode(original.InitialMode), Identity: original.Identity, StateClaim: confirmedState}
	raw, err = json.Marshal(birth)
	if err != nil {
		return nil, err
	}
	birthCM, err := failedBirthClaim(ctx, coreClient, b, newClaim(b, BindingName(b), bindingKind, true, raw), bindingKind, bindingRef)
	if err != nil {
		return nil, err
	}
	ref := budget.ObjectRef{Name: birthCM.Name, UID: string(birthCM.UID)}
	checkpoint := failedBirthSources{State: birth.StateClaim, Binding: ref}
	raw, err = json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	if err := s.writeOnce("birth-close-sources.json", raw); err != nil {
		return nil, err
	}
	if prior == nil {
		if err := s.save(&record{ProtocolVersion: 1, Identity: original.Identity, BindingClaim: ref, Sequence: 1, Floor: original.Owner, LeaseUID: "", HighWater: nil, Pending: nil, Last: nil, LastPublished: nil}); err != nil {
			return nil, err
		}
	}
	return boundRunner(store, coreClient, coord, c, birth, ref)
}

func failedBirthClaim(ctx context.Context, client coreclient.CoreV1Interface, b journal.Binding, expected *core.ConfigMap, kind string, known *budget.ObjectRef) (*core.ConfigMap, error) {
	if known == nil {
		return createClaim(ctx, client, b, expected, kind)
	}
	actual, err := client.ConfigMaps(b.Namespace).Get(ctx, known.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if validClaim(actual, b, *known, kind, *expected.Immutable) != nil || !reflect.DeepEqual(actual.Data, expected.Data) {
		return nil, fmt.Errorf("failed birth confirmed source was deleted or replaced")
	}
	return actual, nil
}

// 不继承已取消的Initialize上下文：在固定宽限内先封存本地关闭，再有界补发
// 同一原文。返回ErrStopped仍让实际Runner入口退出；绝不因补齐成功启动QEMU。
// 原Pod全部容器终态、Node UID/RV结清、VM退休锚点及来源GC沿既有v3协议处理。
func closeFailedBirth(store *journal.Store, coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, c Config, original failedBirth) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := recoverFailedBirth(ctx, store, coreClient, coord, c, original)
	if err != nil {
		return err
	}
	for {
		closed, err := r.CloseUnlaunched(ctx)
		if !closed {
			if err == nil {
				err = fmt.Errorf("failed birth no longer confirms the original unlaunched boundary")
			}
			return err
		}
		if err == nil || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
