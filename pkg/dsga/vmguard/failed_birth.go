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

// failedBirth只记录同一次Initialize在返回任何可执行Runner之前的失败。
// 原nonce已先fsync，已确认的CM UID逐字保留；未知CREATE只能用原nonce/
// 原文取得首次确认。它不是已有运行实例缺日志、无Wait或跨boot的恢复许可。
// 本地屏障不可撤销；补齐的来源只有关闭用途，不能返回给正常启动调用方。
type failedBirth struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	Identity        Identity          `json:"identity"`
	Owner           journal.Owner     `json:"owner"`
	PodName         string            `json:"pod_name"`
	NodeName        string            `json:"node_name"`
	StateClaim      *budget.ObjectRef `json:"state_claim"`
	BindingClaim    *budget.ObjectRef `json:"binding_claim"`
}

func (f *failedBirth) UnmarshalJSON(raw []byte) error {
	type plain failedBirth
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*f = failedBirth(next)
	return nil
}

func (f failedBirth) validate(b journal.Binding, c Config) error {
	if f.ProtocolVersion != 1 || f.Identity.Validate() != nil || f.Owner.Validate() != nil || f.Identity.VMUID != string(b.VMUID) || f.Identity.Incarnation != b.Incarnation || f.Identity.RunnerPodUID != c.PodUID || f.Identity.MinimumBytes != c.MinimumBytes || f.Identity.MaximumBytes != c.MaximumBytes || f.PodName != c.PodName || f.NodeName != c.NodeName || control.ValidateID(f.PodName, "original failed Runner Pod") != nil || control.ValidateID(f.NodeName, "original failed Runner Node") != nil {
		return fmt.Errorf("failed Runner birth lost original identity or geometry")
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
	if err != nil || raw == nil || json.Unmarshal(raw, &original) != nil || original != f.Identity {
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
	if prior != nil && (prior.validate() != nil || prior.ProtocolVersion != 1 || prior.Sequence != 1 || prior.Identity != original.Identity || prior.Floor != original.Owner || prior.LeaseUID != "" || prior.HighWater != nil || prior.Pending != nil || prior.Last != nil || prior.LastPublished != nil) {
		return nil, fmt.Errorf("failed birth cannot replace an existing execution journal")
	}
	if sources != nil {
		stateRef, bindingRef = &sources.State, &sources.Binding
	}
	if prior != nil {
		if bindingRef != nil && *bindingRef != prior.BindingClaim {
			return nil, fmt.Errorf("failed birth changed original local binding UID")
		}
		bindingRef = &prior.BindingClaim
	}
	b := store.Binding()
	initial := Publication{ProtocolVersion: 1, Identity: original.Identity, BindingClaim: nil, State: nil, Actual: nil}
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
	birth := Birth{ProtocolVersion: 1, Identity: original.Identity, StateClaim: confirmedState}
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
