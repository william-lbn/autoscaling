package journal

import (
	"context"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
)

func modelInspectorVM(t *testing.T) (*vmv1.VirtualMachine, Binding) {
	t.Helper()
	f, s := newStoreAPI(t)
	initial, err := s.Initialize(context.Background(), "opaque-1", "inspector-owner")
	require.NoError(t, err)
	_, err = s.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "inspector-config")
	require.NoError(t, err)
	f.mu.Lock()
	vm := f.vm.DeepCopy()
	f.mu.Unlock()
	vm.Status.Phase = vmv1.VmRunning
	vm.Status.PodName = "runner"
	vm.Status.PodIP = "192.0.2.1"
	return vm, testBinding()
}

// 缓存只复用原文解析；fresh元数据和坏原文仍按原Inspector合同处理。
func TestModelInspectorExactBytesAndFreshMetadata(t *testing.T) {
	vm, b := modelInspectorVM(t)
	var inspector ModelInspector
	first, err := inspector.Inspect(vm, b)
	require.NoError(t, err)
	require.True(t, first.Dispatchable())
	original := first.State()
	fresh := vm.DeepCopy()
	fresh.ResourceVersion = "status-only-new-rv"
	next, err := inspector.Inspect(fresh, b)
	require.NoError(t, err)
	require.Equal(t, fresh.ResourceVersion, next.ResourceVersion())
	nextState := next.State()
	nextState.Owner.HolderID = "caller-copy"
	again, err := inspector.Inspect(fresh, b)
	require.NoError(t, err)
	require.Equal(t, original.Owner, again.State().Owner)
	deleted := fresh.DeepCopy()
	now := metav1.Now()
	deleted.DeletionTimestamp = &now
	snapshot, err := inspector.Inspect(deleted, b)
	require.NoError(t, err)
	require.False(t, snapshot.Dispatchable(), "命中不能复用原Snapshot的生命周期")
	pending := fresh.DeepCopy()
	pending.Status.Phase = vmv1.VmPending
	snapshot, err = inspector.Inspect(pending, b)
	require.NoError(t, err)
	require.False(t, snapshot.Dispatchable())
	for _, mode := range []string{"missing", "corrupt", "other-uid", "other-incarnation"} {
		t.Run(mode, func(t *testing.T) {
			bad := fresh.DeepCopy()
			binding := b
			switch mode {
			case "missing":
				delete(bad.Annotations, control.JournalAnnotation)
			case "corrupt":
				bad.Annotations[control.JournalAnnotation] = "X" + bad.Annotations[control.JournalAnnotation][1:]
			case "other-uid":
				bad.UID = "other-vm-uid"
				binding.VMUID = bad.UID
			case "other-incarnation":
				binding.Incarnation = strings.Repeat("b", 32)
				bad.Spec.DSGABootstrap.Incarnation = binding.Incarnation
				bad.Annotations[control.ModeAnnotation] = "1:" + binding.Incarnation
			}
			_, err := inspector.Inspect(bad, binding)
			require.Error(t, err)
			_, err = inspector.Inspect(fresh, b)
			require.NoError(t, err, "坏输入不能替代原有效缓存")
		})
	}
	newer, err := original.Takeover(original.Owner, "new-inspector-owner")
	require.NoError(t, err)
	_, wire, err := encode(newer, MaxEncodedBytes)
	require.NoError(t, err)
	changed := fresh.DeepCopy()
	changed.Annotations[control.JournalAnnotation] = wire
	changed.ResourceVersion = "new-journal-rv"
	snapshot, err = inspector.Inspect(changed, b)
	require.NoError(t, err)
	require.Equal(t, newer.Owner, snapshot.State().Owner)
	require.Equal(t, newer.Sequence, snapshot.State().Sequence)
}

// 用分配量验证重复完整解析确实消除，不用墙钟阈值或内部计数模拟性能通过。
func TestModelInspectorReuseReducesDecodeAllocations(t *testing.T) {
	vm, b := modelInspectorVM(t)
	var inspector ModelInspector
	_, err := inspector.Inspect(vm, b)
	require.NoError(t, err)
	cached := testing.AllocsPerRun(1, func() {
		if _, err := inspector.Inspect(vm, b); err != nil {
			panic("cache inspect failed")
		}
	})
	strict := testing.AllocsPerRun(1, func() {
		if _, err := Inspect(vm, b); err != nil {
			panic("strict inspect failed")
		}
	})
	t.Logf("samebytes cached_allocs=%.0f strict_allocs=%.0f", cached, strict)
	require.Less(t, cached, strict/4)
}
