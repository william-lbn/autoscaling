package hostscheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// 用原真实HTTP fixture模拟首个Pending Runner；Node标量引用实际117/118，
// Outside在本夹具合并表示已有完整预算，不冒系统/平台资格认证。
func preflightFixture(t *testing.T, capacity, used int64) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	_, f, n, pod := schedulerPodFloorFixture(t)
	n.SystemBytes = control.Uint64(used)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Node.Status.Allocatable[core.ResourceMemory] = *resource.NewQuantity(capacity, resource.DecimalSI)
		f.Pods["testing/runner"].Spec.NodeName = ""
		f.Pods["testing/runner"].Finalizers = nil
		f.VMs["testing/vm"].Status.Node = ""
		f.VMs["testing/vm"].Status.MemorySize = nil
	})
	s, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	_, err = s.domains[n.Binding.NodeName].store.InitializeAllocations(context.Background(), "1", n.SystemBytes, n.Profile, []hostguard.Placement{})
	require.NoError(t, err)
	return s, f, n, pod
}

func preflightState(t *testing.T, f *dsgatest.SchedulerAPI) string {
	t.Helper()
	var raw []byte
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		var err error
		raw, err = json.Marshal([]any{f.Node, f.Lease, f.Pods, f.VMs, f.NodeWrites, f.PodWrites, f.VMWrites})
		require.NoError(t, err)
	})
	return string(raw)
}

func TestSchedulerPreflightUsesCompleteElevenGiWithoutWrites(t *testing.T) {
	for _, c := range []struct {
		name           string
		capacity, used int64
		fits           bool
	}{
		{"117", 25213054976, 13390315520, true},
		{"118", 25212862464, 24966594560, false},
		{"119", 24 << 30, 21 << 30, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, f, n, pod := preflightFixture(t, c.capacity, c.used)
			before := preflightState(t, f)
			for i := 0; i < 2; i++ {
				err := s.CheckPlacement(context.Background(), n.Binding.NodeName, pod, 0)
				if c.fits {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "capacity")
				}
			}
			require.Equal(t, before, preflightState(t, f), "试算不能保护Pod/写Node、Lease、VM或改变计数")
			if c.fits {
				_, held, err := s.Reserve(context.Background(), n.Binding.NodeName, pod, 0)
				require.NoError(t, err)
				require.EqualValues(t, 7<<30, held.MemoryBytes)
				require.EqualValues(t, 11<<30, held.PodMemoryLimitBytes)
				require.NotEqual(t, before, preflightState(t, f), "通过预检并非授予，仍需正式Reserve写入")
			}
		})
	}
}

func TestSchedulerPreflightRejectsUnknownOrChangedSourcesWithoutWrites(t *testing.T) {
	for _, mode := range []string{"pod-uid", "vm-uid", "short-limit", "lease-uid", "expired", "node-uid", "profile", "missing-ledger", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, pod := preflightFixture(t, 25213054976, 13390315520)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				switch mode {
				case "pod-uid":
					f.Pods["testing/runner"].UID = "replacement-pod"
				case "vm-uid":
					f.VMs["testing/vm"].UID = "replacement-vm"
				case "short-limit":
					f.Pods["testing/runner"].Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("10Gi")
				case "lease-uid":
					f.Lease.UID = "replacement-lease"
				case "expired":
					stamp := metav1.NewMicroTime(time.Now().Add(-time.Hour))
					f.Lease.Spec.RenewTime = &stamp
				case "node-uid":
					f.Node.UID = "replacement-node"
				case "profile":
					var m hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &m))
					m.Profile.ID = "other-profile"
					raw, err := json.Marshal(m)
					require.NoError(t, err)
					f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
				case "missing-ledger":
					delete(f.Node.Annotations, hostguard.LedgerAnnotation)
				}
			})
			before := preflightState(t, f)
			ctx := context.Background()
			if mode == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			require.Error(t, s.CheckPlacement(ctx, n.Binding.NodeName, pod, 0))
			require.Equal(t, before, preflightState(t, f))
		})
	}
}

func TestSchedulerPreflightKeepsOriginalFloorAndRejectsStaleNodeVersion(t *testing.T) {
	s, f, n, pod := preflightFixture(t, 25213054976, 13390315520)
	ctx := context.Background()
	_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.VMs["testing/vm"].Status.MemorySize = resource.NewQuantity(8<<30, resource.BinarySI)
	})
	before := preflightState(t, f)
	require.NoError(t, s.CheckPlacement(ctx, n.Binding.NodeName, pod, 0))
	require.Equal(t, before, preflightState(t, f), "8Gi物理候选仍11Gi，不把试算写成实际状态")
	_, row, _, err := s.allocation(ctx, n, pod, 0)
	require.NoError(t, err)
	store := s.domains[n.Binding.NodeName].store
	snap, err := store.Load(ctx)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "stale-filter-version" })
	before = preflightState(t, f)
	require.ErrorContains(t, store.CheckPlacement(ctx, snap, row, nil), "Node/Lease changed")
	require.Equal(t, before, preflightState(t, f))
}
