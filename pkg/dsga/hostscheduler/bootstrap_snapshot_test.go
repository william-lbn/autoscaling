package hostscheduler

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/rest"
)

func snapshotBootstrapFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig) {
	t.Helper()
	_, f, n := schedulerBudgetFixture(t)
	n.InitializeAtResourceVersion = ""
	n.InitializeMode = KubernetesSnapshotBootstrap
	s, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	return s, f, n
}

// 部署到启动之间的状态版本变化不能使原身份永久无法建账；普通 Pod 全额入账。
func TestSnapshotBootstrapAllowsMetadataVersionAndAccountsCompletePods(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	p := boundedPod("existing", n.Binding.NodeName, "2Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Node.ResourceVersion = "37"
		f.Node.Labels = map[string]string{"example.test/observation": "new"}
		f.Pods[p.Namespace+"/"+p.Name] = p
	})
	require.NoError(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
	snap, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
	require.NoError(t, err)
	row, err := snap.State().Placement(string(p.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, row.MemoryBytes)
	used, err := snap.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (3<<30)+(16<<20), used)
}

func TestSnapshotBootstrapDoesNotChangeLegacyFrozenVersion(t *testing.T) {
	s, f, n := schedulerBudgetFixture(t)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "37" })
	require.ErrorContains(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]), "Node version changed")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Zero(t, f.NodeWrites); require.Zero(t, f.PodWrites) })
}

func TestSnapshotBootstrapRejectsPolicyAndIdentityDriftBeforeWrite(t *testing.T) {
	for _, field := range []string{"capacity", "allocatable", "node-uid", "lease-uid", "missing-origin", "protected-empty", "malformed-ledger", "unbounded-pod", "over-budget-pod"} {
		t.Run(field, func(t *testing.T) {
			s, f, n := snapshotBootstrapFixture(t)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.Node.ResourceVersion = "37"
				f.Node.Annotations = map[string]string{}
				switch field {
				case "capacity":
					f.Node.Status.Capacity[core.ResourceMemory] = resource.MustParse("13Gi")
				case "allocatable":
					f.Node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("9Gi")
				case "node-uid":
					f.Node.UID = "replacement-node"
				case "lease-uid":
					f.Lease.UID = "replacement-lease"
				case "missing-origin":
					f.Node.Annotations[hostguard.OriginAnnotation] = "1:original"
				case "protected-empty":
					f.Node.Finalizers = []string{hostguard.ProtectionFinalizer}
				case "malformed-ledger":
					f.Node.Annotations[hostguard.LedgerAnnotation] = "{broken"
				case "unbounded-pod", "over-budget-pod":
					p := boundedPod("unsafe", n.Binding.NodeName, "8Gi")
					if field == "unbounded-pod" {
						p.Spec.Containers[0].Resources.Limits = nil
					}
					f.Pods[p.Namespace+"/"+p.Name] = p
				}
			})
			require.Error(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Zero(t, f.NodeWrites)
				require.Zero(t, f.PodWrites)
				require.Zero(t, f.VMWrites)
			})
		})
	}
}

// 首次写已提交但回复丢失：再次协调只能读取并继续同一 Origin，不能重置。
func TestSnapshotBootstrapUnknownReplyRecoversOriginalLedgerAndRestart(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	ctx := context.Background()
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "37"; f.LoseNodeReply = true })
	require.Error(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	first, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	origin := first.State().Origin
	require.NotEmpty(t, origin)
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	second, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, origin, second.State().Origin)
	restart, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Lease.Spec.HolderIdentity = &restart.holder
		v := *f.Lease.Spec.LeaseTransitions + 1
		f.Lease.Spec.LeaseTransitions = &v
	})
	require.NoError(t, restart.reconcile(ctx, restart.domains[n.Binding.NodeName]))
	after, err := restart.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, origin, after.State().Origin)
}

type snapshotConflictTransport struct {
	next    http.RoundTripper
	fixture *dsgatest.SchedulerAPI
	once    atomic.Bool
}

func (r *snapshotConflictTransport) RoundTrip(q *http.Request) (*http.Response, error) {
	if q.Method == http.MethodPatch && strings.Contains(q.URL.Path, "/nodes/") && r.once.CompareAndSwap(false, true) {
		r.fixture.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "changed-before-cas" })
	}
	return r.next.RoundTrip(q)
}

// 快照与写入之间仍需原 RV CAS；冲突后重新读取全部来源，不能沿用过时快照。
func TestSnapshotBootstrapCASConflictReobservesWithoutReset(t *testing.T) {
	s, f, n := snapshotBootstrapFixture(t)
	client := f.Core.CoreV1().RESTClient().(*rest.RESTClient).Client
	next := client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	client.Transport = &snapshotConflictTransport{next: next, fixture: f}
	require.Error(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Empty(t, f.Node.Annotations[hostguard.LedgerAnnotation])
		p := boundedPod("arrived-after-conflict", n.Binding.NodeName, "1Gi")
		f.Pods[p.Namespace+"/"+p.Name] = p
	})
	require.NoError(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
	snap, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
	require.NoError(t, err)
	_, err = snap.State().Placement("arrived-after-conflict-uid")
	require.NoError(t, err)
}

func TestSnapshotBootstrapConfigurationRequiresExplicitExclusivePolicy(t *testing.T) {
	for _, field := range []string{"unknown-mode", "frozen-version", "missing-policy", "profile-drift", "system-drift"} {
		t.Run(field, func(t *testing.T) {
			_, _, n := snapshotBootstrapFixture(t)
			switch field {
			case "unknown-mode":
				n.InitializeMode = "refresh-anything"
			case "frozen-version":
				n.InitializeAtResourceVersion = "1"
			case "missing-policy":
				n.KubernetesBudget = nil
			case "profile-drift":
				n.Profile.VMOverheadBytes++
			case "system-drift":
				n.SystemBytes++
			}
			require.Error(t, (Config{Nodes: []NodeConfig{n}}).Validate(n.Profile.SchedulerName))
		})
	}
}
