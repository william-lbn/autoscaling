package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestManagedCPUControllerRequiresOriginalKernelFacts(t *testing.T) {
	for _, mode := range []string{"valid", "logical-only", "quota-mismatch", "foreign-boot", "foreign-pod", "foreign-node",
		"foreign-compute", "future-generation", "prior-generation", "pending-topology", "wrong-scope"} {
		t.Run(mode, func(t *testing.T) {
			var id runnercpu.Identity
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/cpu_change" {
					writes.Add(1)
					var command api.VCPUChange
					require.NoError(t, json.NewDecoder(r.Body).Decode(&command))
					require.NotNil(t, command.Managed)
					require.Equal(t, id, command.Managed.Identity)
					require.EqualValues(t, 7, command.Managed.Generation)
					require.EqualValues(t, 1500, command.VCPUs)
					return
				}
				observed := observedCPUFixture(id, 7, 1500)
				observed.Managed.ObservationNonce = r.URL.Query().Get("observation_nonce")
				switch mode {
				case "logical-only":
					observed.Managed = nil
				case "quota-mismatch":
					observed.Managed.QuotaUS = 100000
					observed.Managed.TargetMillicores = 1000
					observed.Managed.OnlineVCPUs = 1
				case "foreign-boot":
					observed.Managed.Identity.RunnerBootID = strings.Repeat("f", 32)
				case "foreign-pod":
					observed.Managed.Identity.RunnerPodUID = "replacement"
				case "foreign-node":
					observed.Managed.Identity.NodeUID = "replacement"
				case "foreign-compute":
					observed.Managed.Identity.ComputeInstanceID = "replacement"
				case "future-generation":
					observed.Managed.Generation = 8
				case "prior-generation":
					observed.Managed.Generation = 6
				case "pending-topology":
					observed.Managed.Pending = true
					observed.Managed.OnlineVCPUs = 1
				case "wrong-scope":
					observed.Managed.Scope = "logical"
				}
				require.NoError(t, json.NewEncoder(w).Encode(observed))
			}))
			defer server.Close()
			p, vm, state := managedCPUStatusFixture(t, server.Listener.Addr().String(), vmv1.VmRunning)
			id = originalRunnerCPUIdentity(vm, "runner-uid", "0123456789abcdef0123456789abcdef", "node-uid")
			require.NoError(t, p.client.Create(p.ctx, state))
			originalJournal := vm.Annotations
			_, err := p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
			require.NoError(t, err)
			got := p.getVM()
			condition := meta.FindStatusCondition(got.Status.Conditions, typeDSGACPUReady)
			require.NotNil(t, condition)
			require.Equal(t, originalJournal, got.Annotations, "CPU展示不得改内存原操作")
			require.EqualValues(t, 2<<30, got.Status.MemorySize.Value(), "独立已核内存仍须落盘")
			if mode == "valid" {
				require.Equal(t, metav1.ConditionTrue, condition.Status)
				require.EqualValues(t, 0, writes.Load())
			} else {
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, vmv1.VmScaling, got.Status.Phase)
				expected := int32(0)
				if mode == "prior-generation" || mode == "pending-topology" {
					expected = 1
				}
				require.Equal(t, expected, writes.Load())
			}
		})
	}
}
