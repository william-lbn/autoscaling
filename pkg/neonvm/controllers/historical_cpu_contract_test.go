package controllers

import (
	"encoding/json"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"net/http/httptest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"testing"
)

func TestHistoricalCPUSeparatesGuestServiceFromFractionalEntitlement(t *testing.T) {
	for _, mode := range []string{"actual", "wrong-birth", "old-binary-running", "old-binary-new-generation"} {
		t.Run(mode, func(t *testing.T) {
			var id runnercpu.Identity
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/cpu_current", r.URL.Path, "historical read must never authorize a new CPU write")
				value := api.VCPUCgroup{VCPUs: 1000}
				if mode == "actual" || mode == "wrong-birth" {
					value.VCPUs = 2000
					value.HistoricalGuest = &runnercpu.Historical{ProtocolVersion: 1, Scope: runnercpu.HistoricalScope,
						Identity: id, OnlineVCPUs: 2, MaximumVCPUs: 2, QEMUPID: 42, QEMUStartTicks: 7}
					if mode == "wrong-birth" {
						value.HistoricalGuest.Identity.NodeUID = "replacement"
					}
				}
				require.NoError(t, json.NewEncoder(w).Encode(value))
			}))
			defer server.Close()
			p, vm, state := managedCPUStatusFixture(t, server.Listener.Addr().String(), vmv1.VmRunning)
			vm.Spec.DSGABootstrap.RuntimeContractVersion = 0
			vm.Status.Conditions = []metav1.Condition{{Type: typeDSGACPUReady, Status: metav1.ConditionTrue,
				Reason: "Confirmed", ObservedGeneration: vm.Generation, LastTransitionTime: metav1.Now()}}
			if mode == "old-binary-new-generation" {
				vm.Status.Conditions[0].ObservedGeneration--
			}
			conditions := vm.Status.Conditions
			require.NoError(t, p.client.Update(p.ctx, vm))
			// fake主资源Update会按真实status子资源语义保留旧status，也修改传入对象。
			vm.Status.Conditions = conditions
			require.NoError(t, p.client.Status().Update(p.ctx, vm))
			persisted := p.getVM()
			require.NotNil(t, meta.FindStatusCondition(persisted.Status.Conditions, typeDSGACPUReady))
			confirmed := meta.FindStatusCondition(persisted.Status.Conditions, typeDSGACPUReady)
			require.Equal(t, conditions[0].Status, confirmed.Status)
			require.Equal(t, conditions[0].ObservedGeneration, confirmed.ObservedGeneration)
			require.Equal(t, conditions[0].Reason, confirmed.Reason)
			id = originalRunnerCPUIdentity(vm, "runner-uid", "0123456789abcdef0123456789abcdef", "node-uid")
			require.NoError(t, p.client.Create(p.ctx, state))
			_, err := p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
			require.NoError(t, err)
			got := p.getVM()
			entitlement := meta.FindStatusCondition(got.Status.Conditions, typeDSGACPUEntitlementReady)
			require.NotNil(t, entitlement)
			require.Equal(t, metav1.ConditionFalse, entitlement.Status)
			require.Equal(t, "HistoricalContract", entitlement.Reason)
			condition := meta.FindStatusCondition(got.Status.Conditions, typeDSGACPUReady)
			require.NotNil(t, condition)
			if mode == "actual" {
				require.Equal(t, metav1.ConditionTrue, condition.Status)
				require.Equal(t, vmv1.VmRunning, got.Status.Phase)
			} else if mode == "old-binary-running" {
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, "HistoricalTopologyUnobserved", condition.Reason)
				require.Equal(t, vmv1.VmRunning, got.Status.Phase)
				_, err = p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
				require.NoError(t, err)
				require.Equal(t, vmv1.VmRunning, p.getVM().Status.Phase)
			} else {
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, vmv1.VmScaling, got.Status.Phase)
			}
			require.EqualValues(t, 2<<30, got.Status.MemorySize.Value())
		})
	}
}
