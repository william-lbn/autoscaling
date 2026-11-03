package hostguard

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func budgetPolicyFixture(t *testing.T) (KubernetesBudgetPolicy, *core.Node, Profile) {
	t.Helper()
	profile := Profile{ID: "kubernetes-budget-test", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	digest, err := ProfileSHA256(profile)
	require.NoError(t, err)
	policy := KubernetesBudgetPolicy{ProtocolVersion: 1, Scope: KubernetesBudgetScope, NodeName: "node-one", NodeUID: "node-uid", CapacityBytes: 12 << 30, AllocatableBytes: 8 << 30, SystemReservationBytes: 1 << 30, ProfileSHA256: digest}
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: policy.NodeName, UID: "node-uid", ResourceVersion: "1"}, Status: core.NodeStatus{Capacity: core.ResourceList{core.ResourceMemory: resource.MustParse("12Gi")}, Allocatable: core.ResourceList{core.ResourceMemory: resource.MustParse("8Gi")}}}
	return policy, node, profile
}

func TestKubernetesBudgetUsesAllocatableReservationOnce(t *testing.T) {
	p, node, profile := budgetPolicyFixture(t)
	require.NoError(t, p.Inspect(node, profile, 1<<30))
	// Kube 已扣的 4Gi 不再重复加到原账本 Outside；只保留额外 1Gi。
	require.Error(t, p.Inspect(node, profile, 5<<30))
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	var decoded KubernetesBudgetPolicy
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, p, decoded)
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"absolute_kernel_bound":true}`)...), &decoded))
}

func TestKubernetesBudgetRejectsOriginalSourceDrift(t *testing.T) {
	for _, mode := range []string{"nil", "uid", "name", "rv", "deleted", "capacity", "allocatable", "missing-capacity", "profile", "outside"} {
		t.Run(mode, func(t *testing.T) {
			p, node, profile := budgetPolicyFixture(t)
			outside := p.SystemReservationBytes
			switch mode {
			case "nil":
				node = nil
			case "uid":
				node.UID = "replacement"
			case "name":
				node.Name = "replacement"
			case "rv":
				node.ResourceVersion = ""
			case "deleted":
				now := metav1.Now()
				node.DeletionTimestamp = &now
			case "capacity":
				node.Status.Capacity[core.ResourceMemory] = resource.MustParse("13Gi")
			case "allocatable":
				node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("9Gi")
			case "missing-capacity":
				node.Status.Capacity = nil
			case "profile":
				profile.VMOverheadBytes++
			case "outside":
				outside++
			}
			require.Error(t, p.Inspect(node, profile, outside))
		})
	}
}

func TestKubernetesBudgetRejectsMalformedPolicy(t *testing.T) {
	for _, mode := range []string{"version", "scope", "uid", "capacity", "allocatable", "over-capacity", "reservation", "exhausted", "sha", "uppercase"} {
		t.Run(mode, func(t *testing.T) {
			p, _, _ := budgetPolicyFixture(t)
			switch mode {
			case "version":
				p.ProtocolVersion++
			case "scope":
				p.Scope = "absolute-host-kernel-bound"
			case "uid":
				p.NodeUID = ""
			case "capacity":
				p.CapacityBytes = 0
			case "allocatable":
				p.AllocatableBytes = 0
			case "over-capacity":
				p.AllocatableBytes = p.CapacityBytes + 1
			case "reservation":
				p.SystemReservationBytes = 0
			case "exhausted":
				p.SystemReservationBytes = p.AllocatableBytes
			case "sha":
				p.ProfileSHA256 = ""
			case "uppercase":
				p.ProfileSHA256 = strings.ToUpper(p.ProfileSHA256)
			}
			require.Error(t, p.Validate())
		})
	}
}
