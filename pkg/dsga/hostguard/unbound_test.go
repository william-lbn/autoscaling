package hostguard

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 使用真实HTTP/UID-RV CAS夹具在Reserve最终PATCH前插入删除和原owner屏障。
// 证明屏障使删除前已读Pod的旧Reserve失效；本用例不认证真实Kubelet退出。
func TestUnboundBarrierRejectsInFlightReserveWithoutRemovingCapacity(t *testing.T) {
	ctx := context.Background()
	f, store := newNodeAPI(t)
	profile := Profile{ID: "unbound-test", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	snapshot, err := store.InitializeAllocations(ctx, "node-rv-1", 1<<30, profile, []Placement{})
	require.NoError(t, err)
	controller := true
	pod := core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "unbound", UID: "unbound-uid", ResourceVersion: "1", Finalizers: []string{PodProtectionFinalizer}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-uid", Controller: &controller}}}, Spec: core.PodSpec{SchedulerName: profile.SchedulerName, RestartPolicy: core.RestartPolicyNever, Containers: []core.Container{{Name: "neonvm-runner", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("2Gi")}}}}}}
	row := Placement{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), VMName: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32), Managed: true, MemoryBytes: 1 << 30, HostOverheadBytes: profile.VMOverheadBytes, PodMemoryLimitBytes: 2 << 30}
	var closed UnboundRetirement
	var proof UnboundNode
	sealed := make(chan struct{})
	f.mu.Lock()
	f.pods["testing/unbound"] = pod
	f.beforePatch = func() {
		defer close(sealed)
		stamp := metav1.Now()
		pod.DeletionTimestamp = &stamp
		pod.Status.Phase = core.PodFailed
		f.mu.Lock()
		f.pods["testing/unbound"] = pod
		f.mu.Unlock()
		digest, err := UnboundPodDigest(&pod)
		require.NoError(t, err)
		closed = UnboundRetirement{ProtocolVersion: 1, Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), PodSHA256: digest, VMName: "vm", VMUID: "vm-uid", Incarnation: row.Incarnation, Domains: []Binding{nodeBinding()}, Confirmed: []UnboundNode{}}
		proof, err = store.SealUnboundAbsence(ctx, snapshot, closed)
		require.NoError(t, err)
	}
	f.mu.Unlock()
	_, _, err = store.HoldPlacement(ctx, snapshot, row)
	require.Error(t, err)
	select {
	case <-sealed:
	case <-time.After(time.Second):
		t.Fatal("Reserve did not reach the original Node CAS barrier")
	}
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Empty(t, *after.State().Placements)
	require.Empty(t, after.State().Holds)
	require.NoError(t, InspectUnboundNode(after.Node(), closed, proof))
	_, _, err = store.HoldPlacement(ctx, after, row)
	require.Error(t, err)
	again, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, after.State(), again.State())
}

func TestUnboundClosureRejectsIncompleteAndAmbiguousJSON(t *testing.T) {
	b := nodeBinding()
	r := UnboundRetirement{ProtocolVersion: 1, Namespace: "testing", PodName: "unbound", PodUID: "pod-uid", PodSHA256: strings.Repeat("b", 64), VMName: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32), Domains: []Binding{b}, Confirmed: []UnboundNode{{Binding: b, Origin: strings.Repeat("c", 32), BeforeSequence: 1}}}
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	for _, input := range []string{strings.Replace(string(raw), "{", "{\"unknown\":1,", 1), strings.Replace(string(raw), "{", "{\"protocol_version\":1,", 1), strings.Replace(string(raw), "\"origin\":", "\"unknown_node\":1,\"origin\":", 1)} {
		var parsed UnboundRetirement
		require.Error(t, json.Unmarshal([]byte(input), &parsed))
	}
	require.NoError(t, r.Validate(true))
	r.Confirmed = []UnboundNode{}
	require.NoError(t, r.Validate(false))
	require.Error(t, r.Validate(true))
}

// 128域是可信Host配置的公开上限；用最大长度身份验证完整持久回执往返，
// 再分别超域数与原始字节上限。大对象仍须拒绝未知及任意层重复字段。
func TestUnboundClosureMaximumDomainsRoundTripAndBounds(t *testing.T) {
	id := strings.Repeat("i", 128)
	r := UnboundRetirement{ProtocolVersion: 1, Namespace: id, PodName: id, PodUID: id, PodSHA256: strings.Repeat("b", 64), VMName: id, VMUID: id, Incarnation: strings.Repeat("a", 32), Domains: []Binding{}, Confirmed: []UnboundNode{}}
	for i := 0; i < 128; i++ {
		b := Binding{NodeName: fmt.Sprintf("%03d%s", i, strings.Repeat("n", 125)), NodeUID: types.UID(fmt.Sprintf("%03d%s", i, strings.Repeat("u", 125))), Namespace: id, LeaseName: fmt.Sprintf("%03d%s", i, strings.Repeat("l", 125)), LeaseUID: types.UID(fmt.Sprintf("%03d%s", i, strings.Repeat("e", 125)))}
		r.Domains = append(r.Domains, b)
		r.Confirmed = append(r.Confirmed, UnboundNode{Binding: b, Origin: strings.Repeat("c", 32), BeforeSequence: control.Uint64(^uint64(0) - 1)})
	}
	require.NoError(t, r.Validate(true))
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	require.Greater(t, len(raw), 192*1024)
	var decoded UnboundRetirement
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NoError(t, decoded.Validate(true))
	require.Equal(t, r, decoded)
	again, err := json.Marshal(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, again)
	for _, changed := range [][]byte{
		append(append([]byte("{"), []byte(strings.Repeat(" ", 256*1024))...), raw[1:]...),
		[]byte(strings.Replace(string(raw), "{", "{\"unknown\":1,", 1)),
		[]byte(strings.Replace(string(raw), "{", "{\"protocol_version\":1,", 1)),
	} {
		var invalid UnboundRetirement
		require.Error(t, json.Unmarshal(changed, &invalid))
	}
	next := r.Domains[127]
	next.NodeName = "999" + strings.Repeat("n", 125)
	r.Domains = append(r.Domains, next)
	r.Confirmed = append(r.Confirmed, UnboundNode{Binding: next, Origin: strings.Repeat("c", 32), BeforeSequence: control.Uint64(^uint64(0) - 1)})
	require.Error(t, r.Validate(true))
	raw, err = json.Marshal(r)
	require.NoError(t, err)
	require.Error(t, json.Unmarshal(raw, &decoded))
}
