package cpureader

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

type fixture struct {
	reader  *Reader
	core    *kubefake.Clientset
	vms     *vmfake.Clientset
	binding journal.Binding
	id      runnercpu.Identity
	vm      *vmv1.VirtualMachine
	birth   *core.ConfigMap
	node    *core.Node
	pod     *core.Pod
}

func readerFixture(t *testing.T, address string) *fixture {
	t.Helper()
	id := runnercpu.Identity{VMUID: "vm-one", ComputeInstanceID: "compute-one", Incarnation: strings.Repeat("1", 32),
		RunnerPodUID: "pod-one", RunnerBootID: strings.Repeat("2", 32), NodeUID: "node-one"}
	controller, block, immutable := true, true, true
	mode := vmv1.CpuScalingModeSysfs
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	number, err := strconv.Atoi(port)
	require.NoError(t, err)
	vm := &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: "vm", Namespace: "ns", UID: types.UID(id.VMUID), Generation: 7, ResourceVersion: "17",
		Annotations: map[string]string{control.ModeAnnotation: "1:" + id.Incarnation, control.JournalAnnotation: originalCPUJournalFixture(t, id)}},
		Spec: vmv1.VirtualMachineSpec{DSGABootstrap: &vmv1.DSGABootstrap{ProtocolVersion: 1, RuntimeContractVersion: 1, VMUID: id.VMUID,
			Incarnation: id.Incarnation, ComputeInstanceID: id.ComputeInstanceID, ConfigSecretUID: "config-one", Ready: true},
			RunnerPort: int32(number), CpuScalingMode: &mode, Guest: vmv1.Guest{CPUs: vmv1.CPUs{Min: 500, Max: 2000, Use: 1500}},
			PodResources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}},
		Status: vmv1.VirtualMachineStatus{PodName: "runner", PodIP: host, Phase: vmv1.VmPending}}
	owner := []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: &block}}
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: id.Incarnation}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: vmguard.Identity{VMUID: id.VMUID, Incarnation: id.Incarnation,
		RunnerPodUID: id.RunnerPodUID, RunnerBootID: id.RunnerBootID, NodeUID: id.NodeUID, MinimumBytes: 1 << 30, MaximumBytes: 4 << 30, BlockBytes: 8 << 20},
		StateClaim: budget.ObjectRef{Name: "original-state-not-initialized", UID: "state-one"}}
	raw, err := json.Marshal(birth)
	require.NoError(t, err)
	cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vmguard.BindingName(binding), Namespace: vm.Namespace, UID: "birth-one", ResourceVersion: "1",
		Labels: map[string]string{vmguard.ClaimLabel: "binding-v1"}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: owner},
		Immutable: &immutable, Data: map[string]string{vmguard.ClaimKey: string(raw)}}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "ns", UID: types.UID(id.RunnerPodUID), ResourceVersion: "1", OwnerReferences: owner},
		Spec: core.PodSpec{NodeName: "node", Containers: []core.Container{{Name: "neonvm-runner", Resources: *vm.Spec.PodResources.DeepCopy()}}},
		Status: core.PodStatus{Phase: core.PodRunning, PodIP: host, ContainerStatuses: []core.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://original",
			State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}}}
	node := originalCPUNodeFixture(t, id)
	coreClient, vms := kubefake.NewSimpleClientset(cm, pod, node), vmfake.NewSimpleClientset(vm)
	reader, err := New(coreClient.CoreV1(), vms.NeonvmV1())
	require.NoError(t, err)
	return &fixture{reader: reader, core: coreClient, vms: vms, binding: binding, id: id, vm: vm, birth: cm, node: node, pod: pod}
}

func (f *fixture) observed(r *http.Request) api.VCPUCgroup {
	return api.VCPUCgroup{VCPUs: 1500, Managed: &runnercpu.Observation{ProtocolVersion: 1, Scope: runnercpu.Scope, Identity: f.id,
		ObservationNonce: r.URL.Query().Get("observation_nonce"), ObservedBootTimeNS: 10,
		NodeCommitment: runnercpu.NodeCommitment{ProtocolVersion: 1, Scope: runnercpu.NodeCommitmentScope, LedgerOrigin: strings.Repeat("a", 32),
			LedgerSequence: 1, NodeResourceVersion: "1", CommittedMillicores: 2000}, Generation: 7, CgroupDevice: 1, CgroupInode: 2, QEMUPID: 42, QEMUStartTicks: 7,
		QuotaUS: 150000, PeriodUS: 100000, Weight: 100, OnlineVCPUs: 2, MaximumVCPUs: 2, MaximumMillicores: 2000, CommittedMillicores: 2000, TargetMillicores: 1500}}
}

func TestReadChallengeRequiresCurrentOriginalSourcesAndActualKernel(t *testing.T) {
	cases := []string{"valid-before-pg-initialization", "foreign-nonce", "missing-nonce", "no-kernel-clock", "foreign-boot", "old-vm-generation",
		"partial-topology", "logical-only", "quota-mismatch", "under-min-target", "oversized", "trailing", "http-error",
		"changed-vm-generation", "changed-spec", "changed-pod", "changed-container", "changed-birth", "changed-origin", "node-sequence-advanced"}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			var f *fixture
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/cpu_current", r.URL.Path)
				require.Empty(t, r.Header.Get("Authorization"))
				require.NoError(t, runnercpu.ValidateObservationNonce(r.URL.Query().Get("observation_nonce")))
				answer := f.observed(r)
				switch mode {
				case "foreign-nonce":
					answer.Managed.ObservationNonce = strings.Repeat("b", 32)
				case "missing-nonce":
					answer.Managed.ObservationNonce = ""
				case "no-kernel-clock":
					answer.Managed.ObservedBootTimeNS = 0
				case "foreign-boot":
					answer.Managed.Identity.RunnerBootID = strings.Repeat("f", 32)
				case "old-vm-generation":
					answer.Managed.Generation = 6
				case "partial-topology":
					answer.Managed.Pending = true
					answer.Managed.OnlineVCPUs = 1
				case "logical-only":
					answer.Managed = nil
				case "quota-mismatch":
					answer.VCPUs = 1000
				case "under-min-target":
					answer.Managed.TargetMillicores = 250
					answer.Managed.Pending = true
				case "http-error":
					w.WriteHeader(503)
					return
				case "changed-vm-generation":
					v := f.vm.DeepCopy()
					v.Generation++
					_, err := f.vms.NeonvmV1().VirtualMachines("ns").Update(r.Context(), v, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "changed-spec":
					v := f.vm.DeepCopy()
					v.Spec.RunnerPort++
					_, err := f.vms.NeonvmV1().VirtualMachines("ns").Update(r.Context(), v, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "changed-pod":
					p := f.pod.DeepCopy()
					p.UID = "replacement"
					_, err := f.core.CoreV1().Pods("ns").Update(r.Context(), p, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "changed-container":
					p := f.pod.DeepCopy()
					p.Status.ContainerStatuses[0].ContainerID = "containerd://new"
					_, err := f.core.CoreV1().Pods("ns").Update(r.Context(), p, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "changed-birth":
					cm := f.birth.DeepCopy()
					cm.UID = "replacement"
					_, err := f.core.CoreV1().ConfigMaps("ns").Update(r.Context(), cm, metav1.UpdateOptions{})
					require.NoError(t, err)
				case "changed-origin", "node-sequence-advanced":
					node := f.node.DeepCopy()
					var model hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &model))
					model.Sequence++
					if mode == "changed-origin" {
						model.Origin = strings.Repeat("b", 32)
						node.Annotations[hostguard.OriginAnnotation] = "1:" + model.Origin
					}
					raw, err := json.Marshal(model)
					require.NoError(t, err)
					node.Annotations[hostguard.LedgerAnnotation] = string(raw)
					node.ResourceVersion = "2"
					_, err = f.core.CoreV1().Nodes().Update(r.Context(), node, metav1.UpdateOptions{})
					require.NoError(t, err)
				}
				require.NoError(t, json.NewEncoder(w).Encode(answer))
				if mode == "oversized" {
					_, _ = w.Write([]byte(strings.Repeat(" ", 5000)))
				}
				if mode == "trailing" {
					_, _ = w.Write([]byte("{}"))
				}
			}))
			defer server.Close()
			f = readerFixture(t, server.Listener.Addr().String())
			original := f.vm.Annotations[control.JournalAnnotation]
			observation, err := f.reader.Observe(context.Background(), f.binding, f.id.ComputeInstanceID)
			switch mode {
			case "valid-before-pg-initialization", "node-sequence-advanced":
				require.NoError(t, err)
				require.NotNil(t, observation)
				digest, digestErr := Digest(observation)
				require.NoError(t, digestErr)
				require.Len(t, digest, 64)
			case "old-vm-generation", "partial-topology":
				require.ErrorIs(t, err, ErrPending)
				require.NotNil(t, observation)
			default:
				require.Error(t, err)
				require.Nil(t, observation)
			}
			require.EqualValues(t, 1, calls.Load(), "no internal retry may hide unknown")
			current, getErr := f.vms.NeonvmV1().VirtualMachines("ns").Get(context.Background(), "vm", metav1.GetOptions{})
			require.NoError(t, getErr)
			require.Equal(t, original, current.Annotations[control.JournalAnnotation], "read-only challenge must not mutate original Owner/journal")
			if mode == "valid-before-pg-initialization" {
				for _, action := range f.core.Actions() {
					require.Equal(t, "get", action.GetVerb())
				}
				for _, action := range f.vms.Actions() {
					require.Equal(t, "get", action.GetVerb())
				}
			}
		})
	}
}

func TestReadChallengeRejectsCachedAnswerAndRedirectWithoutFollowing(t *testing.T) {
	var f *fixture
	var cached *api.VCPUCgroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cached == nil {
			value := f.observed(r)
			cached = &value
		}
		require.NoError(t, json.NewEncoder(w).Encode(cached))
	}))
	defer server.Close()
	f = readerFixture(t, server.Listener.Addr().String())
	first, err := f.reader.Observe(context.Background(), f.binding, f.id.ComputeInstanceID)
	require.NoError(t, err)
	second, err := f.reader.Observe(context.Background(), f.binding, f.id.ComputeInstanceID)
	require.Error(t, err)
	require.Nil(t, second)
	require.NotEmpty(t, first.ObservationNonce)
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer redirect.Close()
	f = readerFixture(t, redirect.Listener.Addr().String())
	observation, err := f.reader.Observe(context.Background(), f.binding, f.id.ComputeInstanceID)
	require.Error(t, err)
	require.Nil(t, observation)
	require.Zero(t, redirected.Load())
}

func TestReadChallengeRejectsOriginalClosureAndMissingHostHoldBeforeHTTP(t *testing.T) {
	for _, mode := range []string{"wrong-compute", "historical", "closure", "missing-hold", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			defer server.Close()
			f := readerFixture(t, server.Listener.Addr().String())
			compute := f.id.ComputeInstanceID
			ctx := context.Background()
			switch mode {
			case "wrong-compute":
				compute = "replacement"
			case "historical":
				f.vm.Spec.DSGABootstrap.RuntimeContractVersion = 0
				_, err := f.vms.NeonvmV1().VirtualMachines("ns").Update(ctx, f.vm, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "closure":
				var b vmguard.Birth
				require.NoError(t, json.Unmarshal([]byte(f.birth.Data[vmguard.ClaimKey]), &b))
				_, err := f.core.CoreV1().ConfigMaps("ns").Create(ctx, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vmguard.ExitClaimName(f.binding, b), Namespace: "ns"}}, metav1.CreateOptions{})
				require.NoError(t, err)
			case "missing-hold":
				delete(f.node.Annotations, hostguard.LedgerAnnotation)
				_, err := f.core.CoreV1().Nodes().Update(ctx, f.node, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			observation, err := f.reader.Observe(ctx, f.binding, compute)
			require.Error(t, err)
			require.Nil(t, observation)
			require.Zero(t, calls.Load())
		})
	}
	_, err := New(nil, nil)
	require.Error(t, err)
	_, err = NewForAPIReader(nil)
	require.Error(t, err)
	require.False(t, errors.Is(errors.New("unknown"), ErrPending))
}

func originalCPUJournalFixture(t *testing.T, id runnercpu.Identity) string {
	t.Helper()
	model, err := journal.New(id.VMUID, id.Incarnation, "cpu-owner")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err = writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	encoded, err := json.Marshal(map[string]any{"version": 1, "encoding": "zlib-v1", "incarnation": id.Incarnation,
		"sha256": hex.EncodeToString(sum[:]), "data": compressed.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func originalCPUNodeFixture(t *testing.T, id runnercpu.Identity) *core.Node {
	t.Helper()
	profile := hostguard.Profile{ID: "cpu-fixture-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20,
		PodOverheadBytes: 16 << 20, CPUAccountingVersion: 1, SystemCPUMillicores: 250}
	rows := []hostguard.Placement{{Namespace: "ns", PodName: "runner", PodUID: id.RunnerPodUID, VMName: "vm", VMUID: id.VMUID,
		Incarnation: id.Incarnation, Managed: true, MemoryBytes: 1 << 30, HostOverheadBytes: profile.VMOverheadBytes,
		PodMemoryLimitBytes: 2 << 30, CPUAccountingVersion: 1, CommittedCPUMillicores: 2000}}
	model := hostguard.Model{ProtocolVersion: 11, Binding: hostguard.Binding{NodeName: "node", NodeUID: types.UID(id.NodeUID),
		Namespace: "system", LeaseName: "node-allocator", LeaseUID: "node-lease"}, Origin: strings.Repeat("a", 32),
		LeaseFloor: journal.Owner{HolderID: "cpu-node-owner", Epoch: 1}, Sequence: 1, OutsideBytes: 1 << 30, Holds: []hostguard.Hold{},
		Profile: &profile, Placements: &rows}
	if err := model.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	return &core.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID(id.NodeUID), ResourceVersion: "1",
		Finalizers:  []string{hostguard.ProtectionFinalizer},
		Annotations: map[string]string{hostguard.LedgerAnnotation: string(raw), hostguard.OriginAnnotation: "1:" + model.Origin}},
		Status: core.NodeStatus{Capacity: core.ResourceList{core.ResourceCPU: resource.MustParse("8"), core.ResourceMemory: resource.MustParse("8Gi")},
			Allocatable: core.ResourceList{core.ResourceCPU: resource.MustParse("7"), core.ResourceMemory: resource.MustParse("7Gi")}}}
}
