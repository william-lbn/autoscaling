package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestCPUAuthorityUsesCurrentOriginalObjectsInsteadOfCallerGeneration(t *testing.T) {
	for _, mode := range []string{"valid", "future-caller", "stale-caller", "wrong-pod", "wrong-boot", "wrong-node",
		"wrong-vm", "changed-target", "changed-max", "changed-min", "changed-pod-limit", "held", "migrating", "restarted", "missing-container", "missing-journal", "corrupt-journal", "foreign-journal", "missing-rv", "historical-version", "changed-actual-request", "changed-actual-limit", "missing-actual-container", "container-not-running", "initial-pending-read", "initial-pending-write"} {
		t.Run(mode, func(t *testing.T) {
			id := cpuFixtureIdentity()
			kind := true
			cpuMode := vmv1.CpuScalingModeSysfs
			vm := &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: "vm", Namespace: "ns", UID: types.UID(id.VMUID), Generation: 2, ResourceVersion: "17",
				Annotations: map[string]string{control.ModeAnnotation: "1:" + id.Incarnation, control.JournalAnnotation: originalCPUJournalFixture(t, id)}},
				Spec: vmv1.VirtualMachineSpec{DSGABootstrap: &vmv1.DSGABootstrap{ProtocolVersion: 1, RuntimeContractVersion: vmv1.RuntimeContractFixedCPU, VMUID: id.VMUID,
					Incarnation: id.Incarnation, ComputeInstanceID: id.ComputeInstanceID, ConfigSecretUID: "config-one", Ready: true},
					CpuScalingMode: &cpuMode, Guest: vmv1.Guest{CPUs: vmv1.CPUs{Min: 500, Max: 2000, Use: 1500}},
					PodResources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}},
				Status: vmv1.VirtualMachineStatus{PodName: "runner", Phase: vmv1.VmScaling}}
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "ns", UID: types.UID(id.RunnerPodUID),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: "vm", UID: vm.UID, Controller: &kind}}},
				Spec: core.PodSpec{NodeName: "node", Containers: []core.Container{{Name: "neonvm-runner", Resources: *vm.Spec.PodResources.DeepCopy()}}}, Status: core.PodStatus{Phase: core.PodRunning,
					ContainerStatuses: []core.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://original", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}}}
			node := originalCPUNodeFixture(t, id)
			command := runnercpu.Command{ProtocolVersion: 1, Identity: id, Generation: 2}
			switch mode {

			case "missing-journal":
				delete(vm.Annotations, control.JournalAnnotation)
			case "corrupt-journal":
				vm.Annotations[control.JournalAnnotation] = "{not-a-journal}"
			case "foreign-journal":
				other := id
				other.VMUID = "another-original"
				vm.Annotations[control.JournalAnnotation] = originalCPUJournalFixture(t, other)
			case "missing-rv":
				vm.ResourceVersion = ""
			case "historical-version":
				vm.Spec.DSGABootstrap.RuntimeContractVersion = 0
			case "changed-actual-request":
				pod.Spec.Containers[0].Resources.Requests[core.ResourceCPU] = resource.MustParse("1")
			case "changed-actual-limit":
				pod.Spec.Containers[0].Resources.Limits[core.ResourceCPU] = resource.MustParse("4")
			case "missing-actual-container":
				pod.Spec.Containers = nil
			case "container-not-running":
				pod.Status.ContainerStatuses[0].State.Running = nil
			case "initial-pending-read", "initial-pending-write":
				vm.Status.Phase = vmv1.VmPending
			case "future-caller":
				command.Generation = 3
			case "stale-caller":
				command.Generation = 1
			case "wrong-pod":
				pod.UID = "replacement"
			case "wrong-boot":
				command.Identity.RunnerBootID = strings.Repeat("3", 32)
			case "wrong-node":
				node.UID = "replacement"
			case "wrong-vm":
				vm.UID = "replacement"
			case "changed-target":
				vm.Spec.Guest.CPUs.Use = 500
			case "changed-min":
				vm.Spec.Guest.CPUs.Min = 250
			case "changed-max":
				vm.Spec.Guest.CPUs.Max = 4000
			case "changed-pod-limit":
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("4")
			case "held":
				vm.Spec.DSGABootstrap.Ready = false
			case "migrating":
				vm.Status.Phase = vmv1.VmMigrating
			case "restarted":
				pod.Status.ContainerStatuses[0].RestartCount = 1
			case "missing-container":
				pod.Status.ContainerStatuses = nil
			}
			vms := vmfake.NewSimpleClientset(vm)
			coreClient := kubefake.NewSimpleClientset(pod, node)
			authorize := managedCPUAuthority(id, 500, 2000, "2", vms.NeonvmV1(), coreClient.CoreV1(), "ns", "vm", "runner")
			var err error
			if mode == "initial-pending-read" {
				var fact runnercpu.NodeCommitment
				read := managedCPUAuthorityMode(id, 500, 2000, "2", vms.NeonvmV1(), coreClient.CoreV1(), "ns", "vm", "runner", false, &fact)
				err = read(context.Background(), runnercpu.Command{}, 0)
				if err == nil && fact.Validate() != nil {
					t.Fatal("early CPU source has no original Node commitment")
				}
			} else {
				err = authorize(context.Background(), command, 1500)
			}
			if (err == nil) != (mode == "valid" || mode == "initial-pending-read") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
		})
	}
}

// 原合法模型使用正式 envelope；不是随便放一个非空 annotation 绕过原解码器。
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

func TestManagedCPUObservationAuthorityDoesNotPromoteOrFreezeVMGeneration(t *testing.T) {
	// 原生产对象校验已由上表覆盖；只读资格与写命令的generation条件独立，
	// 具体Controller回归另证明旧观察后必须发当前generation并再次实读。
	state, _, _ := managedCPUFixture()
	state.intent.Generation = 1
	state.intent.Pending = false
	value, _, err := state.observe()
	if err != nil || value.Managed.Generation != 1 {
		t.Fatal("read-side source verification fabricated a new command generation", err)
	}
}

// 原Node是显式HTTP/Kube夹具，只验证生产reader的完整原行，不冒充真实调度资格。
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
