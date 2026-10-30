package controllers

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
)

func dsgaPlacementVM() *vmv1.VirtualMachine {
	vm := defaultVm()
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "placement-test"}
	vm.Spec.TargetArchitecture = lo.ToPtr(vmv1.CPUArchitectureAMD64)
	// 直接构造Pod时显式提供正式DSGA必需的Sysfs模式；正常Reconcile会先完成此校验。
	vm.Spec.CpuScalingMode = lo.ToPtr(vmv1.CpuScalingModeSysfs)
	vm.Spec.NodeSelector = map[string]string{"neon.example/pg17-pool": "memory"}
	return vm
}

func placementNode(name, osName, arch string) *core.Node {
	return &core.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
		"kubernetes.io/hostname": name, "kubernetes.io/os": osName, "kubernetes.io/arch": arch,
		// 故意给116也加相同pool，验证控制器不会用linux/arch的OR绕过名单。
		"neon.example/pg17-pool": "memory",
	}}}
}

func assertPlacement(t *testing.T, pod *core.Pod, name, osName, arch string, allowed bool) {
	t.Helper()
	matched, err := nodeaffinity.GetRequiredNodeAffinity(pod).Match(placementNode(name, osName, arch))
	require.NoError(t, err)
	require.Equal(t, allowed, matched, "node=%s os=%s arch=%s", name, osName, arch)
}

// PG17-U-VM-001用Kubernetes实际选择器验证OR/AND行为，不能只数term。
func TestPG17DSGAPlacementCannotBypassRequiredNodeAllowlist(t *testing.T) {
	vm := dsgaPlacementVM()
	vm.Spec.Affinity = &core.Affinity{NodeAffinity: &core.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{
		{MatchExpressions: []core.NodeSelectorRequirement{{Key: "kubernetes.io/hostname", Operator: core.NodeSelectorOpIn, Values: []string{"worker-117"}}}},
		{MatchExpressions: []core.NodeSelectorRequirement{{Key: "kubernetes.io/hostname", Operator: core.NodeSelectorOpIn, Values: []string{"worker-118", "worker-119"}}}},
	}}}}
	original := vm.DeepCopy()
	pod := &core.Pod{Spec: core.PodSpec{NodeSelector: vm.Spec.NodeSelector, Affinity: affinityForVirtualMachine(vm)}}
	for _, name := range []string{"worker-117", "worker-118", "worker-119"} {
		t.Run(name, func(t *testing.T) { assertPlacement(t, pod, name, "linux", "amd64", true) })
	}
	for _, scenario := range []struct{ name, osName, arch string }{{"ubuntu", "linux", "amd64"}, {"worker-116", "linux", "amd64"}, {"worker-118", "windows", "amd64"}, {"worker-119", "linux", "arm64"}} {
		t.Run(scenario.name+"-"+scenario.osName+"-"+scenario.arch, func(t *testing.T) { assertPlacement(t, pod, scenario.name, scenario.osName, scenario.arch, false) })
	}
	require.Equal(t, original, vm, "构造Runner约束不能改原VM或污染缓存")
	require.Equal(t, pod.Spec.Affinity, affinityForVirtualMachine(vm), "同一冻结输入重试应确定")
}

// PG17-U-VM-002保持显式空选择器、字段约束及相互冲突条件的拒绝语义。
func TestPG17DSGAPlacementPreservesEmptyAndConflictingSelectors(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		selector *core.NodeSelector
		allowed  bool
	}{
		{"absent", nil, true},
		{"nil-affinity", nil, true},
		{"zero-terms", &core.NodeSelector{}, false},
		{"empty-term", &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{}}}, false},
		{"field-name", &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchFields: []core.NodeSelectorRequirement{{Key: "metadata.name", Operator: core.NodeSelectorOpIn, Values: []string{"worker-118"}}}}}}, true},
		{"conflicting-arch", &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchExpressions: []core.NodeSelectorRequirement{{Key: "kubernetes.io/arch", Operator: core.NodeSelectorOpIn, Values: []string{"arm64"}}}}}}, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			vm := dsgaPlacementVM()
			if scenario.name != "nil-affinity" {
				vm.Spec.Affinity = &core.Affinity{NodeAffinity: &core.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: scenario.selector}}
			}
			before := vm.DeepCopy()
			pod := &core.Pod{Spec: core.PodSpec{NodeSelector: vm.Spec.NodeSelector, Affinity: affinityForVirtualMachine(vm)}}
			assertPlacement(t, pod, "worker-118", "linux", "amd64", scenario.allowed)
			assertPlacement(t, pod, "worker-118", "windows", "amd64", false)
			if scenario.name == "field-name" {
				assertPlacement(t, pod, "worker-117", "linux", "amd64", false)
			}
			require.Equal(t, before, vm)
		})
	}
}

// PG17-U-VM-003经过正式podSpec构造，保持pool/反亲和与源码输入独立。
func TestPG17DSGARunnerPodKeepsPlacementWithoutMutatingVM(t *testing.T) {
	t.Setenv("VM_RUNNER_IMAGE", "test-runner")
	vm := dsgaPlacementVM()
	vm.Spec.Affinity = &core.Affinity{NodeAffinity: &core.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchExpressions: []core.NodeSelectorRequirement{{Key: "kubernetes.io/hostname", Operator: core.NodeSelectorOpIn, Values: []string{"worker-117", "worker-118", "worker-119"}}}}}}}, PodAntiAffinity: &core.PodAntiAffinity{}}
	before := vm.DeepCopy()
	pod, err := podSpec(vm, nil, &ReconcilerConfig{})
	require.NoError(t, err)
	require.Equal(t, before, vm)
	require.Equal(t, vm.Spec.NodeSelector, pod.Spec.NodeSelector)
	require.Equal(t, vm.Spec.Affinity.PodAntiAffinity, pod.Spec.Affinity.PodAntiAffinity)
	assertPlacement(t, pod, "worker-118", "linux", "amd64", true)
	assertPlacement(t, pod, "worker-116", "linux", "amd64", false)
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "worker-116"
	require.Equal(t, before, vm, "新Pod变更也不能别名写入原VM")
}
