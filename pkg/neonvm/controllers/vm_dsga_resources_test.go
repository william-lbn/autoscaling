package controllers

import (
	"testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kuberesource "k8s.io/kubernetes/pkg/api/v1/resource"
)

// PG17-U-RUNNER-MEM-001: 经过正式podSpec及Kubernetes PodLimits确认全生命周期峰值。
func TestPG17DSGAExplicitPodResourcesBoundBuiltinInitPeak(t *testing.T) {
	t.Setenv("VM_RUNNER_IMAGE", "test-runner")
	for _, kernel := range []bool{false, true} {
		t.Run(map[bool]string{false: "root-only", true: "root-and-kernel"}[kernel], func(t *testing.T) {
			vm := dsgaPlacementVM()
			if !kernel {
				vm.Spec.Guest.KernelImage = nil
			}
			vm.Spec.PodResources = core.ResourceRequirements{
				Limits:   core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi"), "example.test/gpu": resource.MustParse("1")},
				Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2"), core.ResourceMemory: resource.MustParse("10Gi"), "example.test/gpu": resource.MustParse("1")},
			}
			before := vm.DeepCopy()
			pod, err := podSpec(vm, nil, &ReconcilerConfig{})
			require.NoError(t, err)
			require.Equal(t, before, vm, "构造资源及设备限额不得污染原VM/cached map")
			require.Len(t, pod.Spec.InitContainers, map[bool]int{false: 1, true: 2}[kernel])
			for _, c := range pod.Spec.InitContainers {
				require.Equal(t, "10Gi", c.Resources.Limits.Memory().String())
				require.Equal(t, "4", c.Resources.Limits.Cpu().String())
				require.Equal(t, "10Gi", c.Resources.Requests.Memory().String())
				require.Equal(t, "2", c.Resources.Requests.Cpu().String())
				require.Len(t, c.Resources.Limits, 2, "复制init不得申请GPU/KVM/vhost设备")
				require.Len(t, c.Resources.Requests, 2)
			}
			limit := kuberesource.PodLimits(pod, kuberesource.PodResourcesOptions{})
			require.EqualValues(t, 10<<30, limit.Memory().Value(), "顺序init峰值不可把生命周期Pod cap抬到Runner之上")
			// 量化Host策略：Guest7/8Gi+cap10-min7导出的3Gi，覆盖真实10Gi Pod峰值。
			for _, guest := range []int64{7 << 30, 8 << 30} {
				require.GreaterOrEqual(t, guest+(3<<30), limit.Memory().Value())
			}
			pod.Spec.InitContainers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("1Gi")
			require.Equal(t, "10Gi", pod.Spec.Containers[0].Resources.Limits.Memory().String())
			if kernel {
				require.Equal(t, "10Gi", pod.Spec.InitContainers[1].Resources.Limits.Memory().String())
			}
			require.Equal(t, before, vm, "每个init/app资源map须独立")
		})
	}
}

// PG17-U-RUNNER-MEM-002: 所有DSGA必须有完整硬限；只保留真正legacy的旧行为。
func TestPG17DSGAInitResourcesPreserveLegacyAndRejectPartialLimits(t *testing.T) {
	t.Setenv("VM_RUNNER_IMAGE", "test-runner")
	for _, managed := range []bool{false, true} {
		for _, configured := range []bool{false, true} {
			vm := defaultVm()
			vm.Spec.CpuScalingMode = lo.ToPtr(vmv1.CpuScalingModeSysfs)
			if managed {
				vm.Spec.DSGABootstrap = dsgaPlacementVM().Spec.DSGABootstrap
			}
			if configured {
				vm.Spec.PodResources.Limits = core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi")}
			}
			pod, err := podSpec(vm, nil, &ReconcilerConfig{})
			if managed && !configured {
				require.ErrorContains(t, err, "positive hard CPU and exact memory")
				require.Nil(t, pod, "typed DSGA API不能生成无界Runner/init")
				continue
			}
			require.NoError(t, err)
			for _, c := range pod.Spec.InitContainers {
				require.Equal(t, managed && configured, len(c.Resources.Limits) > 0)
			}
		}
	}
	for _, which := range []string{"empty", "requests-only", "cpu-missing", "cpu-zero", "memory-missing", "memory-zero", "memory-fractional"} {
		t.Run(which, func(t *testing.T) {
			vm := dsgaPlacementVM()
			vm.Spec.PodResources.Limits = core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi")}
			switch which {
			case "empty":
				vm.Spec.PodResources = core.ResourceRequirements{}
			case "requests-only":
				vm.Spec.PodResources = core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi")}}
			case "cpu-missing":
				delete(vm.Spec.PodResources.Limits, core.ResourceCPU)
			case "cpu-zero":
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("0")
			case "memory-missing":
				delete(vm.Spec.PodResources.Limits, core.ResourceMemory)
			case "memory-zero":
				vm.Spec.PodResources.Limits[core.ResourceMemory] = resource.MustParse("0")
			case "memory-fractional":
				vm.Spec.PodResources.Limits[core.ResourceMemory] = resource.MustParse("100m")
			}
			pod, err := podSpec(vm, nil, &ReconcilerConfig{})
			require.ErrorContains(t, err, "positive hard CPU and exact memory")
			require.Nil(t, pod)
		})
	}
	// PG17-U-RUNNER-MEM-003: typed API不能用额外无界init绕过首版固定Pod预算。
	t.Run("extra-init", func(t *testing.T) {
		vm := dsgaPlacementVM()
		vm.Spec.PodResources.Limits = core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi")}
		vm.Spec.ExtraInitContainers = []core.Container{{Name: "extra-init", Image: "unbounded-init"}}
		before := vm.DeepCopy()
		pod, err := podSpec(vm, nil, &ReconcilerConfig{})
		require.ErrorContains(t, err, "extra init containers are unsupported by the fixed Pod budget")
		require.Nil(t, pod)
		require.Equal(t, before, vm)
		// 真正legacy仍接受原额外init，拒绝不扩散到未修改的上游路径。
		vm.Spec.DSGABootstrap = nil
		pod, err = podSpec(vm, nil, &ReconcilerConfig{})
		require.NoError(t, err)
		require.Equal(t, vm.Spec.ExtraInitContainers[0], pod.Spec.InitContainers[len(pod.Spec.InitContainers)-1])
	})

}
