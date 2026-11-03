package controllers

import (
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

// PG17-U-RUNNER-PROC-001：DSGA Runner才追加原进程跨UID读取能力。
// 普通VM保原caps；privilegedfalse、init及原VM/cached输入均不改变。
func TestDSGARunnerOriginalProcessReadCapabilityIsScoped(t *testing.T) {
	t.Setenv("VM_RUNNER_IMAGE", "test-runner")
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary-unchanged", true: "dsga-original-proc-read"}[managed], func(t *testing.T) {
			vm := dsgaPlacementVM()
			vm.Spec.PodResources = core.ResourceRequirements{Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("4"), core.ResourceMemory: resource.MustParse("10Gi")}}
			if !managed {
				vm.Spec.DSGABootstrap = nil
			}
			before := vm.DeepCopy()
			pod, err := podSpec(vm, nil, &ReconcilerConfig{})
			require.NoError(t, err)
			require.Equal(t, before, vm)
			require.Len(t, pod.Spec.Containers, 1)
			require.False(t, pod.Spec.HostPID)
			context := pod.Spec.Containers[0].SecurityContext
			require.NotNil(t, context)
			require.NotNil(t, context.Privileged)
			require.False(t, *context.Privileged)
			expected := []core.Capability{"NET_ADMIN", "SYS_ADMIN", "SYS_RESOURCE"}
			if managed {
				expected = append(expected, "SYS_PTRACE")
			}
			require.Equal(t, expected, context.Capabilities.Add)
			require.Empty(t, context.Capabilities.Drop)
			for _, init := range pod.Spec.InitContainers {
				if init.SecurityContext != nil && init.SecurityContext.Capabilities != nil {
					require.NotContains(t, init.SecurityContext.Capabilities.Add, core.Capability("SYS_PTRACE"))
				}
			}
		})
	}
}
