package v1

import (
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func strictRuntimeCPUVM() *VirtualMachine {
	vm := heldBootstrapVM()
	vm.Spec.DSGABootstrap.RuntimeContractVersion = RuntimeContractFixedCPU
	vm.Spec.Guest.CPUs = CPUs{Min: 500, Use: 500, Max: 2000}
	vm.Spec.PodResources = core.ResourceRequirements{
		Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")},
		Limits:   core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}
	return vm
}

func TestDSGARuntimeCPURequiresFullFrozenPodCommitmentBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"valid", "request-under-max", "limit-under-max", "unequal", "fractional-millicore",
		"overflow", "unknown-version", "max-over-topology", "target-over-max"} {
		t.Run(mode, func(t *testing.T) {
			vm := strictRuntimeCPUVM()
			switch mode {
			case "request-under-max":
				vm.Spec.PodResources.Requests[core.ResourceCPU] = resource.MustParse("1")
			case "limit-under-max":
				vm.Spec.PodResources.Requests[core.ResourceCPU] = resource.MustParse("1")
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("1")
			case "unequal":
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("4")
			case "fractional-millicore":
				vm.Spec.PodResources.Requests[core.ResourceCPU] = resource.MustParse("2.0001")
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("2.0001")
			case "overflow":
				vm.Spec.PodResources.Requests[core.ResourceCPU] = resource.MustParse("9223372036854775807")
				vm.Spec.PodResources.Limits[core.ResourceCPU] = resource.MustParse("9223372036854775807")
			case "unknown-version":
				vm.Spec.DSGABootstrap.RuntimeContractVersion = 2
			case "max-over-topology":
				vm.Spec.Guest.CPUs.Max = 65000
			case "target-over-max":
				vm.Spec.Guest.CPUs.Use = 2500
			}
			held, err := vm.DSGABootstrapHeld()
			require.True(t, held)
			require.Equal(t, mode == "valid", err == nil)
		})
	}
}

func TestDSGARuntimeVersionCannotChangeEvenWhileHeld(t *testing.T) {
	historical := heldBootstrapVM()
	strict := strictRuntimeCPUVM()
	next := historical.DeepCopy()
	next.Spec.DSGABootstrap.RuntimeContractVersion = 1
	require.Error(t, next.validateDSGABootstrapUpdate(historical))
	next = strict.DeepCopy()
	next.Spec.DSGABootstrap.RuntimeContractVersion = 0
	require.Error(t, next.validateDSGABootstrapUpdate(strict))
	require.NoError(t, historical.Spec.validateDSGARuntimeContract(), "historical resource projection is not reinterpreted")
	_, _, err := historical.Spec.DSGARuntimeCPUBudget()
	require.Error(t, err, "historical contract is not a strict CPU qualification")
}
