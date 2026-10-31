//go:build !linux

package vmguard

import (
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func newInitialLifecycle(*Runner, *InitialBootstrapConfig) (initialLifecycle, error) {
	return nil, fmt.Errorf("initial base lifecycle requires Linux")
}
func validateInitialLifecyclePublication(Birth, budget.ObjectRef, InitialBootstrapPublication) error {
	return fmt.Errorf("initial base publication requires Linux verifier")
}
