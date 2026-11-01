//go:build linux && !amd64

package resourceguard

import (
	"context"
	"errors"
	"fmt"
)

func execNormalSystemOrdinaryRoleWithKernelControls(context.Context, *normalSystemLifecycleSource, string) error {
	return errors.Join(ErrUnknown, fmt.Errorf("ordinary signal source requires exact reviewed amd64/369 build"))
}
