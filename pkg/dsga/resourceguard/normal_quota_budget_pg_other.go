//go:build linux && !amd64

package resourceguard

import "context"

func (g *Guard) normalQuotaPGControlsLocked(context.Context, *lockedState, *arrayRegistryOriginal) error {
	return ErrUnknown
}
