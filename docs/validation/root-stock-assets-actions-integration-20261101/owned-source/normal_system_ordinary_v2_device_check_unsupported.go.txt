//go:build !linux

package resourceguard

import "context"

func RunNormalSystemOrdinaryV2DeviceCheck(context.Context) error {
	return ErrNormalSystemOrdinaryV2Unsupported
}
