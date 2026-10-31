//go:build !linux

package resourceguard

import (
	"context"
	"fmt"
)

func RunInitialBootGuest(context.Context) error {
	return fmt.Errorf("fixed initial Guest channel requires Linux")
}
func RecoverInitialBootGuest(context.Context) error {
	return fmt.Errorf("fixed initial Guest channel requires Linux")
}
