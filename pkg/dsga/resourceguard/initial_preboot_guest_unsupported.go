//go:build !linux

package resourceguard

import (
	"context"
	"fmt"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

func (g *Guard) PrepareInitialPreboot(context.Context, bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return bootprotocol.InitialPrebootReceipt{}, fmt.Errorf("initial preboot Guest requires Linux")
}
func (g *Guard) ObserveInitialPreboot(context.Context, bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return bootprotocol.InitialPrebootReceipt{}, fmt.Errorf("initial preboot Guest requires Linux")
}
func (g *Guard) RecoverInitialPreboot(context.Context, bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return bootprotocol.InitialPrebootReceipt{}, fmt.Errorf("initial preboot Guest requires Linux")
}
func (g *Guard) InspectInitialPrebootMovable(context.Context, bootprotocol.InitialPrebootReceipt) (InitialPrebootMovableObservation, error) {
	return InitialPrebootMovableObservation{}, fmt.Errorf("initial movable Guest requires Linux")
}
