//go:build !linux

package resourceguard

import (
	"context"
	"fmt"
)

func InspectNormalSystemSource(context.Context, []byte) (NormalSystemSourceObservation, error) {
	return NormalSystemSourceObservation{}, fmt.Errorf("normal System source requires Linux")
}
func CheckNormalSystemSource(ctx context.Context, raw []byte) (NormalSystemSourceObservation, error) {
	return InspectNormalSystemSource(ctx, raw)
}
