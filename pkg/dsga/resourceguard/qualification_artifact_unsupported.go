//go:build !linux || !amd64

package resourceguard

import (
	"context"
	"fmt"
)

func (g *Guard) InspectGuestQualificationArtifact(context.Context) (GuestQualificationArtifact, error) {
	return GuestQualificationArtifact{}, fmt.Errorf("original Guest qualification requires supported Linux amd64 source")
}
