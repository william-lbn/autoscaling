//go:build !linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
)

func (g *Guard) checkNormalQuotaResourceLocked(_ *lockedState, r *record) error {
	if r != nil && r.NormalSplitQuota != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("quota resource recovery requires supported Linux backend"))
	}
	return nil
}
func (g *Guard) checkNormalQuotaObservationLocked(s *lockedState, r *record) error {
	return g.checkNormalQuotaResourceLocked(s, r)
}

func (g *Guard) confirmNormalQuotaMetadataLocked(_ context.Context, s *lockedState, r *record) error {
	return g.checkNormalQuotaResourceLocked(s, r)
}
