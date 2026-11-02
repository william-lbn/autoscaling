//go:build !linux

package resourceguard

import "context"

func (g *Guard) ApplyNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return NormalQuotaEvidence{}, ErrUnknown
}
func (g *Guard) ObserveNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return NormalQuotaEvidence{}, ErrUnknown
}
func (g *Guard) ReplayNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return NormalQuotaEvidence{}, ErrUnknown
}
func (g *Guard) RecoverNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return NormalQuotaEvidence{}, ErrUnknown
}
