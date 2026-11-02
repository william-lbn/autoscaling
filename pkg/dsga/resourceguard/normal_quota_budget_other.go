//go:build !linux || !amd64

package resourceguard

import "context"

func (g *Guard) prepareNormalQuotaBudgetSource(context.Context) error { return ErrUnknown }
