//go:build linux && !amd64

package resourceguard

import (
 "context"
 control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func (g *Guard) normalQuotaBudgetLocked(context.Context,*lockedState,*record,NormalSplitTopologySnapshot,control.Uint64)(normalQuotaBudget,error) {return normalQuotaBudget{},ErrUnknown}
