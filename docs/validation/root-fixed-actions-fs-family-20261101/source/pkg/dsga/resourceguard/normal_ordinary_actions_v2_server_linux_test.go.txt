//go:build linux

package resourceguard

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNormalOrdinaryActionsV2CannotPrepareWithoutOriginalSources(t *testing.T) {
	g := &Guard{}
	_, e := g.prepareNormalOrdinaryActionsV2Locked(context.Background(), nil, nil, nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
}
