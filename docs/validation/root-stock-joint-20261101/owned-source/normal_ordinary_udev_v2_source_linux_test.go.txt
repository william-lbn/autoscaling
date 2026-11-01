//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrdinaryUdevV2RawJSONAndNewGuardNeverProduceSource(t *testing.T) {
	var q normalOrdinaryUdevV2
	require.NoError(t, json.Unmarshal([]byte(`{}`), &q))
	require.ErrorIs(t, q.checkLocked(context.Background(), nil, nil), ErrUnknown)
	require.ErrorIs(t, q.requireContinuousQualification(), ErrUnknown)
	var g Guard
	source, e := g.prepareNormalOrdinaryUdevV2Locked(context.Background(), nil, nil, nil, nil, nil)
	require.Nil(t, source)
	require.ErrorIs(t, e, ErrUnknown)
}
