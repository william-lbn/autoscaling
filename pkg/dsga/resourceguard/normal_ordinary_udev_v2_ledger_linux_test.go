//go:build linux

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOrdinaryUdevV2LedgerSingleUseAndUnknown(t *testing.T) {
	var l normalOrdinaryUdevV2Ledger
	now := time.Now()
	require.NoError(t, l.add([]byte("own"), map[string]string{"SYNTH_UUID": "u"}, 8, now))
	ticket, e := l.take(8, now)
	require.NoError(t, e)
	_, e = l.take(8, now)
	require.ErrorIs(t, e, ErrUnknown)
	require.NoError(t, l.commit(ticket))
	_, e = l.take(8, now)
	require.ErrorIs(t, e, ErrUnknown)
	require.NoError(t, l.add([]byte("own"), nil, 9, now))
	require.ErrorIs(t, l.expire(now.Add(61*time.Second)), ErrUnknown)
	require.ErrorIs(t, l.add([]byte("own"), nil, 10, now), ErrUnknown)
}
func TestOrdinaryUdevV2LedgerOverflowAndConflictingKernelTicketStayUnknown(t *testing.T) {
	now := time.Now()
	var l normalOrdinaryUdevV2Ledger
	for i := uint64(1); i <= normalOrdinaryUdevV2LedgerSlots; i++ {
		require.NoError(t, l.add([]byte("own"), nil, i, now))
	}
	require.ErrorIs(t, l.add([]byte("own"), nil, 9000, now), ErrUnknown)
	var another normalOrdinaryUdevV2Ledger
	require.NoError(t, another.add([]byte("own"), nil, 1, now))
	require.ErrorIs(t, another.add([]byte("different"), nil, 1, now), ErrUnknown)
}
