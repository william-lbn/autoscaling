package owner

import (
	"context"
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
)

func TestPGInflightGrowRunDefaultFalseKeepsOriginalReentry(t *testing.T) {
	productionInitialService(t, "pg-inflight-audit-run-default")
}
func TestPGInflightGrowRunExplicitLifecycleCancelsAndRejectsRebind(t *testing.T) {
	productionInitialService(t, "pg-inflight-audit-run-enabled")
}

// 原Run写者独占所有业务推进；外层只读快照及显式协议peer同步，不手填Completed。
func verifyPGInflightRunLifecycle(t *testing.T, ctx context.Context, mode string, store *journal.Store, worker *PGWorker, pg *initialServicePG, peer *pgInflightServicePeer) {
	t.Helper()
	enabled := strings.HasSuffix(mode, "-enabled")
	require.False(t, worker.executor.InFlightEnabled(), "构造器/direct Advance仍默认off")
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx, PGLoopOptions{PollInterval: 10 * time.Millisecond, EnableInFlightGrow: enabled})
	}()
	select {
	case <-peer.received:
	case err := <-done:
		t.Fatalf("原Run未送出唯一PG HTTP: %v", err)
	case <-time.After(25 * time.Second):
		t.Fatal("原Run未到达单Resize HTTP")
	}
	require.Equal(t, enabled, worker.executor.InFlightEnabled())
	if enabled {
		close(peer.register)
		select {
		case <-peer.started:
		case <-time.After(5 * time.Second):
			t.Fatal("原Native peer未登记Preparing")
		}
		pg.mu.Lock()
		old := pg.leaseRenewals
		pg.mu.Unlock()
		require.Eventually(t, func() bool { pg.mu.Lock(); defer pg.mu.Unlock(); return pg.leaseRenewals > old }, 55*time.Second, 20*time.Millisecond, "同Run/Session完整来源审计应实际续原Native期限")
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run未随同Session父ctx取消")
	}
	select {
	case <-peer.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("实际原HTTP未取消")
	}
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
	require.Equal(t, &pg.request, worker.client.Pending(), "取消Run不清原SDK Unknown")
	pg.mu.Lock()
	require.Equal(t, 1, pg.resizePosts)
	pg.mu.Unlock()
	if enabled {
		require.False(t, worker.executor.MatchInFlight(snapshot, pg.request))
		err = worker.Run(ctx, PGLoopOptions{PollInterval: 10 * time.Millisecond, EnableInFlightGrow: true})
		require.ErrorContains(t, err, "already bound", "启用后不能reset控制器重新跑而丢MAY")
	} else {
		again, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		err = worker.Run(again, PGLoopOptions{PollInterval: 10 * time.Millisecond})
		stop()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "already bound")
		require.False(t, worker.executor.InFlightEnabled(), "原零值false重入不暗中启用")
	}
	next, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshot.State(), next.State())
	pg.mu.Lock()
	require.Equal(t, 1, pg.resizePosts)
	pg.mu.Unlock()
}

func TestPGInflightActiveSourceCallerKeepsBothReasonsBounded(t *testing.T) {
	sink := new(sourceAuditLogSink)
	klog.SetLogger(logr.New(sink))
	defer klog.ClearLogger()
	recordActiveSourceClosureAudit(nil, false)
	require.Empty(t, sink.read())
	require.NotPanics(t, func() {
		recordActiveSourceClosureAudit(&privateReopenError{}, false)
		recordActiveSourceClosureAudit(nil, true)
		recordActiveSourceClosureAudit(&privateReopenError{}, true)
	})
	rows := sink.read()
	require.Len(t, rows, 3)
	for i, row := range rows {
		require.Equal(t, "DSGA active service closure audit", row.message)
		fields := map[string]any{}
		for j := 0; j < len(row.values); j += 2 {
			key, ok := row.values[j].(string)
			require.True(t, ok)
			fields[key] = row.values[j+1]
		}
		require.Equal(t, "active_source", fields["stage"])
		require.Equal(t, i != 1, fields["source_error"])
		require.Equal(t, i != 0, fields["native_expired"])
		require.IsType(t, "", fields["error_type"])
		require.IsType(t, "", fields["error_leaf"])
		require.Len(t, fields, 5)
	}
}
