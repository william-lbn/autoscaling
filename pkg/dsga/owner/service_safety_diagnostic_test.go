package owner

import (
	"context"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"reflect"
	"strings"
	"testing"
	"time"
)

// 同一正式Session/TLS/原始首次预算/WAL；原Root证据仍为显式peer，不冒Linux实测。
// 反射仅让同一因果测试可在旧生产代码编译：旧版确已关闭但Tick没有首次检查叶。
func TestIdleServiceClosureTickRetainsFirstFailure(t *testing.T) {
	productionInitialService(t, "service-diagnostic-root-drift")
}
func verifyIdleServiceDiagnostic(t *testing.T, ctx context.Context, store *journal.Store, worker *PGWorker, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	healthy, closed := false, false
	err = worker.Run(runCtx, PGLoopOptions{PollInterval: time.Millisecond, Observe: func(tick Tick) {
		if !healthy {
			require.NoError(t, tick.Err)
			require.Equal(t, pgexec.Idle, tick.Progress)
			healthy = true
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes++
			root.mu.Unlock()
			return
		}
		if tick.Progress != pgexec.Recorded {
			return
		}
		s, e := store.Load(ctx)
		require.NoError(t, e)
		require.NotNil(t, s.State().ServiceClosure)
		require.Equal(t, journal.Settled, s.State().ServiceClosure.Phase)
		require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
		require.Nil(t, s.State().Active)
		require.Empty(t, s.State().History)
		require.False(t, pg.shared().Observed.AdmissionOpen)
		posts, _, resizePosts := pg.counts()
		require.Equal(t, 2, posts)
		require.Zero(t, resizePosts)
		closed = true
		// 先证明原关闭已真实成功，再断言旧生产缺少诊断，避免把编译错误充作RED。
		d := reflect.ValueOf(tick).FieldByName("IdleService")
		require.True(t, d.IsValid(), "original closure settled but Tick lost its first source failure")
		require.False(t, d.IsNil())
		require.Equal(t, "root_limits", d.MethodByName("Stage").Call(nil)[0].String())
		require.Equal(t, "resource_unknown", d.MethodByName("Type").Call(nil)[0].String())
		cancel()
	}})
	require.True(t, healthy)
	require.True(t, closed)
	require.ErrorIs(t, err, context.Canceled)
}

type idleCyclicDiagnosticError struct{}

func (*idleCyclicDiagnosticError) Error() string   { panic("private error must never be formatted") }
func (e *idleCyclicDiagnosticError) Unwrap() error { return e }

func TestIdleServiceDiagnosticIsBoundedAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("https://user:private-token/private %w", context.DeadlineExceeded), "context_deadline"},
		{errors.Join(errors.New("Bearer private"), context.Canceled), "context_canceled"},
		{apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "private-name", errors.New("private-token")), "k8s_forbidden"},
		{apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, "private-name"), "k8s_not_found"},
		{resourceguard.ErrUnknown, "resource_unknown"}, {vmguard.ErrUnknown, "runner_unknown"},
		{&resourceguard.ClientError{Code: "private-token", MayHaveExecuted: true}, "resource_client"},
		{&idleCyclicDiagnosticError{}, "validation"},
	} {
		require.NotPanics(t, func() { require.Equal(t, tc.want, idleServiceFailureType(tc.err)) })
	}
	require.Equal(t, "unclassified", (IdleServiceDiagnostic{stage: "Bearer private", kind: "private"}).Stage())
	require.Equal(t, "unclassified", (IdleServiceDiagnostic{stage: "Bearer private", kind: "private"}).Type())
}
func TestIdleServiceDiagnosticKeepsFirstFailureAndOriginalError(t *testing.T) {
	w := &PGWorker{}
	original := &resourceguard.ClientError{Code: "response_unconfirmed", MayHaveExecuted: true}
	w.recordIdleServiceDiagnostic("root_state_read", original)
	w.recordIdleServiceDiagnostic("qualification_read", context.DeadlineExceeded)
	tick := w.diagnosticTick(pgexec.Recorded, nil)
	require.Nil(t, tick.Err)
	require.Equal(t, pgexec.Recorded, tick.Progress)
	require.Equal(t, "root_state_read", tick.IdleService.Stage())
	require.Equal(t, "resource_client", tick.IdleService.Type())
	require.Nil(t, w.diagnosticTick(pgexec.Idle, nil).IdleService, "next Tick cannot inherit an old failure")
	w.recordIdleServiceDiagnostic("runner_original_receipt", original)
	failed := w.diagnosticTick(pgexec.AwaitingFact, original)
	require.Same(t, original, failed.Err)
	require.True(t, original.MayHaveExecuted)
	require.NotContains(t, fmt.Sprint(failed.IdleService), "response_unconfirmed")
	require.True(t, strings.HasPrefix(failed.IdleService.Stage(), "runner_"))
}
