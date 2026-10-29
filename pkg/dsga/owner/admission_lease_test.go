package owner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

func TestProductionNativeLeaseStopsOnMissingOrCorruptOuterWAL(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "native-lease-"+mode) })
	}
}

func TestProductionNativeLeaseRuntimeRequiresNativeSupport(t *testing.T) {
	productionInitialService(t, "native-lease-runtime")
}

// 生产预算/Session/SDK/来源均真实执行，PG期限为显式协议peer；真实后台
// 过期/迟到拒绝由配套PG17测试验证，不把本测试计为物理或Linux验收。
func verifyNativeAdmissionLease(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, worker *PGWorker, pg *initialServicePG) {
	t.Helper()
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, worker.renewAdmissionLease(ctx))
	pg.mu.Lock()
	deadline, count := pg.leaseDeadline, pg.leaseRenewals
	pg.mu.Unlock()
	require.Equal(t, 1, count)
	healthy, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), healthy.State(), "续租不写预算或业务WAL")
	if strings.HasSuffix(mode, "runtime") {
		loopCtx, stop := context.WithCancel(ctx)
		err := worker.Run(loopCtx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 100 * time.Millisecond, Observe: func(tick Tick) { require.NoError(t, tick.Err); stop() }})
		require.ErrorIs(t, err, context.Canceled)
		stop()
		pg.mu.Lock()
		require.Greater(t, pg.leaseRenewals, count)
		pg.leaseReject = true
		pg.mu.Unlock()
		err = worker.Run(ctx, PGLoopOptions{Control: nil, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, PollInterval: 100 * time.Millisecond, Observe: nil})
		require.Error(t, err, "旧网关不能降级成无限期服务")
		return
	}
	api.mu.Lock()
	old := api.vm.DeepCopy()
	if strings.HasSuffix(mode, "missing") {
		delete(api.vm.Annotations, control.JournalAnnotation)
	} else {
		api.vm.Annotations[control.JournalAnnotation] = "{broken"
	}
	api.mu.Unlock()
	require.Error(t, worker.renewAdmissionLease(ctx), "坏/缺WAL不应发送新续租")
	pg.mu.Lock()
	require.Equal(t, deadline, pg.leaseDeadline)
	require.Equal(t, count, pg.leaseRenewals)
	pg.mu.Unlock()
	api.mu.Lock()
	api.vm = old
	api.mu.Unlock()
	restored, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), restored.State(), "坏日志不得释放或改造原预算")
}
