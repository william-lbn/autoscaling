package owner

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 承接真实Agent/Runner完成VM扩容、PG尚未变化的原组合；这里只设置取消和
// 网络故障，不人工组装Cleanup或永久头。生产执行器负责认证构造、Start、
// Record及Finish，再复用Publisher/Scheduler消费链。PG/Root/平台仍为显式
// 协议夹具；本机未运行该组合前不得仅凭编译宣称自动恢复已验收。
func verifyAutomaticActualCancellation(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, aggregate budget.RootAggregate, shared control.SharedCostResponse, mode string, pg *committedAbortPG) {
	t.Helper()
	if mode == "partial-auto-selected-replies" {
		verifySelectedActualCancellation(t, ctx, api, client, store, session, worker, resources, config, aggregate, shared, pg)
		return
	}
	var armed atomic.Bool
	var unavailable atomic.Bool
	unavailable.Store(mode == "partial-auto-source-gone")
	client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		name := action.(kt.GetAction).GetName()
		if name != "outside-original" {
			return false, nil, nil
		}
		if unavailable.Load() {
			return true, nil, apierrors.NewNotFound(core.Resource("configmaps"), name)
		}
		if mode == "partial-auto-record-lost" && !armed.Load() {
			api.mu.Lock()
			vm := api.vm.DeepCopy()
			api.mu.Unlock()
			// Annotation是带摘要的压缩envelope；通过真实codec读取，
			// 不在故障注入中把封装JSON直接当Model或手解压绕过身份检查。
			snapshot, err := journal.Inspect(vm, store.Binding())
			require.NoError(t, err)
			model := snapshot.State()
			if model.Active != nil && model.Active.Cleanup != nil && model.Active.Cleanup.Phase == journal.Unconfirmed && armed.CompareAndSwap(false, true) {
				api.mu.Lock()
				api.loseJournalReply = true
				api.mu.Unlock()
			}
		}
		return false, nil, nil
	})
	if mode == "partial-auto-start-lost" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	progress, err := worker.Advance(ctx)
	if mode == "partial-auto-source-gone" {
		require.Error(t, err)
		s, readErr := store.Load(ctx)
		require.NoError(t, readErr)
		require.Nil(t, s.State().Active.Cleanup)
		require.Nil(t, s.State().CapacityHeld, "缺原来源时不登记自报预算")
		unavailable.Store(false)
		progress, err = worker.Advance(ctx)
	}
	if mode == "partial-auto-start-lost" || mode == "partial-auto-record-lost" {
		var unknown *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &unknown)
		s, readErr := store.Load(ctx)
		require.NoError(t, readErr)
		require.NotNil(t, s.State().Active.Cleanup)
		if mode == "partial-auto-start-lost" {
			require.Equal(t, journal.Unconfirmed, s.State().Active.Cleanup.Phase)
			require.Nil(t, s.State().CapacityHeld)
		} else {
			require.True(t, armed.Load())
			require.Equal(t, journal.Settled, s.State().Active.Cleanup.Phase)
			require.NotNil(t, s.State().CapacityHeld, "Record已提交与永久预算必须同CAS保留")
		}
		rebuilt, rebuildErr := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, rebuildErr)
		defer rebuilt.CloseIdleConnections()
		worker = rebuilt
		pending, closing := worker.client.PendingAdmission()
		require.Nil(t, pending, "只读Cleanup不能重建成开放setter")
		require.Nil(t, closing)
	} else {
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	for i := 0; i < 3; i++ {
		s, readErr := store.Load(ctx)
		require.NoError(t, readErr)
		if s.State().Active == nil {
			break
		}
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active, "自动恢复必须真正Finish，不能只写一个预算头")
	require.NotNil(t, s.State().CapacityHeld)
	require.Nil(t, s.State().BudgetHeld)
	var command budget.ReconciledAbortCommand
	require.NoError(t, json.Unmarshal(s.State().CapacityHeld.Cleanup.Command, &command))
	require.Nil(t, command.Admission)
	require.NoError(t, s.State().CheckClosedCapacity(command.Capacity.Request))
	verifyActualHostSettlement(t, ctx, api, client, store, session, worker, aggregate, shared, command, true)
}
