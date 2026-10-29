package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 从首次开放经过正式Prepare/Host/VM/Guest/PG/Finalize，至原最终开放
// 再注入来源失败；不伪造前驱Completed。PG/Root/Kube/QMP仍是明确peer，
// Session、SDK、WAL、typed CAS及管理处理器为生产实现，不称Linux物理E2E。
func TestProductionResumeSourceFailureClosesAndRetainsActive(t *testing.T) {
	for _, mode := range []string{"unreceived", "applied", "settled", "lost-start", "lost-close", "transfer", "post-send-source-drift", "qualification-releasing", "postmaster-replaced"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "resume-safety-"+mode) })
	}
}

func verifyResumeServiceSafety(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, client *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "resume-safety-")
	if strings.HasPrefix(mode, "planned-") {
		verifyPlannedResumeServiceSafety(t, ctx, strings.TrimPrefix(mode, "planned-"), api, store, client, session, resources, worker, config, source, pg, root, qmp, transfer)
		return
	}
	initial, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, initial.State().BudgetHeld)
	require.Nil(t, initial.State().InitialHeld)
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResumeAdmission).Phase)
	settled := mode == "settled" || mode == "qualification-releasing" || mode == "postmaster-replaced"
	if mode == "post-send-source-drift" {
		pg.afterOpen = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes++; root.mu.Unlock() }
	} else if !settled {
		pg.faults(mode != "applied" && mode != "lost-close", mode == "applied" || mode == "lost-close")
	}
	progress, openErr := worker.Advance(ctx)
	if settled || mode == "post-send-source-drift" {
		// 写后来源漂移也须在同轮先完成安全关闭；不等错误使owner退出。
		require.NoError(t, openErr)
		require.Equal(t, pgexec.Recorded, progress)
	} else {
		require.Error(t, openErr, "原最终开放发送前/后丢回复必须保留未知")
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	var restoreSource func()
	restore := func() {
		if restoreSource != nil {
			restoreSource()
			restoreSource = nil
		}
	}
	defer restore()
	if mode == "post-send-source-drift" {
		// 关闭不得把原最终开放Unknown改写为Completed。
		require.NotNil(t, before.State().ServiceClosure)
		require.False(t, before.State().ServiceClosure.Pending())
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResumeAdmission).Phase)
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	} else {
		require.Nil(t, before.State().ServiceClosure, "健康原来源只保留原开放结果")
		// 这些原正常结案回归现属于永久关闭场景：来源恢复不得自动
		// 重开。单独构造每个生命周期，避免在同一成功链中撤销关闭。
		switch mode {
		case "qualification-releasing":
			cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			changed := cm.DeepCopy()
			stamp := metav1.Now()
			changed.DeletionTimestamp = &stamp
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
			restoreSource = func() {
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			}
		case "postmaster-replaced":
			root.mu.Lock()
			birth := *root.view.Birth
			birth.PostmasterStartTicks++
			root.processOverride = &birth
			root.mu.Unlock()
			restoreSource = func() { root.mu.Lock(); root.processOverride = nil; root.mu.Unlock() }
		default:
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes++
			root.mu.Unlock()
			restoreSource = func() { root.mu.Lock(); root.view.Actual.MemoryMaxBytes--; root.mu.Unlock() }
		}
		if mode == "lost-start" || mode == "transfer" {
			api.mu.Lock()
			api.loseJournalReply = true
			api.mu.Unlock()
		}
		if mode == "lost-close" {
			pg.faults(false, true)
		}
		posts, _, _ := pg.counts()
		progress, err = worker.Advance(ctx)
		s, loadErr := store.Load(ctx)
		require.NoError(t, loadErr)
		require.NotNil(t, s.State().ServiceClosure, "预算已完整结算但原最终开放未知/已确认时，来源失效必须独立关闭")
		switch mode {
		case "lost-start", "transfer":
			var unknown *journal.UnconfirmedWrite
			require.ErrorAs(t, err, &unknown)
			after, _, _ := pg.counts()
			require.Equal(t, posts, after, "关闭写前CAS丢回复零派发")
		case "lost-close":
			require.ErrorIs(t, err, ErrServiceSafety)
		default:
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
		}
	}
	s, err := store.Load(ctx)
	require.NoError(t, err)
	c := s.State().ServiceClosure
	require.NotNil(t, c)
	require.Equal(t, before.State().Active, s.State().Active, "关闭不结案、取消或重写任何原领域事实")
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	var original control.AdmissionRequest
	require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.ResumeAdmission).Command, &original))
	require.Equal(t, original, *c.Original)
	if mode == "transfer" {
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, client, source)
		result, err := worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationContinueServiceClosure, c.CommandSHA256))
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, result.Progress)
		require.Equal(t, before.State().Active, result.State.Active)
	} else {
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		if c.Pending() {
			_, err = worker.Advance(ctx)
			if mode == "lost-start" {
				require.ErrorIs(t, err, ErrServiceSafety)
				_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceClosure, c.CommandSHA256))
			}
			require.NoError(t, err)
		}
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.False(t, s.State().ServiceClosure.Pending())
	require.False(t, pg.shared().Observed.AdmissionOpen)
	require.Equal(t, before.State().Active, s.State().Active)
	require.Equal(t, before.State().BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, before.State().History, s.State().History)
	restore()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pending, closing := worker.client.PendingAdmission()
	require.Nil(t, pending, "已确认关闭不得因保留Active未知而重建旧开放SDK栅栏")
	require.Nil(t, closing)
	_, err = worker.Advance(ctx)
	require.ErrorIs(t, err, ErrServiceSafety)
	key := s.State().Active.Request.Key
	command := closureControl(session, config.Instance, control.CoordinationReplayAdmission, "")
	command.Key = &key
	_, err = worker.executeControl(ctx, command)
	require.ErrorIs(t, err, ErrServiceSafety, "完整合法管理选择也不得绕过独立关闭")
	_, _, resizes := pg.counts()
	require.Equal(t, 1, resizes, "独立关闭不再执行PG伸缩")
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes, "关闭不缩VM或重放QMP")
	qmp.mu.Unlock()
}
