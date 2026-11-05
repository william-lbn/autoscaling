package owner

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 原Owner/全部来源、TLS及WAL均真实运行；只有Native时钟是显式peer。
// 同一轮数次完整审计累计超过30秒，任一单次审计仍必须在原期限内完成。
// peer每次取时也前进1ns，符合真实单调时钟，不会伪造紧邻续租相等deadline。
func TestProductionCompletionAuditsPreserveLeaseAcrossResume(t *testing.T) {
	productionInitialService(t, "completion-audit-resume-slow")
}
func TestProductionCompletionAuditsPreserveLeaseBeforeFinalize(t *testing.T) {
	productionInitialService(t, "completion-audit-before-finalize-slow")
}
func TestProductionCompletionAuditCannotReviveExpiredLease(t *testing.T) {
	productionInitialService(t, "completion-audit-expired")
}
func TestProductionCompletionAuditRejectsAuthorityDrift(t *testing.T) {
	for _, mode := range []string{"vm-rv", "lease-uid", "native-gate", "old-host-missing", "renew-reply-lost"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); productionInitialService(t, "completion-audit-"+mode) })
	}
}

func verifyCompletedServiceAuditLease(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, worker *PGWorker, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	mode = strings.TrimPrefix(mode, "completion-audit-")
	var now, audits atomic.Uint64
	now.Store(uint64(1000 * time.Second))
	pg.mu.Lock()
	pg.leaseClock = func() control.Uint64 { return control.Uint64(now.Add(1)) }
	pg.leaseWindow = control.Uint64(30 * time.Second)
	pg.leaseDeadline = control.Uint64(now.Load()) + pg.leaseWindow
	pg.leaseExpired, pg.leaseRenewals = false, 0
	pg.rearmFixture = &serviceRearmPG{epoch: session.Actor().Epoch, api: api, loseRenewReply: mode == "renew-reply-lost"}
	pg.mu.Unlock()
	root.mu.Lock()
	root.afterProcess = func() {
		audits.Add(1)
		step := uint64(6 * time.Second)
		if mode == "expired" {
			step = uint64(16 * time.Second)
		}
		now.Add(step)
	}
	root.mu.Unlock()
	if mode == "old-host-missing" {
		require.NoError(t, core.CoreV1().Nodes().Delete(ctx, "node-one", metav1.DeleteOptions{}))
	}
	api.mu.Lock()
	originalLeaseUID := api.lease.UID
	api.mu.Unlock()
	if mode == "vm-rv" || mode == "lease-uid" || mode == "native-gate" {
		pg.mu.Lock()
		pg.rearmFixture.afterObserve = func() {
			switch mode {
			case "native-gate":
				pg.gate.Revision++
			case "lease-uid":
				api.mu.Lock()
				api.lease.UID = "completion-audit-other-lease"
				api.mu.Unlock()
			case "vm-rv":
				api.mu.Lock()
				api.vmVersion++
				api.vm.ResourceVersion = fmt.Sprintf("completion-rv-%d", api.vmVersion)
				api.mu.Unlock()
			}
		}
		pg.mu.Unlock()
	}
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, before.State().Active)
	prior := before.State().Active.Request
	baseline := now.Load()
	progress, callErr := worker.Advance(ctx)
	if mode == "lease-uid" {
		api.mu.Lock()
		api.lease.UID = originalLeaseUID
		api.mu.Unlock()
	}
	positive := strings.HasSuffix(mode, "slow")
	if positive {
		require.NoError(t, callErr)
		require.Equal(t, pgexec.Recorded, progress)
		// Run在领域成功后的原普通续租仍执行完整后审，不能删去它求绿。
		require.NoError(t, worker.renewAdmissionLease(ctx))
	}
	after, err := store.Load(ctx)
	require.NoError(t, err)
	pg.mu.Lock()
	renewals, expired, deadline := pg.leaseRenewals, pg.leaseExpired, pg.leaseDeadline
	pg.mu.Unlock()
	require.Equal(t, prior, after.State().Active.Request, "检查点不替换原操作Key/期限")
	if positive {
		require.Greater(t, now.Load()-baseline, uint64(30*time.Second))
		require.False(t, expired)
		require.GreaterOrEqual(t, renewals, 2, "完整前审出口及原后审各有实际续租")
		require.Greater(t, uint64(deadline), now.Load())
		require.Nil(t, after.State().ServiceClosure)
		require.True(t, pg.shared().Observed.AdmissionOpen)
		if mode == "resume-slow" {
			require.Equal(t, journal.Settled, budgetStep(t, store, journal.ResumeAdmission).Phase)
		} else {
			require.Equal(t, journal.Settled, budgetStep(t, store, journal.FinalizeBudget).Phase)
		}
		return
	}
	if callErr == nil {
		// 拒绝业务后原独立关闭可以合法返回Recorded；必须验证真实关闭而非吞错。
		require.Equal(t, pgexec.Recorded, progress)
		require.NotNil(t, after.State().ServiceClosure)
		require.Equal(t, journal.Settled, after.State().ServiceClosure.Phase)
		require.False(t, pg.shared().Observed.AdmissionOpen)
	}
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResumeAdmission).Phase, "失败审计不派发新开放")
	if mode == "renew-reply-lost" {
		require.Equal(t, 1, renewals, "原续租Unknown不能自动重发")
	} else {
		require.Zero(t, renewals, "过期/失源/身份漂移不能调用新增续租")
	}
	if mode == "expired" {
		require.True(t, expired)
	}
}
