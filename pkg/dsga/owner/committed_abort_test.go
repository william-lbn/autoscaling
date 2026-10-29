package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 独立原生PG协议夹具：setter可在执行前/后丢回复，status只认完整原命令。
// Shared同步实际门禁代次，避免用旧静态快照掩盖生产执行器的写后检查。
// 它不模拟PG内核；真实Agent/Root客户端/Runner/QMP/WAL依旧走既有组合代码。
type committedAbortPG struct {
	mu         sync.Mutex
	last       *control.AdmissionRequest
	posts      int
	queries    int
	loseBefore bool
	loseAfter  bool
	afterSet   func()
	// 仅原预算恢复组合用：显式模拟两次请求之间的原生关闭事实，允许
	// 下一条合法开放代次，随后又回到只能重放完整原命令的规则。
	closedOverride bool
	allowNext      bool
}

func (p *committedAbortPG) shared(s control.SharedCostResponse) control.SharedCostResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last != nil {
		s.Observed.AdmissionOpen = p.last.Open
		s.Observed.AdmissionRevision = p.last.Revision
	}
	if p.closedOverride {
		s.Observed.AdmissionOpen = false
		s.Observed.AdmissionRevision++
	}
	return s
}

func (p *committedAbortPG) serve(t *testing.T, w http.ResponseWriter, r *http.Request, s control.Snapshot) {
	t.Helper()
	var request control.AdmissionRequest
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	p.mu.Lock()
	query := strings.HasSuffix(r.URL.Path, "/status")
	disposition := control.AdmissionApplied
	if query {
		p.queries++
		disposition = control.AdmissionNotFound
		if p.last != nil && *p.last == request && !p.closedOverride {
			disposition = control.AdmissionReplayed
		} else if p.last != nil && !p.allowNext {
			require.Equal(t, request, *p.last)
		}
	} else {
		p.posts++
		if p.loseBefore {
			p.loseBefore = false
			p.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if p.last != nil && p.allowNext {
			require.True(t, p.closedOverride)
			require.Equal(t, p.last.Revision+2, request.Revision)
			require.Equal(t, p.last.Instance, request.Instance)
			require.Equal(t, p.last.BudgetCommitmentID, request.BudgetCommitmentID)
			require.Equal(t, p.last.ExpectedCapacityGeneration, request.ExpectedCapacityGeneration)
		} else if p.last != nil {
			require.Equal(t, request, *p.last, "只允许完整原命令重放")
		}
		original := request
		p.last = &original
		p.closedOverride, p.allowNext = false, false
	}
	if p.last != nil {
		s.AdmissionOpen, s.AdmissionRevision = p.last.Open, p.last.Revision
	}
	if p.closedOverride {
		s.AdmissionOpen = false
		s.AdmissionRevision++
	}
	after := p.afterSet
	lose := !query && p.loseAfter
	if !query {
		p.loseAfter, p.afterSet = false, nil
	}
	p.mu.Unlock()
	if !query && after != nil {
		after()
	}
	if lose {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	pgReply(w, control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: hash, Disposition: disposition, Observed: s, VMShrinkPermit: false})
}

// 同一次真实扩/缩组合中串联故障，复用此前容量结果，减少重复构建整条链。
// 返回时只完成Cleanup落账，Finish由调用方独立验证并可接真实Host峰值结清。
func verifyCommittedAbort(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session, worker *PGWorker, pg *committedAbortPG, root *guestRootFixture, mode string, restart func() *PGWorker) *PGWorker {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	req := s.State().Active.Request
	held := s.State().BudgetHeld
	require.NotNil(t, held)
	require.NoError(t, worker.admission.CheckRetainedService(ctx, s), "取消前完整原预算必须真实健康")
	_, err = store.RequestAbort(ctx, s, session.Actor(), req.Key)
	require.NoError(t, err)
	losses := mode == "abort-replies"
	if losses {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	progress, err := worker.Advance(ctx)
	if mode == "abort-original-open-lost" {
		require.NoError(t, err, "必须先对账原Resume未知，不能先建Cleanup")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().Active.Cleanup)
		require.Equal(t, journal.Settled, budgetStep(t, store, journal.ResumeAdmission).Phase)
		progress, err = worker.Advance(ctx)
	}
	if losses {
		require.Error(t, err, "StartCleanup已落盘但回复丢失")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, journal.Unconfirmed, s.State().Active.Cleanup.Phase)
		original := *s.State().Active.Cleanup
		worker = restart()
		defer worker.CloseIdleConnections()
		pending, closing := worker.client.PendingAdmission()
		require.NotNil(t, pending, "进程恢复必须保留原Cleanup SDK未知")
		require.Nil(t, closing)
		_, err = worker.Advance(ctx)
		require.NoError(t, err, "not-found只读等待，不能自动发送")
		pg.mu.Lock()
		require.Zero(t, pg.posts)
		pg.mu.Unlock()
		wrong := req.Key
		wrong.OperationID.Low++
		_, err = worker.ReplayCommittedAbortOriginal(ctx, wrong)
		require.Error(t, err)
		_, err = worker.ReplayCommittedAbortOriginal(ctx, req.Key)
		require.Error(t, err, "显式原命令在原生执行前丢回复")
		_, err = worker.Advance(ctx)
		require.NoError(t, err)
		pg.mu.Lock()
		require.Equal(t, 1, pg.posts, "not-found和轮询均不能自动重发")
		pg.mu.Unlock()
		// 第二次由真实长期循环消费受信显式选择；普通循环先status读取，
		// 只有独立AbortReplay通道才可重发完全相同的原开放命令。
		selections := make(chan control.Key, 1)
		selections <- req.Key
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		err = worker.Run(loopCtx, PGLoopOptions{PollInterval: time.Second, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, VMReplay: nil, AbortReplay: selections, UnchangedAdmissionRecovery: nil, Observe: func(tick Tick) {
			pg.mu.Lock()
			sent := pg.posts
			pg.mu.Unlock()
			if sent == 2 {
				require.Error(t, tick.Err, "原setter已生效但回复丢失")
				stop()
			}
		}})
		require.ErrorIs(t, err, context.Canceled)
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.Error(t, err, "完整Cleanup已Record但回复丢失")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, original.CommandSHA256, s.State().Active.Cleanup.CommandSHA256)
		require.Equal(t, original.DispatchedBy, s.State().Active.Cleanup.DispatchedBy)
	} else if mode == "abort-root-drift" {
		require.NoError(t, err, "setter后真实Root漂移同轮独立关闭，不能记录清理完成")
		require.Equal(t, pgexec.Recorded, progress)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, journal.Unconfirmed, s.State().Active.Cleanup.Phase)
		require.NotNil(t, s.State().ServiceClosure)
		require.False(t, s.State().ServiceClosure.Pending())
		require.Equal(t, held, s.State().BudgetHeld)
		pg.mu.Lock()
		posts := pg.posts
		require.NotNil(t, pg.last)
		require.False(t, pg.last.Open)
		pg.mu.Unlock()
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, ErrServiceSafety, "来源恢复不能清除独立关闭并重开旧Cleanup")
		pg.mu.Lock()
		require.Equal(t, posts, pg.posts)
		pg.mu.Unlock()
		return worker
	} else {
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, held, s.State().BudgetHeld, "不能因业务取消替换完整容量预算")
	require.NotNil(t, s.State().Active.Cleanup, "正常取消必须创建Cleanup，不能把安全关闭Recorded当业务完成：%+v", s.State().ServiceClosure)
	require.NoError(t, s.State().CheckCommittedAbort())
	require.Equal(t, journal.Settled, s.State().Active.Cleanup.Phase)
	var audit budget.CommittedAbortReceipt
	require.NoError(t, json.Unmarshal(s.State().Active.Cleanup.Observation.Data, &audit))
	require.Equal(t, req, audit.Command.Request)
	if mode == "abort-after-open" || mode == "abort-original-open-lost" {
		require.False(t, audit.Command.MaySend)
		require.True(t, audit.Query)
	}
	pg.mu.Lock()
	posts := pg.posts
	pg.mu.Unlock()
	want := 1
	if losses {
		want = 2
	}
	require.Equal(t, want, posts, "取消恢复只开放原预算，不重发PG/VM/Root动作")
	return worker
}

func finishCommittedAbort(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, worker *PGWorker, mode string) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	request := s.State().Active.Request
	if mode == "abort-replies" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.Error(t, err, "Finish已提交但回复丢失后必须重读原历史")
	} else {
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.EqualValues(t, 6, s.State().ProtocolVersion)
	require.Equal(t, journal.AbortedAfterReconcile, s.State().History[len(s.State().History)-1].Outcome)
	require.NoError(t, s.State().CheckClosedBudget(request))
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Idle, progress)
}
