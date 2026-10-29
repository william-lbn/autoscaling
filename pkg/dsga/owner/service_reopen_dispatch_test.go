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

	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 复用生产首次预算、完整来源 Reader、Root、Runner 出生/发布、真实
// Elector/Session/TLS/UID-RV CAS 及 Run 管理通道。PG/Root/Kube/QMP 为
// 明确 peer；本组合检验跨域顺序及未知保留，不冒充原生或 Linux 认证。
func TestProductionServiceReopenAuditsSourcesAndClosesOriginalUnknown(t *testing.T) {
	for _, mode := range []string{"normal", "first-unknown", "source-unavailable", "lost-start", "setter-before", "setter-after", "lost-record", "source-after-selection", "lease-after-selection", "rv-after-selection", "source-after-setter", "source-after-unknown-setter", "lease-after-setter", "rv-after-setter", "close-lost-start", "close-before", "close-after", "close-lost-record"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "service-reopen-"+mode) })
	}
}

func TestProductionServiceReopenCancelAfterNativeWriteRetainsOriginal(t *testing.T) {
	productionInitialService(t, "service-reopen-cancel-after-setter")
}

// 接管恢复从先前原 owner 已选、仍未知的 WAL 开始。完整来源新开放
// 已由上面的生产组合覆盖；本例独立检验来源不可用时的安全发送：
// 真实 Elector 取得更高 owner，旧开放/关闭重放拒绝，接续只选最近摘要。
func TestServiceReopenClosureDispatchAfterRealElectorTakeover(t *testing.T) {
	for _, mode := range []string{"opening", "close-lost-start", "close-before", "close-after", "close-lost-record"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			walMode := "close-notfound"
			if mode == "opening" {
				walMode = "open-notfound"
			}
			original, initial := serviceReopenRecoveryWAL(t, store, walMode)
			elector, err := New(coord, store, DefaultOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				require.Greater(t, session.Actor().Epoch, original.State().Owner.Epoch)
				root, resourceConfig := guestRoot(t, store)
				qualifiedRootFixture(root)
				root.mu.Lock()
				root.view.Bootstrap, root.view.Birth = initial.PGAggregate.Bootstrap, &initial.PGAggregate.Birth
				root.view.Actual.MemoryMaxBytes = initial.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
				root.view.Actual.HugePools = initial.PGAggregate.Bootstrap.Limits.HugePools
				root.mu.Unlock()
				resources, err := session.NewResourceWorker(ctx, resourceConfig)
				require.NoError(t, err)
				defer resources.CloseIdleConnections()
				request := pgRequest(t, session.Actor())
				request.Key.Instance = initial.Instance
				r := original.State().ServiceClosure.Reopen
				pg := new(initialServicePG)
				pg.request, pg.store, pg.before, pg.after = request, store, initial.Shared, initial.Shared
				opened := r.Request
				pg.gate, pg.leaseDeadline = &opened, r.Before.DeadlineNS
				if mode == "opening" {
					// 原开放未执行但仍可能迟到；只读 Run 必须保持未知，
					// 后继关闭仍要封住这个完整原意图，不能依赖未找到。
					_, _, closed, _ := original.State().ServiceClosure.Selection()
					pg.gate = &closed
				}
				pg.reopenFixture = &serviceReopenPG{mode: "", api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
				config := pgConfigWithFence(t, request, func(w http.ResponseWriter, req *http.Request) { pg.serve(t, w, req) }, pg.ownerFence(t), func() control.Snapshot { return pg.shared().Observed })
				worker, err := session.NewPGWorker(ctx, config, resources)
				require.NoError(t, err)
				defer worker.CloseIdleConnections()
				endpoint, stop := reopenRun(t, ctx, worker)
				defer stop()
				_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
				require.ErrorIs(t, err, ErrServiceSafety, "更高owner不能重放旧开放")
				selector, action := r.CommandSHA256, control.CoordinationCloseServiceReopen
				if r.Closing != nil {
					_, _, _, selector = r.Closing.Close.Selection()
					action = control.CoordinationContinueServiceReopenClosure
					_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopenClosure, selector))
					require.ErrorIs(t, err, ErrServiceSafety, "更高owner不能改写旧关闭发送者")
				}
				pg.mu.Lock()
				pg.reopenFixture.mode = mode
				pg.mu.Unlock()
				if mode == "close-lost-start" {
					api.mu.Lock()
					api.loseJournalReply = true
					api.mu.Unlock()
				}
				_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, action, selector))
				if mode == "opening" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				latest, err := store.Load(ctx)
				require.NoError(t, err)
				closeWAL := latest.State().ServiceClosure.Reopen.Closing.Close
				if r.Closing != nil {
					require.NotNil(t, closeWAL.Continuation)
					retained := *closeWAL
					retained.Continuation = nil
					require.Equal(t, *r.Closing.Close, retained)
				}
				_, _, _, selected := closeWAL.Selection()
				if mode == "close-lost-start" || mode == "close-before" {
					pg.mu.Lock()
					beforePosts := pg.reopenFixture.closes
					pg.mu.Unlock()
					_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopen, selected))
					require.ErrorIs(t, err, ErrServiceSafety)
					pg.mu.Lock()
					require.Equal(t, beforePosts, pg.reopenFixture.closes)
					pg.mu.Unlock()
					_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopenClosure, selected))
					require.NoError(t, err)
				}
				state, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
				require.NoError(t, err)
				require.False(t, state.State.ServiceClosure.Reopen.Closing.Close.Pending())
				latest, err = store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, original.State().InitialHeld, latest.State().InitialHeld)
				require.Equal(t, r.Request, latest.State().ServiceClosure.Reopen.Request)
				require.Equal(t, journal.Unconfirmed, latest.State().ServiceClosure.Reopen.Phase)
				pg.mu.Lock()
				require.Zero(t, pg.reopenFixture.posts)
				require.Zero(t, pg.reopenFixture.renewals)
				require.False(t, pg.current().Observed.AdmissionOpen)
				pg.mu.Unlock()
				return nil
			})
			require.NoError(t, err)
		})
	}
}

type serviceReopenPG struct {
	mode                        string
	api                         *ownerAPI
	root                        *guestRootFixture
	posts, closes, queries      int
	renewals                    int
	selectionFault, setterFault bool
	closeFault                  bool
	cancelAfterOpen             context.CancelFunc
}

// 与旧 peer 隔离的有状态新开放窗口。所有 setter 到达时读取真实持久
// WAL，不能在测试中先手填完成记录；查询只命中准确原命令。来源故障
// 通过真实 Reader 随后读取的 Root/VM 状态注入，未修改生产校验器。
func (p *initialServicePG) serveReopenFixture(t *testing.T, w http.ResponseWriter, req *http.Request) bool {
	t.Helper()
	p.mu.Lock()
	f := p.reopenFixture
	p.mu.Unlock()
	if f == nil {
		return false
	}
	switch req.URL.Path {
	case "/dsg/v1/admission/lease", "/dsg/v1/admission", "/dsg/v1/admission/status", "/dsg/v1/admission/close-original", "/dsg/v1/admission/close-original/status", "/dsg/v1/admission/continue-close", "/dsg/v1/admission/continue-close/status":
	default:
		return false
	}
	s, err := p.store.Load(req.Context())
	require.NoError(t, err)
	c := s.State().ServiceClosure
	if c == nil {
		return false
	}
	require.NotNil(t, c)
	r := c.Reopen
	if req.URL.Path == "/dsg/v1/admission/lease" {
		var q control.AdmissionLeaseRequest
		require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
		p.mu.Lock()
		defer p.mu.Unlock()
		if q.Action != "observe" {
			f.renewals++
			t.Errorf("新开放仍保留业务关闭时出现续租/rearm：%s", q.Action)
		}
		fact := control.AdmissionLeaseResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Armed: true, Renewed: false, Expired: p.leaseExpired, DeadlineNS: p.leaseDeadline, LeaseEpoch: q.IssuerEpoch, TimeoutMS: 30000, Observed: p.current().Observed}
		if !f.selectionFault {
			switch {
			case r != nil && r.Attempt == 0 && r.Closing != nil && !r.Closing.Close.Pending() && f.mode == "retry-source-after-selection":
				// 与首次开放窗口一致：最后写前observe返回时来源失效，
				// 写后完整来源复核应在setter之前发现，不放到后审期间。
				f.root.mu.Lock()
				f.root.view.Actual.MemoryMaxBytes++
				f.root.mu.Unlock()
				f.selectionFault = true
			case r == nil && f.mode == "source-after-selection":
				f.root.mu.Lock()
				f.root.view.Actual.MemoryMaxBytes++
				f.root.mu.Unlock()
				f.selectionFault = true
			case r == nil && f.mode == "lease-after-selection":
				p.leaseExpired, f.selectionFault = true, true
			case r != nil && f.mode == "rv-after-selection":
				f.api.mu.Lock()
				f.api.vmVersion++
				f.api.vm.ResourceVersion += "-changed"
				f.api.mu.Unlock()
				f.selectionFault = true
			}
		}
		pgReply(w, fact)
		return true
	}
	require.NotNil(t, r)
	query := strings.HasSuffix(req.URL.Path, "/status")
	if strings.Contains(req.URL.Path, "/close-original") || strings.Contains(req.URL.Path, "/continue-close") {
		var closeRequest control.AdmissionRequest
		var continuation *control.AdmissionContinuationRequest
		if strings.Contains(req.URL.Path, "/continue-close") {
			var q control.AdmissionContinuationRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
			require.NotNil(t, r.Closing)
			require.NotNil(t, r.Closing.Close.Continuation)
			require.Equal(t, r.Closing.Close.Continuation.Request, q)
			closeRequest, continuation = q.Close, &q
		} else {
			var q control.AdmissionCloseRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
			require.NotNil(t, r.Closing)
			require.Equal(t, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: r.Request, Close: r.Closing.Close.Request}, q)
			closeRequest = q.Close
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if query {
			f.queries++
			if p.gate == nil || *p.gate != closeRequest {
				w.WriteHeader(http.StatusConflict)
				return true
			}
		} else {
			require.True(t, r.Closing.Close.Pending(), "关闭setter须有完整未决WAL")
			require.Equal(t, closeRequest.IssuerEpoch, p.current().Observed.IssuerFloorEpoch)
			f.closes++
			if !f.closeFault && f.mode == "close-before" {
				f.closeFault = true
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			selected := closeRequest
			p.gate = &selected
			if !f.closeFault && f.mode == "close-after" {
				f.closeFault = true
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			if !f.closeFault && f.mode == "close-lost-record" {
				f.api.mu.Lock()
				f.api.loseJournalReply = true
				f.api.mu.Unlock()
				f.closeFault = true
			}
		}
		closed := serviceReopenNativeResponse(t, closeRequest, p.current().Observed, query)
		if continuation != nil {
			digest, err := continuation.ContextSHA256()
			require.NoError(t, err)
			pgReply(w, control.AdmissionContinuationResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: *continuation, ContextSHA256: digest, ClosedBy: closed, PriorMayHaveApplied: true})
		} else {
			pgReply(w, control.AdmissionClosure{ProtocolVersion: 1, VMShrinkPermit: false, Original: r.Request, ClosedBy: closed, PriorMayHaveApplied: true})
		}
		return true
	}
	var q control.AdmissionRequest
	require.NoError(t, json.NewDecoder(req.Body).Decode(&q))
	require.Equal(t, r.Request, q)
	p.mu.Lock()
	defer p.mu.Unlock()
	if query {
		f.queries++
		fact := serviceReopenNativeResponse(t, q, p.current().Observed, true)
		if p.gate == nil || *p.gate != q {
			fact.Disposition, fact.Observed = control.AdmissionNotFound, p.current().Observed
		} else if p.leaseExpired {
			// 原生过期已独立关闭。相同 revision 的历史 open 不能被
			// 当前 closed 快照补成成功，只保留准确请求的冲突/未知。
			fact.Disposition, fact.Observed = control.AdmissionConflict, p.current().Observed
		}
		pgReply(w, fact)
		return true
	}
	require.Equal(t, journal.Unconfirmed, r.Phase)
	require.Nil(t, r.Closing)
	f.posts++
	if !f.setterFault && f.mode == "setter-before" {
		f.setterFault = true
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	selected := q
	p.gate = &selected
	fact := serviceReopenNativeResponse(t, q, p.current().Observed, false)
	if !f.setterFault {
		switch f.mode {
		case "source-after-setter", "source-after-unknown-setter", "close-lost-start", "close-before", "close-after", "close-lost-record":
			f.root.mu.Lock()
			f.root.view.Actual.MemoryMaxBytes++
			f.root.mu.Unlock()
			if f.mode == "close-lost-start" {
				f.api.mu.Lock()
				f.api.loseJournalReply = true
				f.api.mu.Unlock()
			}
		case "lease-after-setter":
			p.leaseExpired = true
		case "rv-after-setter":
			f.api.mu.Lock()
			f.api.vmVersion++
			f.api.vm.ResourceVersion += "-after-open"
			f.api.mu.Unlock()
		case "lost-record":
			f.api.mu.Lock()
			f.api.loseJournalReply = true
			f.api.mu.Unlock()
		case "cancel-after-setter":
			// 显式模拟随后原生期限到期；这里只验证取消后的外层恢复，
			// 真实 PG 单调时钟独立关闭由原生测试认证，不能据 peer 外推。
			p.leaseExpired = true
			f.cancelAfterOpen()
		}
		f.setterFault = true
		if f.mode == "setter-after" || f.mode == "source-after-unknown-setter" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
	}
	pgReply(w, fact)
	return true
}

func reopenRun(t *testing.T, ctx context.Context, worker *PGWorker) (*ControlEndpoint, func()) {
	t.Helper()
	endpoint := worker.NewControlEndpoint()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(runCtx, PGLoopOptions{PollInterval: 20 * time.Millisecond, Control: endpoint, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: nil})
	}()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); require.ErrorIs(t, <-done, context.Canceled) }) }
	t.Cleanup(stop)
	return endpoint, stop
}

func verifyProductionServiceReopen(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err := worker.Advance(ctx)
	require.NoError(t, err, "真实来源漂移应先选择并完成原关闭")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	require.False(t, before.State().ServiceClosure.Pending())
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	pg.mu.Lock()
	// 前驱通过直接 Advance 完成，尚未进入 Run 的期限协商；本 peer
	// 从该点提供有限原生期限，不能返回 armed=true/deadline=0 的坏合同。
	if pg.leaseDeadline == 0 {
		pg.leaseDeadline = 1000000000
	}
	pg.reopenFixture = &serviceReopenPG{mode: "", api: api, root: root, posts: 0, closes: 0, queries: 0, renewals: 0, selectionFault: false, setterFault: false, closeFault: false, cancelAfterOpen: nil}
	pg.mu.Unlock()
	endpoint, stop := reopenRun(t, ctx, worker)
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	pg.mu.Lock()
	pg.reopenFixture.mode = mode
	pg.mu.Unlock()
	_, _, _, selector := before.State().ServiceClosure.Selection()
	command := closureControl(session, config.Instance, control.CoordinationReopenService, selector)
	wrong := command.Clone()
	wrong.Selector = strings.Repeat("ab", 32)
	_, err = endpoint.Submit(ctx, wrong)
	require.Error(t, err)
	if mode == "source-unavailable" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes++
		root.mu.Unlock()
		_, err = endpoint.Submit(ctx, command)
		require.ErrorIs(t, err, ErrServiceSafety)
		unchanged, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, before.State(), unchanged.State())
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	if mode == "lost-start" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	if mode == "cancel-after-setter" {
		pg.mu.Lock()
		pg.reopenFixture.cancelAfterOpen = cancelRequest
		pg.mu.Unlock()
	}
	_, err = endpoint.Submit(requestCtx, command)
	if mode == "normal" || mode == "first-unknown" || mode == "source-unavailable" {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
	stop()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	r := s.State().ServiceClosure.Reopen
	require.NotNil(t, r)
	retained := *s.State().ServiceClosure
	retained.Reopen = nil
	require.Equal(t, *before.State().ServiceClosure, retained)
	require.Equal(t, before.State().InitialHeld, s.State().InitialHeld)
	if mode == "first-unknown" {
		require.Equal(t, journal.Unconfirmed, s.State().InitialHeld.Admission.Phase)
	}
	preDispatch := mode == "lost-start" || mode == "source-after-selection" || mode == "lease-after-selection" || mode == "rv-after-selection"
	closing := strings.HasPrefix(mode, "close-") || mode == "source-after-setter" || mode == "source-after-unknown-setter" || mode == "lease-after-setter" || mode == "rv-after-setter"
	pg.mu.Lock()
	posts, closes := pg.reopenFixture.posts, pg.reopenFixture.closes
	pg.mu.Unlock()
	if preDispatch {
		require.Zero(t, posts, "未确认派发屏障时不能发送新开放")
	} else {
		require.Equal(t, 1, posts)
	}
	if closing {
		require.NotNil(t, r.Closing)
		require.Equal(t, journal.Unconfirmed, r.Phase, "后审异常保留原开放未知")
		if mode == "close-lost-start" {
			require.Zero(t, closes)
		} else {
			require.Equal(t, 1, closes)
		}
	} else {
		require.Nil(t, r.Closing)
	}
	if mode == "source-after-selection" {
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		root.mu.Unlock()
	}
	// 重建必须只查询完整原 WAL，任何普通轮询都不能重发。未知且尚未
	// 执行的请求仅由原 owner 明确重放；已过期期限拒绝旧开放重放，
	// 仍可选择独立关闭，不通过测试恢复期限或修改旧 rearm 事实。
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	endpoint, stop = reopenRun(t, ctx, worker)
	defer stop()
	selected := r.CommandSHA256
	if closing {
		_, _, _, selected = r.Closing.Close.Selection()
	}
	_, _ = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationObserveServiceReopen, selected))
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts)
	require.Equal(t, closes, pg.reopenFixture.closes)
	require.Zero(t, pg.reopenFixture.renewals)
	pg.mu.Unlock()
	if mode == "lease-after-selection" || mode == "cancel-after-setter" {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
		require.ErrorIs(t, err, ErrServiceSafety)
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationCloseServiceReopen, r.CommandSHA256))
		require.NoError(t, err)
		closing = true
		closes++
	} else if preDispatch || mode == "setter-before" {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
		require.NoError(t, err)
		posts++
	} else if mode == "close-lost-start" || mode == "close-before" {
		_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopenClosure, selected))
		require.NoError(t, err, "再关闭重放不能依赖已坏Root来源")
		closes++
	}
	// 上面的显式查询/重放已同步返回，直接取同一串行通道的真实状态。
	// 不能将可能受 Kube QPS 限流的 Submit 放进 5 秒 Eventually 的后台
	// 条件 goroutine：计时器可先于一个正确但较慢的读回触发，并让该
	// goroutine 在测试清理后继续访问 Session，制造无关失租错误。
	result, err := endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationState, ""))
	require.NoError(t, err)
	if closing {
		require.NotNil(t, result.State.ServiceClosure.Reopen.Closing)
		require.False(t, result.State.ServiceClosure.Reopen.Closing.Close.Pending())
	} else {
		require.Equal(t, journal.Settled, result.State.ServiceClosure.Reopen.Phase)
	}
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayServiceReopen, r.CommandSHA256))
	require.ErrorIs(t, err, ErrServiceSafety, "已确认或再关闭后不能重新派发开放")
	_, err = endpoint.Submit(ctx, closureControl(session, config.Instance, control.CoordinationReplayInitial, before.State().InitialHeld.Admission.CommandSHA256))
	require.ErrorIs(t, err, ErrServiceSafety)
	after, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
	require.Nil(t, after.State().Active)
	pg.mu.Lock()
	require.Equal(t, posts, pg.reopenFixture.posts)
	require.Equal(t, closes, pg.reopenFixture.closes)
	require.Zero(t, pg.reopenFixture.renewals)
	require.Equal(t, !closing, pg.current().Observed.AdmissionOpen)
	pg.mu.Unlock()
}
