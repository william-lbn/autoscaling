package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Runner旧单域夹具没有永久预算，不能用缺预算v1账本伪造v16关闭。
// 这里明确采用已有完整Initial领域合同作为前置，并通过生产Store保存
// 原预算/原开放；它仅供本测试的关闭边界，不声明运行了首次服务生产者。
func bindRunnerServiceBudget(t *testing.T, f *runnerFixture) {
	t.Helper()
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var domain struct {
		Initial budget.Initial                 `json:"initial"`
		Command budget.InitialAdmissionCommand `json:"command"`
		Receipt budget.InitialAdmissionReceipt `json:"receipt"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	i := domain.Initial
	i.Incarnation = f.store.Binding().Incarnation
	i.RunnerBirth, i.RunnerPodUID, i.NodeName = f.runner.engine.binding, f.runner.birth.Identity.RunnerPodUID, f.config.NodeName
	raw, err = json.Marshal(f.runner.birth.Identity)
	require.NoError(t, err)
	i.RunnerIdentitySHA256 = SourceDigest(raw)
	require.NoError(t, i.Validate())
	domain.Command.InitialSHA256, err = i.Digest()
	require.NoError(t, err)
	domain.Receipt.Command = domain.Command
	require.NoError(t, domain.Receipt.ValidateFor(i, domain.Command, f.actor.Epoch))
	ctx := context.Background()
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	s, err = f.store.BindResource(ctx, s, f.actor, i.PGAggregate.Identity)
	require.NoError(t, err)
	s, err = f.store.BindPG(ctx, s, f.actor, i.Instance)
	require.NoError(t, err)
	s, err = f.store.BindInitialBudget(ctx, s, f.actor, i)
	require.NoError(t, err)
	s, _, err = f.store.StartInitialAdmission(ctx, s, f.actor, domain.Command)
	require.NoError(t, err)
	_, err = f.store.RecordInitialAdmission(ctx, s, f.actor, domain.Receipt)
	require.NoError(t, err)
}

// 使用生产Store写前CAS、Runner固定Kube读者与真实本地flock/fsync。
// PG关闭回执、Kube和QEMU为明确领域夹具；本测试只认证关闭后的Runner边界，
// 不将固定回执或QMP内存夹具称为真实PG关闭、KVM热拔和物理返还。
func closeRunnerService(t *testing.T, f *runnerFixture, c Command, settled bool) {
	t.Helper()
	ctx := context.Background()
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	before := s.State()
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: c.Request.Key.Instance, IssuerEpoch: f.actor.Epoch, Revision: 3, ExpectedCapacityGeneration: c.Request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: c.Request.BudgetCommitmentID}
	s, _, err = f.store.StartActiveServiceClosure(ctx, s, f.actor, request)
	require.NoError(t, err)
	if settled {
		raw, err := os.ReadFile("../../api/dsga/testdata/response-v1.json")
		require.NoError(t, err)
		var fact control.ControlResponse
		require.NoError(t, json.Unmarshal(raw, &fact))
		state := fact.Observed
		state.Instance, state.IssuerFloorEpoch, state.CapacityGeneration = request.Instance, request.IssuerEpoch, request.ExpectedCapacityGeneration
		state.Operation, state.AdmissionOpen, state.AdmissionRevision = nil, false, request.Revision
		hash, err := request.ContextSHA256()
		require.NoError(t, err)
		response := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Disposition: control.AdmissionApplied, ContextSHA256: hash, Observed: state}
		require.NoError(t, response.ValidateFor(request, request.Instance, false))
		s, err = f.store.RecordServiceClosure(ctx, s, f.actor, response, false)
		require.NoError(t, err)
	}
	require.Equal(t, before.Active, s.State().Active, "关闭不能重写原资源步骤或原Abort")
}

func TestRunnerServiceClosureBlocksDispatchAndReplay(t *testing.T) {
	for _, mode := range []string{"closing-unseen", "closed-unseen", "closed-old-replay", "close-after-prepare", "may-have-sent", "original-abort", "selected-unsent", "selected-not-sent", "selected-query-only", "selected-may-have-sent", "selected-takeover"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := runnerBirthFixtureForCompute(t, true, "compute-1")
			native := &memoryFixture{dir: f.config.StateDir, actual: Observation{RequestedBytes: 1 << 30, ActualBytes: 1 << 30}, apply: true, mu: sync.Mutex{}, writes: 0, err: nil, entered: nil, release: nil}
			f.runner.engine.native = native
			require.NoError(t, f.runner.Tick(ctx))
			bindRunnerServiceBudget(t, f)
			c, outer := f.beginGrow(t)
			a, err := f.runner.authorize(ctx)
			require.NoError(t, err)
			require.True(t, a.Dispatch, "关闭前原完整来源确实足以派发，避免坏夹具假通过")
			if mode == "closed-old-replay" || mode == "selected-not-sent" {
				native.err = &notSentError{errors.New("fixture connect failed before any QMP write")}
				require.ErrorIs(t, f.runner.Tick(ctx), ErrNotSent)
				native.err = nil
				reader, err := NewReader(f.core.CoreV1(), f.store.Binding(), "lease-uid")
				require.NoError(t, err)
				original, err := reader.UnsentOriginal(ctx, c, outer, f.actor)
				require.NoError(t, err)
				s, err := f.store.Load(ctx)
				require.NoError(t, err)
				_, err = f.store.SelectVMReplay(ctx, s, f.actor, journal.VMReplaySelection{ProtocolVersion: 1, Key: c.Request.Key, Owner: f.actor, Sources: original.Sources, Nonce: strings.Repeat("a", 32)})
				require.NoError(t, err)
				a, err = f.runner.authorize(ctx)
				require.NoError(t, err)
				require.True(t, a.Dispatch)
				require.NotEmpty(t, a.ReplayNonce, "原重放提示确实有效，关闭必须独立封写")
			}
			if mode == "may-have-sent" || mode == "selected-may-have-sent" {
				native.apply, native.err = false, errors.New("fixture lost QMP reply")
				require.ErrorIs(t, f.runner.Tick(ctx), ErrUnknown)
				require.Equal(t, 1, native.writes)
				require.Equal(t, mayHaveSent, readFixtureRecord(t, f.runner.engine).Pending.Stage)
			}
			if mode == "original-abort" {
				s, err := f.store.Load(ctx)
				require.NoError(t, err)
				_, err = f.store.RequestAbort(ctx, s, f.actor, c.Request.Key)
				require.NoError(t, err)
			}
			if mode == "close-after-prepare" {
				// 两次固定Kube授权包围本地prepared持久点。关闭恰在该窗口
				// 落账时，第二次认证必须阻止may_have_sent/QMP，不只在首次读挡住。
				calls := 0
				f.runner.engine.read = func(ctx context.Context) (authorization, error) {
					calls++
					if calls == 2 {
						closeRunnerService(t, f, c, true)
					}
					return f.runner.authorize(ctx)
				}
				require.ErrorContains(t, f.runner.Tick(ctx), "authority changed")
				require.Zero(t, native.writes)
				require.Equal(t, prepared, readFixtureRecord(t, f.runner.engine).Pending.Stage)
				f.runner.engine.read = f.runner.authorize
			} else {
				closeRunnerService(t, f, c, mode != "closing-unseen")
			}
			if strings.HasPrefix(mode, "selected-") {
				s, err := f.store.Load(ctx)
				require.NoError(t, err)
				_, _, _, selector := s.State().ServiceClosure.Selection()
				_, err = f.store.StartServiceVMRecovery(ctx, s, f.actor, selector, mode != "selected-query-only")
				require.NoError(t, err)
			}
			if mode == "selected-takeover" {
				// 独立选择可以被更高owner只读接续；原命令/原Active仍属于
				// 旧owner，取消见证必须来自本次真实Lease和原VM最新快照。
				s, err := f.store.Load(ctx)
				require.NoError(t, err)
				s, err = f.store.Takeover(ctx, s, f.actor, "next-memory-owner")
				require.NoError(t, err)
				f.actor = s.State().Owner
				lease, err := f.core.CoordinationV1().Leases("testing").Get(ctx, journal.LeaseName(f.store.Binding()), metav1.GetOptions{})
				require.NoError(t, err)
				lease.Spec.HolderIdentity = &f.actor.HolderID
				_, err = f.core.CoordinationV1().Leases("testing").Update(ctx, lease, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			s, err := f.store.Load(ctx)
			require.NoError(t, err)
			originalActive, originalClosure := s.State().Active, s.State().ServiceClosure
			a, err = f.runner.authorize(ctx)
			require.NoError(t, err)
			require.False(t, a.Dispatch)
			require.Empty(t, a.ReplayNonce)
			require.Equal(t, c, *a.Command, "保留原完整命令供旧未知只读恢复")
			require.ErrorContains(t, a.Denied, "service closure")
			switch mode {
			case "original-abort", "selected-unsent", "selected-not-sent", "selected-takeover":
				require.NotNil(t, a.Abort, "关闭不能撤销先前真实Abort的封存资格")
				require.Equal(t, f.actor, a.Abort.Witness.Owner)
				require.Equal(t, s.State().Sequence, a.Abort.Witness.VMSequence)
				require.Equal(t, s.ResourceVersion(), a.Abort.Witness.VMResourceVersion)
				require.NoError(t, f.runner.Tick(ctx))
				r := readFixtureRecord(t, f.runner.engine)
				require.Equal(t, cancelledBeforeSend, r.Last.Kind)
				require.Nil(t, r.Pending)
			case "may-have-sent", "selected-may-have-sent":
				if mode == "may-have-sent" {
					require.Nil(t, a.Abort, "独立关闭本身不能生成未发送取消证明")
				} else {
					require.NotNil(t, a.Abort, "明确选择只提供封存入口，不能擦除已跨发送屏障的原WAL")
				}
				before := readFixtureRecord(t, f.runner.engine)
				require.ErrorIs(t, f.runner.Tick(ctx), ErrUnknown)
				require.Equal(t, before, readFixtureRecord(t, f.runner.engine), "before读回不能擦除可能迟到QMP")
				native.actual = Observation{RequestedBytes: c.Request.TargetVMMemoryBytes, ActualBytes: c.Request.TargetVMMemoryBytes}
				require.NoError(t, f.runner.Tick(ctx))
				r := readFixtureRecord(t, f.runner.engine)
				require.Equal(t, completed, r.Last.Kind)
				require.Nil(t, r.Pending)
				require.Equal(t, 1, native.writes, "已发送原目标只读结清，不能重发")
			case "closed-old-replay", "close-after-prepare":
				require.ErrorIs(t, f.runner.Tick(ctx), ErrNotSent)
				require.Equal(t, notSent, readFixtureRecord(t, f.runner.engine).Last.Kind)
			default:
				require.ErrorContains(t, f.runner.Tick(ctx), "service closure")
				require.Nil(t, readFixtureRecord(t, f.runner.engine).Pending)
			}
			if mode != "may-have-sent" && mode != "selected-may-have-sent" {
				require.Zero(t, native.writes)
			}
			s, err = f.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, originalActive, s.State().Active)
			require.Equal(t, originalClosure, s.State().ServiceClosure, "Runner对账只发布自身事实，不擅自结清或重开外层关闭")
		})
	}
}
