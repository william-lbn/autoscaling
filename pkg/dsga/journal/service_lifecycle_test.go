package journal

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func completedReopenModel(t *testing.T, mode string) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	m, before := reopenModel(t, mode)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	current := cloneLeaseResponse(before)
	current.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	return m, current
}

func TestServiceLifecycleCompletionPreservesUnknownAndAllowsNextOperation(t *testing.T) {
	for _, mode := range []string{"idle", "initial-unknown", "continuation", "rearm"} {
		t.Run(mode, func(t *testing.T) {
			m, current := completedReopenModel(t, mode)
			original := m.Clone()
			done, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			require.NoError(t, err)
			require.Equal(t, original, m)
			require.EqualValues(t, 21, done.ProtocolVersion)
			require.Nil(t, done.ServiceClosure)
			frozen, err := recoveryContext(done.ServiceLifecycle.Context)
			require.NoError(t, err)
			require.Equal(t, original, frozen, "完整原关闭/预算/首次未知必须冻结")
			if mode == "initial-unknown" {
				require.Equal(t, Unconfirmed, done.InitialHeld.Admission.Phase)
				require.Nil(t, done.InitialHeld.Admission.Receipt)
				require.NotNil(t, done.InitialHeld.Covered)
			}
			_, wire, err := encode(done, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, done, loaded)
			request := testRequest(done, 1, control.Grow)
			request.Key.Instance, request.Key.IssuerEpoch = *done.PGInstance, done.Owner.Epoch
			request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
			started, disposition, err := done.Begin(done.Owner, request, 1)
			require.NoError(t, err)
			require.Equal(t, Accepted, disposition)
			require.Equal(t, done.ServiceLifecycle, started.ServiceLifecycle)
			started, err = started.RequestAbort(started.Owner, request.Key)
			require.NoError(t, err)
			finished, err := started.Finish(started.Owner, request.Key)
			require.NoError(t, err)
			require.Nil(t, finished.Active)
			require.Equal(t, done.ServiceLifecycle, finished.ServiceLifecycle)
			cloned := done.Clone()
			cloned.ServiceLifecycle.Context[0] ^= 1
			cloned.ServiceLifecycle.Current.Observed.Arrays[0].CommittedUpperBytes++
			require.NotEqual(t, done, cloned)
		})
	}
}

func TestServiceLifecycleRejectsProjectedOrStaleCompletion(t *testing.T) {
	m, current := completedReopenModel(t, "initial-unknown")
	selector := m.ServiceClosure.Reopen.CommandSHA256
	for name, change := range map[string]func(*control.AdmissionLeaseResponse){
		"expired":            func(b *control.AdmissionLeaseResponse) { b.Expired = true },
		"closed":             func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionOpen = false },
		"newer-open":         func(b *control.AdmissionLeaseResponse) { b.Observed.AdmissionRevision++ },
		"capacity":           func(b *control.AdmissionLeaseResponse) { b.Observed.CapacityGeneration++ },
		"deadline-regressed": func(b *control.AdmissionLeaseResponse) { b.DeadlineNS-- },
		"old-floor":          func(b *control.AdmissionLeaseResponse) { b.Observed.IssuerFloorEpoch-- },
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneLeaseResponse(current)
			change(&bad)
			_, err := m.CompleteServiceReopen(m.Owner, selector, bad)
			require.Error(t, err)
		})
	}
	done, err := m.CompleteServiceReopen(m.Owner, selector, current)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"lost-evidence":  func(m *Model) { m.ServiceLifecycle = nil },
		"downgrade":      func(m *Model) { m.ProtocolVersion = 9 },
		"context-erased": func(m *Model) { m.ServiceLifecycle.Context = nil },
		"counter":        func(m *Model) { m.ServiceLifecycle.Cycle++ },
		"stage-erased":   func(m *Model) { m.ServiceLifecycle.SafetyStage = "" },
		"current-newer":  func(m *Model) { m.ServiceLifecycle.Current.Observed.AdmissionRevision++ },
		"coverage-old":   func(m *Model) { m.InitialHeld.Covered.Request.Revision = 1 },
		"coverage-lost":  func(m *Model) { m.InitialHeld.Covered = nil },
		"false-original": func(m *Model) { m.InitialHeld.Admission.Phase = Settled },
	} {
		t.Run(name, func(t *testing.T) {
			bad := done.Clone()
			change(&bad)
			require.Error(t, bad.Validate())
		})
	}
	_, err = m.CompleteServiceReopen(Owner{HolderID: "old", Epoch: m.Owner.Epoch}, selector, current)
	require.Error(t, err)
	m.Sequence = control.Uint64(math.MaxUint64)
	_, err = m.CompleteServiceReopen(m.Owner, selector, current)
	require.Error(t, err)
}

func TestServiceLifecycleStoreCompletionIsAtomicAndUnknownReplyReadOnly(t *testing.T) {
	for _, mode := range []string{"normal", "lost-record", "stale-rv", "recreated-vm"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			_ = initializeStore(t, api, store)
			m, current := completedReopenModel(t, "initial-unknown")
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
			s, err := store.Load(ctx)
			require.NoError(t, err)
			if mode == "stale-rv" {
				api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"other": "change"} })
			}
			api.mu.Lock()
			api.failAfterCommit = mode == "lost-record"
			if mode == "recreated-vm" {
				api.beforePatch = func() { api.mu.Lock(); api.vm.UID = "other-vm"; api.mu.Unlock() }
			}
			api.mu.Unlock()
			completed, err := store.CompleteServiceReopen(ctx, s, m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
			if mode == "stale-rv" || mode == "recreated-vm" {
				require.Error(t, err)
				require.Nil(t, completed)
				return
			}
			if mode == "lost-record" {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				require.Nil(t, completed)
			} else {
				require.NoError(t, err)
			}
			loaded, err := store.Load(ctx)
			require.NoError(t, err)
			require.Nil(t, loaded.State().ServiceClosure)
			require.NotNil(t, loaded.State().ServiceLifecycle)
			require.Equal(t, m.InitialHeld.Admission, loaded.State().InitialHeld.Admission)
			require.True(t, loaded.State().InitialAdmissionReady())
		})
	}
}

// 连续24轮超过结果缓存寿命，归档不能递归膨胀；最初未知始终完整
// 留在当前Initial，直到它自己的正常预算替换协议完成，而不是随轮数淘汰。
func TestServiceLifecycleRepeatedCyclesRetireOnlySupersededAdmission(t *testing.T) {
	m, current := completedReopenModel(t, "initial-unknown")
	original := cloneInitial(m.InitialHeld)
	var maxBytes int
	for cycle := 1; cycle <= 24; cycle++ {
		var err error
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
		require.NoError(t, err)
		require.EqualValues(t, cycle, m.ServiceLifecycle.Cycle)
		require.Equal(t, original.Admission, m.InitialHeld.Admission)
		context, err := recoveryContext(m.ServiceLifecycle.Context)
		require.NoError(t, err)
		require.Nil(t, context.ServiceLifecycle)
		raw, err := json.Marshal(m)
		require.NoError(t, err)
		if cycle == 2 {
			maxBytes = len(raw) + 2048
		}
		if cycle > 2 {
			require.Less(t, len(raw), maxBytes)
		}
		_, _, err = encode(m, MaxEncodedBytes)
		require.NoError(t, err)
		closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: current.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: context.ServiceClosure.Request.BudgetCommitmentID}
		m, err = m.StartServiceClosure(m.Owner, closed)
		require.NoError(t, err)
		projected := m.Clone()
		projected.ServiceClosure = nil
		require.Error(t, projected.Validate(), "删除下一轮关闭不能投影为上一轮已开放")
		require.EqualValues(t, 21, m.ProtocolVersion)
		require.EqualValues(t, 10, m.ClosureFormatVersion())
		digest, err := closed.ContextSHA256()
		require.NoError(t, err)
		fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, Disposition: control.AdmissionApplied, ContextSHA256: digest, Observed: current.Observed}
		fact.Observed.AdmissionOpen, fact.Observed.AdmissionRevision = false, closed.Revision
		m, err = m.RecordServiceClosure(m.Owner, fact, false)
		require.NoError(t, err)
		before := cloneLeaseResponse(current)
		before.Observed = fact.Observed
		m, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, before)
		require.NoError(t, err)
		m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
		require.NoError(t, err)
		current.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	}
}

func TestServiceLifecycleNextActiveClosureKeepsIndependentFormatAndUnknown(t *testing.T) {
	m, current := completedReopenModel(t, "initial-unknown")
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	request := testRequest(m, 1, control.Grow)
	request.Key.Instance, request.Key.IssuerEpoch = *m.PGInstance, m.Owner.Epoch
	request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
	m = beginModel(t, m, request)
	// 保留原Prepare未知，新的独立安全关闭不能将它改成NoEffect或结案。
	m, err = m.Start(m.Owner, request.Key, PrepareBudget, request.Key.Instance.ComputeInstanceID, testCommand)
	require.NoError(t, err)
	original := m.Clone()
	closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: current.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: request.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	require.EqualValues(t, 21, m.ProtocolVersion)
	require.EqualValues(t, 16, m.ClosureFormatVersion())
	require.Equal(t, original.Active, m.Active)
	require.Equal(t, original.InitialHeld, m.InitialHeld)
	_, err = m.RequestAbort(m.Owner, request.Key)
	require.Error(t, err)
	_, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, current)
	require.Error(t, err)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestServiceLifecyclePreservesRetiredRequestIdentityAndExpirationWatermark(t *testing.T) {
	m, current := completedReopenModel(t, "initial-unknown")
	m, err := m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	request := testRequest(m, 1, control.Grow)
	request.Key.Instance, request.Key.IssuerEpoch = *m.PGInstance, m.Owner.Epoch
	m = beginModel(t, m, request)
	m, err = m.RequestAbort(m.Owner, request.Key)
	require.NoError(t, err)
	m, err = m.Finish(m.Owner, request.Key)
	require.NoError(t, err)
	// 通过现有正式淘汰器移除完整零派发终态，保留准确原HWM和过期计数。
	require.NoError(t, m.expireOldest())
	require.NoError(t, m.Validate())
	closed := current.Observed
	closed.AdmissionOpen, closed.AdmissionRevision = false, closed.AdmissionRevision+1
	q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: closed.AdmissionRevision, ExpectedCapacityGeneration: closed.CapacityGeneration, Open: false, BudgetCommitmentID: m.InitialHeld.Covered.Request.BudgetCommitmentID}
	m, err = m.StartServiceClosure(m.Owner, q)
	require.NoError(t, err)
	hash, err := q.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, Disposition: control.AdmissionApplied, ContextSHA256: hash, Observed: closed}
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	current.Observed = closed
	m, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, current)
	require.NoError(t, err)
	m, err = m.RecordServiceReopen(m.Owner, reopenFact(t, m), false)
	require.NoError(t, err)
	current.Observed = m.ServiceClosure.Reopen.Receipt.Observed
	m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, current)
	require.NoError(t, err)
	bad := m.Clone()
	bad.HighWater.OperationID.Low++
	require.Error(t, bad.Validate(), "历史已淘汰也不能换同revision的原请求身份")
	bad = m.Clone()
	bad.ExpiredResults--
	require.Error(t, bad.Validate(), "生命周期完成不能重置结果过期水位")
}

// 原始JSON界限独立于压缩比。八条完整取消的已结清结果仍在缓存中，
// 每条保存真实上限内的完整领域body；原注解压缩可存，但新开放必须在
// 写前就拒绝无法另存完整恢复上下文的选择，不能发送后才发现落账无空间。
func TestServiceLifecycleReopenReservesRawCompletionContext(t *testing.T) {
	m, _, _ := serviceClosureModel(t)
	for revision := uint64(1); revision <= 8; revision++ {
		request := testRequest(m, revision, control.Grow)
		m = beginModel(t, m, request)
		var err error
		m, err = m.Start(m.Owner, request.Key, PrepareBudget, request.Key.Instance.ComputeInstanceID, testCommand)
		require.NoError(t, err)
		fact := observation(m, m.Active.Steps[0], Completed)
		fact.Data = []byte(`{"complete_domain_body":"` + strings.Repeat("x", 24*1024) + `"}`)
		m, err = m.Record(m.Owner, request.Key, fact)
		require.NoError(t, err)
		m, err = m.RequestAbort(m.Owner, request.Key)
		require.NoError(t, err)
		m, err = m.StartCleanup(m.Owner, request.Key, request.Key.Instance.ComputeInstanceID, testCommand)
		require.NoError(t, err)
		fact = observation(m, *m.Active.Cleanup, Completed)
		fact.Data = []byte(`{"complete_cleanup_body":"` + strings.Repeat("x", 24*1024) + `"}`)
		m, err = m.Record(m.Owner, request.Key, fact)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, request.Key)
		require.NoError(t, err)
	}
	_, closing, fact := serviceClosureModel(t)
	var err error
	m, err = m.StartServiceClosure(m.Owner, closing)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	_, _, err = encode(m, MaxEncodedBytes)
	require.NoError(t, err, "原账本仍在允许的注解/原始字节界限内")
	_, before := reopenModel(t, "idle")
	before.Request.IssuerEpoch, before.Observed.IssuerFloorEpoch, before.LeaseEpoch = m.Owner.Epoch, m.Owner.Epoch, m.Owner.Epoch
	original := m.Clone()
	_, err = m.StartServiceReopen(m.Owner, m.ServiceClosure.CommandSHA256, before)
	require.ErrorContains(t, err, "complete bounded lifecycle evidence")
	require.Equal(t, original, m)
}
