package journal

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 原预算与原生回执在本文件是显式合同夹具；测试真实Model/codec的不变量，
// 不把它称为PG/Root/Host来源认证。生产实际消费由owner组合单独覆盖。
func cleanupClosureModel(t *testing.T, kind string, opening, settled bool) (Model, control.AdmissionRequest, control.AdmissionResponse) {
	t.Helper()
	var m Model
	var snapshot control.Snapshot
	var receipt any
	switch kind {
	case "committed":
		base, f, aggregate := committedAbortModel(t)
		m = startCommittedAbort(t, base, f, !opening)
		r := committedAbortAudit(t, m, f, aggregate, !opening)
		receipt, snapshot = r, r.Shared.Observed
	case "actual":
		base, c, r := partialCapacityModel(t)
		m, snapshot = base, r.Shared.Observed
		if opening {
			c.Admission = &control.AdmissionRequest{ProtocolVersion: 1, Instance: c.Capacity.Request.Key.Instance, IssuerEpoch: m.Owner.Epoch, Revision: snapshot.AdmissionRevision + 1, ExpectedCapacityGeneration: c.Capacity.CapacityGeneration, Open: true, BudgetCommitmentID: c.Capacity.Request.BudgetCommitmentID}
			r.Command, r.Query = c, false
			r.Shared.Observed.AdmissionRevision = c.Admission.Revision
			r.Response = cleanupOpeningProof(t, *c.Admission, r.Shared.Observed)
		}
		raw, err := json.Marshal(c)
		require.NoError(t, err)
		m, err = m.StartCleanup(m.Owner, m.Active.Request.Key, m.Active.Request.Key.Instance.ComputeInstanceID, raw)
		require.NoError(t, err)
		receipt = r
	case "unchanged":
		base, f, r := unchangedAbortModel(t)
		m, snapshot = base, r.Shared.Observed
		if opening {
			c := r.Command
			c.Admission = &control.AdmissionRequest{ProtocolVersion: 1, Instance: c.Request.Key.Instance, IssuerEpoch: m.Owner.Epoch, Revision: snapshot.AdmissionRevision + 1, ExpectedCapacityGeneration: snapshot.CapacityGeneration, Open: true, BudgetCommitmentID: f.Request.BudgetCommitmentID}
			// 预算ID来自旧完整预算，不能按新Active目标或当前PG字段重算。
			var err error
			r.Command, r.Query = c, false
			r.Shared.Observed.AdmissionRevision = c.Admission.Revision
			r.Response = cleanupOpeningProof(t, *c.Admission, r.Shared.Observed)
			// 仅测试构造尚未发送的原合同，重算真实摘要；生产没有改写WAL入口。
			m.Active.Cleanup.Command, err = json.Marshal(c)
			require.NoError(t, err)
			m.Active.Cleanup.CommandSHA256, err = commandDigest(m.Active.RequestSHA256, *m.Active.Cleanup)
			require.NoError(t, err)
		}
		receipt = r
	default:
		t.Fatalf("unknown cleanup fixture %s", kind)
	}
	if settled {
		obs := observation(m, *m.Active.Cleanup, Completed)
		var err error
		obs.Data, err = json.Marshal(receipt)
		require.NoError(t, err)
		m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
		require.NoError(t, err)
	}
	b, err := m.CleanupServiceForClosure()
	require.NoError(t, err)
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: 100, ExpectedCapacityGeneration: b.CapacityGeneration, Open: false, BudgetCommitmentID: b.BudgetCommitmentID}
	if b.Admission != nil {
		request.Revision = b.Admission.Revision + 1
	}
	snapshot.IssuerFloorEpoch, snapshot.CapacityGeneration = request.IssuerEpoch, request.ExpectedCapacityGeneration
	snapshot.AdmissionRevision, snapshot.AdmissionOpen = request.Revision, false
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	fact := control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: snapshot, VMShrinkPermit: false}
	require.NoError(t, fact.ValidateFor(request, *m.PGInstance, false))
	return m, request, fact
}

func cleanupOpeningProof(t *testing.T, request control.AdmissionRequest, snapshot control.Snapshot) *control.AdmissionResponse {
	t.Helper()
	hash, err := request.ContextSHA256()
	require.NoError(t, err)
	return &control.AdmissionResponse{ProtocolVersion: 1, Request: request, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: snapshot, VMShrinkPermit: false}
}

func TestCleanupServiceClosureRetainsThreeOriginalBudgetBoundaries(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		for _, opening := range []bool{false, true} {
			for _, settled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/opening-%t/settled-%t", kind, opening, settled), func(t *testing.T) {
					m, request, fact := cleanupClosureModel(t, kind, opening, settled)
					before := m.Clone()
					m, err := m.StartCleanupServiceClosure(m.Owner, request)
					require.NoError(t, err)
					require.EqualValues(t, 18, m.ProtocolVersion)
					if m.ServiceClosure.Original == nil {
						m, err = m.RecordServiceClosure(m.Owner, fact, false)
					} else {
						proof := control.AdmissionClosure{ProtocolVersion: 1, Original: *m.ServiceClosure.Original, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
						m, err = m.recordOriginalServiceClosure(m.Owner, proof, false, "cleanup_source_unconfirmed")
					}
					require.NoError(t, err)
					require.Equal(t, before.Active, m.Active)
					require.Equal(t, before.BudgetHeld, m.BudgetHeld)
					require.Equal(t, before.CapacityHeld, m.CapacityHeld)
					require.Equal(t, before.History, m.History)
					_, wire, err := encode(m, MaxEncodedBytes)
					require.NoError(t, err)
					loaded, err := decode(wire, m.VMUID, m.Incarnation)
					require.NoError(t, err)
					require.Equal(t, m, loaded)
					old := m.Clone()
					old.ProtocolVersion = 17
					require.Error(t, old.Validate(), "旧模型不能忽略独立Cleanup关闭")
					_, err = m.Finish(m.Owner, m.Active.Request.Key)
					require.Error(t, err, "关闭不能当作Cleanup/Host结清")
					bad := m.Clone()
					bad.Active.Cleanup.Command[0] = '['
					require.Error(t, bad.Validate())
				})
			}
		}
	}
}

func TestCleanupServiceClosureRejectsChangedCapacityBudgetAndOriginal(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		m, request, _ := cleanupClosureModel(t, kind, true, false)
		for _, mode := range []string{"generation", "target-budget", "open", "instance", "revision"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				bad := request
				switch mode {
				case "generation":
					bad.ExpectedCapacityGeneration++
				case "target-budget":
					bad.BudgetCommitmentID += "-substituted"
				case "open":
					bad.Open = true
				case "instance":
					bad.Instance.PGBootID++
				case "revision":
					bad.Revision--
				}
				before := m.Clone()
				_, err := m.StartCleanupServiceClosure(m.Owner, bad)
				require.Error(t, err)
				require.Equal(t, before, m)
			})
		}
	}
}

func TestCleanupServiceClosureContinuationKeepsOriginalCapacityAndUnknown(t *testing.T) {
	for _, kind := range []string{"committed", "actual", "unchanged"} {
		t.Run(kind, func(t *testing.T) {
			m, closeRequest, fact := cleanupClosureModel(t, kind, true, false)
			before := m.Clone()
			m, err := m.StartCleanupServiceClosure(m.Owner, closeRequest)
			require.NoError(t, err)
			original := *m.ServiceClosure
			m, err = m.Takeover(m.Owner, "cleanup-successor")
			require.NoError(t, err)
			r := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: m.ServiceClosure.ProtectedAdmission(), PreviousClose: closeRequest, Close: closeRequest}
			r.Close.IssuerEpoch, r.Close.Revision = m.Owner.Epoch, r.Close.Revision+1
			m, err = m.StartServiceClosureContinuation(m.Owner, original.CommandSHA256, r)
			require.NoError(t, err)
			fact.Request, fact.Disposition = r.Close, control.AdmissionReplayed
			fact.ContextSHA256, _ = r.Close.ContextSHA256()
			fact.Observed.IssuerFloorEpoch, fact.Observed.AdmissionRevision = r.Close.IssuerEpoch, r.Close.Revision
			digest, _ := r.ContextSHA256()
			proof := control.AdmissionContinuationResponse{ProtocolVersion: 1, Request: r, ContextSHA256: digest, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
			bad := proof
			bad.ClosedBy.Observed.CapacityGeneration++
			_, err = m.RecordServiceClosureContinuation(m.Owner, bad, true)
			require.Error(t, err, "更晚容量的旧回执不能结清固定Cleanup边界")
			m, err = m.RecordServiceClosureContinuation(m.Owner, proof, true)
			require.NoError(t, err)
			require.Equal(t, before.Active, m.Active)
			require.Equal(t, before.BudgetHeld, m.BudgetHeld)
			require.Equal(t, Unconfirmed, m.ServiceClosure.Phase)
			require.False(t, m.ServiceClosure.Pending())
		})
	}
}

func TestCleanupServiceClosureStoreRequiresConfirmedCAS(t *testing.T) {
	for _, opening := range []bool{false, true} {
		t.Run(fmt.Sprintf("opening-%t", opening), func(t *testing.T) {
			serviceClosureStoreContract(t, func(t *testing.T) (Model, control.AdmissionRequest, control.AdmissionResponse) {
				return cleanupClosureModel(t, "actual", opening, false)
			})
		})
	}
}

// 旧v2首次实际容量取消尚无永久头；只允许保留完整原Cleanup的安全关闭，
// 不能由格式例外制造缺预算的普通操作或绕过原文认证。生产组合另从真实
// VM已扩、PG未扩链走到这一边界，不在业务代码填造旧预算。
func TestCleanupServiceClosureWithoutPermanentHeaderStaysSafetyOnly(t *testing.T) {
	for _, opening := range []bool{false, true} {
		t.Run(fmt.Sprintf("opening-%t", opening), func(t *testing.T) {
			m, request, fact := cleanupClosureModel(t, "actual", opening, false)
			m.ProtocolVersion, m.BudgetHeld, m.BudgetBoundary = 2, nil, nil
			m.History = []Record{}
			m.Active.PriorBudgetBoundary = nil
			require.NoError(t, m.Validate())
			m, err := m.StartCleanupServiceClosure(m.Owner, request)
			require.NoError(t, err)
			if opening {
				proof := control.AdmissionClosure{ProtocolVersion: 1, Original: *m.ServiceClosure.Original, ClosedBy: fact, PriorMayHaveApplied: true, VMShrinkPermit: false}
				m, err = m.recordOriginalServiceClosure(m.Owner, proof, false, "cleanup_source_unconfirmed")
			} else {
				m, err = m.RecordServiceClosure(m.Owner, fact, false)
			}
			require.NoError(t, err)
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			_, err = decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			bad := m.Clone()
			bad.ServiceClosure = nil
			require.Error(t, bad.Validate(), "不能删除关闭后借v18缺头例外维持普通服务")
			bad = m.Clone()
			bad.Active.Cleanup = nil
			require.Error(t, bad.Validate())
			bad = m.Clone()
			bad.ResourceIdentity = nil
			require.Error(t, bad.Validate())
		})
	}
}
