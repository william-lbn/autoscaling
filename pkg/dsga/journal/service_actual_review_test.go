package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func reviewCapacityDigest(t *testing.T, p *ServiceActualRecovery) {
	t.Helper()
	raw, err := json.Marshal(p.Capacity)
	require.NoError(t, err)
	p.CapacitySHA256 = rawDigest(raw)
}

func reviewOperationDigest(t *testing.T, p *ServiceActualRecovery, op Operation) {
	t.Helper()
	var err error
	p.OperationSHA256, err = operationClosureDigest(&op)
	require.NoError(t, err)
	if p.VM != nil {
		p.VM.OperationSHA256 = p.OperationSHA256
	}
	if p.Guest != nil {
		p.Guest.OperationSHA256 = p.OperationSHA256
	}
	reviewCapacityDigest(t, p)
}

// 改变显式领域夹具后重建合法Root引用。该投影仅用于构造能通过真实
// Root.Validate的反例，避免测试只撞旧摘要而未触及被审查的新交叉守卫。
func reviewRootReference(t *testing.T, root *budget.RootAggregate) {
	t.Helper()
	l := root.Bootstrap.Limits
	if c := root.Held; c != nil {
		l = resourceguard.Limits{MemoryMaxBytes: c.MemoryMaxBytes, PidsMaxTasks: c.PidsMaxTasks, AddressSpaceMaxBytes: c.AddressSpaceMaxBytes, SystemMemoryMaxBytes: c.SystemMemoryMaxBytes, OutsideUpperBytes: c.OutsideUpperBytes, HugePools: c.HugePools}
	}
	total, err := l.TotalUpperBytes()
	require.NoError(t, err)
	reference := struct {
		ProtocolVersion uint32                         `json:"protocol_version"`
		Scope           string                         `json:"scope"`
		Identity        resourceguard.Identity         `json:"identity"`
		Bootstrap       resourceguard.BootstrapCommand `json:"bootstrap"`
		Birth           resourceguard.ProcessIdentity  `json:"birth"`
		Held            *resourceguard.Command         `json:"held,omitempty"`
	}{ProtocolVersion: root.ProtocolVersion, Scope: root.Scope, Identity: root.Identity, Bootstrap: root.Bootstrap, Birth: root.Birth, Held: root.Held}
	raw, err := json.Marshal(reference)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	root.Bound = budget.Bound{Kind: budget.HardLimit, UpperBytes: control.Uint64(total - uint64(l.OutsideUpperBytes)), SourceID: "root-pg-aggregate-" + hex.EncodeToString(sum[:])}
	require.NoError(t, root.Validate())
}

// 正向使用真实Model推进到类别17：原VM完成、Guest应用、原PG Unknown，
// 接管后独立关闭与隔离。领域输入仍是夹具，不冒充真实PG/Root执行或Lease。
func reviewPGActualModel(t *testing.T) Model {
	t.Helper()
	m, capacity, current, root := actualRecoveryDomain(t, "completed")
	closed := m.ServiceClosure.Request
	native := *m.ServiceClosure.VMRecovery.Capacity
	// 这里只重建关闭之前的领域夹具；生产不存在删除关闭或改原操作入口。
	m.ServiceClosure, m.ProtocolVersion = nil, 9
	require.NoError(t, m.Validate())
	for _, step := range m.Active.Steps {
		if step.Action == ResizeVM {
			var err error
			m, err = m.Record(m.Owner, m.Active.Request.Key, observation(m, step, Completed))
			require.NoError(t, err)
		}
	}
	prepared, _, err := m.OriginalServicePrepared()
	require.NoError(t, err)
	l := prepared.Limits
	command := resourceguard.Command{ProtocolVersion: 1, Identity: root.Identity, IssuerEpoch: m.Owner.Epoch, Revision: 1, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID, PostmasterPID: root.Birth.PostmasterPID, PostmasterStartTicks: root.Birth.PostmasterStartTicks, MemoryMaxBytes: l.MemoryMaxBytes, PidsMaxTasks: l.PidsMaxTasks, AddressSpaceMaxBytes: l.AddressSpaceMaxBytes, SystemMemoryMaxBytes: l.SystemMemoryMaxBytes, OutsideUpperBytes: l.OutsideUpperBytes, HugePools: append([]resourceguard.HugePool(nil), l.HugePools...)}
	require.NoError(t, command.Validate())
	root.Held = &command
	reviewRootReference(t, &root)
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	m, err = m.Start(m.Owner, m.Active.Request.Key, GuestReady, command.Identity.ComputeInstanceID, raw)
	require.NoError(t, err)
	for _, step := range m.Active.Steps {
		if step.Action == GuestReady {
			obs := observation(m, step, Completed)
			obs.Data, err = json.Marshal(struct {
				PGAggregate budget.RootAggregate `json:"pg_aggregate"`
			}{PGAggregate: root})
			require.NoError(t, err)
			m, err = m.Record(m.Owner, m.Active.Request.Key, obs)
			require.NoError(t, err)
		}
	}
	raw, err = json.Marshal(m.Active.Request)
	require.NoError(t, err)
	m, err = m.Start(m.Owner, m.Active.Request.Key, ResizePG, m.Active.Request.Key.Instance.ComputeInstanceID, raw)
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "actual-review-pg-successor")
	require.NoError(t, err)
	closed.IssuerEpoch = m.Owner.Epoch
	current.Observed.IssuerFloorEpoch = m.Owner.Epoch
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	hash, err := closed.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: current.Observed}, false)
	require.NoError(t, err)
	_, _, _, selector := m.ServiceClosure.Selection()
	fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: *m.PGInstance, RequestedEpoch: m.Owner.Epoch, Disposition: control.Fenced, Observed: current.Observed, VMShrinkPermit: false}
	status := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: m.Active.Request.Key, Disposition: control.ReceiptNotFound, HasResult: false, Result: nil}, Observed: current.Observed}
	m, err = m.RecordServicePGRecovery(m.Owner, selector, fence, status)
	require.NoError(t, err)
	m, err = m.StartServiceVMRecovery(m.Owner, selector, false)
	require.NoError(t, err)
	m, err = m.RecordServiceVMRecovery(m.Owner, selector, native, nil)
	require.NoError(t, err)
	capacity.IssuerEpoch, capacity.UnchangedPG, capacity.RecoveryPG = m.Owner.Epoch, nil, &budget.RecoveryPG{Fence: fence, Status: status}
	capacity.PGAggregate = root.Bound
	capacity.RootSHA256, err = budget.RootDigest(root)
	require.NoError(t, err)
	require.NoError(t, capacity.Validate())
	m, err = m.RecordServiceActualRecovery(m.Owner, selector, capacity, current, root)
	require.NoError(t, err)
	require.EqualValues(t, 17, m.ClosureFormatVersion())
	require.NoError(t, m.Validate())
	require.NoError(t, m.ServiceClosure.ActualRecovery.validate(*m.Active))
	return m
}

func TestActualServiceReviewRejectsClosedGenerationRegression(t *testing.T) {
	m := reviewPGActualModel(t)
	p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
	p.Closed.ExpectedCapacityGeneration++
	require.NoError(t, p.Capacity.Validate())
	require.ErrorContains(t, p.validate(*m.Active), "regressed confirmed closed generation")
}

func TestActualServiceReviewKeepsInitialOutsideInOriginalDomain(t *testing.T) {
	for _, mode := range []string{"incarnation", "guest-domain"} {
		t.Run(mode, func(t *testing.T) {
			m := selectedActualRecovery(t, "planned")
			p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
			i := p.Capacity.InitialOutside
			if mode == "incarnation" {
				i.Incarnation = "11111111111111111111111111111111"
			} else {
				i.PGAggregate.Identity.GuestBootID = "87654321-4321-4321-4321-cba987654321"
				i.PGAggregate.Bootstrap.Identity, i.PGAggregate.Birth.Identity = i.PGAggregate.Identity, i.PGAggregate.Identity
				reviewRootReference(t, &i.PGAggregate)
			}
			reviewCapacityDigest(t, p)
			require.NoError(t, i.Validate(), "替换的初始来源本身仍是完整合法领域输入")
			require.NoError(t, p.Capacity.Validate(), "反例不能仅由预算schema或旧摘要挡住")
			require.ErrorContains(t, p.validate(*m.Active), "another original boot or resource domain")
		})
	}
}

func TestActualServiceReviewCannotReplaceRetainedPGProof(t *testing.T) {
	m := reviewPGActualModel(t)
	changed := m.Clone()
	p := changed.ServiceClosure.ActualRecovery
	p.Capacity.RecoveryPG.Status.Observed.AdmissionRevision++
	reviewCapacityDigest(t, p)
	require.NoError(t, p.validate(*changed.Active), "独立预算自身仍有效，须由当前WAL固定历史原证明")
	require.ErrorContains(t, changed.Validate(), "replaced retained original PG proof")
}

func TestActualServiceReviewCompletedGuestBindsOriginalCommandAndPreparedLimits(t *testing.T) {
	for _, mode := range []string{"different-command", "prepared-limit"} {
		t.Run(mode, func(t *testing.T) {
			m := reviewPGActualModel(t)
			op := cloneOperation(*m.Active)
			p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
			p.Root.Held.MemoryMaxBytes++
			reviewRootReference(t, &p.Root)
			for i := range op.Steps {
				step := &op.Steps[i]
				if step.Action != GuestReady {
					continue
				}
				var err error
				step.Observation.Data, err = json.Marshal(struct {
					PGAggregate budget.RootAggregate `json:"pg_aggregate"`
				}{PGAggregate: p.Root})
				require.NoError(t, err)
				if mode == "prepared-limit" {
					step.Command, err = json.Marshal(p.Root.Held)
					require.NoError(t, err)
					step.CommandSHA256, err = commandDigest(op.RequestSHA256, *step)
					require.NoError(t, err)
					step.Observation.CommandSHA256 = step.CommandSHA256
				}
			}
			var err error
			p.Capacity.PGAggregate = p.Root.Bound
			p.Capacity.RootSHA256, err = budget.RootDigest(p.Root)
			require.NoError(t, err)
			reviewOperationDigest(t, p, op)
			require.NoError(t, op.Validate())
			require.NoError(t, p.Capacity.Validate())
			require.ErrorContains(t, p.validate(op), "complete original applied Guest constraints")
		})
	}
}

func reviewOriginalPGTerminal(t *testing.T, p *ServiceActualRecovery, op *Operation, committed bool) control.Snapshot {
	t.Helper()
	prepared, _, err := originalServicePrepared(op, &p.Root.Identity)
	require.NoError(t, err)
	r := op.Request
	result := cloneActualSnapshot(prepared.Shared.Observed)
	hash, err := r.ContextSHA256()
	require.NoError(t, err)
	result.Operation = &control.OperationFact{Key: r.Key, Kind: r.Kind, Phase: control.Denied, TargetBuffers: r.TargetBuffers, ExpectedCapacityGeneration: r.ExpectedCapacityGeneration, ContextSHA256: hash, Reason: control.ReasonTimeout, CancelRequested: false, CancelClosed: false, Replayed: false}
	outcome := Rejected
	if committed {
		result.CapacityGeneration++
		result.LowBuffers, result.HighBuffers = r.TargetBuffers, r.TargetBuffers
		result.Operation.Phase, result.Operation.Reason, result.Operation.CancelClosed = control.Committed, control.ReasonNone, true
		outcome = Completed
	}
	require.NoError(t, result.Validate())
	response := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: r.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &result}, Observed: cloneActualSnapshot(result)}
	audit := struct {
		ProtocolVersion uint32                  `json:"protocol_version"`
		Fence           control.FenceResponse   `json:"issuer_fence"`
		Response        control.ControlResponse `json:"response"`
	}{ProtocolVersion: 1, Fence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: r.Key.Instance, RequestedEpoch: r.Key.IssuerEpoch, Disposition: control.Fenced, Observed: cloneActualSnapshot(prepared.Shared.Observed), VMShrinkPermit: false}, Response: response}
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action == ResizePG {
			obs := observation(p.basis(*op), *step, outcome)
			obs.Data, err = json.Marshal(audit)
			require.NoError(t, err)
			step.Phase, step.Observation = Settled, &obs
		}
	}
	require.NoError(t, op.Validate())
	_, retained, err := op.OriginalServicePG()
	require.NoError(t, err)
	require.NotNil(t, retained.Receipt.Result)
	reviewOperationDigest(t, p, *op)
	return result
}

func TestActualServiceReviewCannotContradictCompletedPGOrReplaceItsTerminal(t *testing.T) {
	t.Run("committed-capacity", func(t *testing.T) {
		m := reviewPGActualModel(t)
		op := cloneOperation(*m.Active)
		p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
		_ = reviewOriginalPGTerminal(t, p, &op, true)
		require.NoError(t, p.Capacity.Validate(), "实际预算自身为合法原代次隔离，原生Commit审计不能被它覆盖")
		require.ErrorContains(t, p.validate(op), "contradicts immutable original PG terminal")
	})
	t.Run("immutable-denied-result", func(t *testing.T) {
		m := reviewPGActualModel(t)
		op := cloneOperation(*m.Active)
		p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
		retained := reviewOriginalPGTerminal(t, p, &op, false)
		p.Capacity.RecoveryPG.Status.Receipt.Disposition, p.Capacity.RecoveryPG.Status.Receipt.HasResult, p.Capacity.RecoveryPG.Status.Receipt.Result = control.ReceiptReplayed, true, &retained
		reviewCapacityDigest(t, p)
		require.NoError(t, p.validate(op), "同一原Denied完整终态保留正向")
		p.Capacity.RecoveryPG.Status.Receipt.Result.Operation.Reason = control.ReasonInterrupted
		reviewCapacityDigest(t, p)
		require.NoError(t, p.Capacity.Validate(), "替代终态本身结构合法且摘要已重算")
		require.ErrorContains(t, p.validate(op), "replaced immutable original native PG result")
	})
}

func TestActualServiceReviewSealedVMKeepsOriginalAuditSources(t *testing.T) {
	m := selectedActualRecovery(t, "cancelled")
	op := cloneOperation(*m.Active)
	p := cloneServiceActualRecovery(m.ServiceClosure.ActualRecovery)
	op.AbortRequested, p.VM = true, nil
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action != ResizeVM {
			continue
		}
		obs := observation(p.basis(op), *step, NoEffect)
		var err error
		obs.Data, err = json.Marshal(struct {
			ProtocolVersion uint32               `json:"protocol_version"`
			Sources         budget.RunnerSources `json:"sources"`
		}{ProtocolVersion: 1, Sources: p.Capacity.VM.Sources})
		require.NoError(t, err)
		step.Phase, step.Observation = Settled, &obs
	}
	reviewOperationDigest(t, p, op)
	require.NoError(t, p.validate(op), "原NoEffect的来源投影与当前不可重发来源相同")
	for i := range op.Steps {
		if op.Steps[i].Action == ResizeVM {
			op.Steps[i].Observation.Data = []byte(`{"protocol_version":1}`)
		}
	}
	reviewOperationDigest(t, p, op)
	require.NoError(t, op.Validate())
	require.ErrorContains(t, p.validate(op), "original VM cancellation source audit")
}
