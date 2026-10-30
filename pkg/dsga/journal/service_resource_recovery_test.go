package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 领域输入是显式夹具，仅证明真实Model/Store/codec的持久边界；不称作
// Root内核或VM已完成。原Guest命令由合法Prepare构造，外层始终保留Unknown。
func serviceGuestRecoveryModel(t *testing.T) (Model, resourceguard.Command, resourceguard.View) {
	t.Helper()
	m, _, receipt := partialCapacityModel(t)
	m.Active.AbortRequested = false // 构造取消之前的夹具，生产没有改写原操作入口。
	var prepared budget.Prepared
	for _, step := range m.Active.Steps {
		if step.Action == PrepareBudget {
			require.NoError(t, json.Unmarshal(step.Observation.Data, &prepared))
		}
	}
	root := receipt.PGAggregate
	l := prepared.Limits
	c := resourceguard.Command{ProtocolVersion: 1, Identity: prepared.Identity, IssuerEpoch: m.Owner.Epoch, Revision: 1, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID, PostmasterPID: root.Birth.PostmasterPID, PostmasterStartTicks: root.Birth.PostmasterStartTicks, MemoryMaxBytes: l.MemoryMaxBytes, PidsMaxTasks: l.PidsMaxTasks, AddressSpaceMaxBytes: l.AddressSpaceMaxBytes, SystemMemoryMaxBytes: l.SystemMemoryMaxBytes, OutsideUpperBytes: l.OutsideUpperBytes, HugePools: l.HugePools}
	body, err := json.Marshal(c)
	require.NoError(t, err)
	m, err = m.Start(m.Owner, m.Active.Request.Key, GuestReady, c.Identity.ComputeInstanceID, body)
	require.NoError(t, err)
	q := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: receipt.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: m.Active.Request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: c.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, q)
	require.NoError(t, err)
	pg := receipt.Shared.Observed
	pg.AdmissionOpen, pg.AdmissionRevision = false, q.Revision
	hash, err := q.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: q, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: pg}, false)
	require.NoError(t, err)
	v := resourceguard.View{ProtocolVersion: 1, VMShrinkPermit: false, Identity: c.Identity, IssuerFloorEpoch: m.Owner.Epoch, Bootstrap: root.Bootstrap, BootstrapApplied: true, SystemPlacementObserved: false, Original: nil, Held: nil, Applied: false, Cancelled: nil, Birth: &root.Birth, Actual: resourceguard.Evidence{ProtocolVersion: 1, VMShrinkPermit: false, Identity: c.Identity, MemoryMaxBytes: l.MemoryMaxBytes, MemoryCurrentBytes: 0, SwapMaxBytes: 0, SwapCurrentBytes: 0, GuestMemTotalBytes: prepared.GuestUsableMemoryBytes, HugePools: append([]resourceguard.HugePool(nil), l.HugePools...), WholePoolUpperBytes: 0, HugeOvercommitDisabled: true, HugeSurplusPages: 0, CgroupPopulated: true, TaskLimit: nil, AddressSpace: nil, System: nil}}
	for _, p := range v.Actual.HugePools {
		v.Actual.WholePoolUpperBytes += p.PageSizeKB * 1024 * p.Pages
	}
	require.NoError(t, v.Validate())
	return m, c, v
}

func TestServiceGuestRecoveryKeepsOriginalUnknownAndSeparateEffect(t *testing.T) {
	for _, effect := range []string{"applied", "cancelled"} {
		for _, takeover := range []bool{false, true} {
			t.Run(effect+map[bool]string{false: "-same-owner", true: "-takeover"}[takeover], func(t *testing.T) {
				m, c, view := serviceGuestRecoveryModel(t)
				before := m.Clone()
				_, _, _, selector := m.ServiceClosure.Selection()
				m, err := m.StartServiceGuestRecovery(m.Owner, selector, effect == "cancelled")
				require.NoError(t, err)
				if takeover {
					m, err = m.Takeover(m.Owner, "guest-recovery-successor")
					require.NoError(t, err)
				}
				view.IssuerFloorEpoch = m.Owner.Epoch
				if effect == "applied" {
					view.Original, view.Held, view.Applied = &c, &c, true
				} else {
					view.Cancelled = &c
				}
				m, err = m.RecordServiceGuestRecovery(m.Owner, selector, effect, view, true)
				require.NoError(t, err)
				require.Equal(t, before.Active, m.Active)
				require.Equal(t, before.BudgetHeld, m.BudgetHeld)
				require.Equal(t, before.BudgetBoundary, m.BudgetBoundary)
				require.Equal(t, m.Owner, *m.ServiceClosure.ResourceRecovery.Guest.ObservedBy)
				require.Error(t, m.CheckStableServiceRecovery(), "单Guest事实不能退休其他领域或开放")
				_, err = m.RequestAbort(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				_, err = m.Finish(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				loaded, err := decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
				require.Equal(t, m, loaded)
				view.Actual.HugePools[0].Pages++
				require.NotEqual(t, view, *m.ServiceClosure.ResourceRecovery.Guest.State)
				cloned := m.Clone()
				cloned.ServiceClosure.ResourceRecovery.Guest.State.Bootstrap.Limits.HugePools[0].Pages++
				cloned.ServiceClosure.ResourceRecovery.Guest.ObservedBy.HolderID += "-changed"
				require.NotEqual(t, cloned.ServiceClosure.ResourceRecovery, m.ServiceClosure.ResourceRecovery)
				require.NoError(t, m.Validate())
			})
		}
	}
}

func TestServiceGuestRecoveryRejectsUnprovenEffectAndChangedBoundary(t *testing.T) {
	m, c, view := serviceGuestRecoveryModel(t)
	_, _, _, selector := m.ServiceClosure.Selection()
	_, err := m.StartServiceGuestRecovery(m.Owner, selector+"x", true)
	require.Error(t, err)
	m, err = m.StartServiceGuestRecovery(m.Owner, selector, true)
	require.NoError(t, err)
	view.Cancelled = &c
	m, err = m.RecordServiceGuestRecovery(m.Owner, selector, "cancelled", view, false)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"operation": func(n *Model) { n.ServiceClosure.ResourceRecovery.OperationSHA256 += "x" },
		"original":  func(n *Model) { n.Active.AbortRequested = true },
		"closed":    func(n *Model) { n.ServiceClosure.ResourceRecovery.Closed.Revision++ },
		"before-close": func(n *Model) {
			n.ServiceClosure.ResourceRecovery.StartedSequence = n.ServiceClosure.ResourceRecovery.ClosedSequence
		},
		"future":             func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.RecordedSequence++ },
		"owner":              func(n *Model) { n.ServiceClosure.ResourceRecovery.Origin.HolderID += "x" },
		"observer":           func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.ObservedBy.HolderID += "x" },
		"floor":              func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.State.IssuerFloorEpoch++ },
		"birth":              func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.State.Birth.PostmasterStartTicks++ },
		"unselected-cancel":  func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.CancelUnreceived = false },
		"not-found":          func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.State.Cancelled = nil },
		"actual-drift":       func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.State.Actual.MemoryMaxBytes++ },
		"unpopulated":        func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.State.Actual.CgroupPopulated = false },
		"unknown-with-proof": func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.Phase = Unconfirmed },
		"invent-applied":     func(n *Model) { n.ServiceClosure.ResourceRecovery.Guest.Effect = "applied" },
	} {
		t.Run(name, func(t *testing.T) { next := m.Clone(); change(&next); require.Error(t, next.Validate()) })
	}
	// 取消选择在Root Apply抢先完成时仍可只读确认原Applied；反向不能
	// 把“只查Applied”的选择扩成取消。两者始终不改外层原Guest未知。
	base, c, view := serviceGuestRecoveryModel(t)
	base, err = base.StartServiceGuestRecovery(base.Owner, selector, false)
	require.NoError(t, err)
	view.Cancelled = &c
	_, err = base.RecordServiceGuestRecovery(base.Owner, selector, "cancelled", view, true)
	require.Error(t, err)
	raw, err := json.Marshal(m.ServiceClosure.ResourceRecovery)
	require.NoError(t, err)
	for _, malformed := range [][]byte{append(raw[:len(raw)-1:len(raw)-1], []byte(`,"unknown":true}`)...), append([]byte(`{"origin":null,`), raw[1:]...)} {
		var decoded ServiceResourceRecovery
		require.Error(t, json.Unmarshal(malformed, &decoded))
	}
}

func TestServiceGuestRecoveryPostShrinkKeepsCommittedPGAndUnknownGuest(t *testing.T) {
	m, closing, fact := activeClosureModel(t, control.Shrink, GuestReady, true)
	// 构造合法完整Prepare/Guest输入，原PG与Release前驱仍是明确夹具。
	// 生产没有改写原命令的入口；下面在关闭WAL创建之前重算真实摘要。
	initial, _, _ := initialDomain(t)
	root := initial.PGAggregate
	l := root.Bootstrap.Limits
	var view resourceguard.View
	view.ProtocolVersion, view.Identity, view.IssuerFloorEpoch = 1, root.Identity, m.Owner.Epoch
	view.Bootstrap, view.BootstrapApplied, view.SystemPlacementObserved, view.Birth = root.Bootstrap, true, true, &root.Birth
	view.Actual.ProtocolVersion, view.Actual.Identity, view.Actual.MemoryMaxBytes = 1, root.Identity, l.MemoryMaxBytes
	view.Actual.GuestMemTotalBytes = m.Active.Request.TargetVMMemoryBytes - (256 << 20)
	view.Actual.HugePools = append([]resourceguard.HugePool(nil), l.HugePools...)
	view.Actual.HugeOvercommitDisabled, view.Actual.CgroupPopulated = true, true
	view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: l.PidsMaxTasks, CurrentTasks: 1}
	view.Actual.System = &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: l.SystemMemoryMaxBytes, MemoryCurrentBytes: 0, SwapMaxBytes: 0, SwapCurrentBytes: 0, ProcessCount: 1, PID1Member: true, RootUserspaceCount: 0}
	for _, p := range l.HugePools {
		view.Actual.WholePoolUpperBytes += p.PageSizeKB * 1024 * p.Pages
	}
	prepared := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: m.Active.Request, Identity: *m.ResourceIdentity, RunnerPodName: "runner", RunnerRestartCount: 0, VMBeforeMemoryBytes: m.Active.Request.TargetVMMemoryBytes, GuestUsableMemoryBytes: m.Active.Request.TargetVMMemoryBytes - (256 << 20), Shared: initial.Shared, TargetSharedUpperBytes: initial.Shared.StartupMainUpperBytes, Limits: l, SystemOutsidePG: initial.SystemOutsidePG, Transition: initial.Transition, Safety: initial.Safety, OutsideClaim: nil}
	prepared.Shared.Observed.LowBuffers, prepared.Shared.Observed.HighBuffers = 2048, 2048
	prepared.Shared.Observed.Operation = nil
	prepared.Shared.Observed.CapacityGeneration = m.Active.Request.ExpectedCapacityGeneration
	require.NoError(t, prepared.ValidateFor(m.Active.Request, prepared.Identity))
	c := resourceguard.Command{ProtocolVersion: 1, Identity: prepared.Identity, IssuerEpoch: m.Owner.Epoch, Revision: 1, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID, PostmasterPID: view.Birth.PostmasterPID, PostmasterStartTicks: view.Birth.PostmasterStartTicks, MemoryMaxBytes: l.MemoryMaxBytes, PidsMaxTasks: l.PidsMaxTasks, SystemMemoryMaxBytes: l.SystemMemoryMaxBytes, AddressSpaceMaxBytes: l.AddressSpaceMaxBytes, OutsideUpperBytes: l.OutsideUpperBytes, HugePools: l.HugePools}
	for i := range m.Active.Steps {
		step := &m.Active.Steps[i]
		var err error
		if step.Action == PrepareBudget {
			step.Observation.Data, err = json.Marshal(prepared)
			require.NoError(t, err)
		}
		if step.Action == GuestReady {
			step.Command, err = json.Marshal(c)
			require.NoError(t, err)
			step.CommandSHA256, err = commandDigest(m.Active.RequestSHA256, *step)
			require.NoError(t, err)
		}
	}
	closing.ExpectedCapacityGeneration++
	fact.Request = closing
	fact.ContextSHA256, _ = closing.ContextSHA256()
	fact.Observed.CapacityGeneration++
	m, err := m.StartActiveServiceClosure(m.Owner, closing)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	require.EqualValues(t, 17, m.ClosureFormatVersion())
	original := m.Clone()
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err = m.StartServiceGuestRecovery(m.Owner, selector, true)
	require.NoError(t, err)
	view.Identity = c.Identity
	view.Bootstrap.Identity, view.Birth.Identity, view.Actual.Identity = c.Identity, c.Identity, c.Identity
	view.IssuerFloorEpoch = m.Owner.Epoch
	view.Cancelled = &c
	m, err = m.RecordServiceGuestRecovery(m.Owner, selector, "cancelled", view, true)
	require.NoError(t, err)
	require.Equal(t, original.Active, m.Active)
	require.Error(t, m.CheckStableServiceRecovery())
	_, raw, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(raw, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestStoreServiceGuestRecoveryCASProtectsSelectionAndObservation(t *testing.T) {
	for _, recording := range []bool{false, true} {
		for _, mode := range []string{"normal", "lost-reply", "stale-rv", "recreated-vm"} {
			t.Run(map[bool]string{false: "select/", true: "record/"}[recording]+mode, func(t *testing.T) {
				api, store := newStoreAPI(t)
				_ = initializeStore(t, api, store)
				m, c, view := serviceGuestRecoveryModel(t)
				_, _, _, selector := m.ServiceClosure.Selection()
				if recording {
					var err error
					m, err = m.StartServiceGuestRecovery(m.Owner, selector, true)
					require.NoError(t, err)
				}
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
				s, err := store.Load(context.Background())
				require.NoError(t, err)
				if mode == "stale-rv" {
					api.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"changed": "true"} })
				}
				api.mu.Lock()
				api.failAfterCommit = mode == "lost-reply"
				if mode == "recreated-vm" {
					api.beforePatch = func() { api.mu.Lock(); api.vm.UID = types.UID("replaced-vm"); api.mu.Unlock() }
				}
				api.mu.Unlock()
				var result *Snapshot
				if recording {
					view.Cancelled = &c
					result, err = store.RecordServiceGuestRecovery(context.Background(), s, m.Owner, selector, "cancelled", view, true)
				} else {
					result, err = store.StartServiceGuestRecovery(context.Background(), s, m.Owner, selector, true)
				}
				if mode == "normal" {
					require.NoError(t, err)
					require.NotNil(t, result)
				} else {
					require.Error(t, err)
					require.Nil(t, result, "未确认CAS绝不提供取消票据或完成快照")
				}
				if mode == "lost-reply" {
					var lost *UnconfirmedWrite
					require.ErrorAs(t, err, &lost)
					loaded, err := store.Load(context.Background())
					require.NoError(t, err)
					require.Equal(t, m.Active, loaded.State().Active)
					g := loaded.State().ServiceClosure.ResourceRecovery.Guest
					require.Equal(t, map[bool]Phase{false: Unconfirmed, true: Settled}[recording], g.Phase)
				}
			})
		}
	}
}
