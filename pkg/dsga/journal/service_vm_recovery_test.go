package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/types"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 夹具只证明真实Model/Store/codec的独立持久边界。Runner来源、原生回执和
// 物理值仍由生产Reader认证；不能把本测试中的容量声明称为QMP执行证据。
// 原低层ResizeVM领域夹具在关门前替换为完整既有v1协议；此处不创建发送协议。
func serviceVMRecoveryModel(t *testing.T, settled bool) (Model, budget.NativeCapacity) {
	t.Helper()
	return serviceVMRecoveryModelForClosure(t, settled, false)
}

func serviceVMRecoveryModelForClosure(t *testing.T, settled, successorClose bool) (Model, budget.NativeCapacity) {
	t.Helper()
	m, original, receipt := partialCapacityModel(t)
	m.Active.AbortRequested = false
	var p budget.Prepared
	var hostHash string
	for _, step := range m.Active.Steps {
		if step.Action == PrepareBudget {
			require.NoError(t, json.Unmarshal(step.Observation.Data, &p))
		}
		if step.Action == ReserveHost {
			hostHash = rawDigest(step.Observation.Data)
		}
	}
	native := original.Capacity.VM
	var command serviceVMProjection
	command.ProtocolVersion, command.BindingClaim, command.HostClaim = 1, native.Sources.BirthClaim, native.HostClaim
	command.Request, command.BeforeBytes = m.Active.Request, p.VMBeforeMemoryBytes
	command.PreparedSHA256, command.HostSHA256 = original.PreparedSHA256, hostHash
	command.Identity.VMUID, command.Identity.Incarnation = m.VMUID, m.Incarnation
	command.Identity.RunnerPodUID, command.Identity.RunnerBootID, command.Identity.NodeUID = "runner-uid", testIncarnation, "node-uid"
	command.Identity.MinimumBytes, command.Identity.MaximumBytes, command.Identity.BlockBytes = 512<<20, 4<<30, 128<<20
	raw, err := json.Marshal(command)
	require.NoError(t, err)
	for i := range m.Active.Steps {
		step := &m.Active.Steps[i]
		if step.Action != ResizeVM {
			continue
		}
		step.Command = raw
		step.CommandSHA256, err = commandDigest(m.Active.RequestSHA256, *step)
		require.NoError(t, err)
		native.Sources.JournalSHA256 = step.CommandSHA256
		if settled {
			step.Observation.CommandSHA256 = step.CommandSHA256
		} else {
			step.Phase, step.Observation = Unconfirmed, nil
		}
	}
	domain := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
	native.Sources.CommandSHA256 = hex.EncodeToString(domain[:])
	require.NoError(t, m.Validate())
	if successorClose {
		m, err = m.Takeover(m.Owner, "vm-close-successor")
		require.NoError(t, err)
	}
	closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, Revision: receipt.Shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: m.Active.Request.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID}
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	pg := receipt.Shared.Observed
	pg.AdmissionOpen, pg.AdmissionRevision = false, closed.Revision
	pg.IssuerFloorEpoch = m.Owner.Epoch
	hash, err := closed.ContextSHA256()
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closed, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: pg}, false)
	require.NoError(t, err)
	return m, native
}

func TestServiceVMRecoveryCannotPredateLatestClosureOwner(t *testing.T) {
	m, _ := serviceVMRecoveryModelForClosure(t, false, true)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceVMRecovery(m.Owner, selector, true)
	require.NoError(t, err)
	step, _, err := m.OriginalServiceVM()
	require.NoError(t, err)
	require.Less(t, step.DispatchedBy.Epoch, m.ServiceClosure.Origin.Epoch)
	changed := m.Clone()
	changed.ServiceClosure.VMRecovery.Origin = *step.DispatchedBy
	require.ErrorContains(t, changed.Validate(), "latest complete closure", "独立选择不能退回更早原派发者绕过最近关闭issuer")
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	loaded, err := decode(wire, m.VMUID, m.Incarnation)
	require.NoError(t, err)
	require.Equal(t, m, loaded)
}

func TestServiceVMRecoveryKeepsOriginalUnknownAndIndependentCapacity(t *testing.T) {
	for _, effect := range []string{"completed", "cancelled"} {
		for _, takeover := range []bool{false, true} {
			t.Run(effect+map[bool]string{false: "-same-owner", true: "-takeover"}[takeover], func(t *testing.T) {
				m, capacity := serviceVMRecoveryModel(t, false)
				before := m.Clone()
				_, _, _, selector := m.ServiceClosure.Selection()
				m, err := m.StartServiceVMRecovery(m.Owner, selector, effect == "cancelled")
				require.NoError(t, err)
				if takeover {
					m, err = m.Takeover(m.Owner, "vm-recovery-successor")
					require.NoError(t, err)
				}
				var witness *ServiceVMCancellation
				if effect == "cancelled" {
					capacity.Cancelled = true
					witness = &ServiceVMCancellation{Owner: m.Owner, VMSequence: m.Sequence, VMResourceVersion: "opaque-cancellation-rv"}
				}
				m, err = m.RecordServiceVMRecovery(m.Owner, selector, capacity, witness)
				require.NoError(t, err)
				require.Equal(t, before.Active, m.Active)
				require.Equal(t, before.BudgetHeld, m.BudgetHeld)
				require.Equal(t, before.CapacityHeld, m.CapacityHeld)
				require.Equal(t, before.BudgetBoundary, m.BudgetBoundary)
				require.Equal(t, before.ServiceClosure.Request, m.ServiceClosure.Request)
				require.Equal(t, m.Owner, *m.ServiceClosure.VMRecovery.ObservedBy)
				require.Equal(t, capacity, *m.ServiceClosure.VMRecovery.Capacity)
				require.Error(t, m.CheckStableServiceRecovery(), "VM单域事实不能替代完整实际预算")
				_, err = m.RequestAbort(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				_, err = m.Finish(m.Owner, m.Active.Request.Key)
				require.Error(t, err)
				_, wire, err := encode(m, MaxEncodedBytes)
				require.NoError(t, err)
				loaded, err := decode(wire, m.VMUID, m.Incarnation)
				require.NoError(t, err)
				require.Equal(t, m, loaded)
				capacity.Sources.ReceiptClaim.UID = "caller-changed"
				if witness != nil {
					witness.VMResourceVersion = "caller-changed"
				}
				cloned := m.Clone()
				cloned.ServiceClosure.VMRecovery.ObservedBy.HolderID += "-changed"
				cloned.ServiceClosure.VMRecovery.Capacity.Sources.ReceiptClaim.UID = "clone-changed"
				if cloned.ServiceClosure.VMRecovery.Cancellation != nil {
					cloned.ServiceClosure.VMRecovery.Cancellation.VMResourceVersion = "clone-changed"
				}
				require.Equal(t, loaded, m, "输入及Clone不能共享原Proof的可变指针")
				require.NoError(t, m.Validate())
			})
		}
	}
}

func TestServiceVMRecoveryReadsAlreadyCompletedVMWithoutCancelling(t *testing.T) {
	m, capacity := serviceVMRecoveryModel(t, true)
	original := m.Clone()
	_, _, _, selector := m.ServiceClosure.Selection()
	_, err := m.StartServiceVMRecovery(m.Owner, selector, true)
	require.Error(t, err, "已完成VM只能读取，不选择未发送取消")
	m, err = m.StartServiceVMRecovery(m.Owner, selector, false)
	require.NoError(t, err)
	m, err = m.RecordServiceVMRecovery(m.Owner, selector, capacity, nil)
	require.NoError(t, err)
	require.Equal(t, original.Active, m.Active)
	require.Error(t, m.CheckStableServiceRecovery())
	changed := m.Clone()
	changed.ServiceClosure.VMRecovery.Capacity.Cancelled = true
	changed.ServiceClosure.VMRecovery.Cancellation = &ServiceVMCancellation{Owner: changed.Owner, VMSequence: changed.ServiceClosure.VMRecovery.StartedSequence, VMResourceVersion: "opaque-earlier-cancellation"}
	require.Error(t, changed.Validate(), "原ResizeVM已完成，不能将其改成未发送取消")
}

func TestServiceVMRecoveryShrinkKeepsCommittedPGAndUnknownVM(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "cancelled-before-send"}[cancelled], func(t *testing.T) {
			m, closed, fact := activeClosureModel(t, control.Shrink, ResizeVM, true)
			initial, _, _ := initialDomain(t)
			p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: m.Active.Request, Identity: *m.ResourceIdentity, RunnerPodName: "runner", RunnerRestartCount: 0, VMBeforeMemoryBytes: 2 << 30, GuestUsableMemoryBytes: m.Active.Request.TargetVMMemoryBytes - (256 << 20), Shared: initial.Shared, TargetSharedUpperBytes: initial.Shared.StartupMainUpperBytes, Limits: initial.PGAggregate.Bootstrap.Limits, SystemOutsidePG: initial.SystemOutsidePG, Transition: initial.Transition, Safety: initial.Safety, OutsideClaim: nil}
			p.Shared.Observed.LowBuffers, p.Shared.Observed.HighBuffers = 2048, 2048
			p.Shared.Observed.Operation = nil
			p.Shared.Observed.CapacityGeneration = m.Active.Request.ExpectedCapacityGeneration
			require.NoError(t, p.ValidateFor(m.Active.Request, p.Identity))
			var preparedHash, guestHash string
			// PG/Release/Guest前驱仍是明确的低层领域输入；测试只保证其完整原文
			// 和Guest摘要不会被独立VM事实替换，不将夹具当真实物理回收证据。
			for i := range m.Active.Steps {
				step := &m.Active.Steps[i]
				if step.Action == PrepareBudget {
					var err error
					step.Observation.Data, err = json.Marshal(p)
					require.NoError(t, err)
					preparedHash = rawDigest(step.Observation.Data)
				}
				if step.Action == GuestReady {
					guestHash = rawDigest(step.Observation.Data)
				}
			}
			sources := runnerSources()
			hostClaim := budget.ObjectRef{Name: "host-shrink-recovery", UID: "host-shrink-recovery-uid"}
			var command serviceVMProjection
			command.ProtocolVersion, command.BindingClaim, command.HostClaim = 1, sources.BirthClaim, hostClaim
			command.Request, command.BeforeBytes = m.Active.Request, p.VMBeforeMemoryBytes
			command.PreparedSHA256, command.HostSHA256, command.GuestSHA256 = preparedHash, strings.Repeat("c", 64), guestHash
			command.Identity.VMUID, command.Identity.Incarnation = m.VMUID, m.Incarnation
			command.Identity.RunnerPodUID, command.Identity.RunnerBootID, command.Identity.NodeUID = "runner-uid", testIncarnation, "node-uid"
			command.Identity.MinimumBytes, command.Identity.MaximumBytes, command.Identity.BlockBytes = 512<<20, 4<<30, 128<<20
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			for i := range m.Active.Steps {
				step := &m.Active.Steps[i]
				if step.Action == ResizeVM {
					step.TargetID, step.Command = command.Identity.RunnerPodUID, raw
					step.CommandSHA256, err = commandDigest(m.Active.RequestSHA256, *step)
					require.NoError(t, err)
					sources.JournalSHA256 = step.CommandSHA256
				}
			}
			domain := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
			sources.CommandSHA256 = hex.EncodeToString(domain[:])
			capacity := budget.NativeCapacity{Sources: sources, Request: m.Active.Request, BeforeBytes: p.VMBeforeMemoryBytes, Cancelled: cancelled, PreparedSHA256: preparedHash, GuestSHA256: guestHash, HostClaim: hostClaim}
			closed.ExpectedCapacityGeneration++
			fact.Request = closed
			fact.ContextSHA256, err = closed.ContextSHA256()
			require.NoError(t, err)
			fact.Observed.CapacityGeneration++
			m, err = m.StartActiveServiceClosure(m.Owner, closed)
			require.NoError(t, err)
			m, err = m.RecordServiceClosure(m.Owner, fact, false)
			require.NoError(t, err)
			require.EqualValues(t, 17, m.ClosureFormatVersion())
			before := m.Clone()
			_, _, _, selector := m.ServiceClosure.Selection()
			m, err = m.StartServiceVMRecovery(m.Owner, selector, cancelled)
			require.NoError(t, err)
			var witness *ServiceVMCancellation
			if cancelled {
				m, err = m.Takeover(m.Owner, "vm-shrink-recovery-successor")
				require.NoError(t, err)
				witness = &ServiceVMCancellation{Owner: m.Owner, VMSequence: m.Sequence, VMResourceVersion: "opaque-shrink-cancellation-rv"}
			}
			m, err = m.RecordServiceVMRecovery(m.Owner, selector, capacity, witness)
			require.NoError(t, err)
			require.Equal(t, before.Active, m.Active)
			require.Equal(t, before.InitialHeld, m.InitialHeld)
			require.Error(t, m.CheckStableServiceRecovery())
			_, wire, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			loaded, err := decode(wire, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.Equal(t, m, loaded)
			changed := loaded.Clone()
			changed.ServiceClosure.VMRecovery.Capacity.GuestSHA256 = strings.Repeat("f", 64)
			require.Error(t, changed.Validate(), "缩容VM不能丢失原Guest安全摘要")
		})
	}
}

func TestServiceVMRecoveryRejectsChangedSourcesAndInventedCancellation(t *testing.T) {
	m, capacity := serviceVMRecoveryModel(t, false)
	_, _, _, selector := m.ServiceClosure.Selection()
	_, err := m.StartServiceVMRecovery(m.Owner, selector+"x", true)
	require.Error(t, err)
	selected, err := m.StartServiceVMRecovery(m.Owner, selector, true)
	require.NoError(t, err)
	capacity.Cancelled = true
	witness := &ServiceVMCancellation{Owner: selected.Owner, VMSequence: selected.Sequence, VMResourceVersion: "opaque-cancellation-rv"}
	settled, err := selected.RecordServiceVMRecovery(selected.Owner, selector, capacity, witness)
	require.NoError(t, err)
	for name, change := range map[string]func(*Model){
		"original-abort":  func(n *Model) { n.Active.AbortRequested = true },
		"operation-hash":  func(n *Model) { n.ServiceClosure.VMRecovery.OperationSHA256 = strings.Repeat("f", 64) },
		"command-hash":    func(n *Model) { n.ServiceClosure.VMRecovery.CommandSHA256 = strings.Repeat("f", 64) },
		"closed":          func(n *Model) { n.ServiceClosure.VMRecovery.Closed.Revision++ },
		"closed-sequence": func(n *Model) { n.ServiceClosure.VMRecovery.ClosedSequence-- },
		"before-selection": func(n *Model) {
			n.ServiceClosure.VMRecovery.StartedSequence = n.ServiceClosure.VMRecovery.ClosedSequence
		},
		"future-record":     func(n *Model) { n.ServiceClosure.VMRecovery.RecordedSequence++ },
		"owner":             func(n *Model) { n.ServiceClosure.VMRecovery.Origin.HolderID += "x" },
		"observer":          func(n *Model) { n.ServiceClosure.VMRecovery.ObservedBy.HolderID += "x" },
		"request":           func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.Request.TargetVMMemoryBytes += 128 << 20 },
		"before":            func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.BeforeBytes++ },
		"prepare":           func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.PreparedSHA256 = strings.Repeat("f", 64) },
		"guest":             func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.GuestSHA256 = strings.Repeat("f", 64) },
		"host":              func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.HostClaim.UID += "x" },
		"birth":             func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.Sources.BirthClaim.UID += "x" },
		"journal":           func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.Sources.JournalSHA256 = strings.Repeat("f", 64) },
		"native-command":    func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.Sources.CommandSHA256 = strings.Repeat("f", 64) },
		"unselected-cancel": func(n *Model) { n.ServiceClosure.VMRecovery.CancelUnsent = false },
		"missing-witness":   func(n *Model) { n.ServiceClosure.VMRecovery.Cancellation = nil },
		"premature-witness": func(n *Model) {
			n.ServiceClosure.VMRecovery.Cancellation.VMSequence = n.ServiceClosure.VMRecovery.StartedSequence - 1
		},
		"future-witness": func(n *Model) {
			n.ServiceClosure.VMRecovery.Cancellation.VMSequence = n.ServiceClosure.VMRecovery.RecordedSequence
		},
		"witness-owner":          func(n *Model) { n.ServiceClosure.VMRecovery.Cancellation.Owner.HolderID += "x" },
		"missing-rv":             func(n *Model) { n.ServiceClosure.VMRecovery.Cancellation.VMResourceVersion = "" },
		"oversized-rv":           func(n *Model) { n.ServiceClosure.VMRecovery.Cancellation.VMResourceVersion = strings.Repeat("v", 257) },
		"completed-with-witness": func(n *Model) { n.ServiceClosure.VMRecovery.Capacity.Cancelled = false },
		"unknown-with-fact":      func(n *Model) { n.ServiceClosure.VMRecovery.Phase = Unconfirmed },
	} {
		t.Run(name, func(t *testing.T) {
			n := settled.Clone()
			change(&n)
			require.Error(t, n.Validate())
		})
	}
	_, err = settled.RecordServiceVMRecovery(settled.Owner, selector, capacity, witness)
	require.Error(t, err, "已保存独立事实不得被第二次落账覆盖")
}

func TestServiceVMRecoveryStrictDecodeRejectsAmbiguousOrOversizedProof(t *testing.T) {
	m, _ := serviceVMRecoveryModel(t, false)
	_, _, _, selector := m.ServiceClosure.Selection()
	m, err := m.StartServiceVMRecovery(m.Owner, selector, true)
	require.NoError(t, err)
	raw, err := json.Marshal(m.ServiceClosure.VMRecovery)
	require.NoError(t, err)
	for _, bad := range []string{
		string(raw[:len(raw)-1]) + `,"cancel_unsent":false}`,
		string(raw[:len(raw)-1]) + `,"implicit_replay":true}`,
		strings.Replace(string(raw), `"cancel_unsent":true,`, "", 1),
		strings.Replace(string(raw), m.ServiceClosure.VMRecovery.OperationSHA256, strings.Repeat("f", MaxObservationBytes), 1),
	} {
		var p ServiceVMRecovery
		require.Error(t, json.Unmarshal([]byte(bad), &p))
	}
	var witness ServiceVMCancellation
	require.Error(t, json.Unmarshal([]byte(`{"owner":{"epoch":"1","holder_id":"x"},"vm_sequence":"1","vm_resource_version":"one","vm_resource_version":"two"}`), &witness))
}

func TestStoreServiceVMRecoveryCASPreservesSelectionAndFacts(t *testing.T) {
	for _, recording := range []bool{false, true} {
		for _, mode := range []string{"normal", "lost-reply", "stale-rv", "recreated-vm"} {
			t.Run(map[bool]string{false: "select/", true: "record/"}[recording]+mode, func(t *testing.T) {
				api, store := newStoreAPI(t)
				_ = initializeStore(t, api, store)
				m, capacity := serviceVMRecoveryModel(t, false)
				_, _, _, selector := m.ServiceClosure.Selection()
				if recording {
					var err error
					m, err = m.StartServiceVMRecovery(m.Owner, selector, true)
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
					capacity.Cancelled = true
					witness := &ServiceVMCancellation{Owner: m.Owner, VMSequence: m.Sequence, VMResourceVersion: s.ResourceVersion()}
					result, err = store.RecordServiceVMRecovery(context.Background(), s, m.Owner, selector, capacity, witness)
				} else {
					result, err = store.StartServiceVMRecovery(context.Background(), s, m.Owner, selector, true)
				}
				if mode == "normal" {
					require.NoError(t, err)
					require.NotNil(t, result)
				} else {
					require.Error(t, err)
					require.Nil(t, result, "未确认CAS不能返回选择或完成快照")
				}
				require.Equal(t, m, s.State(), "Store成功、冲突及丢回复均不能修改输入快照")
				if mode == "lost-reply" {
					var lost *UnconfirmedWrite
					require.ErrorAs(t, err, &lost)
					loaded, err := store.Load(context.Background())
					require.NoError(t, err)
					require.Equal(t, m.Active, loaded.State().Active)
					require.Equal(t, map[bool]Phase{false: Unconfirmed, true: Settled}[recording], loaded.State().ServiceClosure.VMRecovery.Phase)
				}
			})
		}
	}
}
