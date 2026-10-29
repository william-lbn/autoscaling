package hostexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 本组是纯保留集合/候选领域测试，payload刻意只保留引用形状；不把
// 它们当有效预算或生产回收许可。实际WAL/Node/Runner组合另测。
func TestRetainedSourceNamesWalksRawHeadsAndHistory(t *testing.T) {
	ref := func(name string) []byte {
		raw, err := json.Marshal(map[string]any{"nested": []any{budget.ObjectRef{Name: name, UID: name + "-uid"}}})
		require.NoError(t, err)
		return raw
	}
	step := func(command, audit string) journal.Step {
		var s journal.Step
		var observation journal.Observation
		s.Command, observation.Data = ref(command), ref(audit)
		s.Observation = &observation
		return s
	}
	var m journal.Model
	var normal journal.HeldBudget
	var capacity journal.HeldCapacity
	var initial journal.HeldInitialBudget
	var admission journal.InitialAdmission
	var history journal.Record
	normal.Finalization = step("normal-command", "normal-audit")
	var recovered journal.RecoveredBudgetOrigin
	recovered.Original.Steps = []journal.Step{step("recovered-original-command", "recovered-original-audit")}
	normal.Recovery = &recovered
	capacity.Cleanup = step("capacity-command", "capacity-audit")
	initial.Budget = ref("initial-budget")
	admission.Command, admission.Receipt = ref("initial-command"), ref("initial-audit")
	initial.Admission = &admission
	history.Operation.Steps = []journal.Step{step("history-command", "history-audit")}
	cleanup := step("history-cleanup", "cleanup-audit")
	history.Operation.Cleanup = &cleanup
	var proof journal.ServiceFinalization
	var sources budget.SettlementSources
	sources.OutsideClaim = budget.ObjectRef{Name: "history-proof-outside", UID: "proof-outside-uid"}
	sources.HostClaim = budget.ObjectRef{Name: "history-proof-host", UID: "proof-host-uid"}
	proof.Command.Finalized.Sources = &sources
	var coverage journal.CommittedServiceCoverage
	coverage.Finalization = &proof
	history.ServiceCoverage = &coverage
	m.BudgetHeld, m.CapacityHeld, m.InitialHeld, m.History = &normal, &capacity, &initial, []journal.Record{history}
	birth := budget.ObjectRef{Name: "original-birth", UID: "birth-uid"}
	var published vmguard.Publication
	published.BindingClaim = &birth
	refs, err := retainedSourceNames(m, published)
	require.NoError(t, err)
	for _, name := range []string{"normal-command", "normal-audit", "recovered-original-command", "recovered-original-audit", "history-proof-outside", "history-proof-host", "capacity-command", "capacity-audit", "initial-budget", "initial-command", "initial-audit", "history-command", "history-audit", "history-cleanup", "cleanup-audit", birth.Name} {
		require.Contains(t, refs, name)
	}
	// 历史淘汰只减历史引用；当前预算即使完全不在History中也继续保留。
	m.History = nil
	published.BindingClaim = nil
	refs, err = retainedSourceNames(m, published)
	require.NoError(t, err)
	require.Contains(t, refs, "capacity-command")
	require.Contains(t, refs, "normal-audit")
	require.Contains(t, refs, "recovered-original-audit", "结果缓存淘汰不能裁掉独立预算内的原未知来源")
	require.NotContains(t, refs, "history-command")
	require.NotContains(t, refs, "history-proof-outside")
	for _, raw := range []string{`{"ref":{"name":"source","uid":null}}`, `{"ref":{"name":"source","uid":"a","uid":"b"}}`, `{"broken":`} {
		m.BudgetHeld.Finalization.Command = []byte(raw)
		_, err = retainedSourceNames(m, published)
		require.Error(t, err, "不完整/重复键不能生成可删集合")
	}
	m.Active = &history.Operation
	_, err = retainedSourceNames(m, published)
	require.Error(t, err)
}

func TestSettledRunnerReceiptCollectionRequiresOriginalBirthAndContentName(t *testing.T) {
	for _, what := range []string{"old", "not-sent", "retained-name", "another-pod", "another-node", "another-runner-boot", "another-birth", "same-position-other-id", "wrong-content-name"} {
		t.Run(what, func(t *testing.T) {
			p, m, birth, birthRef, closed, cm := collectionCandidateFixture(t)
			request := closed.Request
			request.Key.Revision--
			command := vmguard.Command{ProtocolVersion: 1, Identity: birth.Identity, BindingClaim: birthRef, HostClaim: budget.ObjectRef{Name: "host-original", UID: "host-uid"}, Request: request, BeforeBytes: 128 << 20, PreparedSHA256: strings.Repeat("a", 64), HostSHA256: strings.Repeat("b", 64), GuestSHA256: ""}
			keep := make(sourceReferences)
			switch what {
			case "another-pod":
				command.Identity.RunnerPodUID = "other-pod-uid"
			case "another-node":
				command.Identity.NodeUID = "other-node-uid"
			case "another-runner-boot":
				command.Identity.RunnerBootID = strings.Repeat("d", 32)
			case "another-birth":
				command.BindingClaim.UID = "replacement-birth-uid"
			case "same-position-other-id":
				command.Request.Key.Revision = closed.Request.Key.Revision
				command.Request.Key.OperationID.Low++
			}
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			digest := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
			stage, kind := "may_have_sent", "completed"
			actual := command.Request.TargetVMMemoryBytes
			if what == "not-sent" {
				stage, kind, actual = "prepared", "not_sent", command.BeforeBytes
			}
			raw, err = json.Marshal(map[string]any{"intent": map[string]any{"command": command, "command_sha256": hex.EncodeToString(digest[:]), "journal_sha256": strings.Repeat("c", 64), "owner": journal.Owner{HolderID: "original-owner", Epoch: request.Key.IssuerEpoch}, "stage": stage, "replay_nonce": "", "before": vmguard.Observation{RequestedBytes: command.BeforeBytes, ActualBytes: command.BeforeBytes}}, "kind": kind, "actual": vmguard.Observation{RequestedBytes: actual, ActualBytes: actual}})
			require.NoError(t, err)
			var receipt vmguard.Receipt
			require.NoError(t, json.Unmarshal(raw, &receipt), "漂移夹具本身先满足Runner领域协议")
			cm.Name, cm.Data = vmguard.ReceiptSourceName(raw), map[string]string{vmguard.ClaimKey: string(raw)}
			cm.Labels = map[string]string{vmguard.ClaimLabel: vmguard.ReceiptKind}
			cm.Finalizers = []string{vmguard.ProtectionFinalizer}
			if what == "retained-name" {
				keep[cm.Name] = struct{}{}
			}
			if what == "wrong-content-name" {
				cm.Name = "dsga-r-wrong-name"
			}
			f, eligible := p.oldSource(cm, m, birth, birthRef, closed, keep)
			require.Equal(t, what == "old" || what == "not-sent", eligible)
			if eligible {
				require.Equal(t, vmguard.ProtectionFinalizer, f)
			}
		})
	}
}

func TestSourceCollectionThroughKeyNeverOrdersConflictingOperationID(t *testing.T) {
	_, _, _, _, closed, _ := collectionCandidateFixture(t)
	key := closed.Request.Key
	require.False(t, sourceClosedBefore(key, closed), "旧无Through合同不覆盖当前Key")
	closed.ThroughKey, closed.ThroughSequence = key, 10
	require.True(t, sourceClosedBefore(key, closed))
	key.OperationID.Low++
	require.False(t, sourceClosedBefore(key, closed), "相同位置换ID不是旧请求")
	key.Revision--
	require.True(t, sourceClosedBefore(key, closed))
	key.Instance.PGBootID--
	require.False(t, sourceClosedBefore(key, closed), "不能清另一PG boot")
}

func collectionCandidateFixture(t *testing.T) (*Publisher, journal.Model, vmguard.Birth, budget.ObjectRef, hostguard.Settlement, *core.ConfigMap) {
	t.Helper()
	b := journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	store, err := journal.NewStore(vmfake.NewSimpleClientset().NeonvmV1().VirtualMachines(b.Namespace), b)
	require.NoError(t, err)
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var req control.Request
	require.NoError(t, json.Unmarshal(raw, &req))
	req.Key.Instance.VMUID = string(b.VMUID)
	req.Key.IssuerEpoch, req.Key.Revision = 2, 3
	require.NoError(t, req.Validate())
	id := resourceguard.Identity{VMUID: string(b.VMUID), ComputeInstanceID: req.Key.Instance.ComputeInstanceID, GuestBootID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", CgroupDevice: 1, CgroupInode: 2, SystemCgroupDevice: 0, SystemCgroupInode: 0}
	require.NoError(t, id.Validate())
	var m journal.Model
	m.ResourceIdentity = &id
	birthRef := budget.ObjectRef{Name: vmguard.BindingName(b), UID: "birth-uid"}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: vmguard.Identity{VMUID: string(b.VMUID), Incarnation: b.Incarnation, RunnerPodUID: "runner-uid", RunnerBootID: strings.Repeat("c", 32), NodeUID: "node-uid", MinimumBytes: 128 << 20, MaximumBytes: 4 << 30, BlockBytes: 8 << 20}, StateClaim: budget.ObjectRef{Name: "state", UID: "state-uid"}}
	require.NoError(t, birth.Validate())
	var closed hostguard.Settlement
	closed.Request = req
	var out budget.OutsideReservation
	out.ProtocolVersion, out.Request, out.Identity = 1, req, id
	out.Incarnation, out.ResidualProfileID, out.HoldUntilExplicitSettlement = b.Incarnation, "original-platform", true
	out.Request.Key.Revision--
	raw, err = json.Marshal(out)
	require.NoError(t, err)
	immutable, controller := true, true
	cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: "old-outside", UID: "outside-uid", ResourceVersion: "1", Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.OutsideKind}, Finalizers: []string{budgetclaims.ProtectionFinalizer, "unrelated-owner"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}}, Immutable: &immutable, Data: map[string]string{budgetclaims.ClaimKey: string(raw)}}
	var publisher Publisher
	publisher.vm = store
	return &publisher, m, birth, birthRef, closed, cm
}

func TestSettledSourceCandidatesKeepReferencedUnknownAndForeignObjects(t *testing.T) {
	for _, what := range []string{"old", "retained-name", "foreign-owner", "foreign-guest", "foreign-pg", "future-key", "same-position-other-id", "missing-uid", "mutable", "double-kind", "unknown-version", "original-deleting", "finalizer-already-removed"} {
		t.Run(what, func(t *testing.T) {
			p, m, birth, birthRef, closed, cm := collectionCandidateFixture(t)
			keep := make(sourceReferences)
			var out budget.OutsideReservation
			require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &out))
			switch what {
			case "retained-name":
				keep[cm.Name] = struct{}{}
			case "foreign-owner":
				cm.OwnerReferences[0].UID = "other-vm-uid"
			case "foreign-guest":
				out.Identity.GuestBootID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
			case "foreign-pg":
				out.Request.Key.Instance.PGBootID++
			case "future-key":
				out.Request.Key.Revision = closed.Request.Key.Revision + 1
			case "same-position-other-id":
				out.Request.Key.Revision = closed.Request.Key.Revision
				out.Request.Key.OperationID.Low++
			case "missing-uid":
				cm.UID = ""
			case "mutable":
				*cm.Immutable = false
			case "double-kind":
				cm.Labels[vmguard.ClaimLabel] = vmguard.ReceiptKind
			case "unknown-version":
				out.ProtocolVersion = 99
			case "original-deleting":
				stamp := metav1.Now()
				cm.DeletionTimestamp = &stamp
			case "finalizer-already-removed":
				cm.Finalizers = []string{"unrelated-owner"}
			}
			raw, err := json.Marshal(out)
			require.NoError(t, err)
			cm.Data[budgetclaims.ClaimKey] = string(raw)
			f, eligible := p.oldSource(cm, m, birth, birthRef, closed, keep)
			want := what == "old" || what == "original-deleting" || what == "finalizer-already-removed"
			require.Equal(t, want, eligible)
			if want {
				require.Equal(t, budgetclaims.ProtectionFinalizer, f)
			}
		})
	}
}

func TestCollectionProgressDiscardsOnlyDiscoveryOnWatermarkOrVMChange(t *testing.T) {
	p, _, _, _, closed, _ := collectionCandidateFixture(t)
	progress := NewCollectionProgress()
	progress.prepare(p.vm.Binding(), closed)
	progress.continuation, progress.kind, progress.retry = "tail", 1, true
	progress.pending = []collectionCandidate{{Object: budget.ObjectRef{Name: "queued", UID: "queued-uid"}, Finalizer: budgetclaims.ProtectionFinalizer, SHA256: strings.Repeat("a", 64)}}
	progress.prepare(p.vm.Binding(), closed)
	require.Equal(t, "tail", progress.continuation)
	require.Len(t, progress.pending, 1)
	closed.BudgetSequence++
	progress.prepare(p.vm.Binding(), closed)
	require.Empty(t, progress.continuation)
	require.Empty(t, progress.pending)
	require.False(t, progress.retry)
	b := p.vm.Binding()
	b.VMUID = types.UID("replacement-vm")
	progress.continuation = "old-vm-tail"
	progress.prepare(b, closed)
	require.Empty(t, progress.continuation)
}
