package owner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 仅给原预算/准入反向用例补齐最终VM领域的显式协议夹具。实际Runner的文件锁/
// QMP及Agent写前派发另有组合用例；本函数不执行QEMU，不认证Host物理预留。
// 原有“先前领域”边界保持，不能因此把预算反向用例报告成真实VM端到端。
func originalRunnerFixture(t *testing.T, ctx context.Context, coreClient *kubefake.Clientset, store *journal.Store, session *Session, p budget.Prepared, fact budget.VMSettlement) budget.VMSettlement {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	op := s.State().Active
	require.NotNil(t, op)
	var prepared, guest *journal.Step
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Action == journal.PrepareBudget {
			prepared = step
		}
		if step.Action == journal.GuestReady {
			guest = step
		}
	}
	require.NotNil(t, prepared)
	birthRef := budget.ObjectRef{Name: vmguard.BindingName(store.Binding()), UID: "runner-birth-uid"}
	stateRef := budget.ObjectRef{Name: "runner-state-original", UID: "runner-state-uid"}
	id := vmguard.Identity{VMUID: string(store.Binding().VMUID), Incarnation: store.Binding().Incarnation, RunnerPodUID: fact.Host.RunnerPodUID, RunnerBootID: "0123456789abcdef0123456789abcdef", NodeUID: fact.Host.NodeUID, MinimumBytes: 128 << 20, MaximumBytes: 4 << 30, BlockBytes: 8 << 20}
	raw, err := json.Marshal(fact.Host)
	require.NoError(t, err)
	cmd := vmguard.Command{ProtocolVersion: 1, Identity: id, BindingClaim: birthRef, HostClaim: fact.HostClaim, Request: p.Request, BeforeBytes: p.VMBeforeMemoryBytes, PreparedSHA256: vmguard.SourceDigest(prepared.Observation.Data), HostSHA256: vmguard.SourceDigest(raw), GuestSHA256: ""}
	if p.Request.Kind == control.Shrink {
		require.NotNil(t, guest)
		cmd.GuestSHA256 = vmguard.SourceDigest(guest.Observation.Data)
	}
	require.NoError(t, cmd.Validate())
	raw, err = json.Marshal(cmd)
	require.NoError(t, err)
	s, dispatch, err := store.Start(ctx, s, session.Actor(), p.Request.Key, journal.ResizeVM, id.RunnerPodUID, raw)
	require.NoError(t, err)
	hash := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
	cmdSHA := hex.EncodeToString(hash[:])
	stage := "may_have_sent"
	if p.VMBeforeMemoryBytes == p.Request.TargetVMMemoryBytes {
		stage = "prepared"
	}
	intent := map[string]any{"command": cmd, "command_sha256": cmdSHA, "journal_sha256": dispatch.Step.CommandSHA256, "owner": *dispatch.Step.DispatchedBy, "stage": stage, "replay_nonce": "", "before": vmguard.Observation{RequestedBytes: p.VMBeforeMemoryBytes, ActualBytes: p.VMBeforeMemoryBytes}}
	result := map[string]any{"intent": intent, "kind": "completed", "actual": vmguard.Observation{RequestedBytes: p.Request.TargetVMMemoryBytes, ActualBytes: p.Request.TargetVMMemoryBytes}}
	raw, err = json.Marshal(result)
	require.NoError(t, err)
	receiptRef := budget.ObjectRef{Name: "dsga-r-" + vmguard.SourceDigest(raw)[:56], UID: "runner-receipt-uid"}
	newCM := func(ref budget.ObjectRef, kind string, immutable bool, data any) *core.ConfigMap {
		encoded, err := json.Marshal(data)
		require.NoError(t, err)
		controller, block := true, true
		return &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: store.Binding().Namespace, Name: ref.Name, UID: types.UID(ref.UID), ResourceVersion: "source-rv-1", Labels: map[string]string{vmguard.ClaimLabel: kind}, Finalizers: []string{vmguard.ProtectionFinalizer}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: store.Binding().Name, UID: store.Binding().VMUID, Controller: &controller, BlockOwnerDeletion: &block}}}, Immutable: &immutable, Data: map[string]string{vmguard.ClaimKey: string(encoded)}}
	}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: id, StateClaim: stateRef}
	viewData := map[string]any{"protocol_version": 1, "identity": id, "binding_claim": birthRef, "sequence": control.Uint64(2), "floor": session.Actor(), "lease_uid": string(session.Ownership().LeaseUID), "high_water": p.Request.Key, "pending": nil, "last": result, "last_published": receiptRef}
	raw, err = json.Marshal(viewData)
	require.NoError(t, err)
	var view vmguard.View
	require.NoError(t, json.Unmarshal(raw, &view))
	publication := vmguard.Publication{ProtocolVersion: 1, Identity: id, BindingClaim: &birthRef, State: &view, Actual: &vmguard.Observation{RequestedBytes: p.Request.TargetVMMemoryBytes, ActualBytes: p.Request.TargetVMMemoryBytes}}
	for _, cm := range []*core.ConfigMap{newCM(birthRef, "binding-v1", true, birth), newCM(stateRef, "state-v1", false, publication), newCM(receiptRef, "receipt-v1", true, result)} {
		require.NoError(t, coreClient.Tracker().Add(cm))
	}
	fact.Runner = &budget.RunnerSources{BirthClaim: birthRef, StateClaim: stateRef, ReceiptClaim: receiptRef, CommandSHA256: cmdSHA, JournalSHA256: dispatch.Step.CommandSHA256}
	fact.RunnerCommandID = cmdSHA
	raw, err = json.Marshal(fact)
	require.NoError(t, err)
	_, err = store.Record(ctx, s, session.Actor(), p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: journal.ResizeVM, Result: journal.Completed, SourceID: id.RunnerPodUID, Data: raw})
	require.NoError(t, err)
	return fact
}
