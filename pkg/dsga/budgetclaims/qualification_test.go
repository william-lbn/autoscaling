package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 仅模拟Kube原对象，不伪装Linux资格实测；重点验证v2消费者每次传递认证
// 原UID/摘要/保护寿命及真实Pod/Node，而不是只检查Prepare时的一次声明。
func TestOutsideQualificationReaderRejectsReplacementAndLifetimeDrift(t *testing.T) {
	b := journal.Binding{Namespace: "testing", Name: "vm-one", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	controller := true
	owner := []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: resourceguard.Identity{VMUID: string(b.VMUID), ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3, SystemCgroupDevice: 2, SystemCgroupInode: 4}, Incarnation: b.Incarnation, ReportID: "explicit-platform-fixture", RootBootstrapSHA256: strings.Repeat("b", 64), RunnerBirth: budget.ObjectRef{Name: "birth", UID: "birth-uid"}, RunnerIdentitySHA256: strings.Repeat("c", 64), RunnerPodName: "runner", RunnerPodUID: "runner-uid", NodeName: "node-one", NodeUID: "node-uid", RunnerRestartCount: 0, HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: 1 << 30, GuestUsableMemoryBytes: 768 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, PidsMaxTasks: 128, SystemMemoryMaxBytes: 16 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}}}, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20}}}
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	source := budget.QualificationSource{Claim: budget.ObjectRef{Name: "platform", UID: "platform-uid"}, SHA256: hex.EncodeToString(digest[:])}
	system, transition, safety := q.Targets[0].Bounds(source)
	request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: string(b.VMUID), ComputeInstanceID: "compute-1", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 2}}, Kind: control.Grow, TargetBuffers: 1024, ExpectedCapacityGeneration: 1, TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: "budget", DeadlineUnixMillis: 10000}
	out := budget.OutsideReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: request, Identity: q.Identity, Incarnation: b.Incarnation, ResidualProfileID: "qualified-" + source.SHA256, SystemOutsidePG: system, Transition: transition, Safety: safety, HoldUntilExplicitSettlement: true, Qualification: &source}
	outRaw, err := json.Marshal(out)
	require.NoError(t, err)
	cm := func(name, uid, kind string, data []byte) *core.ConfigMap {
		return &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: name, UID: types.UID(uid), ResourceVersion: "original-rv", OwnerReferences: owner, Finalizers: []string{ProtectionFinalizer}, Labels: map[string]string{ClaimLabel: kind}}, Immutable: &controller, Data: map[string]string{ClaimKey: string(data)}}
	}
	qualificationCM := cm(source.Claim.Name, source.Claim.UID, QualificationKind, raw)
	outCM := cm("outside", "outside-uid", OutsideKind, outRaw)
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: q.RunnerPodName, Namespace: b.Namespace, UID: "runner-uid", ResourceVersion: "pod-rv", OwnerReferences: owner}, Spec: core.PodSpec{NodeName: q.NodeName}, Status: core.PodStatus{Phase: core.PodRunning}}
	node := &core.Node{ObjectMeta: metav1.ObjectMeta{Name: q.NodeName, UID: "node-uid", ResourceVersion: "node-rv"}}
	for _, mode := range []string{"valid", "wrong-uid", "changed-body", "releasing", "mutable", "unprotected", "missing", "wrong-pod", "wrong-node"} {
		t.Run(mode, func(t *testing.T) {
			client := kubefake.NewSimpleClientset(qualificationCM.DeepCopy(), outCM.DeepCopy(), pod.DeepCopy(), node.DeepCopy())
			client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
				if action.(kt.GetAction).GetName() != source.Claim.Name || mode == "valid" || mode == "wrong-pod" || mode == "wrong-node" {
					return false, nil, nil
				}
				changed := qualificationCM.DeepCopy()
				switch mode {
				case "wrong-uid":
					changed.UID = "same-name-new-uid"
				case "changed-body":
					changed.Data[ClaimKey] += " "
				case "releasing":
					now := metav1.Now()
					changed.DeletionTimestamp = &now
				case "mutable":
					changed.Immutable = nil
				case "unprotected":
					changed.Finalizers = nil
				case "missing":
					return true, nil, fmt.Errorf("explicit source unavailable")
				}
				return true, changed, nil
			})
			if mode == "wrong-pod" {
				changed := pod.DeepCopy()
				changed.UID = "new-pod"
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), changed, b.Namespace))
			}
			if mode == "wrong-node" {
				changed := node.DeepCopy()
				changed.UID = "new-node"
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), changed, ""))
			}
			reader, err := New(client.CoreV1(), b)
			require.NoError(t, err)
			_, err = reader.ReadOutside(context.Background(), budget.ObjectRef{Name: outCM.Name, UID: string(outCM.UID)})
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
