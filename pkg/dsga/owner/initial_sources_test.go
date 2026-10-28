package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 此处只验首次预算的资格/初始Host读者。真实Session和VM UID/RV绑定，
// Kube Node/Pod、root以及未消费的Runner出生引用是明确夹具；不声称完成
// 原生Runner、首次门禁、Linux资格或首次服务端到端。读者本身不发任何写。
func TestInitialBudgetSourcesRequireOriginalPlacementAndQualification(t *testing.T) {
	api, store, coord := newAPI(t)
	client := vmCore(t)
	api.mu.Lock()
	api.vm.Status.Node = "node-one"
	api.mu.Unlock()
	root, config := guestRoot(t, store)
	qualifiedRootFixture(root)
	root.mu.Lock()
	root.view.Actual.GuestMemTotalBytes = 480 << 20
	root.mu.Unlock()
	elector, err := New(coord, store, DefaultOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, config)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		request := pgRequest(t, session.Actor())
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		snapshot, err = store.BindPG(ctx, snapshot, session.Actor(), request.Key.Instance)
		require.NoError(t, err)
		view, err := resources.client.InspectState(ctx)
		require.NoError(t, err)
		aggregate, err := budget.RootAggregateFromView(view)
		require.NoError(t, err)
		bootstrapRaw, err := json.Marshal(view.Bootstrap)
		require.NoError(t, err)
		q := budget.PlatformQualification{ProtocolVersion: 1, Identity: view.Identity, Incarnation: binding().Incarnation, ReportID: "explicit-initial-source-fixture", RootBootstrapSHA256: vmguard.SourceDigest(bootstrapRaw), RunnerBirth: budget.ObjectRef{Name: "unconsumed-birth-fixture", UID: "unconsumed-birth-fixture-uid"}, RunnerIdentitySHA256: strings.Repeat("a", 64), RunnerPodName: "runner", RunnerPodUID: "runner-uid", RunnerRestartCount: 0, NodeName: "node-one", NodeUID: "node-uid", HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 480 << 20, Limits: view.Bootstrap.Limits, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20}}}
		raw, err := json.Marshal(q)
		require.NoError(t, err)
		qRef := vmReservation(t, client, "initial-platform-qualification", budgetclaims.QualificationKind, q)
		source := budget.QualificationSource{Claim: qRef, SHA256: vmguard.SourceDigest(raw)}
		reader, err := budgetclaims.New(client.CoreV1(), binding())
		require.NoError(t, err)
		observed, err := reader.ReadInitialPlacement(ctx, snapshot, source, 512<<20)
		require.NoError(t, err)
		system, transition, safety := q.Targets[0].Bounds(source)
		_, shared := preparedGuest(t, request, view.Identity)
		i := budget.Initial{ProtocolVersion: 1, VMShrinkPermit: false, Instance: request.Key.Instance, Incarnation: binding().Incarnation, IssuerEpoch: session.Actor().Epoch, Qualification: source, PGAggregate: aggregate, Shared: shared, VMMemoryBytes: 512 << 20, GuestUsableMemoryBytes: 480 << 20, RunnerBirth: q.RunnerBirth, RunnerIdentitySHA256: q.RunnerIdentitySHA256, RunnerPodName: q.RunnerPodName, RunnerPodUID: q.RunnerPodUID, RunnerRestartCount: q.RunnerRestartCount, NodeName: q.NodeName, NodeUID: q.NodeUID, HostProfileID: observed.HostProfileID, HostMemoryBytes: observed.HostMemoryBytes, HostOverheadBytes: observed.HostOverheadBytes, SystemOutsidePG: system, Transition: transition, Safety: safety}
		require.NoError(t, i.Validate())
		require.NoError(t, reader.RecheckInitialBudget(ctx, snapshot, i, observed))
		node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
		require.NoError(t, err)
		pod, err := client.CoreV1().Pods(binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
		require.NoError(t, err)
		qualification, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, qRef.Name, metav1.GetOptions{})
		require.NoError(t, err)
		for _, mode := range []string{"same-name-qualification", "new-pod", "new-node", "underheld", "missing-placement", "changed-profile"} {
			t.Run(mode, func(t *testing.T) {
				changedNode, changedPod, changedQ := node.DeepCopy(), pod.DeepCopy(), qualification.DeepCopy()
				switch mode {
				case "same-name-qualification":
					changedQ.UID = "replacement-qualification"
				case "new-pod":
					changedPod.UID = "replacement-pod"
				case "new-node":
					changedNode.UID = "replacement-node"
				default:
					var ledger hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(changedNode.Annotations[hostguard.LedgerAnnotation]), &ledger))
					if mode == "underheld" {
						(*ledger.Placements)[0].MemoryBytes = 256 << 20
					}
					if mode == "missing-placement" {
						empty := []hostguard.Placement{}
						ledger.Placements = &empty
					}
					if mode == "changed-profile" {
						ledger.Profile.ID = "another-profile"
					}
					raw, err := json.Marshal(ledger)
					require.NoError(t, err)
					changedNode.Annotations[hostguard.LedgerAnnotation] = string(raw)
				}
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), changedNode, ""))
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), changedPod, binding().Namespace))
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changedQ, binding().Namespace))
				_, err := reader.ReadInitialBudget(ctx, snapshot, i)
				require.Error(t, err, "声明数字不替代原来源与原Node持有")
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, binding().Namespace))
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), qualification, binding().Namespace))
			})
		}
		require.NoError(t, reader.RecheckInitialBudget(ctx, snapshot, i, observed))
		require.Nil(t, snapshot.State().Active, "来源读取不能创建一次虚假的Resize")
		return nil
	})
	require.NoError(t, err)
}
