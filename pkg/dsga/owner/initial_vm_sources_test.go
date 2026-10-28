package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 同一完整生产组合的两个寿命点直接调用生产Reader。故障只改显式Kube
// 发布夹具并恢复原对象，不重新初始化boot、不制造真实执行许可或QMP效果。
// pending/last采用自洽的领域历史，并先经Inspect认证，避免仅靠畸形JSON
// 触发拒绝而漏测“无Pending但曾经执行”的关键不变量。
func verifyInitialVMSourceRejection(t *testing.T, ctx context.Context, client *kubefake.Clientset, store *journal.Store, session *Session, initial budget.Initial) {
	t.Helper()
	// owner夹具的实际Lease UID由可信Session固定，不能假定固定文字。
	reader, err := vmguard.NewReader(client.CoreV1(), store.Binding(), string(session.Ownership().LeaseUID))
	require.NoError(t, err)
	proof, err := reader.InitialCapacityFor(ctx, initial, session.Actor())
	require.NoError(t, err)
	cm, err := client.CoreV1().ConfigMaps(store.Binding().Namespace).Get(ctx, proof.Sources.StateClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	for _, mode := range []string{"State-UID", "requested", "actual", "holder", "floor", "pending", "last-not-sent", "last-completed"} {
		bad := cm.DeepCopy()
		var pub vmguard.Publication
		require.NoError(t, json.Unmarshal([]byte(cm.Data[vmguard.ClaimKey]), &pub))
		switch mode {
		case "State-UID":
			bad.UID += "-replacement"
		case "requested":
			pub.Actual.RequestedBytes += proof.Birth.Identity.BlockBytes
		case "actual":
			pub.Actual.ActualBytes += proof.Birth.Identity.BlockBytes
		case "holder":
			pub.State.Floor.HolderID += "-replacement"
		case "floor":
			pub.State.Floor.Epoch++
		default:
			s, err := store.Load(ctx)
			require.NoError(t, err)
			var request control.Request
			if s.State().Active != nil {
				request = s.State().Active.Request
			} else {
				request = s.State().History[0].Operation.Request
			}
			if mode == "last-completed" {
				request.TargetVMMemoryBytes = initial.VMMemoryBytes
			}
			command := vmguard.Command{ProtocolVersion: 1, Identity: proof.Birth.Identity, BindingClaim: initial.RunnerBirth, HostClaim: budget.ObjectRef{Name: "explicit-history-host", UID: "explicit-history-host-uid"}, Request: request, BeforeBytes: initial.VMMemoryBytes, PreparedSHA256: strings.Repeat("a", 64), HostSHA256: strings.Repeat("b", 64), GuestSHA256: ""}
			require.NoError(t, command.Validate())
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			intent := map[string]any{"command": command, "command_sha256": vmguard.SourceDigest(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...)), "journal_sha256": strings.Repeat("c", 64), "owner": session.Actor(), "stage": "prepared", "replay_nonce": "", "before": proof.Actual}
			var state map[string]any
			raw, err = json.Marshal(pub.State)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &state))
			state["high_water"] = request.Key
			if mode == "pending" {
				state["pending"] = intent
			} else {
				kind := "not_sent"
				if mode == "last-completed" {
					kind = "completed"
				}
				state["last"] = map[string]any{"intent": intent, "kind": kind, "actual": proof.Actual}
			}
			raw, err = json.Marshal(state)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, pub.State))
		}
		raw, err := json.Marshal(pub)
		require.NoError(t, err)
		bad.Data[vmguard.ClaimKey] = string(raw)
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), bad, bad.Namespace))
		if strings.HasPrefix(mode, "last-") || mode == "pending" {
			_, _, _, err = reader.Inspect(ctx, &initial.RunnerBirth, session.Actor())
			require.NoError(t, err, "故障历史自洽，不能只测JSON损坏：%s", mode)
		}
		_, err = reader.CheckInitialCapacity(ctx, proof.Sources, initial, session.Actor())
		require.Error(t, err, mode)
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm.DeepCopy(), cm.Namespace))
	}
	_, err = reader.CheckInitialCapacity(ctx, proof.Sources, initial, session.Actor())
	require.NoError(t, err, "只恢复准确原对象即可重读，不丢弃原持有")
	wrong := proof.Sources
	wrong.StateClaim.UID += "-replacement"
	_, err = reader.CheckInitialCapacity(ctx, wrong, initial, session.Actor())
	require.Error(t, err)
	wrong = proof.Sources
	wrong.IdentitySHA256 = strings.Repeat("d", 64)
	_, err = reader.CheckInitialCapacity(ctx, wrong, initial, session.Actor())
	require.Error(t, err)
}
