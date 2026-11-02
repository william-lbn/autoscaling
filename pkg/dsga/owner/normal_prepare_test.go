package owner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 真生产Prepare从新原资格读出与Held相等的N；不手填Prepared/Completed，
// 只检查原CAS持久接口选择。Root/PG/Kube物理域仍为显式协议peer。
func TestProductionPrepareRetainsNormalProtocolAtEqualLimit(t *testing.T) {
	for _, mode := range []string{"normal-prepare-equal-legacy", "normal-prepare-equal-supported"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, mode) })
	}
}

func verifyProductionNormalPrepareEqualLimit(t *testing.T, ctx context.Context, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, supported bool) {
	t.Helper()
	cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var q budget.PlatformQualification
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &q))
	root.mu.Lock()
	held, err := root.view.ConfirmedLimits()
	root.mu.Unlock()
	require.NoError(t, err)
	request := pg.request
	for i := range q.Targets {
		if q.Targets[i].VMMemoryBytes == request.TargetVMMemoryBytes {
			q.Targets[i].Limits.MemoryMaxBytes = held.MemoryMaxBytes
		}
	}
	require.NoError(t, q.Validate())
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	ref := vmReservation(t, client, "prepare-same-normal-original-qualified", budgetclaims.QualificationKind, q)
	selected := budget.QualificationSource{Claim: ref, SHA256: vmguard.SourceDigest(raw)}
	worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), selected)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	_, disposition, err := store.Begin(ctx, snapshot, session.Actor(), request, uint64(time.Now().UnixMilli()))
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	step := budgetStep(t, store, journal.PrepareBudget)
	require.Equal(t, journal.Settled, step.Phase)
	require.Equal(t, journal.Completed, step.Observation.Result)
	var prepared budget.Prepared
	require.NoError(t, json.Unmarshal(step.Observation.Data, &prepared))
	require.NoError(t, prepared.ValidateFor(request, q.Identity))
	require.Equal(t, held.MemoryMaxBytes, prepared.Limits.MemoryMaxBytes)
	if supported {
		require.EqualValues(t, 1, prepared.NormalQuotaProtocolVersion)
		require.NotNil(t, prepared.NormalQuotaFence)
		require.Equal(t, control.Fenced, prepared.NormalQuotaFence.Disposition)
		require.Equal(t, prepared.Shared.Observed, prepared.NormalQuotaFence.Observed)
	} else {
		require.Zero(t, prepared.NormalQuotaProtocolVersion)
		require.Nil(t, prepared.NormalQuotaFence)
	}
	root.mu.Lock()
	require.Zero(t, root.posts, "Prepare不能写Root硬限")
	root.mu.Unlock()
	pg.mu.Lock()
	require.Zero(t, pg.resizePosts, "Prepare不能写PG容量")
	pg.mu.Unlock()
}
