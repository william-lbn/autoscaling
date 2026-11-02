package owner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 复用真实Session、生产worker、双HTTP客户端、已完成首次服务和可信来源读者。
// PG/Root/Kube仍为协议peer；本测试只证明自动策略的能力接线，不冒Linux硬限验收。
func TestProductionAutomaticNormalDecreaseRequiresNegotiatedProtocol(t *testing.T) {
	productionInitialService(t, "automatic-normal-eligibility")
}

func verifyAutomaticNormalEligibility(t *testing.T, ctx context.Context, client *kubefake.Clientset, store *journal.Store, session *Session, config compute.Config, resourceConfig resourceguard.ClientConfig, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture) {
	t.Helper()
	original, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, original.State().Active)
	cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var q budget.PlatformQualification
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &q))
	root.mu.Lock()
	held, err := root.view.ConfirmedLimits()
	root.mu.Unlock()
	require.NoError(t, err)
	require.Greater(t, uint64(held.MemoryMaxBytes), uint64(1))
	q.Targets[0].Limits.MemoryMaxBytes = held.MemoryMaxBytes / 2
	makeSource := func(t *testing.T, name string, mutate func(*budget.PlatformQualification)) budget.QualificationSource {
		t.Helper()
		raw, err := json.Marshal(q)
		require.NoError(t, err)
		var next budget.PlatformQualification
		require.NoError(t, json.Unmarshal(raw, &next))
		if mutate != nil {
			mutate(&next)
		}
		require.NoError(t, next.Validate())
		raw, err = json.Marshal(next)
		require.NoError(t, err)
		ref := vmReservation(t, client, name, budgetclaims.QualificationKind, next)
		return budget.QualificationSource{Claim: ref, SHA256: vmguard.SourceDigest(raw)}
	}
	selected := makeSource(t, "automatic-normal-original-qualified", nil)
	cfg := automaticConfig(policy.Target{VMMemoryBytes: q.Targets[0].VMMemoryBytes, Buffers: pg.shared().Observed.HighBuffers}, policy.Target{VMMemoryBytes: q.Targets[1].VMMemoryBytes, Buffers: 1024})
	observe := func(t *testing.T, src budget.QualificationSource, supported bool, want bool) {
		t.Helper()
		root.mu.Lock()
		if supported {
			root.view.NormalQuotaProtocolVersion = 1
		} else {
			root.view.NormalQuotaProtocolVersion = 0
		}
		posts := root.posts
		root.mu.Unlock()
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), src)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		require.NoError(t, worker.ConfigureAutomaticMemory(cfg, client.CoreV1()))
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		sample, err := worker.sampleAutomatic(ctx, snapshot)
		require.NoError(t, err)
		require.Len(t, sample.Eligible, 2)
		require.Equal(t, want, sample.Eligible[0])
		latest, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot.State(), latest.State(), "能力观察不生成Begin或领域Completed")
		root.mu.Lock()
		require.Equal(t, posts, root.posts, "策略观察不写任何硬限")
		root.mu.Unlock()
	}
	require.True(t, t.Run("old-protocol-keeps-lower-normal-ineligible", func(t *testing.T) { observe(t, selected, false, false) }))
	require.True(t, t.Run("negotiated-protocol-includes-original-lower-normal-target", func(t *testing.T) { observe(t, selected, true, true) }))
	for _, test := range []struct {
		name   string
		change func(*budget.PlatformQualification)
	}{
		{"fixed-as-changed", func(q *budget.PlatformQualification) { q.Targets[0].Limits.AddressSpaceMaxBytes = 64 << 20 }},
		{"fixed-task-count-changed", func(q *budget.PlatformQualification) { q.Targets[0].Limits.PidsMaxTasks++ }},
		{"fixed-system-changed", func(q *budget.PlatformQualification) { q.Targets[0].Limits.SystemMemoryMaxBytes-- }},
		{"original-hstate-omitted", func(q *budget.PlatformQualification) {
			q.Targets[0].Limits.HugePools = q.Targets[0].Limits.HugePools[:1]
		}},
	} {
		require.True(t, t.Run(test.name, func(t *testing.T) { observe(t, makeSource(t, "automatic-normal-"+test.name, test.change), true, false) }))
	}
	root.mu.Lock()
	root.view.NormalQuotaProtocolVersion = 0
	root.mu.Unlock()
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, original.State(), latest.State(), "所有样本不改变原日志状态")
}
