package owner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	coord "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	kt "k8s.io/client-go/testing"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 复用一次真实首次生产，单独验证Host/Scheduler对InitialHeld的消费；PG/
// root/平台/Kube/QMP仍是协议夹具。初始placement保守2GiB、真实VM512MiB，
// 此观察绝不能误退1.5GiB，也不能造Host操作CM或清原资格来假装结清完成。
func verifyInitialHostAndCancel(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, vmCoord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, worker *PGWorker, source budget.QualificationSource, pg *initialServicePG, request control.Request) {
	t.Helper()
	publisher, allocator := actualHostPublisher(t, client, vmCoord, store, session)
	before, err := allocator.Load(ctx)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	qualification, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	wrong := qualification.DeepCopy()
	wrong.UID = "same-name-replacement-initial-qualification"
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), wrong, wrong.Namespace))
	_, err = publisher.SettleCurrent(ctx)
	require.Error(t, err, "初始Host不能因无Active跳过原资格读取")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), qualification, qualification.Namespace))

	s, err := store.Load(ctx)
	require.NoError(t, err)
	initial := s.State().InitialHeld
	s, disposition, err := store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Equal(t, journal.AbortedBeforeDispatch, s.State().History[0].Outcome)
	require.Equal(t, initial, s.State().InitialHeld)
	require.Nil(t, s.State().BudgetBoundary, "不伪造带Resize Key的成功预算关闭证明")
	changed, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.False(t, changed)
	posts, _, resizes := pg.counts()
	require.Equal(t, 1, posts, "零派发取消不重开首次准入")
	require.Zero(t, resizes)

	// 实际Scheduler owner循环必须进入原资格/Runner/Host观察，不能在
	// InitialHeld分支早退，也不能走依赖Resize Request的SettlementFor。
	m := before.State()
	lease, err := client.CoordinationV1().Leases(m.Binding.Namespace).Get(ctx, m.Binding.LeaseName, metav1.GetOptions{})
	require.NoError(t, err)
	lease.Spec = coord.LeaseSpec{HolderIdentity: nil, LeaseDurationSeconds: nil, AcquireTime: nil, RenewTime: nil, LeaseTransitions: nil, Strategy: nil, PreferredHolder: nil}
	require.NoError(t, client.Tracker().Update(coord.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
	var qualificationReads atomic.Int64
	var completedObservation atomic.Bool
	client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if action.(kt.GetAction).GetName() == source.Claim.Name {
			qualificationReads.Add(1)
		}
		return false, nil, nil
	})
	client.PrependReactor("get", "leases", func(action kt.Action) (bool, runtime.Object, error) {
		a := action.(kt.GetAction)
		if a.GetNamespace() != binding().Namespace || a.GetName() != journal.LeaseName(binding()) {
			return false, nil, nil
		}
		lease, err := vmCoord.Leases(binding().Namespace).Get(ctx, a.GetName(), metav1.GetOptions{})
		if err == nil && qualificationReads.Load() >= 2 {
			completedObservation.Store(true)
		}
		return true, lease, err
	})
	pod, err := client.CoreV1().Pods(binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	pod.Spec.SchedulerName = m.Profile.SchedulerName
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	vmClient, err := vmclient.NewForConfig(api.config)
	require.NoError(t, err)
	scheduler, err := hostscheduler.New(client.CoreV1(), client.CoordinationV1(), vmClient, hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: m.Binding, Profile: *m.Profile, SystemBytes: m.OutsideBytes, InitializeAtResourceVersion: ""}}}, m.Profile.SchedulerName)
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	require.NoError(t, scheduler.Run(runCtx, zaptest.NewLogger(t)))
	require.Eventually(t, completedObservation.Load, 5*time.Second, 20*time.Millisecond)
	stop()
	after, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, m.Holds, after.State().Holds)
	require.Equal(t, m.Placements, after.State().Placements)
	require.Equal(t, m.ProtocolVersion, after.State().ProtocolVersion, "初始观察不升级Node结清格式")
	_, err = client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), qualification.Namespace, qualification.Name)
	require.NoError(t, err)
}
