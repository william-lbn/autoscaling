package owner

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

// 只在原本机Kube HTTP peer上计真实generated client GET；不把选主续约PUT
// 或其它后台client混入本次authority数量，不替代真实apiserver/RBAC验收。
type authorityScopeGETs struct {
	mu        sync.Mutex
	paths     []string
	beforeGET func(string)
}

func (c *authorityScopeGETs) add(path string) {
	c.mu.Lock()
	c.paths = append(c.paths, path)
	hook := c.beforeGET
	c.mu.Unlock()
	if hook != nil {
		hook(path)
	}
}
func (c *authorityScopeGETs) setBeforeGET(hook func(string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beforeGET = hook
}
func (c *authorityScopeGETs) reset()     { c.mu.Lock(); defer c.mu.Unlock(); c.paths = nil }
func (c *authorityScopeGETs) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.paths) }

type authorityScopeTransport struct {
	base http.RoundTripper
	gets *authorityScopeGETs
}

func (r authorityScopeTransport) RoundTrip(q *http.Request) (*http.Response, error) {
	if q.Method == http.MethodGet {
		r.gets.add(q.URL.Path)
	}
	return r.base.RoundTrip(q)
}
func countAuthorityScopeGETs(t *testing.T, api *ownerAPI, session *Session) *authorityScopeGETs {
	t.Helper()
	gets := new(authorityScopeGETs)
	config := rest.CopyConfig(api.config)
	config.WrapTransport = func(base http.RoundTripper) http.RoundTripper { return authorityScopeTransport{base: base, gets: gets} }
	vms, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	coord, err := coordclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := journal.NewStore(vms.NeonvmV1().VirtualMachines(session.binding.Namespace), session.binding)
	require.NoError(t, err)
	originalStore, originalClient := session.store, session.client
	session.store, session.client = store, coord.Leases(session.binding.Namespace)
	t.Cleanup(func() { session.store, session.client = originalStore, originalClient })
	return gets
}

func TestNativeCallAuthorityScopeCachedTokenOneFreshCheck(t *testing.T) {
	productionInitialService(t, "native-lease-call-scope")
}

// 测试体只调用已存在的recovery helper/Native SDK。旧版本没有可选接口时
// 仍执行原Session.Check，因此RED须真实观测6GET，不能靠缺API编译失败。
func verifyNativeCallScopeLifecycle(t *testing.T, ctx context.Context, transfer *businessOwnerTransfer, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner) {
	t.Helper()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	closed, err := transfer.store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, closed.State().ServiceClosure)
	pg.mu.Lock()
	pg.leaseExpired, pg.leaseDeadline = true, control.Uint64(999*time.Second)
	pg.leaseClock = func() control.Uint64 { return control.Uint64(1000 * time.Second) }
	pg.mu.Unlock()
	session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
	require.NoError(t, runner.Tick(ctx))
	gets := countAuthorityScopeGETs(t, transfer.api, session)
	config = pgConfigWithFence(t, pg.request, func(w http.ResponseWriter, r *http.Request) { pg.serve(t, w, r) }, pg.ownerFence(t), func() control.Snapshot { return pg.shared().Observed })
	config.Token = func(call context.Context) (string, error) {
		var err error
		if scoped, ok := any(session).(interface{ CheckCredentialCall(context.Context) error }); ok {
			err = scoped.CheckCredentialCall(call)
		} else {
			err = session.Check(call)
		}
		return "private-test-token", err
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	snapshot, err := session.store.Load(ctx)
	require.NoError(t, err)
	_, _, _, selected := snapshot.State().ServiceClosure.Selection()
	command := closureControl(session, config.Instance, control.CoordinationRecoverServiceLifecycle, selected)
	call := worker.scopeServiceRecoveryLease(ctx, command, snapshot)
	for n := range 2 {
		gets.reset()
		require.NoError(t, worker.renewAuditedRecoveryLease(call, snapshot))
		require.Equal(t, 6, gets.count(), "helper保留独立Check；原9GET在wrapper去重后为6GET")
		t.Logf("Native call %d actual generated-client authority GETs=%d; no runtime mutation", n+1, gets.count())
	}
}

// 原首次服务/生产worker已完成，随后只观察Native。该测量区不含helper
// 外层审计，旧Token wrapper+provider实际6GET，不能用原helper9GET替代。
func verifyNativeCallScopeCachedToken(t *testing.T, ctx context.Context, static bool, api *ownerAPI, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, core *kubefake.Clientset, source budget.QualificationSource, pg *initialServicePG) {
	t.Helper()
	gets := countAuthorityScopeGETs(t, api, session)
	config = pgConfigWithFence(t, pg.request, func(w http.ResponseWriter, r *http.Request) { pg.serve(t, w, r) }, pg.ownerFence(t), func() control.Snapshot { return pg.shared().Observed })
	if !static {
		config.Token = func(call context.Context) (string, error) {
			var err error
			if scoped, ok := any(session).(interface{ CheckCredentialCall(context.Context) error }); ok {
				err = scoped.CheckCredentialCall(call)
			} else {
				err = session.Check(call)
			}
			return "private-test-token", err
		}
	}
	worker.CloseIdleConnections()
	worker, err := session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	before, err := session.store.Load(ctx)
	require.NoError(t, err)
	for n := range 2 {
		gets.reset()
		fact, err := worker.observeAdmissionLease(ctx)
		require.NoError(t, err)
		require.True(t, fact.Armed)
		require.Equal(t, "observe", fact.Request.Action)
		require.Equal(t, 3, gets.count(), "一条Native HTTP应真实fresh一轮Lease→VM→Lease；原6GET必须RED")
		t.Logf("Native call %d generated-client GETs=%d static_provider=%t", n+1, gets.count(), static)
	}
	after, err := session.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
}
