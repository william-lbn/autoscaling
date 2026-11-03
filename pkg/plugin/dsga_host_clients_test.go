package plugin

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coord "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

func TestDSGAHostClientConfigKeepsOriginalConnectionAndFiniteBudgets(t *testing.T) {
	originalLimiter := flowcontrol.NewTokenBucketRateLimiter(1, 1)
	wrapper := func(r http.RoundTripper) http.RoundTripper { return r }
	base := &rest.Config{Host: "https://original.invalid", QPS: 50, Burst: 100,
		RateLimiter: originalLimiter, Timeout: time.Second, UserAgent: "original-agent",
		BearerToken: "OFFLINE-TEST-ONLY", ContentConfig: rest.ContentConfig{ContentType: "application/json"}, DisableCompression: true,
		TLSClientConfig: rest.TLSClientConfig{CAData: []byte("OFFLINE-CA"), CertData: []byte("OFFLINE-CERT"), KeyData: []byte("OFFLINE-KEY"), ServerName: "original.invalid"},
		Transport:       http.DefaultTransport, WrapTransport: wrapper}
	before := rest.CopyConfig(base)
	for _, nodes := range []int{1, 3, 128} {
		work, election, err := newDSGAHostClientConfigs(base, nodes)
		require.NoError(t, err)
		require.NotSame(t, base, work)
		require.NotSame(t, work, election)
		require.Equal(t, float32(200), work.QPS)
		require.Equal(t, 400, work.Burst)
		require.Equal(t, float32(max(10, 2*nodes)), election.QPS)
		require.Equal(t, 2*max(10, 2*nodes), election.Burst)
		for _, got := range []*rest.Config{work, election} {
			require.Nil(t, got.RateLimiter, "必须创建新桶，不能继承原RateLimiter")
			require.Equal(t, base.TLSClientConfig, got.TLSClientConfig)
			require.Same(t, base.Transport, got.Transport)
			require.Equal(t, reflect.ValueOf(base.WrapTransport).Pointer(), reflect.ValueOf(got.WrapTransport).Pointer())
			// 恢复允许变化的三个字段后，逐字段结构必须完全等于原config。
			got.QPS, got.Burst, got.RateLimiter = base.QPS, base.Burst, base.RateLimiter
			unchangedBase, unchangedGot := *base, *got
			unchangedBase.WrapTransport, unchangedGot.WrapTransport = nil, nil
			require.Equal(t, unchangedBase, unchangedGot)
		}
	}
	require.Equal(t, before.QPS, base.QPS)
	require.Equal(t, before.Burst, base.Burst)
	require.Same(t, originalLimiter, base.RateLimiter)
	require.Equal(t, before.TLSClientConfig, base.TLSClientConfig)
	for _, nodes := range []int{0, 129} {
		_, _, err := newDSGAHostClientConfigs(base, nodes)
		require.Error(t, err)
	}
	_, _, err := newDSGAHostClientConfigs(nil, 3)
	require.Error(t, err)
}

// 使用真实TLS HTTP+SDK+原LeaseLock GET/UPDATE，明确证明工作桶耗尽不会
// 消耗选主桶；这是离线组件回归，不代表实际K8s续约或完整启动验收。
func TestDSGAHostElectionHTTPRemainsAvailableWhenWorkBucketIsExhausted(t *testing.T) {
	b := hostguard.Binding{NodeName: "node", NodeUID: "node-uid", Namespace: "testing", LeaseName: "host-lease", LeaseUID: "lease-uid"}
	controller := true
	lease := &coord.Lease{TypeMeta: metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}, ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: b.LeaseName, UID: b.LeaseUID, ResourceVersion: "1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(b)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: b.NodeName, UID: b.NodeUID, Controller: &controller}}}}
	var mu sync.Mutex
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer OFFLINE-TEST-ONLY" || r.URL.Path != "/apis/coordination.k8s.io/v1/namespaces/testing/leases/host-lease" {
			http.Error(w, "unexpected offline request", http.StatusForbidden)
			return
		}
		calls.Add(1)
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPut {
			var next coord.Lease
			if json.NewDecoder(r.Body).Decode(&next) != nil || next.UID != b.LeaseUID || next.ResourceVersion != lease.ResourceVersion {
				http.Error(w, "original identity differs", http.StatusConflict)
				return
			}
			next.ResourceVersion = "2"
			lease = next.DeepCopy()
		} else if r.Method != http.MethodGet {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(lease)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	base := &rest.Config{Host: server.URL, QPS: 1, Burst: 1, RateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), BearerToken: "OFFLINE-TEST-ONLY", ContentConfig: rest.ContentConfig{ContentType: "application/json"}, Timeout: time.Second, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
	// 缩小离线工作桶到每1000秒1token，确定性模拟拥塞，不靠抢5ms补充窗口。
	// 生产200/400及原配置不变由上一root精确检查。
	workConfig, electionConfig, err := newDSGAHostClientConfigs(base, 3)
	require.NoError(t, err)
	workConfig.QPS, workConfig.Burst = 0.001, 1
	work, err := kubernetes.NewForConfig(workConfig)
	require.NoError(t, err)
	election, err := coordclient.NewForConfig(electionConfig)
	require.NoError(t, err)
	limiter := work.CoreV1().RESTClient().GetRateLimiter()
	require.Same(t, limiter, work.CoordinationV1().RESTClient().GetRateLimiter(), "所有工作API只共享工作桶")
	require.NotSame(t, limiter, election.RESTClient().GetRateLimiter())
	require.NotSame(t, base.RateLimiter, limiter)
	// 确定性取消不会发HTTP；工作桶所有可用额度被取走后，其短deadline请求
	// 在本地Wait拒绝。再保留相同耗尽状态，通过独立选主桶完成真正续租。
	for i := 0; i < 10000 && limiter.TryAccept(); i++ {
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = work.CoordinationV1().Leases(b.Namespace).Get(short, b.LeaseName, metav1.GetOptions{})
	require.ErrorContains(t, err, "rate limiter Wait")
	require.ErrorContains(t, err, "would exceed context deadline")
	require.Zero(t, calls.Load(), "被工作桶/已过期ctx拒绝的请求不能到达API")
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	lock, err := hostguard.NewLeaseLock(election, b, "host-owner")
	require.NoError(t, err)
	_, _, err = lock.Get(ctx)
	require.NoError(t, err)
	now := metav1.NewTime(time.Now().UTC().Truncate(time.Microsecond))
	require.NoError(t, lock.Update(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: "host-owner", LeaseDurationSeconds: 15, AcquireTime: now, RenewTime: now, LeaderTransitions: 1}))
	require.EqualValues(t, 2, calls.Load())
	mu.Lock()
	require.Equal(t, "host-owner", *lease.Spec.HolderIdentity)
	require.EqualValues(t, 1, *lease.Spec.LeaseTransitions)
	lease.UID = "replacement-uid"
	mu.Unlock()
	_, _, err = lock.Get(ctx)
	require.ErrorContains(t, err, "identity/binding changed", "分桶不放宽原Lease UID门")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, err = election.Leases(b.Namespace).Get(canceled, b.LeaseName, metav1.GetOptions{})
	require.ErrorIs(t, err, context.Canceled)
}
