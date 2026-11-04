package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type runnerBudgetTransport func(*http.Request) (*http.Response, error)

func (f runnerBudgetTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// 只用原Runner/Host/Outside/VM夹具存储；真实typed REST和client-go限流在其外层运行。
// 不是K8s集群、QEMU物理扩缩或资格证明；Tick/engine/WAL/fsync/两次authorizeOnce为原生产件。
func runnerBudgetRESTFixture(t *testing.T) (*runnerFixture, *memoryFixture, *atomic.Int64) {
	t.Helper()
	f := newRunnerFixture(t)
	c, _ := f.beginGrow(t)
	native := &memoryFixture{dir: f.config.StateDir, actual: Observation{RequestedBytes: c.BeforeBytes, ActualBytes: c.BeforeBytes}, apply: true}
	f.runner.engine.native = native
	count := new(atomic.Int64)
	transport := runnerBudgetTransport(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		count.Add(1)
		var object runtime.Object
		var err error
		kind, group := "", ""
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/") && r.Method == http.MethodGet:
			kind, group = "VirtualMachine", "vm.neon.tech"
			object, err = f.vms.NeonvmV1().VirtualMachines("testing").Get(r.Context(), f.store.Binding().Name, metav1.GetOptions{})
		case strings.HasPrefix(path, "/apis/coordination.k8s.io/v1/namespaces/testing/leases/") && r.Method == http.MethodGet:
			kind, group = "Lease", "coordination.k8s.io"
			object, err = f.core.CoordinationV1().Leases("testing").Get(r.Context(), path[strings.LastIndex(path, "/")+1:], metav1.GetOptions{})
		case strings.HasPrefix(path, "/api/v1/nodes/") && r.Method == http.MethodGet:
			kind = "Node"
			object, err = f.core.CoreV1().Nodes().Get(r.Context(), path[strings.LastIndex(path, "/")+1:], metav1.GetOptions{})
		case strings.HasPrefix(path, "/api/v1/namespaces/testing/pods/") && r.Method == http.MethodGet:
			kind = "Pod"
			object, err = f.core.CoreV1().Pods("testing").Get(r.Context(), path[strings.LastIndex(path, "/")+1:], metav1.GetOptions{})
		case strings.HasPrefix(path, "/api/v1/namespaces/testing/configmaps"):
			kind = "ConfigMap"
			switch r.Method {
			case http.MethodGet:
				object, err = f.core.CoreV1().ConfigMaps("testing").Get(r.Context(), path[strings.LastIndex(path, "/")+1:], metav1.GetOptions{})
			case http.MethodPost, http.MethodPut:
				var cm core.ConfigMap
				err = json.NewDecoder(r.Body).Decode(&cm)
				if err == nil {
					if r.Method == http.MethodPost {
						object, err = f.core.CoreV1().ConfigMaps("testing").Create(r.Context(), &cm, metav1.CreateOptions{})
					} else {
						object, err = f.core.CoreV1().ConfigMaps("testing").Update(r.Context(), &cm, metav1.UpdateOptions{})
					}
				}
			default:
				err = fmt.Errorf("fixture unexpected verb")
			}
		default:
			err = fmt.Errorf("fixture unexpected path")
		}
		status := http.StatusOK
		if err != nil {
			status = http.StatusInternalServerError
			if apierrors.IsNotFound(err) {
				status = http.StatusNotFound
			}
			if apierrors.IsConflict(err) {
				status = http.StatusConflict
			}
			object = &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(status), Reason: metav1.StatusReasonInternalError, Message: "fixed fixture API failure"}
		} else {
			version := "v1"
			object.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: version, Kind: kind})
		}
		raw, e := json.Marshal(object)
		if e != nil {
			return nil, e
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)), Request: r}, nil
	})
	cfg := RunnerClientConfig(&rest.Config{Host: "https://original-fixture.invalid", Transport: transport})
	// 模拟长期轮询已消耗burst，不能依赖新进程初始100个token掩盖可持续QPS缺口。
	for cfg.RateLimiter.TryAccept() {
	}
	realCore, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	realVM, err := vmclient.NewForConfig(cfg)
	require.NoError(t, err)
	store, err := journal.NewStore(realVM.NeonvmV1().VirtualMachines("testing"), f.store.Binding())
	require.NoError(t, err)
	f.runner.store = store
	f.runner.core = realCore.CoreV1()
	f.runner.coord = realCore.CoordinationV1()
	f.runner.claims, err = budgetclaims.New(realCore.CoreV1(), store.Binding())
	require.NoError(t, err)
	return f, native, count
}

func TestRunnerRateBudgetFirstDispatchAndPublication(t *testing.T) {
	f, native, calls := runnerBudgetRESTFixture(t)
	err := f.runner.Tick(context.Background())
	record := readFixtureRecord(t, f.runner.engine)
	if err != nil && record.Pending != nil && record.Pending.Stage == prepared && native.writes == 0 {
		t.Log("real_limiter_second_authority_exhausted_after_durable_prepared_without_QMP")
	}
	require.NoError(t, err)
	require.Equal(t, 1, native.writes)
	require.Nil(t, record.Pending)
	require.NotNil(t, record.Last)
	require.Equal(t, completed, record.Last.Kind)
	require.NotNil(t, record.LastPublished)
	require.GreaterOrEqual(t, calls.Load(), int64(30), "原完整双authority和publish必须真实经过typed REST")
}

func TestRunnerRateBudgetKeepsParentCancellationAndOriginalExpiry(t *testing.T) {
	for _, mode := range []string{"parent-cancel", "expired", "expires-after-prepared"} {
		t.Run(mode, func(t *testing.T) {
			f, native, _ := runnerBudgetRESTFixture(t)
			ctx := context.Background()
			switch mode {
			case "parent-cancel":
				closed, cancel := context.WithCancel(ctx)
				cancel()
				ctx = closed
			case "expired":
				f.runner.engine.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
			case "expires-after-prepared":
				f.runner.engine.now = func() time.Time {
					record := readFixtureRecord(t, f.runner.engine)
					if record.Pending != nil {
						return time.Now().Add(2 * time.Hour)
					}
					return time.Now()
				}
			}
			err := f.runner.Tick(ctx)
			require.Error(t, err)
			require.Zero(t, native.writes)
			record := readFixtureRecord(t, f.runner.engine)
			if mode == "expires-after-prepared" {
				require.NotNil(t, record.Pending)
				require.Equal(t, prepared, record.Pending.Stage)
				// 普通下一Tick只能读回not_sent，不能借较长ctx隐式重放。
				f.runner.engine.now = time.Now
				require.ErrorIs(t, f.runner.Tick(context.Background()), ErrNotSent)
				require.Zero(t, native.writes)
			} else {
				require.Nil(t, record.Pending)
			}
		})
	}
}

func TestRunnerRateBudgetUnknownNeverAutomaticallyResends(t *testing.T) {
	f, native, _ := runnerBudgetRESTFixture(t)
	native.apply = false
	require.ErrorIs(t, f.runner.Tick(context.Background()), ErrUnknown)
	record := readFixtureRecord(t, f.runner.engine)
	require.NotNil(t, record.Pending)
	require.Equal(t, mayHaveSent, record.Pending.Stage)
	require.ErrorIs(t, f.runner.Tick(context.Background()), ErrUnknown)
	require.Equal(t, 1, native.writes)
}

func TestRunnerClientBudgetPreservesBaseAndFiniteSharedLimiter(t *testing.T) {
	old := flowcontrol.NewTokenBucketRateLimiter(3, 7)
	base := &rest.Config{Host: "https://original.invalid", QPS: 3, Burst: 7, RateLimiter: old, Timeout: time.Second, UserAgent: "original"}
	next := RunnerClientConfig(base)
	require.NotSame(t, base, next)
	require.Same(t, old, base.RateLimiter)
	require.Equal(t, float32(3), base.QPS)
	require.Equal(t, 7, base.Burst)
	require.Equal(t, time.Second, base.Timeout)
	require.NotSame(t, old, next.RateLimiter)
	require.Equal(t, float32(50), next.RateLimiter.QPS())
	require.Equal(t, 5*time.Second, next.Timeout)
	require.Equal(t, 15*time.Second, runnerAdvanceTimeout)
	require.Equal(t, 5*time.Second, runnerPublicationTimeout)
	require.False(t, errors.Is(ErrUnknown, ErrNotSent))
}
