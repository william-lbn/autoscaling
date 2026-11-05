package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestAgentClientConfigsPreserveOrdinaryAndBoundDSGA(t *testing.T) {
	originalLimiter := flowcontrol.NewTokenBucketRateLimiter(3, 7)
	base := &rest.Config{Host: "http://127.0.0.1", QPS: 3, Burst: 7, RateLimiter: originalLimiter, Timeout: 9 * time.Second}
	work, election, err := agentClientConfigs(base, false)
	require.NoError(t, err)
	require.Nil(t, election)
	require.Equal(t, base, work)
	require.NotSame(t, base, work)
	require.Same(t, originalLimiter, work.RateLimiter)
	work, election, err = agentClientConfigs(base, true)
	require.NoError(t, err)
	require.Equal(t, float32(20), work.QPS)
	require.Equal(t, 40, work.Burst)
	require.Equal(t, float32(10), election.QPS)
	require.Equal(t, 20, election.Burst)
	require.NotSame(t, work.RateLimiter, election.RateLimiter)
	require.NotSame(t, originalLimiter, work.RateLimiter)
	require.NotSame(t, originalLimiter, election.RateLimiter)
	require.Same(t, originalLimiter, base.RateLimiter)
	require.Equal(t, float32(3), base.QPS)
	require.Equal(t, 7, base.Burst)
	require.Equal(t, base.Timeout, work.Timeout)
	require.Equal(t, base.Timeout, election.Timeout)
	_, _, err = agentClientConfigs(nil, true)
	require.Error(t, err)
}

func TestAgentClientsShareBoundedWorkButIsolateElectionREST(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&coordination.Lease{TypeMeta: metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}, ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "testing", UID: "owner-uid", ResourceVersion: "1"}})
	}))
	defer server.Close()
	work, election, err := agentClientConfigs(&rest.Config{Host: server.URL}, true)
	require.NoError(t, err)
	core, err := kubernetes.NewForConfig(work)
	require.NoError(t, err)
	vm, err := vmclient.NewForConfig(work)
	require.NoError(t, err)
	leader, err := coordclient.NewForConfig(election)
	require.NoError(t, err)
	require.Same(t, work.RateLimiter, core.CoreV1().RESTClient().GetRateLimiter())
	require.Same(t, work.RateLimiter, core.CoordinationV1().RESTClient().GetRateLimiter())
	require.Same(t, work.RateLimiter, vm.NeonvmV1().RESTClient().GetRateLimiter())
	require.Same(t, election.RateLimiter, leader.RESTClient().GetRateLimiter())
	for i := 0; i < 40; i++ {
		require.True(t, work.RateLimiter.TryAccept())
	}
	require.False(t, work.RateLimiter.TryAccept(), "business budget must remain finite")
	blocked, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = core.CoreV1().Namespaces().Get(blocked, "testing", metav1.GetOptions{})
	require.Error(t, err)
	require.Zero(t, calls.Load(), "rate-limited business call must not reach REST server")
	ctx, cancelElection := context.WithTimeout(context.Background(), time.Second)
	defer cancelElection()
	_, err = leader.Leases("testing").Get(ctx, "owner", metav1.GetOptions{})
	require.NoError(t, err, "an exhausted business bucket must not block election REST")
	require.Equal(t, int32(1), calls.Load())
}
