package owner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

type electionWorkWaits struct {
	flowcontrol.RateLimiter
	entered chan struct{}
}

func (w *electionWorkWaits) Wait(ctx context.Context) error {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	return w.RateLimiter.Wait(ctx)
}

func TestElectionRenewalSurvivesExhaustedBusinessRESTBucket(t *testing.T) {
	api, store, _ := newAPI(t)
	workConfig := rest.CopyConfig(api.config)
	workLimiter := &electionWorkWaits{RateLimiter: flowcontrol.NewTokenBucketRateLimiter(20, 40), entered: make(chan struct{}, 128)}
	workConfig.QPS, workConfig.Burst, workConfig.RateLimiter = 20, 40, workLimiter
	electionConfig := rest.CopyConfig(api.config)
	electionConfig.QPS, electionConfig.Burst = 10, 20
	electionConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(10, 20)
	work, err := coordclient.NewForConfig(workConfig)
	require.NoError(t, err)
	election, err := coordclient.NewForConfig(electionConfig)
	require.NoError(t, err)
	elector, err := NewWithElectionClient(work, election, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		require.Same(t, work, session.coord, "Session and claims must retain business client")
		for len(workLimiter.entered) > 0 {
			<-workLimiter.entered
		}
		for workLimiter.TryAccept() {
		}
		workCtx, stopWork := context.WithCancel(ctx)
		var requests sync.WaitGroup
		var completed atomic.Int32
		defer func() { stopWork(); requests.Wait() }()
		// 真20/40桶排队超过续租deadline；所有请求仍走原typed REST，无fake reactor。
		for i := 0; i < 60; i++ {
			requests.Add(1)
			go func() {
				defer requests.Done()
				_, _ = work.Leases(binding().Namespace).Get(workCtx, LeaseName(binding()), metav1.GetOptions{})
				completed.Add(1)
			}()
		}
		for i := 0; i < 60; i++ {
			select {
			case <-workLimiter.entered:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		checkCtx, stopCheck := context.WithTimeout(ctx, 5*time.Millisecond)
		checkErr := session.Check(checkCtx)
		stopCheck()
		require.Error(t, checkErr, "business Session.Check must remain rate-limited")
		api.mu.Lock()
		before := api.lease.Spec.RenewTime.DeepCopy()
		api.mu.Unlock()
		require.Eventually(t, func() bool {
			api.mu.Lock()
			defer api.mu.Unlock()
			return api.lease.Spec.RenewTime.After(before.Time)
		}, 2*testOptions().RenewDeadline, 10*time.Millisecond, "boundLease must renew through independent actual REST while business waits")
		require.Less(t, completed.Load(), int32(60), "business backlog must still exist when renew succeeds")
		require.NoError(t, session.Context().Err(), "a blocked business check must not cancel owner")
		return nil
	})
	require.NoError(t, err)
}

func TestOwnerElectionClientsPreserveLegacyAndRejectMissingClients(t *testing.T) {
	_, store, client := newAPI(t)
	legacy, err := New(client, store, testOptions())
	require.NoError(t, err)
	require.NotNil(t, legacy.electionClient)
	require.Same(t, client, legacy.coord)
	_, err = NewWithElectionClient(nil, client, store, testOptions())
	require.Error(t, err)
	_, err = NewWithElectionClient(client, nil, store, testOptions())
	require.Error(t, err)
	_, err = NewWithElectionClient(client, client, nil, testOptions())
	require.Error(t, err)
}
