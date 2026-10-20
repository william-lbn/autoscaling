package owner

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/rest"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

type proofRoundTripper func(*http.Request) (*http.Response, error)

func (roundtrip proofRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundtrip(request)
}

func TestVerifyCurrentRejectsLeaseReplacementDuringJournalRead(t *testing.T) {
	api, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		config := rest.CopyConfig(api.config)
		config.WrapTransport = func(base http.RoundTripper) http.RoundTripper {
			return proofRoundTripper(func(request *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(request)
				if err == nil && request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/apis/vm.neon.tech/") {
					// 精确注入在首次Lease检查之后、第二次检查之前；新Lease
					// 故意沿用相同holder，不能只比较holder忽略对象UID。
					api.mu.Lock()
					api.lease.UID = "lease-recreated-during-read"
					api.mu.Unlock()
				}
				return response, err
			})
		}
		client, err := vmclient.NewForConfig(config)
		require.NoError(t, err)
		wrappedStore, err := journal.NewStore(client.NeonvmV1().VirtualMachines("testing"), binding())
		require.NoError(t, err)
		_, _, err = VerifyCurrent(ctx, coord, wrappedStore, session.Ownership())
		require.ErrorIs(t, err, ErrLeadershipLost)
		api.mu.Lock()
		api.lease.UID = session.leaseUID
		api.mu.Unlock()
		return nil
	})
	require.NoError(t, err)
}
