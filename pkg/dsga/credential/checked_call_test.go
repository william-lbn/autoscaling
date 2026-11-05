package credential

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 这里只验证既有credential缓存/刷新如何调用可选Authority；真实Session私有
// scope的绑定、fresh GET与撤销由owner包的真实generated-client组合独立验证。
type checkedCallAuthorityFixture struct {
	Authority
	checked atomic.Int32
	entered chan struct{}
}

func (a *checkedCallAuthorityFixture) CheckCredentialCall(ctx context.Context) error {
	a.checked.Add(1)
	if a.entered != nil {
		select {
		case a.entered <- struct{}{}:
		default:
		}
	}
	return a.Authority.Check(ctx)
}

func TestCredentialCheckedCallCachedHitAndRefreshFallback(t *testing.T) {
	_, original, _, client, _ := fixture(t, false)
	checked := &checkedCallAuthorityFixture{Authority: original}
	client.authority = checked
	token, err := client.Token(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.EqualValues(t, 2, checked.checked.Load(), "miss仍在真实fetch后再检查")
	checked.checked.Store(0)
	_, err = client.Token(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, checked.checked.Load(), "hit仍调用Authority，不能直接拿旧grant授权")
	original.mu.Lock()
	original.denied = true
	original.mu.Unlock()
	_, err = client.Token(context.Background())
	require.Error(t, err)
	require.NotContains(t, err.Error(), token)
}

func TestCredentialCheckedCallRefreshWaitRechecksAuthority(t *testing.T) {
	issuer, original, _, client, clock := fixture(t, false)
	checked := &checkedCallAuthorityFixture{Authority: original, entered: make(chan struct{}, 32)}
	client.authority = checked
	_, err := client.Token(context.Background())
	require.NoError(t, err)
	for len(checked.entered) != 0 {
		<-checked.entered
	}
	issuer.mu.Lock()
	issuer.block = true
	clock.Add(int64(55 * time.Second))
	issuer.reply.ExpiresAt = time.Unix(0, clock.Load()).Add(time.Minute).Unix()
	issuer.mu.Unlock()
	result := make(chan error, 2)
	go func() { _, err := client.Token(context.Background()); result <- err }()
	select {
	case <-issuer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("leader did not enter original issuer")
	}
	<-checked.entered
	go func() { _, err := client.Token(context.Background()); result <- err }()
	select {
	case <-checked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not check Authority")
	}
	original.mu.Lock()
	original.denied = true
	original.mu.Unlock()
	close(issuer.release)
	for range 2 {
		select {
		case err := <-result:
			require.Error(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("refresh/wait did not return")
		}
	}
	require.GreaterOrEqual(t, checked.checked.Load(), int32(6), "初mint两次+refresh/wait各自前后重新检查")
}
