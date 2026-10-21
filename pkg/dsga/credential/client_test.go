package credential

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

// Authority仅为调用方生命周期夹具；它不能替代服务端真实TokenReview/Lease/
// journal。本包测试使用实际TLS/HTTP/投影文件，中心的真实核验另有跨仓测试。
type testAuthority struct {
	ctx    context.Context
	mu     sync.Mutex
	proof  owner.Ownership
	denied bool
	block  bool
}

func (a *testAuthority) Context() context.Context   { return a.ctx }
func (a *testAuthority) Ownership() owner.Ownership { a.mu.Lock(); defer a.mu.Unlock(); return a.proof }
func (a *testAuthority) Check(ctx context.Context) error {
	a.mu.Lock()
	denied, block := a.denied, a.block
	a.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if denied {
		return errors.New("private authority detail")
	}
	return nil
}

type issuerFixture struct {
	mu        sync.Mutex
	reply     grant
	calls     int
	projected string
	valid     bool
	status    int
	noStore   bool
	extra     bool
	block     bool
	entered   chan struct{}
	release   chan struct{}
}

func (f *issuerFixture) serve(w http.ResponseWriter, r *http.Request) {
	var proof owner.Ownership
	err := json.NewDecoder(r.Body).Decode(&proof)
	_, _ = io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	f.calls++
	f.valid = f.valid && err == nil && proof == testProof() && r.Header.Get("Authorization") == "Bearer "+f.projected && r.Method == http.MethodPost && r.URL.Path == "/computes/"+testProof().VMName+"/dsga-credentials" && r.TLS.ServerName == testProof().VMName+"-management.testing.svc"
	reply, status, noStore, extra, block := f.reply, f.status, f.noStore, f.extra, f.block
	f.mu.Unlock()
	if block {
		select {
		case f.entered <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
			return
		case <-f.release:
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if noStore {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(status)
	if extra {
		data, _ := json.Marshal(reply)
		_, _ = w.Write([]byte(strings.TrimSuffix(string(data), "}") + `,"unexpected":true}`))
		return
	}
	_ = json.NewEncoder(w).Encode(reply)
}

func testProof() owner.Ownership {
	return owner.Ownership{Namespace: "testing", VMName: "cp-0123456789abcdef", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32), LeaseUID: "lease-uid", HolderID: "dsga-" + strings.Repeat("b", 32) + ".pod-uid", IssuerEpoch: 2}
}

func fixture(t *testing.T, guest bool) (*issuerFixture, *testAuthority, context.CancelFunc, *Client, *atomic.Int64) {
	t.Helper()
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	a := &testAuthority{ctx: rootCtx, mu: sync.Mutex{}, proof: testProof(), denied: false, block: false}
	f := &issuerFixture{mu: sync.Mutex{}, reply: grant{TokenType: "Bearer", AccessToken: "test-only-signed-token", ExpiresAt: time.Now().Add(time.Minute).Unix(), ComputeID: testProof().VMName, VMUID: string(testProof().VMUID), ComputeInstanceID: "compute-1", IssuerEpoch: 2}, calls: 0, projected: "projected-SA-token", valid: true, status: 200, noStore: true, extra: false, block: false, entered: make(chan struct{}, 1), release: make(chan struct{})}
	_, rootKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	ca := new(x509.Certificate)
	ca.SerialNumber, ca.NotBefore, ca.NotAfter = big.NewInt(1), now.Add(-time.Hour), now.Add(time.Hour)
	ca.IsCA, ca.BasicConstraintsValid, ca.KeyUsage = true, true, x509.KeyUsageCertSign
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, rootKey.Public(), rootKey)
	require.NoError(t, err)
	ca, err = x509.ParseCertificate(caDER)
	require.NoError(t, err)
	names := []string{"*.testing.svc"}
	if guest {
		names = []string{testProof().VMName + "-management.testing.svc"}
	}
	leaf := new(x509.Certificate)
	leaf.SerialNumber, leaf.NotBefore, leaf.NotAfter = big.NewInt(2), now.Add(-time.Hour), now.Add(time.Hour)
	leaf.DNSNames, leaf.KeyUsage, leaf.ExtKeyUsage = names, x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	certDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, rootKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: nil, Bytes: certDER})
	private := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Headers: nil, Bytes: keyDER})
	pair, err := tls.X509KeyPair(cert, private)
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	server.StartTLS()
	t.Cleanup(server.Close)
	dir := t.TempDir()
	caFile, tokenFile := filepath.Join(dir, "management-ca.crt"), filepath.Join(dir, "projected-token")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: nil, Bytes: caDER}), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte(f.projected), 0o600))
	client, err := New(a, Config{BaseURL: server.URL, CAFile: caFile, TokenFile: tokenFile, Timeout: 2 * time.Second})
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	clock := new(atomic.Int64)
	clock.Store(now.UnixNano())
	client.now = func() time.Time { return time.Unix(0, clock.Load()) }
	return f, a, cancel, client, clock
}

func TestConcurrentCallbacksShareMintAndRefreshProjectedFile(t *testing.T) {
	f, _, _, client, clock := fixture(t, false)
	var workers sync.WaitGroup
	for i := 0; i < 24; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			token, err := client.Token(context.Background())
			require.NoError(t, err)
			require.Equal(t, "test-only-signed-token", token)
		}()
	}
	workers.Wait()
	f.mu.Lock()
	require.Equal(t, 1, f.calls)
	require.True(t, f.valid)
	f.projected = "rotated-SA-token"
	f.reply.AccessToken = "second-short-token"
	clock.Add(int64(55 * time.Second))
	f.reply.ExpiresAt = time.Unix(0, clock.Load()).Add(time.Minute).Unix()
	f.mu.Unlock()
	require.NoError(t, os.WriteFile(client.config.TokenFile, []byte("rotated-SA-token\n"), 0o600))
	token, err := client.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "second-short-token", token)
	identity, err := client.Identity(context.Background())
	require.NoError(t, err)
	require.Equal(t, "compute-1", identity.ComputeInstanceID)
	f.mu.Lock()
	require.Equal(t, 2, f.calls)
	require.True(t, f.valid)
	f.mu.Unlock()
	require.NotContains(t, fmt.Sprintf("%+v %#v", client, client), token)
}

func TestGuestCertificateRejectedBeforeProjectedBearerSent(t *testing.T) {
	f, _, _, client, _ := fixture(t, true)
	_, err := client.Token(context.Background())
	require.Error(t, err)
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.calls, "chain/hostname-valid Guest leaf must still fail before sending the SA header")
}

func TestGrantRejectsWrongBindingSchemaExpiryAndNoCache(t *testing.T) {
	for _, name := range []string{"vm", "compute", "epoch", "instance", "expired", "unbounded", "unknown-field", "cacheable", "http-error"} {
		t.Run(name, func(t *testing.T) {
			f, _, _, client, clock := fixture(t, false)
			f.mu.Lock()
			switch name {
			case "vm":
				f.reply.VMUID = "other-vm"
			case "compute":
				f.reply.ComputeID = "other"
			case "epoch":
				f.reply.IssuerEpoch = 3
			case "instance":
				f.reply.ComputeInstanceID = ""
			case "expired":
				f.reply.ExpiresAt = time.Unix(0, clock.Load()).Add(5 * time.Second).Unix()
			case "unbounded":
				f.reply.ExpiresAt = time.Unix(0, clock.Load()).Add(time.Hour).Unix()
			case "unknown-field":
				f.extra = true
			case "cacheable":
				f.noStore = false
			case "http-error":
				f.status = 403
			}
			f.mu.Unlock()
			_, err := client.Token(context.Background())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "projected-SA-token")
			require.NotContains(t, err.Error(), "test-only-signed-token")
		})
	}
}

func TestComputeIdentityChangePermanentlyClosesThisClient(t *testing.T) {
	f, _, _, client, clock := fixture(t, false)
	_, err := client.Token(context.Background())
	require.NoError(t, err)
	clock.Add(int64(55 * time.Second))
	f.mu.Lock()
	f.reply.ExpiresAt = time.Unix(0, clock.Load()).Add(time.Minute).Unix()
	f.reply.ComputeInstanceID = "compute-2"
	f.mu.Unlock()
	_, err = client.Token(context.Background())
	require.ErrorContains(t, err, "compute_instance_changed")
	f.mu.Lock()
	f.reply.ComputeInstanceID = "compute-1"
	calls := f.calls
	f.mu.Unlock()
	_, err = client.Token(context.Background())
	require.ErrorContains(t, err, "compute_instance_changed")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, calls, f.calls)
}

func TestOwnerCancellationStopsCacheAndInFlightMint(t *testing.T) {
	t.Run("cached", func(t *testing.T) {
		f, _, cancel, client, _ := fixture(t, false)
		_, err := client.Token(context.Background())
		require.NoError(t, err)
		cancel()
		_, err = client.Token(context.Background())
		require.Error(t, err)
		f.mu.Lock()
		defer f.mu.Unlock()
		require.Equal(t, 1, f.calls)
	})
	t.Run("in-flight", func(t *testing.T) {
		f, _, cancel, client, _ := fixture(t, false)
		f.mu.Lock()
		f.block = true
		f.mu.Unlock()
		finished := make(chan error, 1)
		go func() { _, err := client.Token(context.Background()); finished <- err }()
		select {
		case <-f.entered:
		case <-time.After(time.Second):
			t.Fatal("mint did not enter")
		}
		cancel()
		select {
		case err := <-finished:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("owner cancellation did not reach real HTTP")
		}
	})
}

func TestAuthorityAndWaiterDeadlinesAreIndependent(t *testing.T) {
	t.Run("authority", func(t *testing.T) {
		f, a, _, client, _ := fixture(t, false)
		client.config.Timeout = 50 * time.Millisecond
		a.mu.Lock()
		a.block = true
		a.mu.Unlock()
		start := time.Now()
		_, err := client.Token(context.Background())
		require.Error(t, err)
		require.Less(t, time.Since(start), time.Second)
		f.mu.Lock()
		defer f.mu.Unlock()
		require.Zero(t, f.calls)
	})
	t.Run("waiter", func(t *testing.T) {
		f, _, _, client, _ := fixture(t, false)
		f.mu.Lock()
		f.block = true
		f.mu.Unlock()
		leader := make(chan error, 1)
		go func() { _, err := client.Token(context.Background()); leader <- err }()
		<-f.entered
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		_, err := client.Token(ctx)
		require.Error(t, err)
		close(f.release)
		require.NoError(t, <-leader)
		f.mu.Lock()
		defer f.mu.Unlock()
		require.Equal(t, 1, f.calls)
	})
}

func TestChangedAuthorityNeverReturnsCachedToken(t *testing.T) {
	_, a, _, client, _ := fixture(t, false)
	_, err := client.Token(context.Background())
	require.NoError(t, err)
	a.mu.Lock()
	a.denied = true
	a.mu.Unlock()
	_, err = client.Token(context.Background())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private authority detail")
}
