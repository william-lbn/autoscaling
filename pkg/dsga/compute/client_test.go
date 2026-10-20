package compute

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func fixtures(t testing.TB) (control.Request, control.ControlResponse) {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var request control.Request
	require.NoError(t, json.Unmarshal(raw, &request))
	raw, err = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	require.NoError(t, fact.ValidateResize(request, request.Key.Instance))
	return request, fact
}

func reply(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// 真实TLS/HTTP连接测试，服务端是协议夹具，不伪装成已验签Compute或真实PG。
func newTLSClient(t *testing.T, handler http.HandlerFunc) (*Client, Config) {
	t.Helper()
	request, _ := fixtures(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-test-token" || r.Header.Get("Cache-Control") != "no-store" {
			http.Error(w, "invalid test authorization/header", http.StatusForbidden)
			return
		}
		if r.URL.Path == "/dsg/v1/capabilities" {
			history := uint32(16)
			provider := false
			boot := request.Key.Instance.PGBootID
			reply(w, control.Capabilities{
				ProtocolVersion: 1, ProductionReady: false, VMShrinkPermit: false, Supported: true, BufferAPIVersion: 5, Enabled: true, InstanceBound: true,
				HistoryCapacity: &history, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: request.Key.Instance.VMUID, ComputeInstanceID: request.Key.Instance.ComputeInstanceID, PGBootID: &boot},
			})
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	config := Config{BaseURL: server.URL, Instance: request.Key.Instance, IssuerEpoch: request.Key.IssuerEpoch, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "private-test-token", nil }, Pending: nil}
	client, err := New(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	return client, config
}

func TestExecutionAndObservationUseIndependentPoolsAndKeepFence(t *testing.T) {
	intent, terminal := fixtures(t)
	started := make(chan struct{})
	release := make(chan struct{})
	stop := sync.OnceFunc(func() { close(release) })
	defer stop()
	client, _ := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/resize":
			close(started)
			<-release
			reply(w, terminal)
		case "/dsg/v1/resize/status", "/dsg/v1/resize/cancel":
			_, fact := fixtures(t)
			fact.Receipt.Result.Operation.Phase = control.Preparing
			fact.Receipt.Result.Operation.CancelClosed = false
			fact.Receipt.Result.CapacityGeneration = 1
			fact.Receipt.Result.HighBuffers = 512
			fact.Receipt.Result.LowBuffers = 512
			if strings.HasSuffix(r.URL.Path, "/cancel") {
				fact.Receipt.Disposition = control.ReceiptCancelRequested
				fact.Receipt.Result.Operation.CancelRequested = true
			}
			reply(w, fact)
		default:
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	})
	done := make(chan error, 1)
	go func() { _, err := client.Resize(context.Background(), intent); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not reach TLS server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	query := control.Query{ProtocolVersion: 1, Key: intent.Key}
	_, err := client.Status(ctx, query)
	require.NoError(t, err)
	_, err = client.Cancel(ctx, query)
	require.NoError(t, err)
	require.Equal(t, &intent, client.Pending(), "observation/cancellation must not clear an active execution fence")
	_, err = client.Resize(ctx, intent)
	require.Error(t, err, "even an identical call cannot overlap in flight")
	// 先检验独立观察，再结束真实执行请求；即使断言失败也在defer释放服务端。
	stop()
	require.NoError(t, <-done)
	require.Nil(t, client.Pending())
}

func TestLostResponsePreservesExactIntentUntilExplicitReplay(t *testing.T) {
	intent, fact := fixtures(t)
	var calls atomic.Int32
	client, _ := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		got, err := control.DecodeRequest(raw)
		if err != nil || got != intent {
			http.Error(w, "intent changed", http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 {
			http.Error(w, "effect occurred but response lost", http.StatusServiceUnavailable)
			return
		}
		reply(w, fact)
	})
	_, err := client.Resize(context.Background(), intent)
	var callErr *CallError
	require.ErrorAs(t, err, &callErr)
	require.True(t, callErr.MayHaveExecuted)
	require.EqualValues(t, 1, calls.Load(), "no transparent POST replay")
	changed := intent
	changed.BudgetCommitmentID += "-changed"
	_, err = client.Resize(context.Background(), changed)
	require.ErrorAs(t, err, &callErr)
	require.False(t, callErr.MayHaveExecuted)
	require.EqualValues(t, 1, calls.Load())
	view := client.Pending()
	view.TargetBuffers++
	require.Equal(t, &intent, client.Pending())
	_, err = client.Resize(context.Background(), intent)
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
	require.Nil(t, client.Pending())
}

func TestInvalidHeadersBodiesAndFactsRemainUnconfirmed(t *testing.T) {
	intent, _ := fixtures(t)
	for _, kind := range []string{"header", "size", "duplicate", "null", "permit", "wrong_key", "wrong_action", "wrong_context"} {
		t.Run(kind, func(t *testing.T) {
			client, _ := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) {
				_, fact := fixtures(t)
				switch kind {
				case "permit":
					fact.VMShrinkPermit = true
				case "wrong_key":
					fact.Receipt.Key.Revision++
				case "wrong_action":
					fact.Receipt.Disposition = control.ReceiptTooLate
				case "wrong_context":
					fact.Receipt.Result.Operation.ContextSHA256 = strings.Repeat("b", 64)
				}
				raw, err := json.Marshal(fact)
				if err != nil {
					panic(err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				switch kind {
				case "header":
					w.Header().Del("Cache-Control")
				case "size":
					raw = append([]byte(strings.Repeat(" ", maxResponseBytes)), raw...)
				case "duplicate":
					raw = append([]byte(`{"protocol_version":1,`), raw[1:]...)
				case "null":
					raw = []byte(strings.Replace(string(raw), `"vm_shrink_permit":false`, `"vm_shrink_permit":null`, 1))
				}
				_, _ = w.Write(raw)
			})
			_, err := client.Resize(context.Background(), intent)
			var callErr *CallError
			require.ErrorAs(t, err, &callErr)
			require.True(t, callErr.MayHaveExecuted)
			require.Equal(t, &intent, client.Pending())
		})
	}
}

func TestRedirectNeverReplaysOrLeaksCredential(t *testing.T) {
	intent, _ := fixtures(t)
	var leaked atomic.Int32
	other := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer other.Close()
	client, _ := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", other.URL+"/dsg/v1/resize")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	_, err := client.Resize(context.Background(), intent)
	require.Error(t, err)
	require.Zero(t, leaked.Load())
	require.Equal(t, &intent, client.Pending())
}

func TestRestartRestoresPendingWithoutChangingOriginalOrAcceptingNewIssuerExecution(t *testing.T) {
	intent, terminal := fixtures(t)
	client, config := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) { reply(w, terminal) })
	client.CloseIdleConnections()
	config.IssuerEpoch++
	config.Pending = &intent
	restored, err := New(context.Background(), config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	_, err = restored.Resize(context.Background(), intent)
	require.Error(t, err, "new owner cannot execute old epoch")
	_, err = restored.Status(context.Background(), control.Query{ProtocolVersion: 1, Key: intent.Key})
	require.NoError(t, err)
	require.Equal(t, &intent, restored.Pending(), "a query alone does not resolve the external journal")
}

func TestLocalConfigurationAndCredentialFailuresDoNotSendOrLeakSecrets(t *testing.T) {
	intent, terminal := fixtures(t)
	client, config := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) { reply(w, terminal) })
	for _, endpoint := range []string{"http://127.0.0.1", "https://user:secret@example.com", "https://example.com/?secret=1", "https://example.com/#fragment", "https://example.com/not-root"} {
		invalid := config
		invalid.BaseURL = endpoint
		_, err := New(context.Background(), invalid)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	client.token = func(context.Context) (string, error) { return "", errors.New("private secret credential body") }
	_, err := client.Resize(context.Background(), intent)
	var callErr *CallError
	require.ErrorAs(t, err, &callErr)
	require.False(t, callErr.MayHaveExecuted)
	require.NotContains(t, err.Error(), "private")
	require.Nil(t, client.Pending())
}

func TestContextCancellationDoesNotCancelRemoteEffectOrPermitNewIntent(t *testing.T) {
	intent, terminal := fixtures(t)
	started := make(chan struct{})
	finish := make(chan struct{})
	completed := make(chan struct{})
	stop := sync.OnceFunc(func() { close(finish) })
	defer stop()
	var calls atomic.Int32
	client, _ := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-finish
			// 请求context结束仍可能有远端副作用；此信号记录响应丢失后实际完成。
			close(completed)
		}
		reply(w, terminal)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Resize(ctx, intent); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("execution did not start")
	}
	cancel()
	var callErr *CallError
	require.ErrorAs(t, <-done, &callErr)
	require.True(t, callErr.MayHaveExecuted)
	require.Equal(t, &intent, client.Pending())
	changed := intent
	changed.Key.Revision++
	_, err := client.Resize(context.Background(), changed)
	require.Error(t, err)
	require.EqualValues(t, 1, calls.Load())
	stop()
	<-completed
	_, err = client.Resize(context.Background(), intent)
	require.NoError(t, err)
	require.Nil(t, client.Pending())
}

func TestObservedBootChangeIsRejectedWithoutAutomaticRebinding(t *testing.T) {
	intent, terminal := fixtures(t)
	client, config := newTLSClient(t, func(w http.ResponseWriter, _ *http.Request) {
		terminal.Observed.Instance.PGBootID--
		reply(w, control.MemoryResponse{ProtocolVersion: 1, VMShrinkPermit: false, Observed: terminal.Observed})
	})
	_, err := client.Memory(context.Background())
	require.Error(t, err)
	config.Instance.PGBootID--
	_, err = New(context.Background(), config)
	require.ErrorContains(t, err, "binding_changed")
	require.Equal(t, intent.Key.Instance, client.instance)
}
