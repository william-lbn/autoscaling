//go:build dsga_native_e2e

package compute_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
)

// 真实Go TLS→Router/JWT→管理SQL→准确PG17。expired模式用原生无容量
// 变化请求实际淘汰历史，原SDK未知始终保留；不是靠新空SDK绕过生产恢复。
// 本组只验证原生隔离/终态对账，不证明Guest/VM或Host物理返还。
func TestNativeResizeRecovery(t *testing.T) {
	require.Equal(t, "1", os.Getenv("NEON_DSG_NATIVE_TEST"))
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 16*1024+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 16*1024)
	var input struct {
		HTTPBaseURL string           `json:"http_base_url"`
		Instance    control.Instance `json:"instance"`
		IssuerEpoch control.Uint64   `json:"issuer_epoch"`
		Token       string           `json:"token"`
		NextToken   string           `json:"next_token"`
		Original    control.Request  `json:"original"`
		Mode        string           `json:"mode"`
	}
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("invalid private native recovery input")
	}
	u, err := url.Parse(input.HTTPBaseURL)
	require.NoError(t, err)
	ip := net.ParseIP(u.Hostname())
	require.NotNil(t, ip)
	require.True(t, ip.IsLoopback())
	require.Equal(t, "http", u.Scheme)
	require.Nil(t, u.User)
	require.Empty(t, u.Path)
	require.Empty(t, u.RawQuery)
	require.Contains(t, []string{"unreceived", "committed", "expired"}, input.Mode)
	proxy := httputil.NewSingleHostReverseProxy(u)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	proxy.Transport = transport
	var writes atomic.Int32
	var loseReply atomic.Bool
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		if r.URL.Path == "/dsg/v1/resize" {
			writes.Add(1)
		}
		director(r)
	}
	proxy.ModifyResponse = func(r *http.Response) error {
		if loseReply.Swap(false) {
			_ = r.Body.Close()
			return errors.New("intentional native resize response loss")
		}
		return nil
	}
	server := httptest.NewTLSServer(proxy)
	defer server.Close()
	config := compute.Config{BaseURL: server.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Timeout: 10 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }, Pending: nil, AdmissionPending: nil, AdmissionClose: nil, AdmissionContinuation: nil}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	_, err = client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: config.IssuerEpoch})
	require.NoError(t, err)
	if input.Mode != "unreceived" {
		loseReply.Store(true)
		_, err = client.Resize(ctx, input.Original)
		var lost *compute.CallError
		require.ErrorAs(t, err, &lost)
		require.True(t, lost.MayHaveExecuted)
		require.Equal(t, &input.Original, client.Pending())
	}
	if input.Mode == "expired" {
		caps, err := client.Capabilities(ctx)
		require.NoError(t, err)
		require.NotNil(t, caps.HistoryCapacity)
		require.LessOrEqual(t, *caps.HistoryCapacity, uint32(64))
		current, err := client.Memory(ctx)
		require.NoError(t, err)
		generation := current.Observed.CapacityGeneration
		for i := uint32(1); i <= *caps.HistoryCapacity+2; i++ {
			// 测试端模拟历史保留期间其他原生请求：同容量no-op只推进
			// 原生历史，不改原SDK Pending，也不构成协调器新请求许可。
			next := input.Original
			next.Key.Revision += control.Uint64(i)
			next.Key.OperationID.Low += control.Uint64(i)
			next.ExpectedCapacityGeneration = generation
			body, err := json.Marshal(next)
			require.NoError(t, err)
			r, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/dsg/v1/resize", bytes.NewReader(body))
			require.NoError(t, err)
			r.Header.Set("Authorization", "Bearer "+input.Token)
			r.Header.Set("Content-Type", "application/json")
			response, err := server.Client().Do(r)
			require.NoError(t, err)
			var fact control.ControlResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&fact)
			_ = response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, decodeErr)
			require.NoError(t, fact.ValidateResize(next, input.Instance))
			require.NotNil(t, fact.Receipt.Result)
			require.Equal(t, control.Committed, fact.Receipt.Result.Operation.Phase)
			require.Equal(t, generation, fact.Observed.CapacityGeneration)
		}
		expired, err := client.Status(ctx, control.Query{ProtocolVersion: 1, Key: input.Original.Key})
		require.NoError(t, err)
		require.Equal(t, control.ReceiptResultExpired, expired.Receipt.Disposition)
		require.Nil(t, expired.Receipt.Result)
		require.NotNil(t, client.Pending())
	}
	config.Pending = &input.Original
	if input.Mode != "committed" {
		config.IssuerEpoch++
		config.Token = func(context.Context) (string, error) { return input.NextToken, nil }
	}
	restored, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	fenceRequest := control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: config.IssuerEpoch}
	fence, err := restored.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	status, err := restored.Status(ctx, control.Query{ProtocolVersion: 1, Key: input.Original.Key})
	require.NoError(t, err)
	effect, err := control.ValidateResizeRecovery(input.Original, fence, status)
	require.NoError(t, err)
	if input.Mode == "committed" {
		require.Equal(t, control.ResizeRecoveredCommitted, effect)
	} else {
		require.Equal(t, control.ResizeRecoveredIsolated, effect)
	}
	require.NotNil(t, restored.Pending())
	before := writes.Load()
	_, err = restored.ReconcileRecovery(ctx, input.Original, fence, status, fence)
	require.NoError(t, err)
	require.Nil(t, restored.Pending())
	require.Equal(t, before, writes.Load(), "独立对账没有Resize/Cancel重放")
	if input.Mode != "committed" {
		late, err := client.Resize(ctx, input.Original)
		require.NoError(t, err)
		require.Equal(t, control.ReceiptStale, late.Receipt.Disposition)
		require.Nil(t, late.Receipt.Result)
		require.Equal(t, status.Observed.CapacityGeneration, late.Observed.CapacityGeneration)
	}
}
