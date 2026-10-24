//go:build dsga_native_e2e

package compute_test

import (
	"context"
	"encoding/json"
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

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/stretchr/testify/require"
)

// 仅由Rust严格门禁启动：测试TLS终结点不模拟任何PG能力/回执，实际Router、
// JWT、管理SQL和隔离PG生成所有body。凭据只经匿名stdin，不输出配置/口令。
func TestNativeTerminalReconcile(t *testing.T) {
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
	}
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("invalid private native terminal input")
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
	require.NoError(t, input.Original.Validate())
	require.Equal(t, input.Instance, input.Original.Key.Instance)
	proxy := httputil.NewSingleHostReverseProxy(u)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	proxy.Transport = transport
	var writes atomic.Int32
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		if r.URL.Path == "/dsg/v1/resize" {
			writes.Add(1)
		}
		director(r)
	}
	server := httptest.NewTLSServer(proxy)
	defer server.Close()
	config := compute.Config{BaseURL: server.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Timeout: 10 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	fenceRequest := control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: config.IssuerEpoch}
	fence, err := client.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	require.Equal(t, control.Fenced, fence.Disposition)
	saved, err := client.Resize(ctx, input.Original)
	require.NoError(t, err)
	require.NotNil(t, saved.Receipt.Result)
	require.Equal(t, control.Committed, saved.Receipt.Result.Operation.Phase)
	// 模拟从已落账原命令重建SDK，不能仅因已知终态而不恢复Pending。
	config.Pending = &input.Original
	restored, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	_, err = restored.Status(ctx, control.Query{ProtocolVersion: 1, Key: input.Original.Key})
	require.NoError(t, err)
	require.Equal(t, &input.Original, restored.Pending())
	fence, err = restored.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	_, err = restored.ReconcileTerminal(ctx, input.Original, saved, fence)
	require.NoError(t, err)
	require.Nil(t, restored.Pending())
	require.EqualValues(t, 1, writes.Load(), "终态对账不能重发原Resize")
	// 实际更高owner推进floor并执行另一Grow，原结果成为历史；观察当前大池
	// 不能取代历史原回执。新owner仍可只读核对旧完整终态，不借换epoch重写原命令。
	config.Pending = nil
	config.IssuerEpoch++
	config.Token = func(context.Context) (string, error) { return input.NextToken, nil }
	next, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer next.CloseIdleConnections()
	fenceRequest.IssuerEpoch = config.IssuerEpoch
	fence, err = next.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	other := input.Original
	other.Key.IssuerEpoch, other.Key.Revision, other.Key.OperationID.Low = config.IssuerEpoch, 1, 999
	other.ExpectedCapacityGeneration = fence.Observed.CapacityGeneration
	other.TargetBuffers = input.Original.TargetBuffers * 2
	advanced, err := next.Resize(ctx, other)
	require.NoError(t, err)
	require.Equal(t, control.Committed, advanced.Receipt.Result.Operation.Phase)
	config.Pending = &input.Original
	historical, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer historical.CloseIdleConnections()
	fence, err = historical.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	fact, err := historical.ReconcileTerminal(ctx, input.Original, saved, fence)
	require.NoError(t, err)
	require.Nil(t, historical.Pending())
	require.Equal(t, input.Original.TargetBuffers, fact.Receipt.Result.HighBuffers)
	require.Equal(t, other.TargetBuffers, fact.Observed.HighBuffers)
	require.EqualValues(t, 2, writes.Load(), "接管及历史对账均不能重发原Resize")
	// 不将这两个真实PG Grow和只读核对称作Host/Guest预算或VM物理完成。
}
