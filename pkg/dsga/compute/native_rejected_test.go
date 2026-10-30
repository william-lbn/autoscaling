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

// 原生Router/JWT/SQL/PG生成拒绝和隔离事实；只验证新拒绝屏障，
// 不重复完整扩缩或资源矩阵，不把原生floor当Guest/VM释放证据。
func TestNativeRejectedResizeBarrier(t *testing.T) {
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
	input.Original.DeadlineUnixMillis = 1 // 原生超时入口的明确拒绝；不生成操作终态。
	fenceRequest := control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: config.IssuerEpoch}
	fence, err := client.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	require.Equal(t, control.Fenced, fence.Disposition)

	original, err := client.Resize(ctx, input.Original)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptRejected, original.Receipt.Disposition)
	require.Nil(t, original.Receipt.Result)
	require.Equal(t, input.Original.ExpectedCapacityGeneration, original.Observed.CapacityGeneration)
	config.IssuerEpoch++
	config.Pending = &input.Original
	config.Token = func(context.Context) (string, error) { return input.NextToken, nil }
	restored, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	fenceRequest.IssuerEpoch = config.IssuerEpoch
	fence, err = restored.Fence(ctx, fenceRequest)
	require.NoError(t, err)
	require.Equal(t, control.Fenced, fence.Disposition)
	status, err := restored.Status(ctx, control.Query{ProtocolVersion: 1, Key: input.Original.Key})
	require.NoError(t, err)
	require.NoError(t, control.ValidateRejectedResizeBarrier(input.Original, original, fence, status))
	require.Equal(t, &input.Original, restored.Pending(), "普通查询保留原SDK栅栏")
	_, err = restored.ReconcileRejected(ctx, input.Original, original, fence, status, fence)
	require.NoError(t, err)
	require.Nil(t, restored.Pending())
	require.EqualValues(t, 1, writes.Load(), "隔离对账只有原始拒绝的一次Resize")
	// 真实原生floor必须在再次派发同原Key时先返回Stale，不能依赖本地SDK
	// 或把原明确拒绝改成Denied。这次刻意旧writer验证不参与恢复流程。
	late, err := client.Resize(ctx, input.Original)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptStale, late.Receipt.Disposition)
	require.Nil(t, late.Receipt.Result)
	require.Equal(t, input.Original.ExpectedCapacityGeneration, late.Observed.CapacityGeneration)
	require.EqualValues(t, 2, writes.Load())
}
