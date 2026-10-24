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

// Rust严格门禁创建本次PG/Router/一次性签名，TLS仅本机终结；不模拟Work body，
// 不访问用户集群。匿名stdin传配置，不在失败信息里回显JWT或原始配置。
func TestNativeWorkState(t *testing.T) {
	require.Equal(t, "1", os.Getenv("NEON_DSG_NATIVE_TEST"))
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 16*1024+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 16*1024)
	var input struct {
		HTTPBaseURL string           `json:"http_base_url"`
		Instance    control.Instance `json:"instance"`
		IssuerEpoch control.Uint64   `json:"issuer_epoch"`
		Token       string           `json:"token"`
		Original    control.Request  `json:"original"`
	}
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("invalid private native work input")
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
	require.Empty(t, u.Fragment)
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
		if r.Method != http.MethodGet {
			writes.Add(1)
		}
		director(r)
	}
	server := httptest.NewTLSServer(proxy)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := compute.New(ctx, compute.Config{BaseURL: server.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Pending: &input.Original, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }})
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	before, err := client.Memory(ctx)
	require.NoError(t, err)
	fact, err := client.WorkState(ctx)
	require.NoError(t, err)
	require.NoError(t, fact.ValidateFor(input.Instance))
	require.True(t, fact.ObserverExcluded)
	require.EqualValues(t, 2, fact.WorkAPIVersion)
	require.EqualValues(t, 1, fact.StartupClients, "真实GSS后未发startup包的连接也必须登记")
	require.False(t, fact.VMShrinkPermit)
	require.Equal(t, before.Observed, fact.Observed)
	require.Equal(t, &input.Original, client.Pending(), "只读前台事实不得清未知原请求")
	require.Zero(t, writes.Load(), "观察不能重放Resize或写准入")
}
