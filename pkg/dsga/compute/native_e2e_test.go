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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 只在显式跨仓库门禁编入；常规测试既不将它skip算通过，也不连接已有服务。
// Rust为本次进程创建私有PG/一次性JWT，经stdin传入，Go不打印配置/凭据。
func TestNativeComputeHTTPChain(t *testing.T) {
	require.Equal(t, "1", os.Getenv("NEON_DSG_NATIVE_TEST"), "run through the native cross-repository gate")
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 16*1024+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 16*1024)
	var input struct {
		HTTPBaseURL   string           `json:"http_base_url"`
		Instance      control.Instance `json:"instance"`
		IssuerEpoch   control.Uint64   `json:"issuer_epoch"`
		Token         string           `json:"token"`
		RejectedToken string           `json:"rejected_token"`
		NextToken     string           `json:"next_token"`
		Original      control.Request  `json:"original"`
		AfterRestart  bool             `json:"after_restart"`
	}
	// 解析失败仅输出固定错误类别，不能让测试框架把含JWT的原始输入回显。
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("invalid private native test input")
	}
	endpoint, err := url.Parse(input.HTTPBaseURL)
	require.NoError(t, err)
	address := net.ParseIP(endpoint.Hostname())
	require.NotNil(t, address)
	require.True(t, address.IsLoopback())
	require.Equal(t, "http", endpoint.Scheme)
	require.Empty(t, endpoint.Path)
	require.Empty(t, endpoint.RawQuery)
	require.Nil(t, endpoint.User)
	require.NoError(t, input.Original.Validate())
	require.Equal(t, input.Instance, input.Original.Key.Instance)

	// 测试终结TLS，不伪造PG回执/能力。上游固定本次Rust loopback Router，禁用
	// 环境代理；每个HTTP body均由真实Compute/SQL/PG生成。仅第一次已成功的
	// resize响应被改成503，模拟提交后的网络丢回复，其他字节原样传递。
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	proxy.Transport = transport
	var resizeCalls atomic.Int32
	director := proxy.Director
	proxy.Director = func(request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/dsg/v1/resize" {
			resizeCalls.Add(1)
		}
		director(request)
	}
	var dropped atomic.Bool
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.URL.Path == "/dsg/v1/resize" && response.StatusCode == http.StatusOK && dropped.CompareAndSwap(false, true) {
			_ = response.Body.Close()
			body := `{"test":"reply_lost_after_real_pg_commit"}`
			response.StatusCode = http.StatusServiceUnavailable
			response.Status = "503 Service Unavailable"
			response.Body = io.NopCloser(strings.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Set("Content-Length", strconv.Itoa(len(body)))
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "native upstream unavailable", http.StatusServiceUnavailable)
	}
	tls := httptest.NewTLSServer(proxy)
	defer tls.Close()
	config := compute.Config{BaseURL: tls.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Timeout: 10 * time.Second, Transport: tls.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }, Pending: nil}
	client, err := compute.New(context.Background(), config)
	if input.AfterRestart {
		// Rust已真实重启本次PG并确认旧Compute观察通道503。客户端不能因新建
		// Go对象而重新绑定旧boot，或把丢失的旧执行上下文当成空闲。
		var callError *compute.CallError
		require.ErrorAs(t, err, &callError)
		require.Equal(t, "http_unconfirmed", callError.Code)
		require.Nil(t, client)
		return
	}
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	denied := config
	denied.Token = func(context.Context) (string, error) { return input.RejectedToken, nil }
	_, err = compute.New(context.Background(), denied)
	require.Error(t, err, "signed legacy scope must fail in actual Rust middleware")
	capability, err := client.Capabilities(context.Background())
	require.NoError(t, err)
	require.False(t, capability.ProductionReady)
	require.False(t, capability.VMShrinkPermit)
	initialFence, err := client.Fence(context.Background(), control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch})
	require.NoError(t, err)
	require.Equal(t, control.Fenced, initialFence.Disposition)
	require.Equal(t, input.IssuerEpoch, initialFence.Observed.IssuerFloorEpoch)
	require.Equal(t, input.Original.ExpectedCapacityGeneration, initialFence.Observed.CapacityGeneration)

	original := input.Original
	store := nativeJournal(t, original)
	executor, err := pgexec.New(store, client, journal.Owner{HolderID: "native-pg-owner", Epoch: input.IssuerEpoch})
	require.NoError(t, err)
	progress, err := executor.Advance(context.Background())
	require.Equal(t, pgexec.AwaitingFact, progress)
	var callError *compute.CallError
	require.ErrorAs(t, err, &callError)
	require.True(t, callError.MayHaveExecuted)
	require.True(t, dropped.Load())
	require.Equal(t, &original, client.Pending())
	require.EqualValues(t, 1, resizeCalls.Load(), "no automatic POST replay")
	query := control.Query{ProtocolVersion: 1, Key: original.Key}
	status, err := client.Status(context.Background(), query)
	require.NoError(t, err)
	require.NoError(t, status.ValidateResize(original, input.Instance))
	require.Equal(t, control.Committed, status.Receipt.Result.Operation.Phase)
	require.Equal(t, original.TargetBuffers, status.Receipt.Result.HighBuffers)
	require.Equal(t, &original, client.Pending(), "query cannot silently clear external uncertainty")
	// 重新构造无本地执行状态的适配器；恢复只读取持久完整意图，再独立查询真实PG。
	recovered, err := pgexec.New(store, client, journal.Owner{HolderID: "native-pg-owner", Epoch: input.IssuerEpoch})
	require.NoError(t, err)
	progress, err = recovered.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	require.EqualValues(t, 1, resizeCalls.Load(), "journal recovery must not resend resize")
	checkNativeJournal(t, store, original)
	require.Equal(t, &original, client.Pending())
	changed := original
	changed.Key.Revision++
	changed.TargetBuffers = 2048
	_, err = client.Resize(context.Background(), changed)
	require.Error(t, err)
	require.EqualValues(t, 1, resizeCalls.Load())
	progress, err = recovered.ReplayOriginal(context.Background())
	require.NoError(t, err)
	require.Equal(t, pgexec.Replayed, progress)
	replayed, err := client.Status(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptReplayed, replayed.Receipt.Disposition)
	require.Nil(t, client.Pending())
	finishNativeJournal(t, store, original)

	shrink := original
	shrink.Key.Revision = 2
	shrink.Key.OperationID.Low = 2
	shrink.ExpectedCapacityGeneration = replayed.Observed.CapacityGeneration
	shrink.Kind = control.Shrink
	shrink.TargetBuffers = 256
	shrunk, err := client.Resize(context.Background(), shrink)
	require.NoError(t, err)
	require.EqualValues(t, 256, shrunk.Observed.HighBuffers)
	require.False(t, shrunk.VMShrinkPermit, "PG logical shrink is never a VM permit")
	regrow := shrink
	regrow.Key.Revision = 3
	regrow.Key.OperationID.Low = 3
	regrow.Kind = control.Grow
	regrow.TargetBuffers = 2048
	regrow.ExpectedCapacityGeneration = shrunk.Observed.CapacityGeneration
	grown, err := client.Resize(context.Background(), regrow)
	require.NoError(t, err)
	require.EqualValues(t, 2048, grown.Observed.HighBuffers)
	history, err := client.Status(context.Background(), query)
	require.NoError(t, err)
	require.NoError(t, history.ValidateResize(original, input.Instance))
	require.Equal(t, original.TargetBuffers, history.Receipt.Result.HighBuffers)
	require.EqualValues(t, 2048, history.Observed.HighBuffers)
	cancelled, err := client.Cancel(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptTooLate, cancelled.Receipt.Disposition)
	current, err := client.Memory(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2048, current.Observed.HighBuffers)
	conflict := original
	conflict.BudgetCommitmentID += "-different"
	fact, err := client.Resize(context.Background(), conflict)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptConflict, fact.Receipt.Disposition)
	require.Nil(t, fact.Receipt.Result)
	require.Equal(t, &conflict, client.Pending(), "conflict does not prove old effect absence")
	// 使用真实签名的新代次令牌接管。只核验PG floor与原意图保留；这里没有
	// owner租约/完整VM预算证明，不执行第二代次的资源动作或清理外层未知效果。
	nextConfig := config
	nextConfig.IssuerEpoch = input.IssuerEpoch + 1
	nextConfig.Token = func(context.Context) (string, error) { return input.NextToken, nil }
	nextClient, err := compute.New(context.Background(), nextConfig)
	require.NoError(t, err)
	defer nextClient.CloseIdleConnections()
	nextFence, err := nextClient.Fence(context.Background(), control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: nextConfig.IssuerEpoch})
	require.NoError(t, err)
	require.Equal(t, control.Fenced, nextFence.Disposition)
	require.Equal(t, nextConfig.IssuerEpoch, nextFence.Observed.IssuerFloorEpoch)
	require.Equal(t, &conflict, client.Pending())
	require.EqualValues(t, 2048, nextFence.Observed.HighBuffers)
}
