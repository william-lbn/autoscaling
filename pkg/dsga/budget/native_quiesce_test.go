//go:build dsga_native_e2e

package budget_test

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
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

// 正式Go SDK→本机TLS→生产Rust Router/JWT→正式SQL→准确PG17。只测试新关闭/
// 同点事实消费；root硬约束和Prepared外部成本明确为模型，不声称真实Linux预算。
// 私密输入只经匿名stdin，不回显token/原配置，不连接用户数据库。
func TestNativeQuiesceConsumptionKeepsRetainedPayload(t *testing.T) {
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
		t.Fatal("invalid private native quiesce input")
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
	proxy := httputil.NewSingleHostReverseProxy(u)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	proxy.Transport = transport
	defer transport.CloseIdleConnections()
	var closes, resizes atomic.Int32
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		if r.URL.Path == "/dsg/v1/admission" {
			closes.Add(1)
		}
		if r.URL.Path == "/dsg/v1/resize" {
			resizes.Add(1)
		}
		director(r)
	}
	server := httptest.NewTLSServer(proxy)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := compute.New(ctx, compute.Config{BaseURL: server.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }})
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	// 只读DSM旧门禁无需提升floor；本测试新增真实setter，须遵守实际执行器
	// 的先fence后关门顺序。不能把尚未fence的原生Stale当成执行成功。
	fence, err := client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch})
	require.NoError(t, err)
	require.Equal(t, control.Fenced, fence.Disposition)
	shared, err := client.SharedCost(ctx)
	require.NoError(t, err)
	req := input.Original
	req.Kind = control.Shrink
	req.TargetBuffers = shared.Observed.HighBuffers / 2
	req.ExpectedCapacityGeneration = shared.Observed.CapacityGeneration
	req.TargetVMMemoryBytes = 1 << 30
	require.NoError(t, req.Validate())
	require.Greater(t, shared.Observed.HighBuffers, req.TargetBuffers)
	closeIntent := control.AdmissionRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Revision: shared.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: shared.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: req.BudgetCommitmentID}
	fact, err := client.SetAdmission(ctx, closeIntent)
	require.NoError(t, err)
	require.True(t, fact.Confirms())
	var dsm control.DSMResponse
	for {
		dsm, err = client.DSMState(ctx)
		require.NoError(t, err)
		if dsm.Prepared.Connections.Acknowledged && dsm.Prepared.Connections.PublicConnectionsBlocked && dsm.Prepared.Connections.Work.ForegroundIdle {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native close acknowledgement or foreground drain timed out")
		case <-time.After(10 * time.Millisecond):
		}
	}
	require.EqualValues(t, 1, dsm.Prepared.PreparedSlots)
	require.True(t, dsm.LimitEnforced)
	require.GreaterOrEqual(t, uint64(dsm.ReservedUpperBytes), uint64(2<<20))
	id := resourceguard.Identity{VMUID: input.Instance.VMUID, ComputeInstanceID: input.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	limits := resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}}}
	view := resourceguard.View{ProtocolVersion: 1, Identity: id, IssuerFloorEpoch: input.IssuerEpoch, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: input.IssuerEpoch, BudgetCommitmentID: "explicit-test-root-model", Limits: limits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 456}, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: limits.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: 768 << 20, HugePools: limits.HugePools, HugeOvercommitDisabled: true, CgroupPopulated: true}}
	aggregate, err := budget.RootAggregateFromView(view)
	require.NoError(t, err)
	bound := func(id string, n control.Uint64) budget.Bound {
		return budget.Bound{Kind: budget.CommittedUpper, UpperBytes: n, SourceID: id}
	}
	p := budget.Prepared{ProtocolVersion: 1, Request: req, Identity: id, RunnerPodName: "test-runner-model", VMBeforeMemoryBytes: 2 << 30, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: shared.CurrentMainAndArraysUpperBytes, Limits: limits, SystemOutsidePG: bound("test-system-model", 24<<20), Transition: bound("test-transition-model", 4<<20), Safety: bound("test-safety-model", 4<<20)}
	q := budget.Quiesced{ProtocolVersion: 1, Request: req, Close: closeIntent, DSM: dsm, PGAggregate: aggregate}
	require.NoError(t, q.ValidateFor(p, input.IssuerEpoch), "真实2PC/DSM保留非零，只排空前台不补零其他成本")
	require.False(t, q.VMShrinkPermit)
	require.EqualValues(t, 1, closes.Load())
	require.Zero(t, resizes.Load())
	lower := p
	lower.Limits.MemoryMaxBytes = 256 << 20
	require.NoError(t, lower.ValidateFor(req, id))
	require.ErrorContains(t, q.ValidateFor(lower, input.IssuerEpoch), "memory.max decrease")
}
