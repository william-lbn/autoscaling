//go:build dsga_native_e2e

package compute_test

import (
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

// 本根只核早期原预算恢复新增的原生边界：按失败请求ID关闭，再按旧预算ID
// 开放，丢回复重建只查同一原意图，迟到关闭不得覆盖新开放。所有PG能力/
// SharedCost/准入/有限期限来自准确PG17，TLS仅为本机终结点；不模拟PG回执，
// 也不将本根当作Guest/VM/Host预算或Linux物理回收认证。
func TestNativeRetainedServiceUsesOriginalBudgetWithoutResize(t *testing.T) {
	require.Equal(t, "1", os.Getenv("NEON_DSG_NATIVE_TEST"))
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 16*1024+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 16*1024)
	var input struct {
		HTTPBaseURL string           `json:"http_base_url"`
		Instance    control.Instance `json:"instance"`
		IssuerEpoch control.Uint64   `json:"issuer_epoch"`
		Token       string           `json:"token"`
	}
	if json.Unmarshal(raw, &input) != nil {
		t.Fatal("invalid private retained service native input")
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
	defer transport.CloseIdleConnections()
	proxy.Transport = transport
	var admissionWrites, resizeWrites atomic.Int32
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/dsg/v1/admission" {
			admissionWrites.Add(1)
		}
		if r.Method == http.MethodPost && r.URL.Path == "/dsg/v1/resize" {
			resizeWrites.Add(1)
		}
		director(r)
	}
	var loseOpen atomic.Bool
	proxy.ModifyResponse = func(r *http.Response) error {
		if r.Request.URL.Path == "/dsg/v1/admission" && loseOpen.Swap(false) {
			_ = r.Body.Close()
			return errors.New("intentional native retained reopen response loss")
		}
		return nil
	}
	server := httptest.NewTLSServer(proxy)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config := compute.Config{BaseURL: server.URL, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return input.Token, nil }, Pending: nil, AdmissionPending: nil, AdmissionClose: nil, AdmissionContinuation: nil}
	client, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	fence, err := client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch})
	require.NoError(t, err)
	require.Equal(t, control.Fenced, fence.Disposition)
	before, err := client.SharedCost(ctx)
	require.NoError(t, err)
	closed := control.AdmissionRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, Revision: before.Observed.AdmissionRevision + 1, ExpectedCapacityGeneration: before.Observed.CapacityGeneration, Open: false, BudgetCommitmentID: "native-failed-prepare-target"}
	closedFact, err := client.SetAdmission(ctx, closed)
	require.NoError(t, err)
	require.True(t, closedFact.Confirms())
	opened := closed
	opened.Revision++
	opened.Open, opened.BudgetCommitmentID = true, "native-original-retained-budget"
	loseOpen.Store(true)
	_, err = client.SetAdmission(ctx, opened)
	var lost *compute.CallError
	require.ErrorAs(t, err, &lost)
	require.True(t, lost.MayHaveExecuted)
	pending, closing := client.PendingAdmission()
	require.Equal(t, &opened, pending)
	require.Nil(t, closing)
	config.AdmissionPending = &opened
	restored, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	writes := admissionWrites.Load()
	openFact, err := restored.AdmissionStatus(ctx, opened)
	require.NoError(t, err)
	require.True(t, openFact.Confirms())
	require.Equal(t, opened, openFact.Request)
	require.Equal(t, writes, admissionWrites.Load(), "丢回复重建只查询，不能重发或换预算ID")
	late, err := client.SetAdmission(ctx, closed)
	require.Error(t, err, "原SDK仍保留开放未知时不允许另发迟到关闭")
	_ = late
	// 独立旧SDK并无本地开放pending，真正把原关闭送达准确PG，验证原生
	// revision屏障；不是把上面的本地拒绝当作PG已经挡住迟到写。
	config.AdmissionPending = nil
	old, err := compute.New(ctx, config)
	require.NoError(t, err)
	defer old.CloseIdleConnections()
	late, err = old.SetAdmission(ctx, closed)
	require.NoError(t, err)
	require.Equal(t, control.AdmissionStale, late.Disposition)
	require.True(t, late.Observed.AdmissionOpen)
	require.Equal(t, opened.Revision, late.Observed.AdmissionRevision)
	current, err := restored.SharedCost(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Observed.CapacityGeneration, current.Observed.CapacityGeneration)
	require.Equal(t, before.Observed.LowBuffers, current.Observed.LowBuffers)
	require.Equal(t, before.Observed.HighBuffers, current.Observed.HighBuffers)
	require.Equal(t, before.CurrentMainAndArraysUpperBytes, current.CurrentMainAndArraysUpperBytes)
	require.Equal(t, closed, closedFact.Request, "原关闭保留新目标ID，不随旧预算重开改写")
	require.Zero(t, resizeWrites.Load())
	lease, err := restored.AdmissionLease(ctx, control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: input.Instance, IssuerEpoch: input.IssuerEpoch, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, ExpectedAdmissionRevision: opened.Revision, ExpectedDeadlineNS: 0, Action: "observe"})
	require.NoError(t, err)
	require.True(t, lease.Armed)
	require.False(t, lease.Expired)
	require.False(t, lease.Renewed)
	t.Log("native retained budget ID switch, lost reply readback and late close isolation verified without Resize")
}
