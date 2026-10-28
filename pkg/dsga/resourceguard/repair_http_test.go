package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeldRepairHTTPRejectsChangedScopeAndStrictSelectionBeforeWrites(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, _, request := heldRepairFixture(t, true)
	h.guard = g
	k.e.MemoryMaxBytes /= 2
	writes := k.writes
	body, err := json.Marshal(request)
	require.NoError(t, err)
	for _, change := range []func(*resourceClaims){
		func(c *resourceClaims) { c.Scope = "compute_ctl:dsga_initialization" },
		func(c *resourceClaims) { c.IssuerEpoch++ },
		func(c *resourceClaims) { c.VMUID = "another-vm" },
		func(c *resourceClaims) { c.Expires = c.NotBefore + 1 },
	} {
		w := resourceRequest(h, "/dsg/v1/resource/repair-held", resourceToken(t, h, key, change), body)
		require.NotEqual(t, http.StatusOK, w.Code)
	}
	token := resourceToken(t, h, key, nil)
	for _, malformed := range [][]byte{
		bytes.Replace(body, []byte(`"issuer_epoch":"1"`), []byte(`"issuer_epoch":1`), 1),
		bytes.Replace(body, []byte(`"issuer_epoch":"1"`), []byte(`"issuer_epoch":"1","issuer_epoch":"1"`), 1),
		append([]byte(`{"memory_max_bytes":"999999",`), body[1:]...),
		append([]byte(`{"postmaster_pid":321,`), body[1:]...),
		bytes.Replace(body, []byte(`"source_sha256":"`), []byte(`"source_sha256":"A`), 1),
		[]byte(`{"protocol_version":1}`),
	} {
		w := resourceRequest(h, "/dsg/v1/resource/repair-held", token, malformed)
		require.Equal(t, http.StatusForbidden, w.Code)
	}
	w := resourceRequest(h, "/dsg/v1/resource/repair-held?override=1", token, body)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, writes, k.writes)
	w = resourceRequest(h, "/dsg/v1/resource/repair-held", token, body)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, writes+1, k.writes)
}

// 真实Ed25519→本机TLS→root handler→Mac锁/fsync，内核仍是明确夹具。
// 先模拟写已完成但回复丢失，再以独立GET查询恢复SDK；本用例绝不以
// 同值重发POST冒充“查询恢复”，也不把root限额修复外推为VM物理完成。
func TestHeldRepairClientLostReplyObservesOriginalWithoutResending(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, command, request := heldRepairFixture(t, true)
	h.guard = g
	k.e.MemoryMaxBytes /= 2
	var calls atomic.Int32
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/resource/repair-held" {
			calls.Add(1)
			dropped := httptest.NewRecorder()
			h.ServeHTTP(dropped, r)
			require.Equal(t, http.StatusOK, dropped.Code)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		h.ServeHTTP(w, r)
	})
	client := resourceClientFixture(t, wrapped, command, resourceToken(t, h, key, nil), nil)
	view, err := client.InspectState(context.Background())
	require.NoError(t, err)
	writes := k.writes
	_, err = client.RepairHeld(context.Background(), view, request.SourceSHA256)
	var call *ClientError
	require.ErrorAs(t, err, &call)
	require.True(t, call.MayHaveExecuted)
	require.Equal(t, &request, client.PendingHeldRepair())
	require.Equal(t, writes+1, k.writes)
	next := cloneCommand(command)
	next.Revision++
	_, err = client.Apply(context.Background(), next)
	require.ErrorAs(t, err, &call)
	require.Equal(t, "original_pending", call.Code)
	require.EqualValues(t, 1, calls.Load())
	_, err = client.ObserveHeldRepair(context.Background(), request.SourceSHA256)
	require.NoError(t, err)
	require.Nil(t, client.PendingHeldRepair())
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, writes+1, k.writes)
	// source选择更换时，不能因上界同值就清掉另一个修复的本地未知。
	k.e.MemoryMaxBytes /= 2
	_, err = client.RepairHeld(context.Background(), view, request.SourceSHA256)
	require.Error(t, err)
	_, err = client.ObserveHeldRepair(context.Background(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Error(t, err)
	require.NotNil(t, client.PendingHeldRepair())
	_, err = client.ObserveHeldRepair(context.Background(), request.SourceSHA256)
	require.NoError(t, err)
	require.Nil(t, client.PendingHeldRepair())
}

func TestHeldRepairClientKeepsSameOriginalPendingForItsOwnObserve(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, command, request := heldRepairFixture(t, true)
	h.guard = g
	client := resourceClientFixture(t, h, command, resourceToken(t, h, key, nil), &command)
	view, err := client.InspectState(context.Background())
	require.NoError(t, err)
	k.e.MemoryMaxBytes /= 2
	_, err = client.RepairHeld(context.Background(), view, request.SourceSHA256)
	require.NoError(t, err)
	require.Equal(t, &command, client.Pending(), "修复不是原GuestReady落账，不能清它的Pending")
	_, err = client.ObserveHeldRepair(context.Background(), request.SourceSHA256)
	require.NoError(t, err)
	require.Equal(t, &command, client.Pending())
	_, err = client.Observe(context.Background(), command)
	require.NoError(t, err)
	require.Nil(t, client.Pending())
	other := cloneCommand(command)
	other.Revision++
	client = resourceClientFixture(t, h, command, resourceToken(t, h, key, nil), &other)
	k.e.MemoryMaxBytes /= 2
	writes := k.writes
	_, err = client.RepairHeld(context.Background(), view, request.SourceSHA256)
	require.Error(t, err)
	require.Equal(t, writes, k.writes)
	require.Equal(t, &other, client.Pending())
}

func TestHeldRepairHTTPNewOwnerSelectsSameSourceWithoutRewritingOldCommand(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, command, request := heldRepairFixture(t, true)
	h.guard = g
	_, err := g.Fence(context.Background(), 2)
	require.NoError(t, err)
	k.e.MemoryMaxBytes /= 2
	writes := k.writes
	raw, err := json.Marshal(request)
	require.NoError(t, err)
	w := resourceRequest(h, "/dsg/v1/resource/repair-held", resourceToken(t, h, key, nil), raw)
	require.Equal(t, http.StatusConflict, w.Code, "旧签名/floor不能执行修复")
	require.Equal(t, writes, k.writes)
	configCommand := cloneCommand(command)
	configCommand.IssuerEpoch = 2 // 仅配置新客户端，不改变Root原日志中的命令。
	token := resourceToken(t, h, key, func(c *resourceClaims) { c.IssuerEpoch = 2 })
	client := resourceClientFixture(t, h, configCommand, token, nil)
	view, err := client.InspectState(context.Background())
	require.NoError(t, err)
	digest, err := HeldSourceSHA256(view)
	require.NoError(t, err)
	require.Equal(t, request.SourceSHA256, digest)
	_, err = client.RepairHeld(context.Background(), view, digest)
	require.NoError(t, err)
	_, err = client.ObserveHeldRepair(context.Background(), digest)
	require.NoError(t, err)
	view, err = client.InspectState(context.Background())
	require.NoError(t, err)
	require.Equal(t, command, *view.Original)
	require.Equal(t, writes+1, k.writes)
}

func TestHeldRepairClientFreezesOriginalLimitSliceBeforeRPC(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, command, request := heldRepairFixture(t, true)
	h.guard = g
	var source View
	entered := make(chan struct{})
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dsg/v1/resource/repair-held" {
			h.ServeHTTP(w, r)
			return
		}
		<-entered // Token回调位于SDK已固定目标之后，明确建立跨goroutine顺序。
		response := httptest.NewRecorder()
		h.ServeHTTP(response, r)
		source.Held.HugePools[0].Pages++ // 仅改调用者View，Root原日志未变。
		for name, values := range response.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	})
	token := resourceToken(t, h, key, nil)
	client := resourceClientFixture(t, wrapped, command, token, nil)
	var err error
	source, err = client.InspectState(context.Background())
	require.NoError(t, err)
	client.config.Token = func(context.Context) (string, error) { close(entered); return token, nil }
	k.e.MemoryMaxBytes /= 2
	evidence, err := client.RepairHeld(context.Background(), source, request.SourceSHA256)
	require.NoError(t, err, "验回执必须使用发送前固定的目标，而不是网络等待中被改写的slice")
	require.NoError(t, evidence.ValidateFor(command))
}
