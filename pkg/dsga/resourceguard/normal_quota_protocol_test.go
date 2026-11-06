package resourceguard

import (
	"context"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"net/http"
	"os"
	"reflect"
	"sync"
	"testing"
)

// 合同/HTTPS和SDK测试；PG/预算/cgroup由明确夹具提供，不宣称真实normal授权。
func normalQuotaProtocolFixture(t *testing.T, c Command) NormalQuotaEnvelope {
	t.Helper()
	raw, e := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, e)
	var q control.Request
	require.NoError(t, json.Unmarshal(raw, &q))
	q.Kind = control.Shrink
	q.Key.Instance.VMUID = c.Identity.VMUID
	q.Key.Instance.ComputeInstanceID = c.Identity.ComputeInstanceID
	q.Key.IssuerEpoch = c.IssuerEpoch
	q.BudgetCommitmentID = c.BudgetCommitmentID
	raw, e = os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, e)
	var r control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	hash, e := q.ContextSHA256()
	require.NoError(t, e)
	r.Receipt.Key = q.Key
	for _, s := range []*control.Snapshot{r.Receipt.Result, &r.Observed} {
		s.Instance = q.Key.Instance
		s.IssuerFloorEpoch = q.Key.IssuerEpoch
		s.CapacityGeneration = q.ExpectedCapacityGeneration + 1
		s.LowBuffers = q.TargetBuffers
		s.HighBuffers = q.TargetBuffers
		s.AdmissionOpen = false
		s.Operation.Key = q.Key
		s.Operation.Kind = q.Kind
		s.Operation.TargetBuffers = q.TargetBuffers
		s.Operation.ExpectedCapacityGeneration = q.ExpectedCapacityGeneration
		s.Operation.ContextSHA256 = hash
		s.Operation.Phase = control.Committed
		for i := range s.Arrays {
			a := &s.Arrays[i]
			key := q.Key
			a.ReleaseKey = &key
			a.ReleaseCapacityGeneration = s.CapacityGeneration
			a.ReleaseState = control.ReleaseVerified
			a.AllocationUnknown = false
			a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, RequestedOffset: 4096, RequestedBytes: 4096, AffectedOffset: 4096, AffectedBytes: 4096, PhysicalReleaseVerified: true}
		}
	}
	v := NormalQuotaEnvelope{ProtocolVersion: 1, Phase: "released-shrink", Command: c, PGRequest: q, PGFence: control.FenceResponse{ProtocolVersion: 1, RequestedInstance: q.Key.Instance, RequestedEpoch: q.Key.IssuerEpoch, Disposition: control.Fenced, Observed: r.Observed}, PGResponse: &r}
	require.NoError(t, v.Validate())
	return v
}
func TestNormalQuotaEnvelopeRejectsIncompleteAndMixedOriginal(t *testing.T) {
	_, _, c := fixtureGuard(t)
	v := normalQuotaProtocolFixture(t, c)
	for name, change := range map[string]func(*NormalQuotaEnvelope){
		"future-boot":      func(v *NormalQuotaEnvelope) { v.PGRequest.Key.Instance.PGBootID++ },
		"partial-release":  func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[2].Provider.ReleaseMayBePartial = true },
		"old-array-key":    func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[0].ReleaseKey.Revision++ },
		"old-generation":   func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[1].ReleaseCapacityGeneration++ },
		"public-open":      func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.AdmissionOpen = true },
		"unfenced":         func(v *NormalQuotaEnvelope) { v.PGFence.Disposition = control.FencePending },
		"mixed-phase":      func(v *NormalQuotaEnvelope) { v.Phase = "pre-grow" },
		"different-budget": func(v *NormalQuotaEnvelope) { v.Command.BudgetCommitmentID = "another" },
	} {
		t.Run(name, func(t *testing.T) { x := cloneNormalQuota(v); change(&x); require.Error(t, x.Validate()) })
	}
	x := cloneNormalQuota(v)
	x.PGRequest.Kind = control.Grow
	x.PGRequest.TargetBuffers = x.PGResponse.Observed.HighBuffers + 128
	s := x.PGResponse.Observed
	s.Operation = nil
	x.PGRequest.ExpectedCapacityGeneration = s.CapacityGeneration
	x.PGRequest.Key.Revision++
	x.PGRequest.Key.OperationID.Low++
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 1, StartupArraysUpperBytes: s.StartupSharedUpperBytes - 1, CurrentMainAndArraysUpperBytes: 1, Observed: s}
	// Cost正控实际按完整数组费用构造，绝不拿测试布尔宣称PG来源。
	for _, a := range s.Arrays {
		shared.CurrentMainAndArraysUpperBytes += a.CommittedUpperBytes
	}
	x.Phase = "pre-grow"
	x.PGResponse = nil
	x.PGShared = &shared
	x.PGFence.Observed = s
	require.NoError(t, x.Validate())
	x.PGResponse = v.PGResponse
	require.Error(t, x.Validate(), "oneof不可混入历史完成回执")
}

type normalQuotaHTTPFixture struct {
	*Guard
	mu    sync.Mutex
	calls int
	env   *NormalQuotaEnvelope
	k     *fixtureKernel
}

func (b *normalQuotaHTTPFixture) run(v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	x := cloneNormalQuota(v)
	b.env = &x
	raw, _ := json.Marshal(v)
	actual, e := b.k.Read(v.Command)
	return NormalQuotaEvidence{ProtocolVersion: 1, EnvelopeSHA256: physicalDigest(raw), Command: v.Command, Actual: actual}, e
}
func (b *normalQuotaHTTPFixture) ApplyNormalQuota(_ context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return b.run(v)
}
func (b *normalQuotaHTTPFixture) ObserveNormalQuota(_ context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return b.run(v)
}
func (b *normalQuotaHTTPFixture) ReplayNormalQuota(_ context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return b.run(v)
}
func (b *normalQuotaHTTPFixture) RecoverNormalQuota(_ context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return b.run(v)
}
func TestNormalQuotaHTTPAndSDKRetainOriginalAfterLostReply(t *testing.T) {
	h, key, g, k, c := resourceHTTPFixture(t)
	v := normalQuotaProtocolFixture(t, c)
	h.source.Config.ExecutionModel = TrustedControllerExecutionModel
	h.arrayProtocolVersion = 1
	// 假内核回执也必须满足全部目标硬限；否则真实HTTP校验应拒绝。
	k.e.MemoryMaxBytes = c.MemoryMaxBytes
	k.e.SwapMaxBytes = 0
	k.e.HugePools = append([]HugePool(nil), c.HugePools...)
	k.e.HugeOvercommitDisabled = true
	k.e.HugeSurplusPages = 0
	k.e.WholePoolUpperBytes = 0
	for _, pool := range c.HugePools {
		k.e.WholePoolUpperBytes += pool.PageSizeKB * 1024 * pool.Pages
	}
	backend := &normalQuotaHTTPFixture{Guard: g, k: k}
	h.guard = backend
	token := resourceToken(t, h, key, nil)
	raw, _ := json.Marshal(v)
	changed := cloneNormalQuota(v)
	changed.Command.IssuerEpoch++
	bad, _ := json.Marshal(changed)
	require.NotEqual(t, http.StatusOK, resourceRequest(h, "/dsg/v1/resource/normal-apply", token, bad).Code)
	require.Zero(t, backend.calls, "错epoch不得抵达backend")
	h.source.Config.ExecutionModel = ""
	require.NotEqual(t, http.StatusOK, resourceRequest(h, "/dsg/v1/resource/normal-apply", token, raw).Code)
	require.Zero(t, backend.calls, "旧profile不得开启normal入口")
	h.source.Config.ExecutionModel = TrustedControllerExecutionModel
	var lost sync.Once
	server := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		drop := false
		if r.URL.Path == "/dsg/v1/resource/normal-apply" {
			lost.Do(func() { drop = true })
		}
		if drop {
			var packet NormalQuotaEnvelope
			require.NoError(t, json.NewDecoder(r.Body).Decode(&packet))
			_, _ = backend.run(packet)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	})
	client := resourceClientFixture(t, server, c, token, nil)
	client.normalProtocolVersion = 1 // 这里只测SDK原意图，不把夹具cap当生产来源。
	_, e := client.ApplyNormalQuota(context.Background(), v)
	require.Error(t, e)
	require.NotNil(t, client.NormalQuotaPending())
	require.NotNil(t, client.Pending())
	pending := client.NormalQuotaPending()
	pending.PGResponse.Observed.Arrays[0].CommittedUpperBytes++
	require.False(t, reflect.DeepEqual(*pending, *client.NormalQuotaPending()), "原信封必须深拷贝")
	_, e = client.ApplyNormalQuota(context.Background(), v)
	require.Error(t, e)
	require.Equal(t, 1, backend.calls, "普通Apply不得重发未知写")
	_, e = client.ObserveNormalQuota(context.Background(), v)
	require.NoError(t, e)
	require.Nil(t, client.NormalQuotaPending())
	require.Nil(t, client.Pending())
	require.Equal(t, 2, backend.calls)
	require.Zero(t, resourceHTTPKernelWrites(k), "HTTP夹具没有写真实内核")
}

// 新在线模式须同时绑定写前 PG fence 和原 ReleasePG。旧 1 版仍要求关闭。
// 此用例只核验协议，真实 A/N 写入继续由持锁 Root 和原生来源授权。
func TestOnlineNormalQuotaBindsEnforcedAdmissionAndReleasedTail(t *testing.T) {
	_, _, command := fixtureGuard(t)
	proof := cloneNormalQuota(normalQuotaProtocolFixture(t, command))
	proof.ProtocolVersion = 2
	proof.Phase = "online-released-shrink"
	before := &proof.PGFence.Observed
	before.Operation = nil
	before.AdmissionOpen = true
	before.AdmissionRevision = 17
	before.CapacityGeneration = proof.PGRequest.ExpectedCapacityGeneration
	before.HighBuffers = proof.PGRequest.TargetBuffers + 1
	before.LowBuffers = before.HighBuffers
	// 写前快照不能携带未来代次的 Release 回执。
	for i := range before.Arrays {
		before.Arrays[i].ReleaseKey = nil
		before.Arrays[i].ReleaseCapacityGeneration = 0
		before.Arrays[i].ReleaseState = control.ReleaseNone
		before.Arrays[i].Provider = control.ProviderFact{Status: control.ProviderUnknown}
	}
	require.NoError(t, before.Validate())
	for _, after := range []*control.Snapshot{proof.PGResponse.Receipt.Result, &proof.PGResponse.Observed} {
		after.AdmissionOpen = true
		after.AdmissionRevision = 17
	}
	require.NoError(t, proof.Validate())

	for name, change := range map[string]func(*NormalQuotaEnvelope){
		"legacy-open": func(v *NormalQuotaEnvelope) {
			v.ProtocolVersion = 1
			v.Phase = "released-shrink"
		},
		"old-writer":          func(v *NormalQuotaEnvelope) { v.PGFence.Observed.IssuerFloorEpoch++ },
		"before-not-serving":  func(v *NormalQuotaEnvelope) { v.PGFence.Observed.AdmissionOpen = false },
		"intervening-closure": func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.AdmissionRevision++ },
		"no-start-generation": func(v *NormalQuotaEnvelope) { v.PGFence.Observed.CapacityGeneration++ },
		"no-physical-change": func(v *NormalQuotaEnvelope) {
			v.PGFence.Observed.HighBuffers = v.PGRequest.TargetBuffers
			v.PGFence.Observed.LowBuffers = v.PGRequest.TargetBuffers
		},
		"partial-allocation":       func(v *NormalQuotaEnvelope) { v.PGFence.Observed.Arrays[0].AllocationUnknown = true },
		"pending-release":          func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[0].Provider.ReleaseMayBePartial = true },
		"old-release":              func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[0].ReleaseCapacityGeneration++ },
		"missing-physical-release": func(v *NormalQuotaEnvelope) { v.PGResponse.Observed.Arrays[0].Provider.PhysicalReleaseVerified = false },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneNormalQuota(proof)
			change(&copy)
			require.Error(t, copy.Validate())
		})
	}
}
