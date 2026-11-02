package resourceexec

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// 原模型前驱/Root count是明确协议peer；真实typed Store/HTTPS SDK确实执行。
// 真实count4/cap零写/fsync另由原handoff的直接测试验证，不冒物理服务验收。
type normalDurabilityFixture struct {
	mu                                                                              sync.Mutex
	model                                                                           journal.Model
	prepared                                                                        budget.Prepared
	command                                                                         resourceguard.Command
	executor                                                                        *Executor
	prefix                                                                          int
	recoverCalls, stateCalls, observeCalls, pgCalls, patchCalls, forbiddenRootCalls int
	afterRecover                                                                    func()
	allowPGFence                                                                    bool
}

func normalDurabilityModel(t *testing.T, p budget.Prepared, c resourceguard.Command) journal.Model {
	t.Helper()
	m, err := journal.New(p.Request.Key.Instance.VMUID, "1234567890abcdef1234567890abcdef", "durability-owner")
	require.NoError(t, err)
	m, err = m.BindResource(m.Owner, p.Identity)
	require.NoError(t, err)
	m, err = m.BindPG(m.Owner, p.Request.Key.Instance)
	require.NoError(t, err)
	m, disposition, err := m.Begin(m.Owner, p.Request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	for _, step := range m.Active.Steps {
		data := []byte(`{"protocol_peer_predecessor":true}`)
		if step.Action == journal.PrepareBudget {
			data, err = json.Marshal(p)
			require.NoError(t, err)
		}
		if step.Action == journal.GuestReady {
			data, err = json.Marshal(c)
			require.NoError(t, err)
		}
		m, err = m.Start(m.Owner, p.Request.Key, step.Action, p.Identity.ComputeInstanceID, data)
		require.NoError(t, err)
		if step.Action == journal.GuestReady {
			break
		}
		var current journal.Step
		for _, s := range m.Active.Steps {
			if s.Action == step.Action {
				current = s
			}
		}
		m, err = m.Record(m.Owner, p.Request.Key, journal.Observation{Key: p.Request.Key, RequestSHA256: m.Active.RequestSHA256, CommandSHA256: current.CommandSHA256, Ordinal: current.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: current.TargetID, Data: data})
		require.NoError(t, err)
	}
	require.NoError(t, m.Validate())
	return m
}

func newNormalDurabilityFixture(t *testing.T) *normalDurabilityFixture {
	t.Helper()
	read := func(n string, out any) {
		raw, err := os.ReadFile("../../api/dsga/testdata/" + n)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, out))
	}
	var q control.Request
	read("request-v1.json", &q)
	q.Key.IssuerEpoch = 1
	q.Key.Revision = 1
	var response control.ControlResponse
	read("response-v1.json", &response)
	s := response.Observed
	s.Instance = q.Key.Instance
	s.IssuerFloorEpoch = 1
	s.CapacityGeneration = 1
	s.LowBuffers = 512
	s.HighBuffers = 512
	s.Operation = nil
	s.StartupSharedUpperBytes = 32 << 20
	for i := range s.Arrays {
		a := &s.Arrays[i]
		a.CommittedUpperBytes = 20 << 20
		a.AllocationUnknown = false
		a.ReleaseKey = nil
		a.ReleaseState = control.ReleaseNone
		a.ReleaseCapacityGeneration = 0
		a.Provider = control.ProviderFact{Status: control.ProviderUnknown}
	}
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 96 << 20, Observed: s}
	id := resourceguard.Identity{VMUID: q.Key.Instance.VMUID, ComputeInstanceID: q.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	limits := resourceguard.Limits{MemoryMaxBytes: 256 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}}
	fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: q.Key.Instance, RequestedEpoch: 1, Disposition: control.Fenced, Observed: s}
	upper, err := budget.GrowSharedUpper(shared, q.TargetBuffers)
	require.NoError(t, err)
	p := budget.Prepared{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, NormalQuotaFence: &fence, Request: q, Identity: id, RunnerPodName: "runner", VMBeforeMemoryBytes: 512 << 20, GuestUsableMemoryBytes: q.TargetVMMemoryBytes, Shared: shared, TargetSharedUpperBytes: upper, Limits: limits, SystemOutsidePG: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 24 << 20, SourceID: "system"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety"}}
	require.NoError(t, p.ValidateFor(q, id))
	c := resourceguard.Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: q.BudgetCommitmentID, PostmasterPID: 123, PostmasterStartTicks: 10, MemoryMaxBytes: limits.MemoryMaxBytes, OutsideUpperBytes: limits.OutsideUpperBytes, HugePools: limits.HugePools}
	f := &normalDurabilityFixture{model: normalDurabilityModel(t, p, c), prepared: p, command: c, prefix: 4}
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		require.NoError(t, json.NewEncoder(w).Encode(v))
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method != http.MethodGet {
			f.patchCalls++
			http.Error(w, "all writes forbidden", 409)
			return
		}
		raw, err := json.Marshal(f.model)
		require.NoError(t, err)
		// 使用原journal codec的zlib-v1外壳，仅编码测试模型；由正式Store解码校验。
		var compressed bytes.Buffer
		compressor := zlib.NewWriter(&compressed)
		_, err = compressor.Write(raw)
		require.NoError(t, err)
		require.NoError(t, compressor.Close())
		sum := sha256.Sum256(raw)
		encoded, err := json.Marshal(struct {
			Version     uint32 `json:"version"`
			Encoding    string `json:"encoding"`
			Incarnation string `json:"incarnation"`
			SHA256      string `json:"sha256"`
			Data        []byte `json:"data"`
		}{1, "zlib-v1", f.model.Incarnation, hex.EncodeToString(sum[:]), compressed.Bytes()})
		require.NoError(t, err)
		vm := &vmv1.VirtualMachine{TypeMeta: metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"}, ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "durability-vm", UID: "dsg-vm-1", ResourceVersion: "original-1", Annotations: map[string]string{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: string(encoded)}}}
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: id.ComputeInstanceID, VMUID: id.VMUID, ConfigSecretUID: "config-secret-1", Ready: true}
		vm.Status.Phase = vmv1.VmRunning
		vm.Status.PodName = "runner"
		vm.Status.PodIP = "127.0.0.1"
		reply(w, vm)
	}))
	t.Cleanup(api.Close)
	generated, err := vmclient.NewForConfig(&rest.Config{Host: api.URL})
	require.NoError(t, err)
	store, err := journal.NewStore(generated.NeonvmV1().VirtualMachines("test"), journal.Binding{Namespace: "test", Name: "durability-vm", VMUID: "dsg-vm-1", Incarnation: "1234567890abcdef1234567890abcdef"})
	require.NoError(t, err)
	pgServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/capabilities" {
			history := uint32(16)
			provider := false
			boot := q.Key.Instance.PGBootID
			reply(w, control.Capabilities{ProtocolVersion: 1, Supported: true, BufferAPIVersion: 6, Enabled: true, InstanceBound: true, HistoryCapacity: &history, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: id.VMUID, ComputeInstanceID: id.ComputeInstanceID, PGBootID: &boot}})
			return
		}
		f.mu.Lock()
		f.pgCalls++
		allowed := f.allowPGFence && r.URL.Path == "/dsg/v1/issuer/fence"
		f.mu.Unlock()
		if !allowed {
			t.Errorf("Root-only确认触及PG %s", r.URL.Path)
		}
		http.Error(w, "PG operation deliberately unavailable", 503)
	}))
	t.Cleanup(pgServer.Close)
	transport := func(server *httptest.Server) *http.Transport {
		roots := x509.NewCertPool()
		roots.AddCert(server.Certificate())
		return &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	}
	pg, err := compute.New(context.Background(), compute.Config{BaseURL: pgServer.URL, Instance: q.Key.Instance, IssuerEpoch: 1, Token: func(context.Context) (string, error) { return "fixture", nil }, Timeout: time.Second, Transport: transport(pgServer)})
	require.NoError(t, err)
	t.Cleanup(pg.CloseIdleConnections)
	actual := resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: c.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: q.TargetVMMemoryBytes, HugePools: limits.HugePools, WholePoolUpperBytes: 16 << 20, HugeOvercommitDisabled: true}
	view := resourceguard.View{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, Identity: id, IssuerFloorEpoch: 1, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap", Limits: limits}, BootstrapApplied: true, Original: &c, Applied: false, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 10}, Actual: actual}
	require.NoError(t, view.Validate())
	rootServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		if r.URL.Path == "/dsg/v1/resource/state" {
			f.stateCalls++
			f.mu.Unlock()
			reply(w, view)
			return
		}
		if r.URL.Path != "/dsg/v1/resource/normal-recover" && r.URL.Path != "/dsg/v1/resource/normal-observe" {
			f.forbiddenRootCalls++
			f.mu.Unlock()
			t.Errorf("unexpected Root action %s", r.URL.Path)
			http.Error(w, "forbidden", 409)
			return
		}
		recover := r.URL.Path == "/dsg/v1/resource/normal-recover"
		if recover {
			f.recoverCalls++
		} else {
			f.observeCalls++
		}
		prefix, after := f.prefix, f.afterRecover
		f.mu.Unlock()
		var envelope resourceguard.NormalQuotaEnvelope
		require.NoError(t, json.NewDecoder(r.Body).Decode(&envelope))
		require.Equal(t, c, envelope.Command)
		require.Equal(t, q, envelope.PGRequest)
		if prefix != 4 {
			http.Error(w, "protocol peer: complete prefix required", 409)
			return
		}
		if recover && after != nil {
			after()
		}
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		sum := sha256.Sum256(raw)
		reply(w, resourceguard.NormalQuotaEvidence{ProtocolVersion: 1, EnvelopeSHA256: hex.EncodeToString(sum[:]), Command: c, Actual: actual})
	}))
	t.Cleanup(rootServer.Close)
	root, err := resourceguard.NewClient(resourceguard.ClientConfig{Context: context.Background(), BaseURL: rootServer.URL, Identity: id, IssuerEpoch: 1, Token: func(context.Context) (string, error) { return "fixture", nil }, Timeout: time.Second, Transport: transport(rootServer)})
	require.NoError(t, err)
	t.Cleanup(root.CloseIdleConnections)
	f.executor, err = New(store, pg, root, f.model.Owner)
	require.NoError(t, err)
	return f
}

func (f *normalDurabilityFixture) assertNoContinuation(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.pgCalls, "包括Fence/SharedCost在内的PG请求均为零")
	require.Zero(t, f.patchCalls, "GuestReady CAS为零")
	require.Zero(t, f.observeCalls, "不进入后续Root Observe")
	require.Zero(t, f.forbiddenRootCalls, "无Apply/Replay/Cancel")
}

func TestNormalQuotaDurabilityConfirmsOriginalWithoutContinuation(t *testing.T) {
	f := newNormalDurabilityFixture(t)
	before := f.model.Clone()
	progress, err := f.executor.ConfirmNormalQuotaDurability(context.Background(), f.commandKey())
	require.NoError(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress)
	f.assertNoContinuation(t)
	f.mu.Lock()
	require.Equal(t, 1, f.recoverCalls)
	require.Equal(t, before, f.model)
	f.mu.Unlock()
	require.Nil(t, f.executor.resources.NormalQuotaPending())
}
func (f *normalDurabilityFixture) commandKey() control.Key { return f.prepared.Request.Key }

func TestNormalQuotaDurabilityRejectsInvalidCancelledAndPartialOriginal(t *testing.T) {
	for _, mode := range []string{"wrong-key", "cancelled", "owner-changed", "guestready-planned", "partial"} {
		t.Run(mode, func(t *testing.T) {
			f := newNormalDurabilityFixture(t)
			key := f.commandKey()
			switch mode {
			case "wrong-key":
				key.Revision++
			case "cancelled":
				var err error
				f.model, err = f.model.RequestAbort(f.model.Owner, key)
				require.NoError(t, err)
			case "owner-changed":
				var err error
				f.model, err = f.model.Takeover(f.model.Owner, "next-owner")
				require.NoError(t, err)
			case "guestready-planned":
				for i := range f.model.Active.Steps {
					step := &f.model.Active.Steps[i]
					if step.Action == journal.GuestReady {
						step.Phase = journal.Planned
						step.TargetID = ""
						step.Command = nil
						step.CommandSHA256 = ""
						step.DispatchedBy = nil
					}
				}
				require.NoError(t, f.model.Validate())
			case "partial":
				f.prefix = 3
			}
			before := f.model.Clone()
			progress, err := f.executor.ConfirmNormalQuotaDurability(context.Background(), key)
			require.Error(t, err)
			switch mode {
			case "wrong-key", "owner-changed":
				require.ErrorContains(t, err, "normal recovery original owner or identity changed")
			case "cancelled":
				require.ErrorContains(t, err, "normal durability confirmation cannot adopt cancelled")
			case "guestready-planned":
				require.ErrorContains(t, err, "normal recovery original GuestReady unavailable")
			}
			require.Equal(t, pgexec.AwaitingFact, progress)
			f.assertNoContinuation(t)
			f.mu.Lock()
			want := 0
			if mode == "partial" {
				want = 1
			}
			require.Equal(t, want, f.recoverCalls)
			require.Equal(t, before, f.model)
			f.mu.Unlock()
		})
	}
}

func TestNormalQuotaDurabilityRechecksOriginalAfterRootConfirmation(t *testing.T) {
	for _, mode := range []string{"cancel-during-rpc", "owner-during-rpc", "command-during-rpc"} {
		t.Run(mode, func(t *testing.T) {
			f := newNormalDurabilityFixture(t)
			f.afterRecover = func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				var err error
				switch mode {
				case "cancel-during-rpc":
					f.model, err = f.model.RequestAbort(f.model.Owner, f.commandKey())
				case "owner-during-rpc":
					f.model, err = f.model.Takeover(f.model.Owner, "next-owner")
				case "command-during-rpc":
					c := f.command
					c.Revision++
					f.model = normalDurabilityModel(t, f.prepared, c)
				}
				require.NoError(t, err)
			}
			progress, err := f.executor.ConfirmNormalQuotaDurability(context.Background(), f.commandKey())
			require.ErrorIs(t, err, resourceguard.ErrUnknown)
			require.Equal(t, pgexec.AwaitingFact, progress)
			f.assertNoContinuation(t)
			f.mu.Lock()
			require.Equal(t, 1, f.recoverCalls)
			f.mu.Unlock()
		})
	}
}

func TestNormalQuotaDurabilityKeepsLegacyRecoverContinuation(t *testing.T) {
	f := newNormalDurabilityFixture(t)
	f.allowPGFence = true
	progress, err := f.executor.RecoverNormalQuota(context.Background(), f.commandKey())
	require.Error(t, err)
	require.Equal(t, pgexec.AwaitingFact, progress)
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, 1, f.recoverCalls)
	require.Equal(t, 1, f.pgCalls, "旧Recover仍进入原Fence后验，未改为Root-only")
	require.Equal(t, 1, f.observeCalls)
	require.Zero(t, f.patchCalls)
	require.Zero(t, f.forbiddenRootCalls)
}
