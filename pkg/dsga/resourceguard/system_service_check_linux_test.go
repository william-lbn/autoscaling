//go:build linux

package resourceguard

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"time"

	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func populatedSystemServiceFixture(t *testing.T) (*Guard, *fixtureKernel, SourceConfig, View) {
	t.Helper()
	g, k, b, c := systemFixture(t)
	g.config.ExecutionModel = TrustedControllerExecutionModel
	require.NoError(t, func() error { _, e := g.Bootstrap(context.Background(), b); return e }())
	require.NoError(t, func() error { _, e := g.placeSystem(context.Background(), fixturePlacement(k)); return e }())
	birth := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	require.NoError(t, g.launchWithBirth(context.Background(), nil, &birth, func() error { k.process = &birth; return nil }))
	k.e.CgroupPopulated = true
	source := sourceFixture(c)
	source.Config.ExecutionModel = TrustedControllerExecutionModel
	source.Config.SystemCgroup = true
	require.NoError(t, source.Validate())
	v, e := g.InspectState(context.Background())
	require.NoError(t, e)
	return g, k, source, v
}

func TestSystemServiceReadinessAllowsOriginalPGWithoutAnotherBirth(t *testing.T) {
	g, k, source, v := populatedSystemServiceFixture(t)
	before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	writes := k.writes
	for _, role := range []string{"kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource"} {
		require.NoError(t, checkSystemServiceState(role, source, v), role)
	}
	execs := 0
	require.EqualError(t, g.launch(context.Background(), func() error { execs++; return nil }), "bootstrap cgroup is not empty")
	require.Zero(t, execs, "new System readiness must not authorize another PG")
	require.Equal(t, writes, k.writes)
	after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	require.Equal(t, before, after, "readiness cannot write WAL")
}

func TestSystemServiceReadinessKeepsCurrentHeldAndFailsClosed(t *testing.T) {
	t.Run("current-held", func(t *testing.T) {
		g, k, source, _ := populatedSystemServiceFixture(t)
		c := Command{ProtocolVersion: 1, Identity: k.id, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "runtime-held", PostmasterPID: k.process.PostmasterPID, PostmasterStartTicks: k.process.PostmasterStartTicks, MemoryMaxBytes: 80 << 20, OutsideUpperBytes: source.Limits.OutsideUpperBytes, SystemMemoryMaxBytes: source.Limits.SystemMemoryMaxBytes, HugePools: append([]HugePool(nil), source.Limits.HugePools...)}
		_, e := g.Apply(context.Background(), c)
		require.NoError(t, e)
		v, e := g.InspectState(context.Background())
		require.NoError(t, e)
		require.NotEqual(t, source.Limits.MemoryMaxBytes, v.Actual.MemoryMaxBytes)
		writes := k.writes
		require.NoError(t, checkSystemServiceState("resource", source, v))
		require.Equal(t, writes, k.writes)
	})
	for _, which := range []string{"compute", "allow-start", "unknown-role", "source-uid", "source-budget", "source-mode", "identity", "bounds", "placement", "system-cap", "unknown-write"} {
		t.Run(which, func(t *testing.T) {
			g, k, source, v := populatedSystemServiceFixture(t)
			role := "daemon"
			raw, e := json.Marshal(v)
			require.NoError(t, e)
			var clone View
			require.NoError(t, json.Unmarshal(raw, &clone))
			v = clone
			switch which {
			case "compute", "allow-start":
				role = which
			case "unknown-role":
				role = "resource;exec-forbidden"
			case "source-uid":
				source.Config.VMUID = "different-vm"
			case "source-budget":
				source.BudgetCommitmentID = "different-bootstrap"
			case "source-mode":
				source.Config.ExecutionModel = ""
			case "identity":
				v.Identity.GuestBootID = "00000000-0000-0000-0000-000000000099"
			case "bounds":
				v.Actual.MemoryMaxBytes++
			case "placement":
				v.Actual.System.RootUserspaceCount = 1
			case "system-cap":
				v.Actual.System.MemoryMaxBytes++
			case "unknown-write":
				c := Command{ProtocolVersion: 1, Identity: v.Identity, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "unknown-runtime", PostmasterPID: k.process.PostmasterPID, PostmasterStartTicks: k.process.PostmasterStartTicks, MemoryMaxBytes: 80 << 20, OutsideUpperBytes: source.Limits.OutsideUpperBytes, SystemMemoryMaxBytes: source.Limits.SystemMemoryMaxBytes, HugePools: append([]HugePool(nil), source.Limits.HugePools...)}
				v.Original = &c
				v.Applied = false
			}
			before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			writes := k.writes
			err := checkSystemServiceState(role, source, v)
			require.Error(t, err)
			if which == "unknown-write" {
				require.True(t, errors.Is(err, ErrUnknown), err)
			}
			require.Equal(t, writes, k.writes)
			after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, CheckSystemService(ctx, "resource"), context.Canceled)
	})
}

// 现有handoff夹具保真实WAL/锁/fsync，只有内核与完整Source为模型。
type resourceRestartHandoffKernel struct {
	*handoffFixtureKernel
	systemErr   error
	systemReads int
}

func (k *resourceRestartHandoffKernel) readResourceServiceSystem(id Identity, limits Limits) (*SystemEvidence, error) {
	k.systemReads++
	if k.systemErr != nil {
		return nil, k.systemErr
	}
	if id != k.snapshot.Identity || limits.SystemMemoryMaxBytes == 0 {
		return nil, ErrUnknown
	}
	x := *k.fixtureKernel.e.System
	return &x, nil
}
func resourceRecoveryCheck(ctx context.Context, g *Guard, source SourceConfig) error {
	return g.checkResourceServiceSystem(ctx, source)
}
func pendingResourceServiceFixture(t *testing.T, stage string) (*handoffFixture, *Guard, *resourceRestartHandoffKernel, SourceConfig) {
	t.Helper()
	f := newHandoffFixture(t)
	ctx := context.Background()
	disk := normalQuotaHandoffDiskIO()
	save := disk.save
	calls := 0
	disk.save = func(s *lockedState, r *record) error {
		calls++
		e := save(s, r)
		if e == nil && ((stage == "pending" && calls == 1) || (stage == "held-visible" && calls == 2)) {
			return errors.New("injected original persistence reply loss")
		}
		return e
	}
	_, e := f.run(ctx, f.q.auth, "apply", disk)
	require.ErrorIs(t, e, ErrUnknown)
	g := f.q.f.g
	g.config.ExecutionModel = TrustedControllerExecutionModel
	k := &resourceRestartHandoffKernel{handoffFixtureKernel: f.k}
	g.kernel = k
	r := f.state(t)
	source := sourceFixture(f.q.auth.original.Command)
	source.Limits = r.Bootstrap.Original.Limits
	source.BudgetCommitmentID = r.Bootstrap.Original.BudgetCommitmentID
	source.Config.ExecutionModel = TrustedControllerExecutionModel
	source.Config.SystemCgroup = true
	require.True(t, g.config.SystemCgroup)
	require.NoError(t, source.Validate())
	require.NoError(t, sameBootstrapSource(source, r.Bootstrap.Original))
	require.NotNil(t, k.fixtureKernel.e.System)
	return f, g, k, source
}
func TestResourceServiceUnknownCanListenWithoutConfirmingQuota(t *testing.T) {
	for _, stage := range []string{"pending", "held-visible"} {
		t.Run(stage, func(t *testing.T) {
			f, g, k, source := pendingResourceServiceFixture(t, stage)
			ctx := context.Background()
			before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			writes := len(f.k.quotaWrites)
			require.NoError(t, resourceRecoveryCheck(ctx, g, source), "Unknown resource quota must not prevent proven System transport from starting")
			after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
			require.Equal(t, writes, len(f.k.quotaWrites))
			require.Zero(t, f.k.fixtureKernel.writes)
			// 真正的本地UDS/HTTP/JWT；不调用会写owner/source的生产构造器。
			public, private, e := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, e)
			source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(public)
			h, e := newResourceHandler(source, g)
			require.NoError(t, e)
			dir, e := os.MkdirTemp("/tmp", "rs-")
			require.NoError(t, e)
			defer os.RemoveAll(dir)
			sock := filepath.Join(dir, "s")
			listener, e := net.Listen("unix", sock)
			require.NoError(t, e)
			server := &http.Server{Handler: h}
			go func() { _ = server.Serve(listener) }()
			defer server.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			request, e := http.NewRequest(http.MethodGet, "http://resource/dsg/v1/resource/state", nil)
			require.NoError(t, e)
			request.Header.Set("Authorization", "Bearer "+resourceToken(t, h, private, nil))
			response, e := client.Do(request)
			require.NoError(t, e)
			raw, e := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, e)
			if stage == "pending" {
				require.Equal(t, http.StatusOK, response.StatusCode)
				var view View
				require.NoError(t, json.Unmarshal(raw, &view))
				require.False(t, view.Applied)
				require.NotNil(t, view.Original)
				require.Nil(t, view.GuestQualification)
				_, e = view.ConfirmedLimits()
				require.ErrorIs(t, e, ErrUnknown)
			} else {
				require.NotEqual(t, http.StatusOK, response.StatusCode)
			}
			command := f.q.auth.original.Command
			_, e = g.Observe(ctx, command)
			require.ErrorIs(t, e, ErrUnknown)
			_, e = g.Apply(ctx, command)
			require.ErrorIs(t, e, ErrUnknown)
			_, e = g.InspectGuestQualificationArtifact(ctx)
			require.Error(t, e)
			execs := 0
			require.Error(t, g.launch(ctx, func() error { execs++; return nil }))
			require.Zero(t, execs)
			after, e = os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
			require.Equal(t, writes, len(f.k.quotaWrites))
			require.Zero(t, f.k.fixtureKernel.writes)
			require.Positive(t, k.systemReads)
		})
	}
}
func TestResourceServiceSystemOnlyRejectsUnprovenDomain(t *testing.T) {
	for _, which := range []string{"system-unknown", "system-cap", "system-current", "swap-current", "placement", "source-uid", "source-budget", "guest-identity", "cancelled"} {
		t.Run(which, func(t *testing.T) {
			f, g, k, source := pendingResourceServiceFixture(t, "pending")
			ctx := context.Background()
			switch which {
			case "system-unknown":
				k.systemErr = ErrUnknown
			case "system-cap":
				k.fixtureKernel.e.System.MemoryMaxBytes++
			case "system-current":
				k.fixtureKernel.e.System.MemoryCurrentBytes = k.fixtureKernel.e.System.MemoryMaxBytes + 1
			case "swap-current":
				k.fixtureKernel.e.System.SwapCurrentBytes = 1
			case "placement":
				k.fixtureKernel.e.System.RootUserspaceCount = 1
			case "source-uid":
				source.Config.VMUID = "another-vm"
			case "source-budget":
				source.BudgetCommitmentID = "another-bootstrap"
			case "guest-identity":
				k.fixtureKernel.id.GuestBootID = "00000000-0000-0000-0000-000000000099"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			writes := len(f.k.quotaWrites)
			require.Error(t, resourceRecoveryCheck(ctx, g, source))
			after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			require.NoError(t, e)
			require.Equal(t, before, after)
			require.Equal(t, writes, len(f.k.quotaWrites))
			require.Zero(t, f.k.fixtureKernel.writes)
		})
	}
}
