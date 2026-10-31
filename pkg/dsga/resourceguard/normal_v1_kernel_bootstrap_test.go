//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// U只模拟kernel/ISO；flock、原文件权限、O_EXCL与fsync使用真实临时目录。
type kernelPrepFixtureBackend struct {
	*fixtureKernel
	mu     sync.Mutex
	s      kernelPrepSnapshot
	empty  bool
	writes []string
	fault  string
}

func copyKernelPrepSnapshot(s kernelPrepSnapshot) kernelPrepSnapshot {
	s.Zones = append([]PhysicalKernelZone(nil), s.Zones...)
	s.LoadedModules = append([]string{}, s.LoadedModules...)
	s.Capabilities.ActiveLSMs = append([]string(nil), s.Capabilities.ActiveLSMs...)
	if s.Capabilities.HardPIDMaxLimit != nil {
		h := *s.Capabilities.HardPIDMaxLimit
		s.Capabilities.HardPIDMaxLimit = &h
	}
	return s
}
func (k *kernelPrepFixtureBackend) kernelPrepRead() (kernelPrepSnapshot, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return copyKernelPrepSnapshot(k.s), nil
}
func (k *kernelPrepFixtureBackend) kernelPrepPGEmpty() error {
	if !k.empty {
		return errors.New("U recursive PG populated")
	}
	return nil
}
func (k *kernelPrepFixtureBackend) kernelPrepCensus(uint32) error {
	if k.fault == "census" {
		k.fault = ""
		return errors.New("U census interrupted")
	}
	return nil
}
func (k *kernelPrepFixtureBackend) kernelPrepWrite(name string, v uint64) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writes = append(k.writes, name)
	switch name {
	case "modules_disabled":
		k.s.ModulesDisabled = uint32(v)
	case "numa_balancing":
		k.s.NUMABalancing = uint32(v)
	case "demotion_enabled":
		k.s.DemotionEnabled = v != 0
	case "watermark_boost_factor":
		k.s.Capabilities.WatermarkBoostFactor = uint32(v)
	case "compaction_proactiveness":
		k.s.Capabilities.CompactionProactiveness = uint32(v)
	case "percpu_pagelist_high_fraction":
		k.s.Capabilities.PerCPUPagelistHighFraction = uint32(v)
	case "watermark_scale_factor":
		k.s.Capabilities.WatermarkScaleFactor = uint32(v)
	case "min_free_kbytes":
		k.s.Capabilities.MinFreeKBytes = control.Uint64(v)
	default:
		return errors.New("U unexpected write")
	}
	if k.fault == name {
		k.fault = ""
		return errors.New("U interrupted after real logical write")
	}
	return nil
}

type kernelPrepFixture struct {
	g         *Guard
	k         *kernelPrepFixtureBackend
	in        kernelPrepRawInputs
	directory string
}

func kernelPrepFixtureInputs(t *testing.T) (Config, kernelPrepRawInputs) {
	t.Helper()
	g, _, c := fixtureGuard(t)
	cfg := g.config
	cfg.StateDir = FixedStateDir
	source := sourceFixture(c)
	source.Config = cfg
	require.NoError(t, source.Validate())
	raw, e := json.Marshal(source)
	require.NoError(t, e)
	doc := RuntimeDocument{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "kernel-prep-test-harness", SourceSHA256: physicalDigest(raw), SourceJSON: raw}
	runtimeRaw, e := json.Marshal(doc)
	require.NoError(t, e)
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	physical := PhysicalKernelOriginPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, KernelRelease: ref.KernelRelease, KernelNotesSHA256: ref.KernelNotesSHA256, KernelArtifactSHA256: ref.KernelArtifactSHA256, KernelConfigSHA256: ref.KernelConfigSHA256, RuntimeDocumentSHA256: physicalDigest(runtimeRaw)}
	physicalRaw, e := json.Marshal(physical)
	require.NoError(t, e)
	policy := NormalV1KernelBootstrapPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, RuntimeDocumentSHA256: physicalDigest(runtimeRaw), PhysicalOriginPolicySHA256: physicalDigest(physicalRaw), Controls: NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 4096, WatermarkScaleFactor: 10, PerCPUPagelistHighFraction: 128}, ModulesDisabled: 1}
	policyRaw, e := json.Marshal(policy)
	require.NoError(t, e)
	return cfg, kernelPrepRawInputs{RuntimeRaw: runtimeRaw, PhysicalPolicyRaw: physicalRaw, PolicyRaw: policyRaw}
}
func newKernelPrepFixture(t *testing.T) *kernelPrepFixture {
	t.Helper()
	_, k, c := fixtureGuard(t)
	directory := t.TempDir()
	require.NoError(t, os.Chmod(directory, 0700))
	cfg, in := kernelPrepFixtureInputs(t)
	o := reviewedHardPIDFixture(t, NormalSplitV1PhysicalKernelProfileID)
	o.Identity = c.Identity
	for i := range o.Zones {
		if o.Zones[i].Name == "Movable" {
			o.Zones[i].PresentPages = 0
		}
	}
	caps := *o.RuntimeCapabilities
	caps.WatermarkBoostFactor = 15000
	caps.CompactionProactiveness = 20
	caps.PerCPUPagelistHighFraction = 0
	var st unix.Stat_t
	require.NoError(t, unix.Stat(directory, &st))
	s := kernelPrepSnapshot{Identity: o.Identity, KernelRelease: o.KernelRelease, KernelNotesSHA256: o.KernelNotesSHA256, CommandLine: "cgroup.memory=nokmem,nosocket,nobpf memhp_default_state=online_movable neonvm.pid_max_limit=512 " + NormalV1KernelBootstrapArg, PossibleCPUs: 2, PIDMax: 512, HardPIDMax: 512, Capabilities: caps, Zones: o.Zones, LoadedModules: []string{}, NUMABalancing: 1, Namespaces: [4]control.Uint64{0xEFFFFFFC, 2, 3, 4}, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino)}
	bk := &kernelPrepFixtureBackend{fixtureKernel: k, s: s, empty: true}
	return &kernelPrepFixture{g: &Guard{config: cfg, kernel: bk}, k: bk, in: in, directory: directory}
}
func (f *kernelPrepFixture) run(ctx context.Context, op kernelPrepOperation) (NormalV1KernelBootstrapEvidence, error) {
	s, e := lockState(ctx, f.directory)
	if e != nil {
		return NormalV1KernelBootstrapEvidence{}, e
	}
	defer s.close()
	return f.g.normalKernelPreparationLocked(ctx, f.in, op, s)
}

func TestNormalKernelBootstrapPrepareObserveReplay(t *testing.T) {
	f := newKernelPrepFixture(t)
	e, err := f.run(context.Background(), kernelPrepPrepare)
	require.NoError(t, err)
	require.True(t, e.DurabilityConfirmed())
	require.False(t, e.VMShrinkPermit)
	require.False(t, e.AllSourcesClosed)
	require.Len(t, f.k.writes, 8)
	require.NotContains(t, f.k.writes, "pid_max")
	raw, err := os.ReadFile(filepath.Join(f.directory, kernelPrepIntentName))
	require.NoError(t, err)
	done, err := os.ReadFile(filepath.Join(f.directory, kernelPrepCompleteName))
	require.NoError(t, err)
	for _, op := range []kernelPrepOperation{kernelPrepObserve, kernelPrepPrepare, kernelPrepReplay} {
		got, err := f.run(context.Background(), op)
		require.NoError(t, err)
		require.Equal(t, e.Nonce, got.Nonce)
		require.Equal(t, op == kernelPrepReplay, got.DurabilityConfirmed())
		require.Len(t, f.k.writes, 8)
	}
	after, _ := os.ReadFile(filepath.Join(f.directory, kernelPrepIntentName))
	require.Equal(t, raw, after)
	after, _ = os.ReadFile(filepath.Join(f.directory, kernelPrepCompleteName))
	require.Equal(t, done, after)
	_, err = os.Stat(filepath.Join(f.directory, "state.json"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(f.directory, physicalOriginDataName))
	require.True(t, os.IsNotExist(err))
}
func TestNormalKernelBootstrapUnknownOnlyExplicitOriginalReplay(t *testing.T) {
	for _, fault := range []string{"modules_disabled", "demotion_enabled", "watermark_boost_factor", "min_free_kbytes", "census"} {
		t.Run(fault, func(t *testing.T) {
			f := newKernelPrepFixture(t)
			f.k.fault = fault
			_, err := f.run(context.Background(), kernelPrepPrepare)
			require.ErrorIs(t, err, ErrUnknown)
			raw, err := os.ReadFile(filepath.Join(f.directory, kernelPrepIntentName))
			require.NoError(t, err)
			writes := len(f.k.writes)
			for _, op := range []kernelPrepOperation{kernelPrepPrepare, kernelPrepObserve} {
				_, err = f.run(context.Background(), op)
				require.ErrorIs(t, err, ErrUnknown)
				require.Len(t, f.k.writes, writes)
			}
			got, err := f.run(context.Background(), kernelPrepReplay)
			require.NoError(t, err)
			require.True(t, got.DurabilityConfirmed())
			after, _ := os.ReadFile(filepath.Join(f.directory, kernelPrepIntentName))
			require.Equal(t, raw, after)
		})
	}
}
func TestNormalKernelBootstrapRejectsUnboundInitialSources(t *testing.T) {
	for _, scenario := range []string{"modules-already-disabled", "module-loaded", "Movable", "recursive-PG", "wrong-notes", "wrong-state", "runtime-byte-drift", "policy-byte-drift", "Origin-present", "birth-present", "third-profile", "readonly-hard-missing", "runtime-pid-not-hard"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKernelPrepFixture(t)
			switch scenario {
			case "modules-already-disabled":
				f.k.s.ModulesDisabled = 1
			case "module-loaded":
				f.k.s.LoadedModules = []string{"untrusted"}
			case "Movable":
				for i := range f.k.s.Zones {
					if f.k.s.Zones[i].Name == "Movable" {
						f.k.s.Zones[i].PresentPages = 1
					}
				}
			case "recursive-PG":
				f.k.empty = false
			case "wrong-notes":
				f.k.s.KernelNotesSHA256 = physicalDigest([]byte("foreign"))
			case "wrong-state":
				f.k.s.StateInode++
			case "runtime-byte-drift":
				f.in.RuntimeRaw = append(f.in.RuntimeRaw, ' ')
			case "policy-byte-drift":
				f.in.PhysicalPolicyRaw = append(f.in.PhysicalPolicyRaw, ' ')
			case "Origin-present":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, physicalOriginDataName), []byte("{}"), 0600))
			case "birth-present":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, "birth-intent.json"), []byte("{}"), 0600))
			case "third-profile":
				var p NormalV1KernelBootstrapPolicy
				require.NoError(t, json.Unmarshal(f.in.PolicyRaw, &p))
				p.ProfileID = NormalV1PIDCappedPhysicalKernelProfileID
				f.in.PolicyRaw, _ = json.Marshal(p)
			case "readonly-hard-missing":
				f.k.s.Capabilities.HardPIDMaxLimit = nil
			case "runtime-pid-not-hard":
				f.k.s.PIDMax = 511
			}
			_, err := f.run(context.Background(), kernelPrepPrepare)
			require.Error(t, err)
			require.Empty(t, f.k.writes)
		})
	}
}
func TestNormalKernelBootstrapKeepsOriginalOnCorruptOrThirdPartyDrift(t *testing.T) {
	for _, scenario := range []string{"intent-truncated", "intent-whitespace", "completion-truncated", "symlink-intent", "no-intent-completion", "third-control", "third-demotion", "early-Movable", "changed-source"} {
		t.Run(scenario, func(t *testing.T) {
			f := newKernelPrepFixture(t)
			if scenario != "symlink-intent" && scenario != "no-intent-completion" {
				f.k.fault = "modules_disabled"
				_, e := f.run(context.Background(), kernelPrepPrepare)
				require.ErrorIs(t, e, ErrUnknown)
			}
			switch scenario {
			case "intent-truncated":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, kernelPrepIntentName), []byte("{"), 0600))
			case "intent-whitespace":
				p := filepath.Join(f.directory, kernelPrepIntentName)
				b, _ := os.ReadFile(p)
				require.NoError(t, os.WriteFile(p, append(b, ' '), 0600))
			case "completion-truncated", "no-intent-completion":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, kernelPrepCompleteName), []byte("{"), 0600))
			case "symlink-intent":
				require.NoError(t, os.Symlink("not-original", filepath.Join(f.directory, kernelPrepIntentName)))
			case "third-control":
				f.k.s.Capabilities.WatermarkBoostFactor = 1
			case "third-demotion":
				f.k.s.DemotionEnabled = true
			case "early-Movable":
				for i := range f.k.s.Zones {
					if f.k.s.Zones[i].Name == "Movable" {
						f.k.s.Zones[i].PresentPages = 1
					}
				}
			case "changed-source":
				f.in.PolicyRaw = append(f.in.PolicyRaw, ' ')
			}
			writes := len(f.k.writes)
			for _, op := range []kernelPrepOperation{kernelPrepPrepare, kernelPrepObserve, kernelPrepReplay} {
				_, e := f.run(context.Background(), op)
				require.Error(t, e)
			}
			require.Len(t, f.k.writes, writes)
		})
	}
}
func TestNormalKernelBootstrapCompletedDriftNeverRepairs(t *testing.T) {
	f := newKernelPrepFixture(t)
	_, e := f.run(context.Background(), kernelPrepPrepare)
	require.NoError(t, e)
	f.k.s.NUMABalancing = 1
	writes := len(f.k.writes)
	for _, op := range []kernelPrepOperation{kernelPrepPrepare, kernelPrepObserve, kernelPrepReplay} {
		_, e = f.run(context.Background(), op)
		require.Error(t, e)
	}
	require.Len(t, f.k.writes, writes)
}
func TestNormalKernelBootstrapCanonicalPolicyAndMarker(t *testing.T) {
	_, in := kernelPrepFixtureInputs(t)
	for _, key := range []string{"modules_disabled", "numa_balancing", "demotion_enabled"} {
		t.Run("missing-"+key, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(in.PolicyRaw, &fields))
			delete(fields, key)
			raw, _ := json.Marshal(fields)
			var p NormalV1KernelBootstrapPolicy
			require.Error(t, json.Unmarshal(raw, &p))
		})
	}
	for _, extra := range []string{"", "neonvm.dsga.kernel_bootstrap", "neonvm.dsga.kernel_bootstrap=old", NormalV1KernelBootstrapArg + " " + NormalV1KernelBootstrapArg} {
		t.Run(extra, func(t *testing.T) { require.Error(t, kernelBootstrapMarker(extra)) })
	}
	require.NoError(t, kernelBootstrapMarker("console=ttyS0 "+NormalV1KernelBootstrapArg))
	raw := strings.Replace(string(in.PolicyRaw), `"modules_disabled":1`, `"modules_disabled":1,"modules_disabled":1`, 1)
	var p NormalV1KernelBootstrapPolicy
	require.Error(t, json.Unmarshal([]byte(raw), &p))
}
func TestNormalKernelBootstrapSerializesSingleOriginalIntent(t *testing.T) {
	f := newKernelPrepFixture(t)
	var wg sync.WaitGroup
	out := make(chan NormalV1KernelBootstrapEvidence, 4)
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := f.run(context.Background(), kernelPrepPrepare)
			out <- e
			errs <- err
		}()
	}
	wg.Wait()
	close(out)
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	nonce := ""
	durable := 0
	for e := range out {
		if nonce == "" {
			nonce = e.Nonce
		}
		require.Equal(t, nonce, e.Nonce)
		if e.DurabilityConfirmed() {
			durable++
		}
	}
	require.Equal(t, 1, durable)
	require.Len(t, f.k.writes, 8)
}
