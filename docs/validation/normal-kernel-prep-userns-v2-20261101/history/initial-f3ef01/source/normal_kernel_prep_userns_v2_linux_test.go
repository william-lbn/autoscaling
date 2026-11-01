//go:build linux

package resourceguard

import (
	"bytes"
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

// U仅模型化kernel与ISO；锁、原WAL权限/O_EXCL/fsync/private inode witness真实。
type kernelPrepV2FixtureBackend struct {
	*kernelPrepFixtureBackend
	vmu         sync.Mutex
	v           normalKernelPrepV2Snapshot
	zeroWrites  int
	zeroFault   bool
	censusCalls int
	censusAfter bool
}

func (b *kernelPrepV2FixtureBackend) kernelPrepV2Read() (normalKernelPrepV2Snapshot, error) {
	b.vmu.Lock()
	defer b.vmu.Unlock()
	s := b.v
	s.Kernel, _ = b.kernelPrepFixtureBackend.kernelPrepRead()
	s.Namespaces = append([]normalSystemRoleNamespaceIdentity(nil), s.Namespaces...)
	return s, nil
}
func (b *kernelPrepV2FixtureBackend) kernelPrepV2ZeroUserNamespaces(w normalKernelPrepV2Object) error {
	b.vmu.Lock()
	defer b.vmu.Unlock()
	if w != b.v.Control {
		return ErrUnknown
	}
	b.zeroWrites++
	b.v.MaxUserNamespaces = 0
	if b.zeroFault {
		b.zeroFault = false
		return errors.New("U fault after actual logical fixed zero")
	}
	return nil
}
func (b *kernelPrepV2FixtureBackend) kernelPrepCensus(h uint32) error {
	b.vmu.Lock()
	b.censusCalls++
	fail := b.censusAfter && b.censusCalls == 2
	b.vmu.Unlock()
	if fail {
		return errors.New("U census interrupted after original zero/write")
	}
	return b.kernelPrepFixtureBackend.kernelPrepCensus(h)
}
func (b *kernelPrepV2FixtureBackend) close() {}

type kernelPrepV2Fixture struct {
	g   *Guard
	b   *kernelPrepV2FixtureBackend
	in  kernelPrepRawInputs
	dir string
}

func kernelPrepV2Inputs(t *testing.T) (Config, kernelPrepRawInputs) {
	t.Helper()
	cfg, in := kernelPrepFixtureInputs(t)
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	physical := PhysicalKernelOriginPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, KernelRelease: ref.KernelRelease, KernelNotesSHA256: ref.KernelNotesSHA256, KernelArtifactSHA256: ref.KernelArtifactSHA256, KernelConfigSHA256: ref.KernelConfigSHA256, RuntimeDocumentSHA256: physicalDigest(in.RuntimeRaw)}
	in.PhysicalPolicyRaw, e = json.Marshal(physical)
	require.NoError(t, e)
	p, e := NewNormalV2KernelBootstrapPolicy(physicalDigest(in.RuntimeRaw), physicalDigest(in.PhysicalPolicyRaw), NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 4096, WatermarkScaleFactor: 10, PerCPUPagelistHighFraction: 128})
	require.NoError(t, e)
	in.PolicyRaw, e = json.Marshal(p)
	require.NoError(t, e)
	return cfg, in
}
func newKernelPrepV2Fixture(t *testing.T) *kernelPrepV2Fixture {
	t.Helper()
	old := newKernelPrepFixture(t)
	cfg, in := kernelPrepV2Inputs(t)
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	old.k.s.KernelRelease = ref.KernelRelease
	old.k.s.KernelNotesSHA256 = ref.KernelNotesSHA256
	old.k.s.CommandLine = strings.ReplaceAll(old.k.s.CommandLine, NormalV1KernelBootstrapArg, NormalV2KernelBootstrapArg)
	ns := []normalSystemRoleNamespaceIdentity{}
	for i, n := range []string{"mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time"} {
		ino := uint64(100 + i)
		if n == "pid" {
			ino = 0xEFFFFFFC
		}
		if n == "user" {
			ino = 0xEFFFFFFD
		}
		ns = append(ns, normalSystemRoleNamespaceIdentity{Name: n, Device: 4, Inode: ino})
	}
	v := normalKernelPrepV2Snapshot{OriginalRoot: nativeBirthConsumer{PID: 101, StartTicks: 123, InstanceNonce: strings.Repeat("ab", 32)}, ProducerELFSHA256: physicalDigest([]byte("TEST_ONLY original producer")), Namespaces: ns, Control: normalKernelPrepV2Object{Device: 4, Inode: 888, MountID: 3, Mode: unix.S_IFREG | 0644}, MaxUserNamespaces: 1024}
	b := &kernelPrepV2FixtureBackend{kernelPrepFixtureBackend: old.k, v: v}
	return &kernelPrepV2Fixture{g: &Guard{config: cfg, kernel: old.k}, b: b, in: in, dir: old.directory}
}
func (f *kernelPrepV2Fixture) run(t *testing.T, op kernelPrepOperation) (NormalV2KernelBootstrapEvidence, error) {
	t.Helper()
	state, e := lockState(context.Background(), f.dir)
	if e != nil {
		return NormalV2KernelBootstrapEvidence{}, e
	}
	defer state.close()
	return f.g.normalKernelPreparationV2Locked(context.Background(), f.in, op, state, f.b)
}
func TestKernelPrepUserNSV2StrictPolicyAndLegacyBytes(t *testing.T) {
	_, in := kernelPrepV2Inputs(t)
	var p NormalV2KernelBootstrapPolicy
	require.NoError(t, json.Unmarshal(in.PolicyRaw, &p))
	for name, raw := range map[string][]byte{"missingzero": bytes.Replace(in.PolicyRaw, []byte(`,"max_user_namespaces":"0"`), nil, 1), "nullzero": bytes.Replace(in.PolicyRaw, []byte(`"max_user_namespaces":"0"`), []byte(`"max_user_namespaces":null`), 1), "relaxed": bytes.Replace(in.PolicyRaw, []byte(`"max_user_namespaces":"0"`), []byte(`"max_user_namespaces":"1"`), 1), "oldversion": bytes.Replace(in.PolicyRaw, []byte(`"protocol_version":2`), []byte(`"protocol_version":1`), 1), "case": bytes.Replace(in.PolicyRaw, []byte(`"profile_id"`), []byte(`"Profile_ID"`), 1), "duplicate": bytes.Replace(in.PolicyRaw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"protocol_version":2`), 1), "wrongprofile": bytes.Replace(in.PolicyRaw, []byte(NormalSplitMetadataV1PhysicalKernelProfileID), []byte(NormalSplitV1PhysicalKernelProfileID), 1), "unknown": bytes.Replace(in.PolicyRaw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"normal":true`), 1)} {
		t.Run(name, func(t *testing.T) { require.NotEqual(t, in.PolicyRaw, raw); require.Error(t, json.Unmarshal(raw, &p)) })
	}
	_, old := kernelPrepFixtureInputs(t)
	var v1 NormalV1KernelBootstrapPolicy
	require.NoError(t, json.Unmarshal(old.PolicyRaw, &v1))
	again, e := json.Marshal(v1)
	require.NoError(t, e)
	require.Equal(t, old.PolicyRaw, again)
	require.Error(t, json.Unmarshal(old.PolicyRaw, &p))
	require.Error(t, json.Unmarshal(in.PolicyRaw, &v1))
	for _, line := range []string{"", NormalV1KernelBootstrapArg, NormalV2KernelBootstrapArg + " " + NormalV2KernelBootstrapArg} {
		require.Error(t, normalKernelPrepV2Marker(line))
	}
	require.NoError(t, normalKernelPrepV2Marker("quiet "+NormalV2KernelBootstrapArg))
}
func TestKernelPrepUserNSV2OriginalPrepareObserveReplay(t *testing.T) {
	f := newKernelPrepV2Fixture(t)
	e, x := f.run(t, kernelPrepPrepare)
	require.NoError(t, x)
	require.True(t, e.DurabilityConfirmed())
	require.Equal(t, 1, f.b.zeroWrites)
	require.Len(t, f.b.writes, 8)
	require.Zero(t, e.After.MaxUserNamespaces)
	source, x := e.originalUserNamespaceBarrierSource()
	require.NoError(t, x)
	require.ErrorIs(t, source.requireContinuousQualification(context.Background()), ErrUnknown)
	state, x := lockState(context.Background(), f.dir)
	require.NoError(t, x)
	ref, x := source.referenceLocked(context.Background(), state)
	require.NoError(t, x)
	require.NotEmpty(t, ref)
	state.close()
	before, x := os.ReadFile(filepath.Join(f.dir, normalKernelPrepV2IntentName))
	require.NoError(t, x)
	var intent normalKernelPrepV2Intent
	require.NoError(t, json.Unmarshal(before, &intent))
	require.Equal(t, control.Uint64(1024), intent.Before.MaxUserNamespaces)
	for _, op := range []kernelPrepOperation{kernelPrepObserve, kernelPrepPrepare, kernelPrepReplay} {
		got, x := f.run(t, op)
		require.NoError(t, x)
		require.Equal(t, e.Nonce, got.Nonce)
		require.Equal(t, op == kernelPrepReplay, got.DurabilityConfirmed())
		require.Equal(t, 1, f.b.zeroWrites)
		require.Len(t, f.b.writes, 8)
		if op != kernelPrepReplay {
			_, x = got.originalUserNamespaceBarrierSource()
			require.ErrorIs(t, x, ErrUnknown)
		}
	}
	after, x := os.ReadFile(filepath.Join(f.dir, normalKernelPrepV2IntentName))
	require.NoError(t, x)
	require.Equal(t, before, after)
	raw, x := json.Marshal(e)
	require.NoError(t, x)
	var copied NormalV2KernelBootstrapEvidence
	require.NoError(t, json.Unmarshal(raw, &copied))
	_, x = copied.originalUserNamespaceBarrierSource()
	require.ErrorIs(t, x, ErrUnknown)
	for _, n := range []string{kernelPrepIntentName, kernelPrepCompleteName, "state.json", physicalOriginDataName} {
		require.NoFileExists(t, filepath.Join(f.dir, n))
	}
}
func TestKernelPrepUserNSV2PartialOnlyOriginalReplay(t *testing.T) {
	for _, fault := range []string{"zero", "modules_disabled", "min_free_kbytes", "census"} {
		t.Run(fault, func(t *testing.T) {
			f := newKernelPrepV2Fixture(t)
			if fault == "zero" {
				f.b.zeroFault = true
			} else if fault == "census" {
				f.b.censusAfter = true
			} else {
				f.b.fault = fault
			}
			_, e := f.run(t, kernelPrepPrepare)
			require.ErrorIs(t, e, ErrUnknown)
			raw, e := os.ReadFile(filepath.Join(f.dir, normalKernelPrepV2IntentName))
			require.NoError(t, e)
			count := f.b.zeroWrites
			writes := len(f.b.writes)
			for _, op := range []kernelPrepOperation{kernelPrepPrepare, kernelPrepObserve} {
				_, e = f.run(t, op)
				require.ErrorIs(t, e, ErrUnknown)
				require.Equal(t, count, f.b.zeroWrites)
				require.Len(t, f.b.writes, writes)
			}
			got, e := f.run(t, kernelPrepReplay)
			require.NoError(t, e)
			require.True(t, got.DurabilityConfirmed())
			after, e := os.ReadFile(filepath.Join(f.dir, normalKernelPrepV2IntentName))
			require.NoError(t, e)
			require.Equal(t, raw, after)
		})
	}
}
func TestKernelPrepUserNSV2RejectsActorsAndUnknownSources(t *testing.T) {
	for _, mode := range []string{"alreadyzero", "moduleprepared", "PGactor", "otherhistory", "foreignRoot", "foreignNamespace", "foreignControl", "thirdvalue", "samebyteIntentReplacement", "missingIntent", "completeValueDrift", "completeReplacement"} {
		t.Run(mode, func(t *testing.T) {
			f := newKernelPrepV2Fixture(t)
			switch mode {
			case "alreadyzero":
				f.b.v.MaxUserNamespaces = 0
			case "moduleprepared":
				f.b.s.ModulesDisabled = 1
			case "PGactor":
				f.b.empty = false
			case "otherhistory":
				require.NoError(t, os.WriteFile(filepath.Join(f.dir, physicalOriginDataName), []byte("history"), 0600))
			default:
				if strings.HasPrefix(mode, "complete") {
					_, e := f.run(t, kernelPrepPrepare)
					require.NoError(t, e)
				} else {
					f.b.zeroFault = true
					_, e := f.run(t, kernelPrepPrepare)
					require.ErrorIs(t, e, ErrUnknown)
				}
				switch mode {
				case "foreignRoot":
					f.b.v.OriginalRoot.InstanceNonce = strings.Repeat("cd", 32)
				case "foreignNamespace":
					f.b.v.Namespaces[0].Inode++
				case "foreignControl":
					f.b.v.Control.Inode++
				case "thirdvalue":
					f.b.v.MaxUserNamespaces = 7
				case "missingIntent":
					require.NoError(t, os.Remove(filepath.Join(f.dir, normalKernelPrepV2IntentName)))
				case "completeValueDrift":
					f.b.v.MaxUserNamespaces = 1
				case "samebyteIntentReplacement", "completeReplacement":
					name := normalKernelPrepV2IntentName
					if mode == "completeReplacement" {
						name = normalKernelPrepV2CompleteName
					}
					path := filepath.Join(f.dir, name)
					raw, e := os.ReadFile(path)
					require.NoError(t, e)
					temp := filepath.Join(f.dir, "replacement")
					require.NoError(t, os.WriteFile(temp, raw, 0600))
					require.NoError(t, os.Rename(temp, path))
				}
			}
			count := f.b.zeroWrites
			writes := len(f.b.writes)
			op := kernelPrepPrepare
			if mode == "thirdvalue" || strings.HasPrefix(mode, "foreign") || mode == "samebyteIntentReplacement" || mode == "completeReplacement" {
				op = kernelPrepReplay
			}
			_, e := f.run(t, op)
			require.Error(t, e)
			require.Equal(t, count, f.b.zeroWrites)
			require.Len(t, f.b.writes, writes)
		})
	}
}
func TestKernelPrepUserNSV2OpaqueWitnessRejectsReplacement(t *testing.T) {
	f := newKernelPrepV2Fixture(t)
	e, x := f.run(t, kernelPrepPrepare)
	require.NoError(t, x)
	source, x := e.originalUserNamespaceBarrierSource()
	require.NoError(t, x)
	path := filepath.Join(f.dir, normalKernelPrepV2CompleteName)
	raw, x := os.ReadFile(path)
	require.NoError(t, x)
	temp := filepath.Join(f.dir, "replaced")
	require.NoError(t, os.WriteFile(temp, raw, 0600))
	require.NoError(t, os.Rename(temp, path))
	state, x := lockState(context.Background(), f.dir)
	require.NoError(t, x)
	defer state.close()
	_, x = source.referenceLocked(context.Background(), state)
	require.ErrorIs(t, x, ErrUnknown)
}
