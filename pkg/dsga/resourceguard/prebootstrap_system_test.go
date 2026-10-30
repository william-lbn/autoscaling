//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 明确模型kernel/ISO/归组，仅WAL/原flock/fsync为真实Linux。没有把临时目录
// 伪称持久ext4，测试通过私有locked入口注入目录FD而不改生产Source验证。
type preSystemFixtureKernel struct {
	*fixtureKernel
	phaseMu                       sync.Mutex
	snapshot                      prebootstrapSystemSnapshot
	fail                          string
	readCalls, boundWrites, moves int
}

func copyPreSystemSnapshot(s prebootstrapSystemSnapshot) prebootstrapSystemSnapshot {
	s.Physical.Zones = append([]PhysicalKernelZone(nil), s.Physical.Zones...)
	if c := s.Physical.RuntimeCapabilities; c != nil {
		next := *c
		next.ActiveLSMs = append([]string(nil), c.ActiveLSMs...)
		s.Physical.RuntimeCapabilities = &next
	}
	return s
}
func (k *preSystemFixtureKernel) prebootstrapSystemRead() (prebootstrapSystemSnapshot, error) {
	k.phaseMu.Lock()
	defer k.phaseMu.Unlock()
	k.readCalls++
	if k.fail == "readback" && k.readCalls == 2 {
		k.fail = ""
		return prebootstrapSystemSnapshot{}, errors.New("U kernel readback fault")
	}
	return copyPreSystemSnapshot(k.snapshot), nil
}
func (k *preSystemFixtureKernel) prebootstrapSystemApply(cap control.Uint64) error {
	k.phaseMu.Lock()
	defer k.phaseMu.Unlock()
	if k.snapshot.System.SwapMaxBytes != 0 {
		k.snapshot.System.SwapMaxBytes = 0
		k.boundWrites++
	}
	if k.fail == "bound" {
		k.fail = ""
		return errors.New("U partial System bound fault")
	}
	if k.snapshot.System.MemoryMaxBytes != cap {
		k.snapshot.System.MemoryMaxBytes = cap
		k.boundWrites++
	}
	return nil
}
func (k *preSystemFixtureKernel) rootUserspace() ([]systemProcess, error) {
	k.phaseMu.Lock()
	defer k.phaseMu.Unlock()
	var result []systemProcess
	for i := uint64(0); i < uint64(k.snapshot.System.RootUserspaceCount); i++ {
		result = append(result, systemProcess{pid: int(i) + 1, ticks: i})
	}
	return result, nil
}
func (k *preSystemFixtureKernel) moveSystem(p systemProcess) error {
	k.phaseMu.Lock()
	defer k.phaseMu.Unlock()
	k.moves++
	if k.fail == "move" && k.moves == 2 {
		k.fail = ""
		return errors.New("U partial placement fault")
	}
	if k.snapshot.System.RootUserspaceCount > 0 {
		k.snapshot.System.RootUserspaceCount--
		k.snapshot.System.ProcessCount++
	}
	if p.pid == 1 {
		k.snapshot.System.PID1Member = true
	}
	return nil
}

type preSystemFixture struct {
	g         *Guard
	k         *preSystemFixtureKernel
	in        prebootstrapSystemInputs
	directory string
}

func newPreSystemFixture(t *testing.T) *preSystemFixture {
	t.Helper()
	old, k, b, c := systemFixture(t)
	directory := old.config.StateDir
	config := old.config
	config.StateDir = FixedStateDir
	b.Limits.MemoryMaxBytes = 512 << 20
	b.Limits.OutsideUpperBytes = 64 << 20
	b.Limits.PidsMaxTasks = 16
	b.Limits.AddressSpaceMaxBytes = 128 << 20
	b.Limits.HugePools = append([]HugePool(nil), b.Limits.HugePools...)
	for i := range b.Limits.HugePools {
		b.Limits.HugePools[i].Pages = 0
	}
	source := sourceFixture(c)
	source.Config = config
	source.Limits = b.Limits
	require.NoError(t, source.Validate())
	o := normalV1PhysicalFixture(t)
	o.Identity = b.Identity
	for i := range o.Zones {
		if o.Zones[i].Name == "Movable" {
			o.Zones[i].PresentPages = 0
		}
	}
	require.NoError(t, o.Validate())
	ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	require.NoError(t, err)
	runtimeSHA := physicalDigest([]byte("explicit U runtime ISO model"))
	policy := PhysicalKernelOriginPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, KernelRelease: ref.KernelRelease, KernelNotesSHA256: ref.KernelNotesSHA256, KernelArtifactSHA256: ref.KernelArtifactSHA256, KernelConfigSHA256: ref.KernelConfigSHA256, RuntimeDocumentSHA256: runtimeSHA}
	rawSource, err := json.Marshal(source)
	require.NoError(t, err)
	origin := PhysicalKernelOrigin{ProtocolVersion: 1, Scope: physicalOriginScope, Nonce: "11111111222233334444555555555555", Policy: policy, PolicySHA256: physicalDigest([]byte("explicit U physical policy bytes")), ResourceSourceSHA256: physicalDigest(rawSource), RuntimeDocumentSHA256: runtimeSHA, RuntimeIncarnation: "22222222333344445555666666666666", ConfigSecretUID: "pre-system-model-secret", Config: config, ExpectedBootstrap: b, First: o}
	require.NoError(t, origin.Validate())
	originSHA, err := origin.ReferenceSHA256()
	require.NoError(t, err)
	stagePolicy := prebootstrapSystemPolicy{ProtocolVersion: 1, Scope: preSystemScope, PhysicalProfileID: ref.ProfileID, RuntimeDocumentSHA256: runtimeSHA, FilesystemProfileSHA256: physicalDigest([]byte("explicit U filesystem reference, not qualified")), BootMemoryUpperBytes: 256 << 20}
	rawPolicy, err := json.Marshal(stagePolicy)
	require.NoError(t, err)
	in := prebootstrapSystemInputs{source: source, sourceRaw: rawSource, policy: stagePolicy, policyRaw: rawPolicy, policySHA: physicalDigest(rawPolicy), origin: origin, originSHA: originSHA}
	var st unix.Stat_t
	require.NoError(t, unix.Stat(directory, &st))
	snapshot := prebootstrapSystemSnapshot{Physical: o, System: SystemEvidence{Scope: SystemChargedScope, MemoryMaxBytes: control.Uint64(math.MaxUint64), SwapMaxBytes: control.Uint64(math.MaxUint64), RootUserspaceCount: 3}, Namespaces: [4]control.Uint64{o.RuntimeCapabilities.InitialPIDNamespaceInode, 2, 3, 4}, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino), TaskCount: 10, MaximumTaskID: 25, PGEmpty: true, MemoryHighUnlimited: true, RootFilesystemReadonly: true}
	fk := &preSystemFixtureKernel{fixtureKernel: k, snapshot: copyPreSystemSnapshot(snapshot)}
	g := &Guard{config: config, kernel: fk}
	require.NoError(t, in.validate(config))
	s, err := lockState(context.Background(), directory)
	require.NoError(t, err)
	raw, err := json.Marshal(origin)
	require.NoError(t, err)
	require.NoError(t, s.createPhysicalFile(physicalOriginDataName, raw))
	require.NoError(t, s.confirmPhysical(raw))
	s.close()
	return &preSystemFixture{g: g, k: fk, in: in, directory: directory}
}

func (f *preSystemFixture) run(ctx context.Context, op prebootstrapSystemOperation) (PrebootstrapSystemResult, error) {
	s, err := lockState(ctx, f.directory)
	if err != nil {
		return PrebootstrapSystemResult{}, err
	}
	defer s.close()
	return f.g.prebootstrapSystemLocked(ctx, f.in, op, s)
}

func TestPrebootstrapSystemRecordsOnlyOriginalStageBeforeFullBudget(t *testing.T) {
	f := newPreSystemFixture(t)
	got, err := f.run(context.Background(), preSystemPrepare)
	require.NoError(t, err)
	require.True(t, got.DurabilityConfirmed)
	require.False(t, got.VMShrinkPermit)
	require.Equal(t, preSystemScope, got.Scope)
	require.True(t, got.Current.System.PID1Member)
	require.Zero(t, got.Current.System.RootUserspaceCount)
	_, err = os.Stat(filepath.Join(f.directory, "state.json"))
	require.True(t, os.IsNotExist(err), "先行阶段不得创建正式Bootstrap")
	require.Zero(t, f.k.fixtureKernel.writes, "没有PG/Huge池/正式资源写")
	intent, err := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
	require.NoError(t, err)
	complete, err := os.ReadFile(filepath.Join(f.directory, preSystemCompleteName))
	require.NoError(t, err)
	writes, moves := f.k.boundWrites, f.k.moves
	for _, op := range []prebootstrapSystemOperation{preSystemObserve, preSystemPrepare, preSystemReplay} {
		next, err := f.run(context.Background(), op)
		require.NoError(t, err)
		require.Equal(t, got.Original.Nonce, next.Original.Nonce)
		require.Equal(t, got.CompletionSHA256, next.CompletionSHA256)
		require.Equal(t, op != preSystemObserve, next.DurabilityConfirmed)
	}
	require.Equal(t, writes, f.k.boundWrites)
	require.Equal(t, moves, f.k.moves)
	after, _ := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
	require.Equal(t, intent, after)
	after, _ = os.ReadFile(filepath.Join(f.directory, preSystemCompleteName))
	require.Equal(t, complete, after)
	// 原正式预算检查没有因先行归组而放宽：完整计划仍超过小base的真实MemTotal。
	limits := f.in.source.Limits
	e := Evidence{ProtocolVersion: 1, Identity: f.in.origin.First.Identity, MemoryMaxBytes: limits.MemoryMaxBytes, GuestMemTotalBytes: 256 << 20, HugePools: append([]HugePool(nil), limits.HugePools...), HugeOvercommitDisabled: true,
		TaskLimit: &TaskLimitEvidence{MaxTasks: limits.PidsMaxTasks}, AddressSpace: &AddressSpaceEvidence{Irreversible: true}, System: &got.Current.System}
	require.ErrorContains(t, e.matchesLimits(e.Identity, limits), "resource budget exceeds actual Guest MemTotal")
	e.GuestMemTotalBytes = 1 << 30
	require.NoError(t, e.matchesLimits(e.Identity, limits), "除base容量外所有限额来源已准确，避免错误负例假绿")
}

func TestPrebootstrapSystemUnknownRequiresSameOriginalReplay(t *testing.T) {
	for _, fault := range []string{"bound", "move", "readback"} {
		t.Run(fault, func(t *testing.T) {
			f := newPreSystemFixture(t)
			f.k.fail = fault
			_, err := f.run(context.Background(), preSystemPrepare)
			require.ErrorIs(t, err, ErrUnknown)
			before, err := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
			require.NoError(t, err)
			writes, moves := f.k.boundWrites, f.k.moves
			_, err = f.run(context.Background(), preSystemPrepare)
			require.ErrorIs(t, err, ErrUnknown)
			_, err = f.run(context.Background(), preSystemObserve)
			require.ErrorIs(t, err, ErrUnknown)
			require.Equal(t, writes, f.k.boundWrites)
			require.Equal(t, moves, f.k.moves)
			result, err := f.run(context.Background(), preSystemReplay)
			require.NoError(t, err)
			require.True(t, result.DurabilityConfirmed)
			after, _ := os.ReadFile(filepath.Join(f.directory, preSystemIntentName))
			require.Equal(t, before, after)
			require.True(t, result.Current.System.PID1Member)
			require.Zero(t, result.Current.System.RootUserspaceCount)
		})
	}
}

func TestPrebootstrapSystemRejectsHistoryAndSourceDriftWithoutWrites(t *testing.T) {
	for _, scenario := range []string{"old-state", "PG-present", "Movable-present", "wrong-namespace", "wrong-state-inode", "wrong-state-device", "rootfs-writable", "high-lowered", "task-out-of-range", "source-policy", "raw-source", "typed-source", "raw-policy", "typed-policy", "origin-missing", "origin-confirmation"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPreSystemFixture(t)
			switch scenario {
			case "old-state":
				s, err := lockState(context.Background(), f.directory)
				require.NoError(t, err)
				require.NoError(t, s.save(&record{Version: 1, Identity: f.in.origin.First.Identity, Floor: 1}))
				s.close()
			case "PG-present":
				f.k.snapshot.PGEmpty = false
			case "Movable-present":
				for i := range f.k.snapshot.Physical.Zones {
					if f.k.snapshot.Physical.Zones[i].Name == "Movable" {
						f.k.snapshot.Physical.Zones[i].PresentPages = 1
					}
				}
			case "wrong-namespace":
				f.k.snapshot.Namespaces[0] = 0
			case "wrong-state-inode":
				f.k.snapshot.StateInode++
			case "wrong-state-device":
				f.k.snapshot.StateDevice++
			case "rootfs-writable":
				f.k.snapshot.RootFilesystemReadonly = false
			case "high-lowered":
				f.k.snapshot.MemoryHighUnlimited = false
			case "task-out-of-range":
				f.k.snapshot.MaximumTaskID = 512
			case "source-policy":
				f.in.policy.RuntimeDocumentSHA256 = physicalDigest([]byte("other runtime"))
			case "raw-source":
				f.in.sourceRaw = append(append([]byte(nil), f.in.sourceRaw...), ' ')
			case "typed-source":
				f.in.source.ControlPublicKey = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQI"
			case "raw-policy":
				f.in.policyRaw = append(append([]byte(nil), f.in.policyRaw...), ' ')
			case "typed-policy":
				f.in.policy.FilesystemProfileSHA256 = physicalDigest([]byte("unbound typed policy"))
			case "origin-missing":
				require.NoError(t, os.Remove(filepath.Join(f.directory, physicalOriginDataName)))
			case "origin-confirmation":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, physicalOriginConfirmName), []byte("{}"), 0600))
			}
			_, err := f.run(context.Background(), preSystemPrepare)
			require.Error(t, err)
			require.Zero(t, f.k.boundWrites)
			require.Zero(t, f.k.moves)
			require.Zero(t, f.k.fixtureKernel.writes)
		})
	}
}

func TestPrebootstrapSystemKeepsPendingOnEarlyMovableOrCorruptFiles(t *testing.T) {
	for _, scenario := range []string{"Movable-before-replay", "changed-policy", "partial-intent", "partial-completion", "completion-without-intent", "symlink-intent"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPreSystemFixture(t)
			if scenario != "completion-without-intent" && scenario != "symlink-intent" {
				f.k.fail = "bound"
				_, err := f.run(context.Background(), preSystemPrepare)
				require.ErrorIs(t, err, ErrUnknown)
			}
			intentPath := filepath.Join(f.directory, preSystemIntentName)
			switch scenario {
			case "Movable-before-replay":
				for i := range f.k.snapshot.Physical.Zones {
					if f.k.snapshot.Physical.Zones[i].Name == "Movable" {
						f.k.snapshot.Physical.Zones[i].PresentPages = 1
					}
				}
			case "changed-policy":
				f.in.policy.FilesystemProfileSHA256 = physicalDigest([]byte("other FS"))
				f.in.policyRaw, _ = json.Marshal(f.in.policy)
				f.in.policySHA = physicalDigest(f.in.policyRaw)
			case "partial-intent":
				require.NoError(t, os.WriteFile(intentPath, []byte("{"), 0600))
			case "partial-completion", "completion-without-intent":
				require.NoError(t, os.WriteFile(filepath.Join(f.directory, preSystemCompleteName), []byte("{"), 0600))
			case "symlink-intent":
				require.NoError(t, os.Symlink("not-owned", intentPath))
			}
			writes, moves := f.k.boundWrites, f.k.moves
			for _, op := range []prebootstrapSystemOperation{preSystemPrepare, preSystemObserve, preSystemReplay} {
				_, err := f.run(context.Background(), op)
				require.Error(t, err)
			}
			require.Equal(t, writes, f.k.boundWrites)
			require.Equal(t, moves, f.k.moves)
		})
	}
}

func TestPrebootstrapSystemSerializesOriginalWALAndAllowsOnlyPostCompleteGrowth(t *testing.T) {
	f := newPreSystemFixture(t)
	var wg sync.WaitGroup
	results := make(chan PrebootstrapSystemResult, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := f.run(context.Background(), preSystemPrepare)
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	nonce := ""
	for r := range results {
		if nonce == "" {
			nonce = r.Original.Nonce
		}
		require.Equal(t, nonce, r.Original.Nonce)
	}
	require.Equal(t, 2, f.k.boundWrites)
	require.Equal(t, 3, f.k.moves)
	f.k.phaseMu.Lock()
	for i := range f.k.snapshot.Physical.Zones {
		if f.k.snapshot.Physical.Zones[i].Name == "Movable" {
			f.k.snapshot.Physical.Zones[i].PresentPages = 32768
		}
	}
	f.k.phaseMu.Unlock()
	result, err := f.run(context.Background(), preSystemObserve)
	require.NoError(t, err)
	require.False(t, result.DurabilityConfirmed)
	require.Equal(t, nonce, result.Original.Nonce)
	require.False(t, result.VMShrinkPermit)
	f.k.snapshot.PGEmpty = false
	_, err = f.run(context.Background(), preSystemObserve)
	require.Error(t, err, "无正式Bootstrap时不能收养已运行PG")
}
