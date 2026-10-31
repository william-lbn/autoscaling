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
	"testing"
)

// 原Source/PG/cgroup为明确模型；flock、私有dirfd、O_EXCL、fsync是真实Linux U。
// 临时目录不冒称发行ext4，绝未调用PG、ptrace、宿主cgroup或发行资格。
func nativeBirthWALFixture(t *testing.T) (*lockedState, *record, nativeBirthSource, nativeBirthPending, nativeBirthTraceTranscript) {
	t.Helper()
	source, _, _, _ := nativeBirthSourceFixture(t)
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	state, err := lockState(context.Background(), dir)
	require.NoError(t, err)
	t.Cleanup(state.close)
	var st unix.Stat_t
	require.NoError(t, unix.Fstat(int(state.dir.Fd()), &st))
	_, _, c := fixtureGuard(t)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	c.Identity.GuestBootID = strings.TrimSpace(string(boot))
	c.AddressSpaceMaxBytes = source.resource.Limits.AddressSpaceMaxBytes
	c.PidsMaxTasks = source.resource.Limits.PidsMaxTasks
	original := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: 5432, PostmasterStartTicks: 100, AddressSpaceMaxBytes: c.AddressSpaceMaxBytes}
	consumer, err := nativeBirthCurrentConsumerIdentity()
	require.NoError(t, err)
	nonce, err := newNativeBirthNonce()
	require.NoError(t, err)
	pending := nativeBirthPending{ProtocolVersion: 1, Scope: nativeBirthStageScope, Nonce: nonce, Source: source.binding, Original: original, Consumer: consumer, PeerUID: 1000, PeerGID: 1000, PrivateDevice: original.Identity.CgroupDevice, PrivateInode: original.Identity.CgroupInode + 1, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino), ResourceFloor: 1}
	r := &record{Version: 1, Identity: c.Identity, Floor: 1, Bootstrap: &bootstrapRecord{Applied: true, Original: BootstrapCommand{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, BudgetCommitmentID: source.resource.BudgetCommitmentID, Limits: source.resource.Limits}}}
	require.NoError(t, r.validate(r.Identity))
	require.NoError(t, pending.validate())
	expected, err := pending.Source.expected(nonce, c.AddressSpaceMaxBytes)
	require.NoError(t, err)
	lengths, total, err := expected.geometry()
	require.NoError(t, err)
	req := nativeBirthRequest{Original: expected, TotalReservedBytes: total}
	base := uint64(0x100000000)
	for i := range req.Ranges {
		req.Ranges[i] = nativeBirthRange{base, lengths[i], expected.ElementBytes[i]}
		base += lengths[i] + 4096
	}
	binarySHA, err := nativeBirthDigest(source.binding.BinarySHA256)
	require.NoError(t, err)
	tr := nativeBirthTraceTranscript{Original: original, Request: req, BinarySHA256: binarySHA, AuditSHA256: [32]byte{1}, TracerTID: 77, BaseVerified: true}
	return state, r, source, pending, tr
}
func nativeBirthOpenFixture(t *testing.T) (*nativeBirthStage, NativeBirthRef, *record, nativeBirthSource) {
	t.Helper()
	s, r, source, p, _ := nativeBirthWALFixture(t)
	ref, err := createNativeBirthPendingLocked(s, r, source, p)
	require.NoError(t, err)
	r.Birth = &p.Original
	stage, err := openNativeBirthPendingLocked(s, r, source, ref)
	require.NoError(t, err)
	return stage, ref, r, source
}
func TestNativeBirthStageWALOrderedAndNonAuthorizing(t *testing.T) {
	s, r, source, p, tr := nativeBirthWALFixture(t)
	ref, err := createNativeBirthPendingLocked(s, r, source, p)
	require.NoError(t, err)
	require.NoError(t, ref.validate())
	_, err = openNativeBirthPendingLocked(s, r, source, ref)
	require.Error(t, err, "shared Birth not yet saved")
	r.Birth = &p.Original
	stage, err := openNativeBirthPendingLocked(s, r, source, ref)
	require.NoError(t, err)
	require.NoError(t, stage.persist(context.Background(), nativeFilterPMBase, tr))
	tr.BasePersisted = true
	tr.FinalVerified = true
	require.NoError(t, stage.persist(context.Background(), nativeFilterPMFinal, tr))
	snap, err := observeNativeBirthStageLocked(s, ref)
	require.NoError(t, err)
	require.Equal(t, nativeBirthStageFinal, snap.Phase)
	_, err = openNativeBirthPendingLocked(s, r, source, ref)
	require.ErrorIs(t, err, ErrUnknown)
	tr.FinalPersisted = true
	tr.DetachedNonAuthorizing = true
	require.NoError(t, stage.released(context.Background(), tr))
	snap, err = observeNativeBirthStageLocked(s, ref)
	require.NoError(t, err)
	require.Equal(t, nativeBirthStageReleased, snap.Phase)
	require.Len(t, snap.Entries, 3)
	require.Equal(t, p, snap.Pending)
	require.Error(t, stage.released(context.Background(), tr), "repeat release cannot replay")
	_, err = createNativeBirthPendingLocked(s, r, source, p)
	require.ErrorIs(t, err, ErrUnknown)
}
func TestNativeBirthStageWALRejectsOrphanPartialAndGap(t *testing.T) {
	for _, fault := range []string{"partial-pending", "foreign-family", "unknown-phase", "gap-final", "changed-pending", "symlink-phase"} {
		t.Run(fault, func(t *testing.T) {
			s, r, source, p, tr := nativeBirthWALFixture(t)
			ref, err := createNativeBirthPendingLocked(s, r, source, p)
			require.NoError(t, err)
			dir := s.dir.Name()
			switch fault {
			case "partial-pending":
				require.NoError(t, os.WriteFile(filepath.Join(dir, nativeBirthStageName(p.Nonce, "pending")), []byte("{"), 0600))
			case "foreign-family":
				require.NoError(t, os.WriteFile(filepath.Join(dir, nativeBirthStageName(strings.Repeat("ab", 32), "pending")), []byte("{}"), 0600))
			case "unknown-phase":
				require.NoError(t, os.WriteFile(filepath.Join(dir, nativeBirthStageName(p.Nonce, "unexpected")), []byte("{}"), 0600))
			case "gap-final":
				require.NoError(t, os.WriteFile(filepath.Join(dir, nativeBirthStageName(p.Nonce, "final")), []byte("{}"), 0600))
			case "changed-pending":
				next := p
				next.PeerUID++
				raw, err := json.Marshal(next)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(dir, nativeBirthStageName(p.Nonce, "pending")), raw, 0600))
			case "symlink-phase":
				require.NoError(t, os.Symlink("/dev/null", filepath.Join(dir, nativeBirthStageName(p.Nonce, "base"))))
			}
			_, err = observeNativeBirthStageLocked(s, ref)
			require.ErrorIs(t, err, ErrUnknown)
			r.Birth = &p.Original
			_, err = openNativeBirthPendingLocked(s, r, source, ref)
			require.Error(t, err)
			_ = tr
		})
	}
}
func TestNativeBirthStageWALOriginalConsumerAndSource(t *testing.T) {
	for _, fault := range []string{"root-pid", "root-start", "root-instance", "guest-boot", "source", "private-inode", "state-inode", "resource-floor", "foreign-birth"} {
		t.Run(fault, func(t *testing.T) {
			s, r, source, p, _ := nativeBirthWALFixture(t)
			switch fault {
			case "root-pid":
				p.Consumer.PID++
			case "root-start":
				p.Consumer.StartTicks++
			case "root-instance":
				p.Consumer.InstanceNonce = strings.Repeat("ab", 32)
			case "guest-boot":
				p.Original.Identity.GuestBootID = "12345678-1234-1234-1234-123456789abc"
				r.Identity = p.Original.Identity
				r.Bootstrap.Original.Identity = p.Original.Identity
			case "source":
				p.Source.PolicySHA256 = strings.Repeat("ab", 32)
			case "private-inode":
				p.PrivateInode = p.Original.Identity.CgroupInode
			case "state-inode":
				p.StateInode++
			case "resource-floor":
				p.ResourceFloor++
			case "foreign-birth":
				next := p.Original
				next.PostmasterStartTicks++
				r.Birth = &next
			}
			ref, err := createNativeBirthPendingLocked(s, r, source, p)
			if fault == "root-pid" || fault == "root-start" || fault == "root-instance" || fault == "guest-boot" {
				require.NoError(t, err)
				r.Birth = &p.Original
				_, err = openNativeBirthPendingLocked(s, r, source, ref)
				require.ErrorIs(t, err, ErrUnknown)
				snap, observeErr := observeNativeBirthStageLocked(s, ref)
				require.NoError(t, observeErr)
				require.Equal(t, p.Nonce, snap.Ref.Nonce, "new Root only observes original nonce")
			} else {
				require.Error(t, err)
			}
		})
	}
}
func TestNativeBirthStageWALFailuresPoisonWriterNoReplay(t *testing.T) {
	for _, fault := range []string{"early-final", "wrong-arrays", "wrong-binary", "duplicate-base", "cancel", "foreign-phase-file"} {
		t.Run(fault, func(t *testing.T) {
			s, r, source, p, tr := nativeBirthWALFixture(t)
			ref, err := createNativeBirthPendingLocked(s, r, source, p)
			require.NoError(t, err)
			r.Birth = &p.Original
			stage, err := openNativeBirthPendingLocked(s, r, source, ref)
			require.NoError(t, err)
			ctx := context.Background()
			phase := nativeFilterPMBase
			switch fault {
			case "early-final":
				phase = nativeFilterPMFinal
			case "wrong-arrays":
				tr.Request.Ranges[0].Base++
			case "wrong-binary":
				tr.BinarySHA256[0]++
			case "duplicate-base":
				require.NoError(t, stage.persist(ctx, phase, tr))
			case "cancel":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "foreign-phase-file":
				require.NoError(t, os.WriteFile(filepath.Join(s.dir.Name(), nativeBirthStageName(p.Nonce, "base")), []byte("{"), 0600))
			}
			require.Error(t, stage.persist(ctx, phase, tr))
			require.ErrorIs(t, stage.persist(context.Background(), nativeFilterPMBase, tr), ErrUnknown)
			snap, err := observeNativeBirthStageLocked(s, ref)
			if err == nil {
				require.NotEqual(t, nativeBirthStageReleased, snap.Phase)
			} else {
				require.True(t, errors.Is(err, ErrUnknown))
			}
		})
	}
}

func TestNativeBirthSourceLinuxReaderRejectsUnqualifiedFiles(t *testing.T) {
	self, err := os.Executable()
	require.NoError(t, err)
	executable, err := os.Open(self)
	require.NoError(t, err)
	defer executable.Close()
	require.NoError(t, nativeBirthBoundedELF(executable), "actual Go test ELF headers must be bounded")

	for _, path := range []string{"/", "relative", "/x/../y", "/x\x00y"} {
		require.Error(t, nativeBirthReadonlyPath(path))
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "not-an-original-iso")
	require.NoError(t, os.WriteFile(file, []byte("{}"), 0600))
	_, err = nativeBirthReadISO(file, 16384)
	require.Error(t, err, "ordinary temporary file is not original readonly ISO")
	bad, err := os.Open(file)
	require.NoError(t, err)
	defer bad.Close()
	require.Error(t, nativeBirthBoundedELF(bad))
	f, err := nativeBirthOpenRO(file)
	if f != nil {
		f.Close()
	}
	require.Error(t, err, "writable temporary source is not an immutable build")
}
