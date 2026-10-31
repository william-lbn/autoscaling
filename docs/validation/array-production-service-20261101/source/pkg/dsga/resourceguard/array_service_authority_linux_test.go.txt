//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Source/Birth/Key均明确模型；私有目录/O_EXCL/flock/fsync/文件身份为真实Linux U。
// 本层没有启动helper/读取宿主cgroup，完整实际producer由独立原pin入口强制。
func arrayAuthorityWALFixture(t *testing.T) (*lockedState, arrayAuthorityOriginal) {
	t.Helper()
	s, _, _, p, _ := nativeBirthWALFixture(t)
	_, _, _, _, _, c := arrayHTTPOriginal(t)
	return s, arrayAuthorityOriginal{Command: c, Birth: NativeBirthRef{ProtocolVersion: 1, Nonce: p.Nonce, PendingSHA256: strings.Repeat("a", 64)}, ReleasedSHA256: strings.Repeat("b", 64), RegistrationNonce: strings.Repeat("c", 64), MappingGeneration: 1}
}
func arrayAuthorityBytes(t *testing.T, s *lockedState, c ArrayAuthorityCommand) map[string]string {
	t.Helper()
	out := map[string]string{}
	key := arrayAuthorityKey(c)
	for _, phase := range []string{"pending", "complete"} {
		name := arrayAuthorityName(key, phase)
		raw, found, err := s.readPhysicalFile(name)
		require.NoError(t, err)
		if found {
			out[name] = string(raw)
		}
	}
	return out
}

// PG17-U-ARRAY-WAL-001：同完整Key幂等不重造nonce；任何原来源/正文漂移冲突。
func TestArrayAuthorityWALOriginalBytesAndNoImplicitReplacement(t *testing.T) {
	s, b := arrayAuthorityWALFixture(t)
	j := new(arrayAuthorityJournal)
	ctx := context.Background()
	result, err := j.applyLocked(ctx, s, b, "authorize", arrayAuthorityDiskIO())
	require.NoError(t, err)
	require.NoError(t, result.validateFor(b.Command))
	require.False(t, result.VMShrinkPermit)
	before := arrayAuthorityBytes(t, s, b.Command)
	for _, mode := range []string{"authorize", "observe", "replay"} {
		_, err = j.applyLocked(ctx, s, b, mode, arrayAuthorityDiskIO())
		require.NoError(t, err)
		require.Equal(t, before, arrayAuthorityBytes(t, s, b.Command))
	}
	for _, field := range []string{"target", "source", "birth", "registration", "mapping"} {
		t.Run(field, func(t *testing.T) {
			n := b
			switch field {
			case "target":
				n.Command.Request.TargetBuffers++
			case "source":
				n.Command.ResourceReferenceSHA256 = strings.Repeat("d", 64)
			case "birth":
				n.Birth.PendingSHA256 = strings.Repeat("d", 64)
			case "registration":
				n.RegistrationNonce = strings.Repeat("d", 64)
			case "mapping":
				n.MappingGeneration++
			}
			_, err := j.applyLocked(ctx, s, n, "replay", arrayAuthorityDiskIO())
			require.ErrorIs(t, err, ErrUnknown)
			require.Equal(t, before, arrayAuthorityBytes(t, s, b.Command))
		})
	}
}

// PG17-U-ARRAY-WAL-002：注入complete可见但同步失败；普通Observe/gate零写。
func TestArrayAuthorityWALVisibleCompleteNeedsDurabilityAndExplicitReplay(t *testing.T) {
	for _, fault := range []string{"complete-create", "pending-sync", "complete-sync", "dir-sync"} {
		t.Run(fault, func(t *testing.T) {
			s, b := arrayAuthorityWALFixture(t)
			j := new(arrayAuthorityJournal)
			disk := arrayAuthorityDiskIO()
			realCreate, realSync, realDir := disk.create, disk.syncFile, disk.syncDir
			disk.create = func(s *lockedState, n string, raw []byte) error {
				if err := realCreate(s, n, raw); err != nil {
					return err
				}
				if fault == "complete-create" && strings.Contains(n, ".complete.") {
					return errors.New("injected fsync ambiguity after visibility")
				}
				return nil
			}
			disk.syncFile = func(s *lockedState, n string) error {
				if fault == "pending-sync" && strings.Contains(n, ".pending.") || fault == "complete-sync" && strings.Contains(n, ".complete.") {
					return errors.New("injected file fsync")
				}
				return realSync(s, n)
			}
			disk.syncDir = func(s *lockedState) error {
				if fault == "dir-sync" {
					return errors.New("injected directory fsync")
				}
				return realDir(s)
			}
			_, err := j.applyLocked(context.Background(), s, b, "authorize", disk)
			require.ErrorIs(t, err, ErrUnknown)
			before := arrayAuthorityBytes(t, s, b.Command)
			require.Len(t, before, 2)
			zeroWrites := arrayAuthorityIO{create: func(*lockedState, string, []byte) error { t.Fatal("ordinary gate wrote"); return nil }, syncFile: func(*lockedState, string) error { t.Fatal("ordinary gate synced"); return nil }, syncDir: func(*lockedState) error { t.Fatal("ordinary gate synced dir"); return nil }}
			for _, mode := range []string{"observe", "authorize"} {
				_, err = j.applyLocked(context.Background(), s, b, mode, zeroWrites)
				require.ErrorIs(t, err, ErrUnknown)
				require.Equal(t, before, arrayAuthorityBytes(t, s, b.Command))
			}
			if fault != "complete-create" {
				_, err = j.applyLocked(context.Background(), s, b, "replay", disk)
				require.ErrorIs(t, err, ErrUnknown)
			}
			_, err = j.applyLocked(context.Background(), s, b, "replay", arrayAuthorityDiskIO())
			require.NoError(t, err)
			require.Equal(t, before, arrayAuthorityBytes(t, s, b.Command))
			_, err = j.applyLocked(context.Background(), s, b, "observe", zeroWrites)
			require.NoError(t, err)
		})
	}
}

// PG17-U-ARRAY-WAL-003：新Root/同字节新inode/partial一律不继承原内存见证。
func TestArrayAuthorityWALRestartIdentityAndPartialRemainUnknown(t *testing.T) {
	t.Run("new-root-and-replaced-inode", func(t *testing.T) {
		s, b := arrayAuthorityWALFixture(t)
		old := new(arrayAuthorityJournal)
		_, err := old.applyLocked(context.Background(), s, b, "authorize", arrayAuthorityDiskIO())
		require.NoError(t, err)
		before := arrayAuthorityBytes(t, s, b.Command)
		fresh := new(arrayAuthorityJournal)
		_, err = fresh.applyLocked(context.Background(), s, b, "observe", arrayAuthorityDiskIO())
		require.ErrorIs(t, err, ErrUnknown)
		_, err = fresh.applyLocked(context.Background(), s, b, "replay", arrayAuthorityDiskIO())
		require.NoError(t, err)
		require.Equal(t, before, arrayAuthorityBytes(t, s, b.Command))
		name := arrayAuthorityName(arrayAuthorityKey(b.Command), "complete")
		path := filepath.Join(s.dir.Name(), name)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path+".replacement", raw, 0600))
		require.NoError(t, os.Rename(path+".replacement", path))
		_, err = fresh.applyLocked(context.Background(), s, b, "observe", arrayAuthorityDiskIO())
		require.ErrorIs(t, err, ErrUnknown)
		_, err = fresh.applyLocked(context.Background(), s, b, "replay", arrayAuthorityDiskIO())
		require.NoError(t, err)
	})
	for _, fault := range []string{"partial", "foreign", "orphan"} {
		t.Run(fault, func(t *testing.T) {
			s, b := arrayAuthorityWALFixture(t)
			key := arrayAuthorityKey(b.Command)
			name := arrayAuthorityName(key, "pending")
			if fault == "foreign" {
				name = "array-authority.unexpected"
			}
			if fault == "orphan" {
				name = arrayAuthorityName(key, "complete")
			}
			require.NoError(t, os.WriteFile(filepath.Join(s.dir.Name(), name), []byte("{"), 0600))
			for _, mode := range []string{"authorize", "observe", "replay"} {
				_, err := new(arrayAuthorityJournal).applyLocked(context.Background(), s, b, mode, arrayAuthorityDiskIO())
				require.ErrorIs(t, err, ErrUnknown)
			}
		})
	}
}

// PG17-U-ARRAY-WAL-004：原签名Request产生native context；每一完整Key维度都核。
func TestArrayAuthorityWALPopulateMatchesOriginalSignedRequest(t *testing.T) {
	_, b := arrayAuthorityWALFixture(t)
	c := b.Command
	hash, err := c.Request.ContextSHA256()
	require.NoError(t, err)
	digest, err := hex.DecodeString(hash)
	require.NoError(t, err)
	raw := arrayRegistryWireRequest{Op: arrayRegistryPopulate, PGBoot: uint64(c.Request.Key.Instance.PGBootID), IssuerEpoch: uint64(c.IssuerEpoch), Revision: uint64(c.Request.Key.Revision), OperationHigh: uint64(c.Request.Key.OperationID.High), OperationLow: uint64(c.Request.Key.OperationID.Low), Generation: uint64(c.Request.ExpectedCapacityGeneration), TargetBuffers: uint64(c.Request.TargetBuffers)}
	copy(raw.ContextSHA[:], digest)
	require.NoError(t, arrayAuthorityMatchesPopulate(c, raw))
	for _, field := range []string{"boot", "epoch", "revision", "hi", "lo", "gen", "target", "context", "op"} {
		t.Run(field, func(t *testing.T) {
			n := raw
			switch field {
			case "boot":
				n.PGBoot++
			case "epoch":
				n.IssuerEpoch++
			case "revision":
				n.Revision++
			case "hi":
				n.OperationHigh++
			case "lo":
				n.OperationLow++
			case "gen":
				n.Generation++
			case "target":
				n.TargetBuffers++
			case "context":
				n.ContextSHA[0] ^= 1
			case "op":
				n.Op = arrayRegistryRegister
			}
			require.Error(t, arrayAuthorityMatchesPopulate(c, n))
		})
	}
}
