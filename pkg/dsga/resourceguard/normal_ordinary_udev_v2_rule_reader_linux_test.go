//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"testing"
)

type normalOrdinaryRuleDEVReader struct {
	rules []normalOrdinaryUdevV2RuleSource
	err   error
	call  func()
}

func (r *normalOrdinaryRuleDEVReader) duplicateOriginalRules(context.Context) ([]normalOrdinaryUdevV2RuleSource, error) {
	if r.call != nil {
		r.call()
	}
	return r.rules, r.err
}
func normalOrdinaryRuleDEVSystem(raw []byte) *normalSystemLifecycleSource {
	return &normalSystemLifecycleSource{manifest: NormalSystemSourceManifest{Files: []NormalSystemSourceFile{{Path: "/lib/udev/rules.d/50-own.rules", CanonicalPath: "/lib/udev/rules.d/50-own.rules", Kind: "config", Bytes: uint64(len(raw)), SHA256: physicalDigest(raw)}}}}
}
func TestNormalOrdinaryUdevV2RuleReaderNoConfigELFFallback(t *testing.T) {
	ctx := context.Background()
	raw := []byte("# exact original rule\n")
	s := normalOrdinaryRuleDEVSystem(raw)
	for _, mode := range []string{"nil reader", "closed System", "no rules", "wrong kind", "duplicate manifest", "reader failure", "missing", "extra", "unknown path", "wrong SHA", "nil FD"} {
		t.Run(mode, func(t *testing.T) {
			system := normalOrdinaryRuleDEVSystem(raw)
			reader := &normalOrdinaryRuleDEVReader{}
			switch mode {
			case "nil reader":
				_, e := takeNormalOrdinaryUdevV2OriginalRules(ctx, s, nil)
				require.ErrorIs(t, e, ErrUnknown)
				return
			case "closed System":
				system.closed = true
			case "no rules":
				system.manifest.Files = nil
			case "wrong kind":
				system.manifest.Files[0].Kind = "executable"
			case "duplicate manifest":
				system.manifest.Files = append(system.manifest.Files, system.manifest.Files[0])
			case "reader failure":
				reader.err = errors.New("original RO pin cannot duplicate")
			case "missing":
			case "extra":
				reader.rules = make([]normalOrdinaryUdevV2RuleSource, 2)
			default:
				reader.rules = []normalOrdinaryUdevV2RuleSource{{path: system.manifest.Files[0].Path, sha: physicalDigest(raw), raw: raw}}
				if mode == "unknown path" {
					reader.rules[0].path = "/etc/udev/rules.d/extra.rules"
				}
				if mode == "wrong SHA" {
					reader.rules[0].sha = physicalDigest([]byte("other"))
				}
			}
			_, e := takeNormalOrdinaryUdevV2OriginalRules(ctx, system, reader)
			require.ErrorIs(t, e, ErrUnknown)
		})
	}
}
func TestNormalOrdinaryUdevV2RuleReaderClosesTransferredPinsOnFailure(t *testing.T) {
	for _, mode := range []string{"reader error", "count mismatch", "RW mount", "O_PATH", "write FD", "no CLOEXEC", "wrong bytes"} {
		t.Run(mode, func(t *testing.T) {
			raw := []byte("# original rule\n")
			system := normalOrdinaryRuleDEVSystem(raw)
			path := t.TempDir() + "/rule"
			require.NoError(t, os.WriteFile(path, raw, 0600))
			flags := unix.O_RDONLY | unix.O_CLOEXEC
			if mode == "O_PATH" {
				flags = unix.O_PATH | unix.O_CLOEXEC
			}
			if mode == "write FD" {
				flags = unix.O_RDWR | unix.O_CLOEXEC
			}
			if mode == "no CLOEXEC" {
				flags = unix.O_RDONLY
			}
			fd, e := unix.Open(path, flags, 0)
			require.NoError(t, e)
			f := os.NewFile(uintptr(fd), "own rule pin")
			reader := &normalOrdinaryRuleDEVReader{rules: []normalOrdinaryUdevV2RuleSource{{path: system.manifest.Files[0].Path, sha: physicalDigest(raw), raw: raw, original: f}}}
			if mode == "reader error" {
				reader.err = errors.New("dup partial error")
			}
			if mode == "count mismatch" {
				reader.rules = append(reader.rules, normalOrdinaryUdevV2RuleSource{})
			}
			if mode == "wrong bytes" {
				reader.rules[0].raw = []byte("# claimed rules\n")
			}
			_, e = takeNormalOrdinaryUdevV2OriginalRules(context.Background(), system, reader)
			require.ErrorIs(t, e, ErrUnknown)
			var st unix.Stat_t
			require.ErrorIs(t, unix.Fstat(fd, &st), unix.EBADF, "failure must close transferred original FD")
		})
	}
}
func TestNormalOrdinaryUdevV2RuleReaderReleasesSystemBeforeInputs(t *testing.T) {
	raw := []byte("# rule\n")
	system := normalOrdinaryRuleDEVSystem(raw)
	reader := &normalOrdinaryRuleDEVReader{err: errors.New("stop before ordinary FD")}
	reader.call = func() {
		require.True(t, system.mu.TryLock(), "Inputs reader must run without System lock")
		system.mu.Unlock()
	}
	_, e := takeNormalOrdinaryUdevV2OriginalRules(context.Background(), system, reader)
	require.ErrorIs(t, e, ErrUnknown)
}

type normalOrdinaryRuleDEVPreparation struct{ onClose func() }

func (p *normalOrdinaryRuleDEVPreparation) referenceLocked(context.Context, *lockedState) (normalOrdinaryUdevV2PreparationReference, error) {
	return normalOrdinaryUdevV2PreparationReference{}, ErrUnknown
}
func (p *normalOrdinaryRuleDEVPreparation) projectionForOriginalDeviceLocked(context.Context, *lockedState, *normalOrdinaryUdevV2CatalogEntry) (normalOrdinaryUdevV2DeviceProjection, error) {
	return normalOrdinaryUdevV2DeviceProjection{}, ErrUnknown
}
func (p *normalOrdinaryRuleDEVPreparation) close() { p.onClose() }
func TestNormalOrdinaryUdevV2RuleReaderCloseOwnPreparation(t *testing.T) {
	q := &normalOrdinaryUdevV2{emitter: -1}
	calls := 0
	q.preparation = &normalOrdinaryRuleDEVPreparation{onClose: func() {
		calls++
		require.True(t, q.mu.TryLock(), "prep close must run without source lock")
		require.True(t, q.closed, "source must close before releasing original prep")
		q.mu.Unlock()
	}}
	q.close()
	q.close()
	require.Equal(t, 1, calls, "one original instance releases its original prep once")
}
