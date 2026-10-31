//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func initialBootTestState(t *testing.T) (*lockedState, string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	s, e := lockState(context.Background(), dir)
	require.NoError(t, e)
	t.Cleanup(s.close)
	return s, dir
}
func TestInitialBootstrapV2WALOriginalNonceAndNoAdoption(t *testing.T) {
	// PG17-U-INITIAL-H01：真实临时目录flock/fsync/link；内容是明确模型来源。
	s, _ := initialBootTestState(t)
	source := initialBootModelSource(t)
	ref, e := initialBootFreeze(s, source)
	require.NoError(t, e)
	require.Len(t, ref.Nonce, 64)
	again, e := initialBootFreeze(s, source)
	require.NoError(t, e)
	require.Equal(t, ref, again)
	entries, e := initialBootRead(s)
	require.NoError(t, e)
	require.Len(t, entries, 1)
	source.CommandSHA256 = strings.Repeat("ab", 32)
	_, e = initialBootFreeze(s, source)
	require.Error(t, e)

	_, e = initialBootChallenge(entries[0].Source, ref)
	require.Error(t, e)
	baseRef, e := initialBootAppend(s, ref, json.RawMessage(`{"model_only":"base observation"}`))
	require.NoError(t, e)
	challenge, e := initialBootChallenge(entries[0].Source, baseRef)
	require.NoError(t, e)
	receipt := initialPrebootReceipt{Challenge: challenge, GuestBootID: "12345678-1234-1234-1234-1234567890ab", RootInstanceNonce: strings.Repeat("ef", 32), KernelPreparationSHA256: strings.Repeat("ab", 32), PhysicalOriginSHA256: strings.Repeat("bc", 32), PreSystemSHA256: strings.Repeat("cd", 32), CurrentPhysicalSHA256: strings.Repeat("de", 32)}
	require.NoError(t, receipt.MatchChallenge(challenge))
	altered := challenge
	altered.Nonce = strings.Repeat("ee", 32)
	require.Error(t, receipt.MatchChallenge(altered))
	invalid := challenge
	invalid.BusinessMinimum++
	receipt.Challenge = invalid
	require.Error(t, receipt.MatchChallenge(invalid))
	receipt.Challenge = challenge
	receipt.RootInstanceNonce = ""
	require.Error(t, receipt.MatchChallenge(challenge))
	old := ref
	ref = baseRef

	for i := 2; i < len(initialBootStages); i++ {
		ref, e = initialBootAppend(s, ref, json.RawMessage(`{"model_only":"not a real Guest/QMP proof"}`))
		require.NoError(t, e)
		require.Equal(t, initialBootStages[i], ref.Stage)
	}
	_, e = initialBootAppend(s, old, json.RawMessage(`{}`))
	require.Error(t, e)
	_, e = initialBootAppend(s, ref, json.RawMessage(`{}`))
	require.Error(t, e)
	adapter := &initialBaseQMP{}
	_, e = adapter.Read(context.Background())
	require.ErrorIs(t, e, errInitialBootUnavailable)
	require.ErrorIs(t, adapter.Set(context.Background(), source.Identity.MinimumBytes), errInitialBootUnavailable)
	_, e = initialBootGuestReceipt(context.Background(), initialPrebootChallenge{})
	require.ErrorIs(t, e, errInitialBootUnavailable)
}
func TestInitialBootstrapV2WALRejectsCorruptionAndPartialFacts(t *testing.T) {
	// PG17-U-INITIAL-H02：首次失败均保留，不清坏链或猜pristine。
	for _, name := range []string{"partial", "duplicate", "case_alias", "unknown", "gap", "wrong_nonce", "wrong_source", "wrong_prev", "symlink", "mode", "oversize", "foreign_version"} {
		t.Run(name, func(t *testing.T) {
			s, dir := initialBootTestState(t)
			source := initialBootModelSource(t)
			ref, e := initialBootFreeze(s, source)
			require.NoError(t, e)
			if name == "wrong_nonce" || name == "wrong_source" || name == "wrong_prev" {
				_, e = initialBootAppend(s, ref, json.RawMessage(`{}`))
				require.NoError(t, e)
			}
			file := filepath.Join(dir, initialBootFile(0))
			raw, e := os.ReadFile(file)
			require.NoError(t, e)
			switch name {
			case "partial":
				raw = raw[:len(raw)/2]
			case "duplicate":
				raw = bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"protocol_version":2`), 1)
			case "case_alias":
				raw = bytes.Replace(raw, []byte(`"scope":`), []byte(`"Scope":`), 1)
			case "unknown":
				require.NoError(t, os.WriteFile(filepath.Join(dir, "initial-boot-v2-99.json"), []byte(`{}`), 0600))
			case "gap":
				require.NoError(t, os.Rename(file, filepath.Join(dir, initialBootFile(2))))
				file = ""
			case "wrong_nonce", "wrong_source", "wrong_prev":
				file = filepath.Join(dir, initialBootFile(1))
				b, e := os.ReadFile(file)
				require.NoError(t, e)
				var v initialBootEntry
				require.NoError(t, json.Unmarshal(b, &v))
				switch name {
				case "wrong_nonce":
					v.Nonce = strings.Repeat("cc", 32)
				case "wrong_source":
					v.Source.ResourceSHA256 = strings.Repeat("ab", 32)
					v.SourceSHA256 = initialBootSHA(initialBootCanonical(v.Source))
				case "wrong_prev":
					v.PreviousSHA256 = strings.Repeat("aa", 32)
				}
				raw = initialBootCanonical(v)
			case "symlink":
				require.NoError(t, os.Remove(file))
				require.NoError(t, os.Symlink("/etc/passwd", file))
				file = ""
			case "mode":
				require.NoError(t, os.Chmod(file, 0644))
				file = ""
			case "oversize":
				raw = bytes.Repeat([]byte(" "), 16385)
			case "foreign_version":
				raw = bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":1`), 1)
			}
			if file != "" {
				require.NoError(t, os.WriteFile(file, raw, 0600))
			}
			_, e = initialBootRead(s)
			require.Error(t, e)
			_, e = initialBootFreeze(s, source)
			require.Error(t, e)
		})
	}
	for _, raw := range []json.RawMessage{nil, []byte(`null`), []byte(`[]`), []byte(`{"x":1,"x":2}`), bytes.Repeat([]byte(" "), 8193)} {
		s, _ := initialBootTestState(t)
		ref, e := initialBootFreeze(s, initialBootModelSource(t))
		require.NoError(t, e)
		_, e = initialBootAppend(s, ref, raw)
		require.Error(t, e)
		entries, e := initialBootRead(s)
		require.NoError(t, e)
		require.Len(t, entries, 1)
	}
}
func TestInitialBootstrapV2WALLockCancellationAndWriteFailure(t *testing.T) {
	// PG17-U-INITIAL-H03：真实竞争者deadline与真实EBADF，不冒充fsync/ENOSPC覆盖。
	s, dir := initialBootTestState(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, e := lockState(ctx, dir)
	require.ErrorIs(t, e, context.DeadlineExceeded)

	source := initialBootModelSource(t)
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("file_sync_recovery_error_%d", failAt), func(t *testing.T) {
			state, dir2 := initialBootTestState(t)
			ref, e := initialBootFreeze(state, source)
			require.NoError(t, e)
			_, e = initialBootAppend(state, ref, json.RawMessage(`{"model_only":"visible old base observation"}`))
			require.NoError(t, e)
			before, e := os.ReadFile(filepath.Join(dir2, initialBootFile(0)))
			require.NoError(t, e)
			op := &initialBootFailFileSync{failAt: failAt}
			recovered, e := initialBootFreezeSync(state, source, op)
			require.ErrorIs(t, e, unix.EIO)
			require.Equal(t, initialBootRef{}, recovered)
			require.Equal(t, failAt, op.files)
			require.Equal(t, failAt-1, op.dirs, "file.Sync失败后不能目录sync掩盖")
			after, e := os.ReadFile(filepath.Join(dir2, initialBootFile(0)))
			require.NoError(t, e)
			require.Equal(t, before, after)
			_, e = os.Stat(filepath.Join(dir2, initialBootFile(2)))
			require.True(t, os.IsNotExist(e))
		})
	}
	require.NoError(t, s.dir.Close())
	_, e = initialBootFreeze(s, source)
	require.Error(t, e)
	_, e = os.Stat(filepath.Join(dir, initialBootFile(0)))
	require.True(t, os.IsNotExist(e))
}

// EIO是明确单元注入，未伪称真实磁盘错误。其他文件/目录同步仍实际走原fd。
type initialBootFailFileSync struct{ files, dirs, failAt int }

func (op *initialBootFailFileSync) file(f *os.File) error {
	op.files++
	if _, e := f.Stat(); e != nil {
		return e
	}
	if op.files == op.failAt {
		return unix.EIO
	}
	return f.Sync()
}
func (op *initialBootFailFileSync) directory(f *os.File) error { op.dirs++; return f.Sync() }
