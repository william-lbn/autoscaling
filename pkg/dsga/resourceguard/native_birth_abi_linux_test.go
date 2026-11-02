//go:build linux

package resourceguard

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 仅实际C编译器组件：range地址是fixture，不是实际PG映射，也不签安装/Root来源。
// 原ELF固定SHA来自独立27b构建；C用自身sizeof重新计算，Go拒任何回显差异。
func TestNativeBirthPG18ActualELFFilterABI(t *testing.T) {
	path := os.Getenv("NEON_TEST_PG18_POSTGRES")
	if path == "" {
		t.Skip("requires explicitly locked real PG18 binary in isolated native runtime")
	}
	require.Equal(t, 0, os.Getuid())
	expectedSHA := os.Getenv("NEON_TEST_PG18_SHA256")
	_, err := nativeBirthDigest(expectedSHA)
	require.NoError(t, err)
	pg, err := os.Open(path)
	require.NoError(t, err)
	defer pg.Close()
	before, err := nativeBirthFileHash(pg)
	require.NoError(t, err)
	require.Equal(t, expectedSHA, before)
	_, err = nativeBirthParseELF(pg)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	version := exec.CommandContext(ctx, "/proc/self/fd/3", "--version")
	version.ExtraFiles = []*os.File{pg}
	version.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}
	raw, err := version.CombinedOutput()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimSpace(string(raw)), "postgres (PostgreSQL) 18."), "actual ELF must be18")
	source, _, _, _ := nativeBirthVersionSourceFixture(t, 18, expectedSHA)
	require.Equal(t, before, source.binding.BinarySHA256)
	expected, err := source.binding.expected(strings.Repeat("21", 32), source.resource.Limits.AddressSpaceMaxBytes)
	require.NoError(t, err)
	request := func(e nativeBirthExpected) nativeBirthRequest {
		lengths, total, err := e.geometry()
		require.NoError(t, err)
		r := nativeBirthRequest{Original: e, TotalReservedBytes: total}
		base := uint64(1 << 30)
		for i, length := range lengths {
			r.Ranges[i] = nativeBirthRange{base, length, e.ElementBytes[i]}
			base += length + 4096
		}
		return r
	}
	kernel := &nativeBirthTraceKernel{binary: pg}
	bundle, err := kernel.compile(ctx, request(expected))
	require.NoError(t, err)
	require.NoError(t, normalPGMemoryControlReviewBundle(bundle))
	t.Logf("actualPG18ELF=%s layout=%v filterSHA=%x programs=%d,%d,%d peer_is_model=false Source=false normal=false", before, expected.ElementBytes, bundle.EncodedSHA256, len(bundle.Programs[0]), len(bundle.Programs[1]), len(bundle.Programs[2]))
	t.Run("wrong-actual-C-layout", func(t *testing.T) {
		wrong := expected
		wrong.ElementBytes[3] = 24
		_, err := kernel.compile(ctx, request(wrong))
		require.Error(t, err, "actual C sizeof must refuse another declared geometry")
	})
	after, err := nativeBirthFileHash(pg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
