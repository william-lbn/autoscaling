//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalMemoryCommit2GuardCannotReconstructConfirmedPointer(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	ctx := context.Background()
	s, e := lockState(ctx, dir)
	require.NoError(t, e)
	defer s.close()
	g := &Guard{config: Config{StateDir: dir}}
	sameConfigNewGuard := &Guard{config: g.config}
	defer g.normalMemoryDiscardOriginalConfirmedSource()
	defer sameConfigNewGuard.normalMemoryDiscardOriginalConfirmedSource()
	var fromJSON normalMemoryOriginSource
	require.NoError(t, json.Unmarshal([]byte(`{}`), &fromJSON))
	require.ErrorIs(t, g.normalMemoryInstallOriginalConfirmedSourceLocked(ctx, s, &fromJSON), ErrUnknown)
	for _, guard := range []*Guard{g, sameConfigNewGuard} {
		source, e := guard.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
		require.Nil(t, source)
		require.ErrorIs(t, e, ErrUnknown)
	}
	for _, name := range normalMemoryConfirmedFiles() {
		_, e := os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(e), "no wire/JSON should create history")
	}
}
func TestNormalMemoryCommit2ConfirmedGetterFailsClosedAndCleanupIsLocal(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	ctx := context.Background()
	s, e := lockState(ctx, dir)
	require.NoError(t, e)
	defer s.close()
	g := &Guard{}
	other := &Guard{}
	// 恶意/错误内部值也不变成opaque来源。只验证getter防御，不伪造Source正控。
	normalMemoryOriginalConfirmedSources.Store(g, "not-a-source")
	normalMemoryOriginalConfirmedSources.Store(other, "other-guard-invalid")
	defer other.normalMemoryDiscardOriginalConfirmedSource()
	source, e := g.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
	require.Nil(t, source)
	require.ErrorIs(t, e, ErrUnknown)
	g.normalMemoryDiscardOriginalConfirmedSource()
	_, exists := normalMemoryOriginalConfirmedSources.Load(g)
	require.False(t, exists)
	_, exists = normalMemoryOriginalConfirmedSources.Load(other)
	require.True(t, exists)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	source, e = other.normalMemoryOriginalConfirmedSourceLocked(cancelled, s)
	require.Nil(t, source)
	require.True(t, errors.Is(e, ErrUnknown))
}
func TestNormalMemoryCommit2SelectorComesFromExactOriginalAssets(t *testing.T) {
	d := runtimeTemplatesFullFixture(t)
	raw0, e := json.Marshal(d)
	require.NoError(t, e)
	assets0, e := RenderRuntimePolicyAssets(raw0)
	require.NoError(t, e)
	assets0[RuntimeResourceAssetName] = raw0
	old, e := DeriveRuntimeBootRequirements(raw0, assets0)
	require.NoError(t, e)
	require.Zero(t, old.MemoryOriginProtocolVersion())
	envelope, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range envelope.Templates {
		if envelope.Templates[i].Asset != "dsga-normal-split.json" {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal(envelope.Templates[i].Policy, &fields))
		fields["protocol_version"] = 3
		fields["memory_origin_protocol_version"] = 2
		envelope.Templates[i].Policy, e = json.Marshal(fields)
		require.NoError(t, e)
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(envelope.Templates))
	raw2, e := json.Marshal(d)
	require.NoError(t, e)
	assets2, e := RenderRuntimePolicyAssets(raw2)
	require.NoError(t, e)
	assets2[RuntimeResourceAssetName] = raw2
	selected, e := DeriveRuntimeBootRequirements(raw2, assets2)
	require.NoError(t, e)
	require.Equal(t, uint32(2), selected.MemoryOriginProtocolVersion())
	require.Equal(t, old.RequiredKernelArgs(), selected.RequiredKernelArgs())
	require.Equal(t, old.ProfileID(), selected.ProfileID())
	_, e = DeriveRuntimeBootRequirements(raw0, assets2)
	require.Error(t, e)
	_, e = DeriveRuntimeBootRequirements(raw2, assets0)
	require.Error(t, e)
}
