//go:build linux && dsga_native_kernel_bootstrap

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

// 只在隔离构建工具中导出测试ISO原字节；没有将fixture当作生产来源资格。
func TestExportKernelBootstrapNativeFixture(t *testing.T) {
	dir := os.Getenv("DSGA_KERNEL_BOOTSTRAP_EXPORT")
	if dir != "/out/isofiles" {
		t.Fatal("isolated exporter only")
	}
	_, in := kernelPrepFixtureInputs(t)
	require.NoError(t, os.MkdirAll(dir, 0755))
	for n, b := range map[string][]byte{"dsga-resource.json": in.RuntimeRaw, "dsga-physical-kernel.json": in.PhysicalPolicyRaw, "dsga-kernel-bootstrap.json": in.PolicyRaw} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), b, 0444))
	}
}

type nativeKernelPrepWriteFault struct {
	*linuxKernel
	injected bool
}

func (k *nativeKernelPrepWriteFault) kernelPrepWrite(n string, v uint64) error {
	if e := k.linuxKernel.kernelPrepWrite(n, v); e != nil {
		return e
	}
	if !k.injected {
		k.injected = true
		return errors.New("isolated native injected error after actual modules write")
	}
	return nil
}

// 真369、真ISO、真ext4和真实sysctl；仅故障返回由test wrapper注入，内核写未mock。
// 不运行PG/Origin/Bootstrap/热增，不授予完整normal来源或许可。
func TestNativeKernelBootstrapComponent(t *testing.T) {
	require.Equal(t, "isolated-kvm-kernel-bootstrap-v1", os.Getenv("DSGA_KERNEL_BOOTSTRAP_NATIVE"))
	require.Zero(t, os.Geteuid())
	mode := os.Getenv("DSGA_KERNEL_BOOTSTRAP_MODE")
	source, _, e := runtimeSource()
	require.NoError(t, e)
	g, e := Open(source.Config)
	require.NoError(t, e)
	k := g.kernel.(*linuxKernel)
	before, e := k.kernelPrepRead()
	require.NoError(t, e)
	require.Equal(t, uint32(512), before.HardPIDMax)
	if mode == "modules-already-disabled" || mode == "Movable" {
		_, e = g.PrepareKernelBeforeOrigin(context.Background())
		require.Error(t, e)
		_, e = os.Stat(filepath.Join(source.Config.StateDir, kernelPrepIntentName))
		require.True(t, os.IsNotExist(e))
		after, e := k.kernelPrepRead()
		require.NoError(t, e)
		require.Equal(t, before, after)
		fmt.Printf("DSGA_KERNEL_BOOTSTRAP_NEGATIVE={\"mode\":%q,\"pass\":true}\n", mode)
		return
	}
	require.Contains(t, []string{"positive", "partial-replay"}, mode)
	if mode == "partial-replay" {
		g.kernel = &nativeKernelPrepWriteFault{linuxKernel: k}
		_, e = g.PrepareKernelBeforeOrigin(context.Background())
		require.ErrorIs(t, e, ErrUnknown)
		g.kernel = k
		pending, e := os.ReadFile(filepath.Join(source.Config.StateDir, kernelPrepIntentName))
		require.NoError(t, e)
		middle, e := k.kernelPrepRead()
		require.NoError(t, e)
		require.Equal(t, uint32(1), middle.ModulesDisabled)
		for _, op := range []func(context.Context) (NormalV1KernelBootstrapEvidence, error){g.PrepareKernelBeforeOrigin, g.ObserveKernelBeforeOrigin} {
			_, e = op(context.Background())
			require.ErrorIs(t, e, ErrUnknown)
		}
		unchanged, e := k.kernelPrepRead()
		require.NoError(t, e)
		require.Equal(t, middle, unchanged)
		evidence, e := g.ReplayKernelBeforeOrigin(context.Background())
		require.NoError(t, e)
		require.True(t, evidence.DurabilityConfirmed())
		after, e := os.ReadFile(filepath.Join(source.Config.StateDir, kernelPrepIntentName))
		require.NoError(t, e)
		require.Equal(t, pending, after)
	} else {
		evidence, e := g.PrepareKernelBeforeOrigin(context.Background())
		require.NoError(t, e)
		require.True(t, evidence.DurabilityConfirmed())
	}
	observed, e := g.ObserveKernelBeforeOrigin(context.Background())
	require.NoError(t, e)
	require.False(t, observed.DurabilityConfirmed())
	again, e := g.PrepareKernelBeforeOrigin(context.Background())
	require.NoError(t, e)
	require.False(t, again.DurabilityConfirmed())
	replayed, e := g.ReplayKernelBeforeOrigin(context.Background())
	require.NoError(t, e)
	require.True(t, replayed.DurabilityConfirmed())
	require.Equal(t, observed.Nonce, replayed.Nonce)
	require.False(t, replayed.AllSourcesClosed)
	require.False(t, replayed.VMShrinkPermit)
	// 隔离Guest故意尝试放宽，精确内核必须拒绝；生产组件本身从不写0。
	require.Error(t, writeNumber("/proc/sys/kernel/modules_disabled", 0))
	after, e := k.kernelPrepRead()
	require.NoError(t, e)
	require.True(t, after.target(mustKernelPrepPolicy(t, g)))
	for _, n := range []string{"state.json", physicalOriginDataName} {
		_, e = os.Stat(filepath.Join(source.Config.StateDir, n))
		require.True(t, os.IsNotExist(e))
	}
	b, e := json.Marshal(struct {
		Mode         string                          `json:"mode"`
		Before       kernelPrepSnapshot              `json:"before"`
		Evidence     NormalV1KernelBootstrapEvidence `json:"evidence"`
		Durability   bool                            `json:"replay_durability_confirmed"`
		SourceClosed bool                            `json:"all_sources_closed"`
	}{mode, before, replayed, replayed.DurabilityConfirmed(), false})
	require.NoError(t, e)
	fmt.Printf("DSGA_KERNEL_BOOTSTRAP_COMPONENT=%s\n", b)
}
func mustKernelPrepPolicy(t *testing.T, g *Guard) NormalV1KernelBootstrapPolicy {
	t.Helper()
	in, e := g.normalKernelBootstrapInputs()
	require.NoError(t, e)
	d, e := in.decode(g.config)
	require.NoError(t, e)
	return d.policy
}
