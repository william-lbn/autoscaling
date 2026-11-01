//go:build linux && dsga_native_metadata_wiring

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

// 机制S必须由新的无PG18c VM在原producer提供RO FD3后显式调用；不重开路径、不签R2/fullSource。
func TestNativeKernelPrepV2HandoffOriginalFDPread(t *testing.T) {
	require.Equal(t, "isolated18c-original-fd-pread-v1", os.Getenv("DSGA_METADATA_PREAD_NATIVE"))
	require.Equal(t, "3", os.Getenv("DSGA_METADATA_PREAD_FD"))
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	release, e := kernelText("/proc/sys/kernel/osrelease")
	require.NoError(t, e)
	require.Equal(t, ref.KernelRelease, release)
	f := os.NewFile(3, "original dedicated producer RO control")
	require.NotNil(t, f)
	defer f.Close()
	object, e := normalKernelPrepV2ControlObject(int(f.Fd()))
	require.NoError(t, e)
	require.NoError(t, normalKernelPrepV2TransferredControl(f, object))
	for i := 0; i < 200; i++ {
		_, e := normalKernelPrepV2ReadAtControl(f)
		require.NoError(t, e)
	}
	t.Log("ORIGINAL_FD_PREAD_COMPONENT_ONLY normal=false fullSource=false")
}
