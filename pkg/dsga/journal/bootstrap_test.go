package journal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestBootstrapHoldPreventsBeginTakeoverAndDispatch(t *testing.T) {
	_, store := newStoreAPI(t)
	snapshot, err := store.Initialize(context.Background(), "opaque-1", "bootstrap-owner")
	require.NoError(t, err)
	require.True(t, PristineBootstrap(snapshot.State()))
	require.False(t, snapshot.Dispatchable())
	_, err = store.Takeover(context.Background(), snapshot, snapshot.State().Owner, "premature-owner")
	require.Error(t, err)
	_, _, err = store.Begin(context.Background(), snapshot, snapshot.State().Owner, testRequest(snapshot.State(), 1, control.Grow), 0)
	require.Error(t, err)
}

func TestBootstrapLostReleaseReplyUsesOriginalDurableIdentityWithoutReset(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot, err := store.Initialize(context.Background(), "opaque-1", "bootstrap-owner")
	require.NoError(t, err)
	f.mu.Lock()
	f.failAfterCommit = true
	f.mu.Unlock()
	result, err := store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), "config-uid")
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	require.Nil(t, result)
	observed, err := store.Load(context.Background())
	require.NoError(t, err)
	require.True(t, observed.VirtualMachine().Spec.DSGABootstrap.Ready)
	require.Equal(t, snapshot.State(), observed.State(), "release must not recreate/reset journal")
	f.mu.Lock()
	patches := f.patches
	f.mu.Unlock()
	confirmed, err := store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	require.Equal(t, observed.State(), confirmed.State())
	_, err = store.ReleaseBootstrap(context.Background(), observed.ResourceVersion(), "other-config")
	require.Error(t, err)
	f.mu.Lock()
	require.Equal(t, patches, f.patches, "readback confirmation must not resend PATCH")
	f.mu.Unlock()
}

func TestBootstrapReleaseCASDoesNotAffectRecreatedOrChangedVM(t *testing.T) {
	for _, scenario := range []string{"uid", "resourceVersion", "bootstrap"} {
		t.Run(scenario, func(t *testing.T) {
			f, store := newStoreAPI(t)
			snapshot, err := store.Initialize(context.Background(), "opaque-1", "bootstrap-owner")
			require.NoError(t, err)
			f.mu.Lock()
			f.beforePatch = func() {
				f.change(func(vm *vmv1.VirtualMachine) {
					switch scenario {
					case "uid":
						vm.UID = "recreated-vm"
					case "bootstrap":
						vm.Spec.DSGABootstrap.ComputeInstanceID = "changed-compute"
					case "resourceVersion":
						vm.Labels = map[string]string{"concurrent": "writer"}
					}
				})
				if scenario == "uid" {
					// UID防线必须独立于RV：模拟同名新对象具有相同opaque RV，
					// 移除UID条件时即使客户端因回包不符报错，也不能已写坏新VM。
					f.mu.Lock()
					f.vm.ResourceVersion = snapshot.ResourceVersion()
					f.mu.Unlock()
				}
			}
			f.mu.Unlock()
			result, err := store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), "config-uid")
			require.Error(t, err)
			require.Nil(t, result)
			f.mu.Lock()
			require.False(t, f.vm.Spec.DSGABootstrap.Ready)
			f.mu.Unlock()
		})
	}
}

func TestBootstrapReleaseNeverReinitializesAnEmptyLookingOldGeneration(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot, err := store.Initialize(context.Background(), "opaque-1", "bootstrap-owner")
	require.NoError(t, err)
	next, err := snapshot.State().Takeover(snapshot.State().Owner, "previous-generation")
	require.NoError(t, err)
	_, encoded, err := encode(next, MaxEncodedBytes)
	require.NoError(t, err)
	f.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = encoded })
	snapshot, err = store.Load(context.Background())
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), "config-uid")
	require.Error(t, err)
}

func TestBootstrapRequiresHoldFromCreateAndInspectCopiesState(t *testing.T) {
	f, store := newStoreAPI(t)
	f.change(func(vm *vmv1.VirtualMachine) { vm.Spec.DSGABootstrap = nil })
	f.mu.Lock()
	rv := f.vm.ResourceVersion
	f.mu.Unlock()
	_, err := store.Initialize(context.Background(), rv, "owner")
	require.Error(t, err)
	f, store = newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	input := snapshot.VirtualMachine()
	inspected, err := Inspect(input, testBinding())
	require.NoError(t, err)
	input.Spec.DSGABootstrap.Ready = false
	require.True(t, inspected.VirtualMachine().Spec.DSGABootstrap.Ready)
}
