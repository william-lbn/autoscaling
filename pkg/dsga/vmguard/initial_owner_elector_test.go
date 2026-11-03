//go:build linux

package vmguard_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func initialElectorOptions() owner.Options {
	return owner.Options{LeaseDuration: time.Second, RenewDeadline: 400 * time.Millisecond, RetryPeriod: 40 * time.Millisecond, PodUID: types.UID("00000000-0000-0000-0000-000000000001")}
}

// PG17-U-INITIAL-OWNER-PRODUCTION-001：真实CLI hold同Store.Initialize参数
// →真实client-go Elector首CREATE(transitions0)→原journal CAS(epoch2)
// →完整原Runner freeze/source WAL。Kube API/Host预算/文件明确是model，
// 不执行QEMU、不签Guest或K8s验收；不再手填LeaseTransitions掩盖生产序列。
func TestInitialOwnerRealElectorFirstLeaseToFullSourceFreeze(t *testing.T) {
	f := vmguard.NewInitialOwnerElectorFixtureForTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.EqualValues(t, 1, f.BootstrapOwner.Epoch)
	require.Equal(t, "bootstrap-"+f.Store.Binding().Incarnation, f.BootstrapOwner.HolderID)
	elector, err := owner.New(f.Core.CoordinationV1(), f.Store, initialElectorOptions())
	require.NoError(t, err)
	done := make(chan error, 1)
	observed := make(chan vmguard.InitialOwnerFreezeObservationForTest, 1)
	go func() {
		done <- elector.Run(ctx, func(call context.Context, session *owner.Session) error {
			if err := session.Check(call); err != nil {
				return err
			}
			if session.Actor().Epoch != 2 {
				return fmt.Errorf("first real journal epoch is not2: %d", session.Actor().Epoch)
			}
			frozen, err := f.FreezeForTest(call)
			if err == nil {
				observed <- frozen
			}
			return err
		})
	}()
	select {
	case <-f.TakeoverRequestedForTest():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	binding := f.Store.Binding()
	lease, err := f.Core.CoordinationV1().Leases(binding.Namespace).Get(ctx, journal.LeaseName(binding), metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.LeaseTransitions)
	require.EqualValues(t, 0, *lease.Spec.LeaseTransitions, "首Lease字段必须由真实client-go CREATE")
	before, err := f.Store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, f.BootstrapOwner, before.State().Owner, "Takeover CAS尚未可见")
	ready, uid, err := f.PollForTest(ctx)
	require.NoError(t, err)
	require.False(t, ready, "只有Lease而journal仍bootstrap不能授权")
	require.Equal(t, string(lease.UID), uid)
	f.AllowTakeoverForTest()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	frozen := <-observed
	require.EqualValues(t, 2, frozen.Owner.Epoch)
	require.Equal(t, *lease.Spec.HolderIdentity, frozen.Owner.HolderID)
	require.Equal(t, string(lease.UID), frozen.LeaseUID)
	require.Equal(t, "source_frozen", frozen.Stage)
	require.Equal(t, 1, frozen.WALStages)
	require.False(t, frozen.ProcessStarted)
}

// PG17-U-INITIAL-OWNER-PRODUCTION-002：再一次真实Elector换主产生journal3/
// LeaseTransitions1，仍不能用transitions+1推VM epoch；原UID及完整freeze保留。
func TestInitialOwnerRealElectorSuccessorEpochToFullSourceFreeze(t *testing.T) {
	f := vmguard.NewInitialOwnerElectorFixtureForTest(t)
	f.AllowTakeoverForTest()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	first, err := owner.New(f.Core.CoordinationV1(), f.Store, initialElectorOptions())
	require.NoError(t, err)
	err = first.Run(ctx, func(call context.Context, session *owner.Session) error {
		if session.Actor().Epoch != 2 {
			return fmt.Errorf("first epoch differs")
		}
		return f.AwaitForTest(call)
	})
	require.NoError(t, err)
	next, err := owner.New(f.Core.CoordinationV1(), f.Store, initialElectorOptions())
	require.NoError(t, err)
	var frozen vmguard.InitialOwnerFreezeObservationForTest
	err = next.Run(ctx, func(call context.Context, session *owner.Session) error {
		if session.Actor().Epoch != 3 {
			return fmt.Errorf("successor journal epoch differs: %d", session.Actor().Epoch)
		}
		observed, freezeErr := f.FreezeForTest(call)
		frozen = observed
		return freezeErr
	})
	require.NoError(t, err)
	lease, err := f.Core.CoordinationV1().Leases(f.Store.Binding().Namespace).Get(ctx, journal.LeaseName(f.Store.Binding()), metav1.GetOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 1, *lease.Spec.LeaseTransitions)
	require.EqualValues(t, 3, frozen.Owner.Epoch)
	require.NotEqualValues(t, uint64(*lease.Spec.LeaseTransitions)+1, frozen.Owner.Epoch)
	require.Equal(t, *lease.Spec.HolderIdentity, frozen.Owner.HolderID)
	require.Equal(t, string(lease.UID), frozen.LeaseUID)
	require.Equal(t, "source_frozen", frozen.Stage)
	require.False(t, frozen.ProcessStarted)
}
