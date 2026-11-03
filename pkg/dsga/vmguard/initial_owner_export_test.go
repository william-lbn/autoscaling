//go:build linux

package vmguard

// 本文件仅在go test构建：external vmguard_test复用原完整fixture和私有freeze，
// 让真实owner.Elector跨包调用原生产路径，不向Runner发行物导出任何新API。
import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
)

type InitialOwnerElectorFixtureForTest struct {
	Core              *kubefake.Clientset
	Store             *journal.Store
	BootstrapOwner    journal.Owner
	fixture           *runnerFixture
	raw               []byte
	command           *exec.Cmd
	takeoverRequested chan struct{}
	allowTakeover     chan struct{}
	allowOnce         sync.Once
}

type initialOwnerGatedVMForTest struct {
	vmclient.VirtualMachineInterface
	requested chan struct{}
	proceed   chan struct{}
	once      sync.Once
}

func (v *initialOwnerGatedVMForTest) Patch(ctx context.Context, name string, patchType types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*vmv1.VirtualMachine, error) {
	var wait bool
	v.once.Do(func() { close(v.requested); wait = true })
	if wait {
		select {
		case <-v.proceed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return v.VirtualMachineInterface.Patch(ctx, name, patchType, data, options, subresources...)
}

func NewInitialOwnerElectorFixtureForTest(t *testing.T) *InitialOwnerElectorFixtureForTest {
	t.Helper()
	f, raw, cmd := initialBootFixture(t, false)
	oldLease := initialOwnerResetToCLIHold(t, f)
	binding := f.store.Binding()
	require.NoError(t, f.core.CoordinationV1().Leases(binding.Namespace).Delete(context.Background(), oldLease.Name, metav1.DeleteOptions{}))
	// 模拟API服务器分配metadata，绝不写HolderIdentity或LeaseTransitions。
	// 这两个Spec字段只能来自下面external test的真实client-go Elector CREATE。
	f.core.PrependReactor("create", "leases", func(a kt.Action) (bool, runtime.Object, error) {
		lease := a.(kt.CreateAction).GetObject().(*coordination.Lease)
		if lease.Namespace == binding.Namespace && lease.Name == oldLease.Name {
			lease.UID = oldLease.UID
			lease.ResourceVersion = "created-by-model-api"
		}
		return false, nil, nil
	})
	requested, proceed := make(chan struct{}), make(chan struct{})
	client := &initialOwnerGatedVMForTest{VirtualMachineInterface: f.vms.NeonvmV1().VirtualMachines(binding.Namespace), requested: requested, proceed: proceed}
	store, err := journal.NewStore(client, binding)
	require.NoError(t, err)
	return &InitialOwnerElectorFixtureForTest{Core: f.core, Store: store, BootstrapOwner: f.actor, fixture: f, raw: raw, command: cmd, takeoverRequested: requested, allowTakeover: proceed}
}
func (f *InitialOwnerElectorFixtureForTest) TakeoverRequestedForTest() <-chan struct{} {
	return f.takeoverRequested
}
func (f *InitialOwnerElectorFixtureForTest) AllowTakeoverForTest() {
	f.allowOnce.Do(func() { close(f.allowTakeover) })
}
func (f *InitialOwnerElectorFixtureForTest) PollForTest(ctx context.Context) (bool, string, error) {
	return f.fixture.runner.initialOwnerPoll(ctx, "")
}
func (f *InitialOwnerElectorFixtureForTest) AwaitForTest(ctx context.Context) error {
	return f.fixture.runner.awaitInitialOwner(ctx, 5*time.Second, time.Millisecond)
}

type InitialOwnerFreezeObservationForTest struct {
	Owner          journal.Owner
	LeaseUID       string
	Stage          string
	WALStages      int
	ProcessStarted bool
}

func (f *InitialOwnerElectorFixtureForTest) FreezeForTest(ctx context.Context) (InitialOwnerFreezeObservationForTest, error) {
	var out InitialOwnerFreezeObservationForTest
	r := f.fixture.runner
	if err := f.AwaitForTest(ctx); err != nil {
		return out, err
	}
	digest, err := r.validateProcessCommand(f.command)
	if err != nil {
		return out, err
	}
	launch := processLaunch{Identity: r.birth.Identity, BirthClaim: r.engine.binding, Nonce: strings.Repeat("56", 16), CommandSHA256: digest, ContainerID: "containerd://model-only"}
	err = r.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
		return s.writeOnce("process-launch.json", initialBootCanonical(launch))
	})
	if err != nil {
		return out, err
	}
	ref, err := r.freezeInitialBootstrap(ctx, f.raw, f.command, 256<<20)
	if err != nil {
		return out, err
	}
	s, err := lockState(ctx, f.fixture.config.StateDir)
	if err != nil {
		return out, err
	}
	defer s.close()
	entries, err := initialBootRead(s)
	if err != nil {
		return out, err
	}
	if len(entries) != 1 || ref.Stage != "source_frozen" {
		return out, fmt.Errorf("original source WAL is incomplete")
	}
	source := entries[0].Source
	out = InitialOwnerFreezeObservationForTest{Owner: source.Owner, LeaseUID: source.LeaseUID, Stage: ref.Stage, WALStages: len(entries), ProcessStarted: f.command.Process != nil}
	return out, nil
}
