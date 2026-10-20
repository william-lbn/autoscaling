//go:build dsga_native_e2e

package compute_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// Kubernetes仍是本机UID/RV协议夹具，PG事实由真实Rust/SQL生成。Lease选主、
// journal接管及PG循环均使用生产实现；不以此证明集群RBAC/etcd/VM预算或物理释放。
func nativeOwnerStore(t *testing.T, instance control.Instance) (*journal.Store, coordclient.CoordinationV1Interface) {
	t.Helper()
	var mu sync.Mutex
	var vmStatus vmv1.VirtualMachineStatus
	vm := &vmv1.VirtualMachine{TypeMeta: metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"}, ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "owner-native-vm", UID: types.UID(instance.VMUID), ResourceVersion: "vm-1"}, Spec: vmv1.VirtualMachineSpec{}, Status: vmStatus}
	var lease *coordination.Lease
	vmVersion, leaseVersion := 1, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		failure := func(code int) {
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": map[int]string{404: "NotFound", 409: "Conflict"}[code], "code": code})
		}
		if strings.HasPrefix(r.URL.Path, "/apis/coordination.k8s.io/v1/namespaces/testing/leases") {
			switch r.Method {
			case http.MethodGet:
				if lease == nil {
					failure(http.StatusNotFound)
					return
				}
			case http.MethodPost, http.MethodPut:
				var next coordination.Lease
				if json.NewDecoder(r.Body).Decode(&next) != nil {
					failure(http.StatusBadRequest)
					return
				}
				if r.Method == http.MethodPost {
					if lease != nil {
						failure(http.StatusConflict)
						return
					}
					next.UID = "native-lease-uid"
				} else if lease == nil || lease.UID != next.UID || lease.ResourceVersion != next.ResourceVersion {
					failure(http.StatusConflict)
					return
				}
				leaseVersion++
				next.ResourceVersion = "lease-" + strconv.Itoa(leaseVersion)
				lease = &next
			default:
				failure(http.StatusMethodNotAllowed)
				return
			}
			_ = json.NewEncoder(w).Encode(lease)
			return
		}
		if r.URL.Path != "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/owner-native-vm" {
			failure(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPatch {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			operations, err := jsonpatch.DecodePatch(body)
			require.NoError(t, err)
			previous, err := json.Marshal(vm)
			require.NoError(t, err)
			next, err := operations.Apply(previous)
			if err != nil {
				failure(http.StatusUnprocessableEntity)
				return
			}
			require.NoError(t, json.Unmarshal(next, &vm))
			vmVersion++
			vm.ResourceVersion = "vm-" + strconv.Itoa(vmVersion)
		} else if r.Method != http.MethodGet {
			failure(http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(vm)
	}))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host = server.URL
	config.QPS = 10000
	config.Burst = 10000
	config.Timeout = time.Second
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := journal.NewStore(client.NeonvmV1().VirtualMachines("testing"), journal.Binding{Namespace: "testing", Name: "owner-native-vm", VMUID: types.UID(instance.VMUID), Incarnation: "1234567890abcdef1234567890abcdef"})
	require.NoError(t, err)
	_, err = store.Initialize(context.Background(), "vm-1", "native-bootstrap")
	require.NoError(t, err)
	mu.Lock()
	vm.Status.Phase = vmv1.VmRunning
	vm.Status.PodName = "native-runner"
	vm.Status.PodIP = "127.0.0.1"
	vmVersion++
	vm.ResourceVersion = "vm-" + strconv.Itoa(vmVersion)
	mu.Unlock()
	coord, err := coordclient.NewForConfig(config)
	require.NoError(t, err)
	return store, coord
}

func nativeOwnedPG(t *testing.T, intent control.Request, config compute.Config) {
	t.Helper()
	store, coord := nativeOwnerStore(t, intent.Key.Instance)
	elector, err := owner.New(coord, store, owner.DefaultOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *owner.Session) error {
		require.Equal(t, config.IssuerEpoch, session.Actor().Epoch)
		intent.Key.IssuerEpoch = session.Actor().Epoch
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		snapshot, disposition, err := store.Begin(ctx, snapshot, session.Actor(), intent, 0)
		require.NoError(t, err)
		require.Equal(t, journal.Accepted, disposition)
		for _, step := range snapshot.State().Active.Steps {
			if step.Action == journal.ResizePG {
				break
			}
			snapshot = nativeNonPGStep(t, store, snapshot, step)
		}
		worker, err := session.NewPGWorker(ctx, config)
		require.NoError(t, err)
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		unknown := 0
		err = worker.Run(loopCtx, owner.PGLoopOptions{PollInterval: 50 * time.Millisecond, Wake: nil, Replay: nil, Observe: func(tick owner.Tick) {
			if tick.Err != nil {
				var call *compute.CallError
				require.True(t, errors.As(tick.Err, &call))
				require.True(t, call.MayHaveExecuted)
				unknown++
			}
			if tick.Progress == pgexec.Recorded {
				stop()
			}
		}})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, unknown, "owned loop must observe one actual post-commit lost reply")
		checkNativeJournal(t, store, intent)
		progress, err := worker.ReplayOriginal(ctx, intent.Key)
		require.NoError(t, err)
		require.Equal(t, pgexec.Replayed, progress)
		finishNativeJournal(t, store, intent)
		return nil
	})
	require.NoError(t, err)
}
