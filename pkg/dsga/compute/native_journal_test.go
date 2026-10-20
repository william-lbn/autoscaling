//go:build dsga_native_e2e

package compute_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 只模拟VM对象的RFC6902持久提交，不模拟Compute/PG事实或真实apiserver认证。
// native主流程的PG步骤由生产pgexec发往真实Router/JWT/SQL，其他领域明确使用
// test_only_non_pg_domain夹具；本门禁不证明预算、Host/Guest/VM完成或租约归属。
func nativeJournal(t *testing.T, intent control.Request) *journal.Store {
	t.Helper()
	var mu sync.Mutex
	version := 1
	var status vmv1.VirtualMachineStatus
	vm := &vmv1.VirtualMachine{
		TypeMeta:   metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "native-vm", UID: types.UID(intent.Key.Instance.VMUID), ResourceVersion: "rv-1"},
		Spec:       vmv1.VirtualMachineSpec{}, Status: status,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			operations, err := jsonpatch.DecodePatch(body)
			require.NoError(t, err)
			previous, err := json.Marshal(vm)
			require.NoError(t, err)
			next, err := operations.Apply(previous)
			if err != nil {
				http.Error(w, "CAS failed", http.StatusUnprocessableEntity)
				return
			}
			require.NoError(t, json.Unmarshal(next, &vm))
			version++
			vm.ResourceVersion = "rv-" + strconv.Itoa(version)
		} else if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(vm)
	}))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host = server.URL
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := journal.NewStore(client.NeonvmV1().VirtualMachines("testing"), journal.Binding{
		Namespace: "testing", Name: "native-vm", VMUID: vm.UID, Incarnation: "1234567890abcdef1234567890abcdef",
	})
	require.NoError(t, err)
	_, err = store.Initialize(context.Background(), "rv-1", "native-pg-owner")
	require.NoError(t, err)
	mu.Lock()
	vm.Status.Phase = vmv1.VmRunning
	vm.Status.PodName = "native-fixture-runner"
	vm.Status.PodIP = "127.0.0.1"
	version++
	vm.ResourceVersion = "rv-" + strconv.Itoa(version)
	mu.Unlock()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	snapshot, disposition, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, intent, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.ResizePG {
			break
		}
		snapshot = nativeNonPGStep(t, store, snapshot, step)
	}
	return store
}

func nativeNonPGStep(t *testing.T, store *journal.Store, snapshot *journal.Snapshot, step journal.Step) *journal.Snapshot {
	t.Helper()
	state := snapshot.State()
	var dispatch *journal.Dispatch
	snapshot, dispatch, err := store.Start(context.Background(), snapshot, state.Owner, state.Active.Request.Key, step.Action,
		"non-pg-test-fixture", []byte(`{"test_only_non_pg_domain":true}`))
	require.NoError(t, err)
	snapshot, err = store.Record(context.Background(), snapshot, state.Owner, state.Active.Request.Key, journal.Observation{
		Key: dispatch.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256,
		Ordinal: dispatch.Step.Ordinal, Action: dispatch.Step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID,
		Data: []byte(`{"test_only_non_pg_domain":true}`),
	})
	require.NoError(t, err)
	return snapshot
}

func checkNativeJournal(t *testing.T, store *journal.Store, intent control.Request) {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.ResizePG {
			require.Equal(t, journal.Settled, step.Phase)
			require.Equal(t, journal.Completed, step.Observation.Result)
			var audit struct {
				ProtocolVersion uint32                  `json:"protocol_version"`
				Fence           control.FenceResponse   `json:"issuer_fence"`
				Response        control.ControlResponse `json:"response"`
			}
			require.NoError(t, json.Unmarshal(step.Observation.Data, &audit))
			require.NoError(t, audit.Response.ValidateResize(intent, intent.Key.Instance))
			require.Equal(t, control.Committed, audit.Response.Receipt.Result.Operation.Phase)
			require.False(t, audit.Response.VMShrinkPermit)
		}
	}
}

func finishNativeJournal(t *testing.T, store *journal.Store, intent control.Request) {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	for _, step := range snapshot.State().Active.Steps {
		if step.Phase == journal.Planned {
			snapshot = nativeNonPGStep(t, store, snapshot, step)
		}
	}
	closed, err := store.Finish(context.Background(), snapshot, snapshot.State().Owner, intent.Key)
	require.NoError(t, err)
	require.Nil(t, closed.State().Active)
	require.Equal(t, intent, closed.State().History[0].Operation.Request)
}
