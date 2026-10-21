package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/util"
)

func modeRunner(t *testing.T, annotations map[string]string, changedUID bool, failedGet bool) (*Runner, *atomic.Int64) {
	t.Helper()
	calls := new(atomic.Int64)
	vm := new(vmv1.VirtualMachine)
	vm.Name, vm.Namespace, vm.UID, vm.ResourceVersion = "vm", "testing", "vm-uid", "opaque-1"
	vm.Annotations = annotations
	if changedUID {
		vm.UID = "same-name-new-uid"
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/vm", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if failedGet {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(vm)
	}))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host = server.URL
	clients, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	global := new(agentState)
	global.config = new(Config)
	global.vmClient = clients
	global.envArgs = EnvArgs{ConfigPath: "", K8sNodeName: "node-a", K8sPodIP: "127.0.0.1", K8sPodUID: "agent-pod-uid"}
	global.metrics, _ = makeGlobalMetrics()
	status := new(lockedPodStatus)
	status.startTime = time.Now()
	info := new(api.VmInfo)
	info.UID, info.Name, info.Namespace = "vm-uid", "vm", "testing"
	runner := global.newRunner(*info, util.NamespacedName{Namespace: "testing", Name: "runner"}, "10.0.0.1")
	runner.status = status
	return runner, calls
}

func TestManagedRunnerNeverStartsLegacyOnMissingOrMalformedConfiguration(t *testing.T) {
	for _, annotations := range []map[string]string{
		{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: "not-decoded-before-config"},
		{control.ModeAnnotation: "broken"},
		{control.JournalAnnotation: "orphaned-journal"},
	} {
		runner, calls := modeRunner(t, annotations, false, false)
		_, updates := util.NewCondChannelPair()
		err := runner.Run(context.Background(), zap.NewNop(), updates)
		require.Error(t, err)
		require.EqualValues(t, 1, calls.Load())
		require.Zero(t, runner.backgroundWorkerCount.Load())
		require.Nil(t, runner.executorStateDump)
		require.Nil(t, runner.monitor)
		state, err := runner.State(context.Background())
		require.NoError(t, err, "status during managed startup must not dereference absent legacy executor")
		require.NotEmpty(t, state.DSGA.Phase)
	}
}

func TestModeReadCannotAdoptRecreatedVMOrUnknownAPIState(t *testing.T) {
	for _, test := range []struct {
		changed bool
		failed  bool
	}{{true, false}, {false, true}} {
		runner, _ := modeRunner(t, nil, test.changed, test.failed)
		_, updates := util.NewCondChannelPair()
		managed, err := runner.runDSGA(context.Background(), zap.NewNop(), updates)
		require.True(t, managed)
		require.Error(t, err)
		require.Zero(t, runner.backgroundWorkerCount.Load())
	}
	runner, calls := modeRunner(t, nil, false, false)
	_, updates := util.NewCondChannelPair()
	managed, err := runner.runDSGA(context.Background(), zap.NewNop(), updates)
	require.False(t, managed)
	require.NoError(t, err)
	require.EqualValues(t, 1, calls.Load())
}

func TestManagedHeartbeatDoesNotUseAbsentLegacyMonitor(t *testing.T) {
	global := new(agentState)
	global.config = new(Config)
	stat := new(podStatus)
	stat.dsga = true
	stat.startTime = time.Now().Add(-time.Hour)
	now := time.Now()
	stat.dsgaLastSuccessfulCheck = &now
	stuck, _ := stat.isStuck(global, now)
	require.False(t, stuck)
	stuck, reasons := stat.isStuck(global, now.Add(time.Minute))
	require.True(t, stuck)
	require.Equal(t, []string{"DSGA owner reconciliation check is stale"}, reasons)
}
