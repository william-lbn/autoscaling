package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/util"
)

// 明确单元模型：真实生产Agent Runner/OwnerElector/client-go运行，API以有UID/RV
// 的内存tracker代替，不是Kubernetes/Guest/内存资格证书。
func initialAgentFixture(t *testing.T) (*agentState, *vmv1.VirtualMachine, *core.Pod, *atomic.Int64, <-chan struct{}) {
	t.Helper()
	vm := new(vmv1.VirtualMachine)
	// 资源/未Ready status取本轮f169形状：CPU/memory实际status均缺省，只有Spec有7/8Gi。
	require.NoError(t, json.Unmarshal([]byte(`{"metadata":{"namespace":"testing","name":"vm","uid":"22222222-2222-4222-8222-222222222222","resourceVersion":"1","labels":{"autoscaling.neon.tech/enabled":"true"}},"spec":{"schedulerName":"autoscale-scheduler","serviceAccountName":"runner","guest":{"cpus":{"min":2,"max":2,"use":2},"memorySlotSize":"1Gi","memorySlots":{"min":7,"max":8,"use":7}},"dsgaBootstrap":{"protocolVersion":1,"incarnation":"1234567890abcdef1234567890abcdef","computeInstanceID":"vm","ready":true,"vmUID":"22222222-2222-4222-8222-222222222222","configSecretUID":"44444444-4444-4444-8444-444444444444"}},"status":{"phase":"Pending","podName":"runner"}}`), vm))
	yes := true
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "testing", UID: "33333333-3333-4333-8333-333333333333", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &yes}}}, Spec: core.PodSpec{NodeName: "node-a", ServiceAccountName: "runner"}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: "10.0.0.9", ContainerStatuses: []core.ContainerStatus{{Name: "neonvm-runner", Ready: false, State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}}}
	kc := kubefake.NewSimpleClientset(pod)
	held := vm.DeepCopy()
	held.Spec.DSGABootstrap.Ready = false
	held.Spec.DSGABootstrap.VMUID = ""
	held.Spec.DSGABootstrap.ConfigSecretUID = ""
	held.Status = vmv1.VirtualMachineStatus{}
	vc := vmfake.NewSimpleClientset(held)
	gvr := schema.GroupVersionResource{Group: "vm.neon.tech", Version: "v1", Resource: "virtualmachines"}
	var patchMu sync.Mutex
	version := 1
	vc.PrependReactor("patch", "virtualmachines", func(action clienttesting.Action) (bool, runtime.Object, error) {
		patchMu.Lock()
		defer patchMu.Unlock()
		original, e := vc.Tracker().Get(gvr, "testing", "vm")
		if e != nil {
			return true, nil, e
		}
		raw, e := json.Marshal(original)
		if e != nil {
			return true, nil, e
		}
		patch, e := jsonpatch.DecodePatch(action.(clienttesting.PatchAction).GetPatch())
		if e != nil {
			return true, nil, e
		}
		raw, e = patch.Apply(raw)
		if e != nil {
			return true, nil, e
		}
		next := new(vmv1.VirtualMachine)
		if e = json.Unmarshal(raw, next); e != nil {
			return true, nil, e
		}
		version++
		next.ResourceVersion = strconv.Itoa(version)
		e = vc.Tracker().Update(gvr, next, "testing")
		return true, next.DeepCopy(), e
	})
	kc.PrependReactor("create", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		next := action.(clienttesting.CreateAction).GetObject().(*coordination.Lease).DeepCopy()
		next.UID = "55555555-5555-4555-8555-555555555555"
		next.ResourceVersion = "1"
		e := kc.Tracker().Create(coordination.SchemeGroupVersion.WithResource("leases"), next, next.Namespace)
		return true, next, e
	})
	calls := new(atomic.Int64)
	kc.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { calls.Add(1); return false, nil, nil })
	cfg := new(Config)
	cfg.Scheduler.SchedulerName = "autoscale-scheduler"
	cfg.DSGA = &DSGAConfig{ComputeTimeoutSeconds: 10, PollIntervalSeconds: 1, RequestTimeoutSeconds: 5}
	// 实际TLS端点把首次凭据HTTP停在读阶段，验证Ready后同Session仍持租；不伪造grant。
	entered := make(chan struct{})
	var once sync.Once
	public, private, e := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, e)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vm-management.testing.svc"}, DNSNames: []string{"vm-management.testing.svc"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, public, private)
	require.NoError(t, e)
	leafPublic, leafPrivate, e := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, e)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "*.testing.svc"}, DNSNames: []string{"*.testing.svc"}, NotBefore: cert.NotBefore, NotAfter: cert.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	leafDER, e := x509.CreateCertificate(rand.Reader, leaf, cert, leafPublic, private)
	require.NoError(t, e)
	key, e := x509.MarshalPKCS8PrivateKey(leafPrivate)
	require.NoError(t, e)
	pair, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	require.NoError(t, e)
	releaseHTTP := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		select {
		case <-r.Context().Done():
		case <-releaseHTTP:
		}
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(releaseHTTP) }) // LIFO: release handler before Server.Close.
	dir := t.TempDir()
	cfg.DSGA.ManagementCAFile = filepath.Join(dir, "ca.crt")
	cfg.DSGA.ProjectedTokenFile = filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(cfg.DSGA.ManagementCAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	require.NoError(t, os.WriteFile(cfg.DSGA.ProjectedTokenFile, []byte("unit-bound-pod-token"), 0600))
	cfg.DSGA.GatewayURL = server.URL
	s := &agentState{lock: util.NewChanMutex(), pods: map[util.NamespacedName]*podState{}, baseLogger: zap.NewNop(), config: cfg, kubeClient: kc, vmClient: vc, envArgs: EnvArgs{K8sNodeName: "node-a", K8sPodUID: "11111111-1111-4111-8111-111111111111"}}
	s.metrics, _ = makeGlobalMetrics()
	s.vmMetrics, _ = makePerVMMetrics()
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	store, e := journal.NewStore(vc.NeonvmV1().VirtualMachines(vm.Namespace), binding)
	require.NoError(t, e)
	snap, e := store.Initialize(context.Background(), "1", "bootstrap-"+binding.Incarnation)
	require.NoError(t, e)
	vm = snap.VirtualMachine()
	vm.Spec.DSGABootstrap.Ready = true
	vm.Spec.DSGABootstrap.VMUID = string(vm.UID)
	vm.Spec.DSGABootstrap.ConfigSecretUID = "44444444-4444-4444-8444-444444444444"
	vm.Status = vmv1.VirtualMachineStatus{Phase: vmv1.VmPending, PodName: pod.Name}
	version++
	vm.ResourceVersion = strconv.Itoa(version)
	require.NoError(t, vc.Tracker().Update(gvr, vm, vm.Namespace))
	return s, vm, pod, calls, entered
}

func TestDSGAInitialOwnerScheduledNotReadyToSameReadySession(t *testing.T) {
	s, vm, pod, reads, credentialEntered := initialAgentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selected := map[types.UID]dsgaInitialSelection{}
	require.Empty(t, vm.Status.Node)
	require.Empty(t, vm.Status.PodIP)
	require.Nil(t, vm.Status.CPUs)
	require.Nil(t, vm.Status.MemorySize)
	s.syncInitialDSGA(ctx, zap.NewNop(), []*vmv1.VirtualMachine{vm}, selected)
	binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
	store, e := journal.NewStore(s.vmClient.NeonvmV1().VirtualMachines(vm.Namespace), binding)
	require.NoError(t, e)
	var lease *coordination.Lease
	var actor journal.Owner
	require.Eventually(t, func() bool {
		var e error
		lease, e = s.kubeClient.CoordinationV1().Leases(vm.Namespace).Get(ctx, journal.LeaseName(binding), metav1.GetOptions{})
		if e != nil {
			return false
		}
		snap, e := store.Load(ctx)
		if e != nil {
			return false
		}
		actor = snap.State().Owner
		return actor.Epoch > 1 && *lease.Spec.HolderIdentity == actor.HolderID
	}, 4*time.Second, 10*time.Millisecond)
	require.NoError(t, journal.ValidateLease(lease, binding, journal.LeaseName(binding), lease.UID))
	key := util.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	s.lock.Lock()
	first := s.pods[key].runner
	s.lock.Unlock()
	require.Equal(t, string(pod.UID), first.dsgaPodUID)
	actual, e := s.vmClient.NeonvmV1().VirtualMachines(vm.Namespace).Get(ctx, vm.Name, metav1.GetOptions{})
	require.NoError(t, e)
	actual.Status.Node = pod.Spec.NodeName
	actual.Status.PodIP = pod.Status.PodIP
	actual.Status.Phase = vmv1.VmRunning
	_, e = s.vmClient.NeonvmV1().VirtualMachines(vm.Namespace).UpdateStatus(ctx, actual, metav1.UpdateOptions{})
	require.NoError(t, e)
	event, e := makeVMEvent(zap.NewNop(), actual, vmEventAdded)
	require.NoError(t, e)
	var concurrent sync.WaitGroup
	for i := 0; i < 8; i++ {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			require.NoError(t, s.ensureInitialDSGA(ctx, zap.NewNop(), actual, pod))
			s.handleEvent(ctx, zap.NewNop(), event)
		}()
	}
	concurrent.Wait()
	s.syncInitialDSGA(ctx, zap.NewNop(), []*vmv1.VirtualMachine{actual}, selected)
	before := reads.Load()
	for i := 0; i < 5; i++ {
		s.syncInitialDSGA(ctx, zap.NewNop(), []*vmv1.VirtualMachine{actual}, selected)
	}
	require.Equal(t, before, reads.Load(), "正常Ready交原watch，不再每秒Pod GET")
	select {
	case <-credentialEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("same owner did not continue into original credential client")
	}
	snap, e := store.Load(ctx)
	require.NoError(t, e)
	require.Equal(t, actor, snap.State().Owner)
	s.lock.Lock()
	require.Same(t, first, s.pods[key].runner)
	require.Len(t, s.pods, 1)
	s.lock.Unlock()
	cancel()
	s.Stop()
}

func TestDSGAInitialSelectionRejectsTerminalAndSameNameReplacement(t *testing.T) {
	s, vm, pod, _, _ := initialAgentFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selected := map[types.UID]dsgaInitialSelection{}
	s.syncInitialDSGA(ctx, zap.NewNop(), []*vmv1.VirtualMachine{vm}, selected)
	key := util.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	s.lock.Lock()
	original := s.pods[key]
	s.lock.Unlock()
	for _, mutate := range []func(*core.Pod){func(p *core.Pod) { p.UID = "same-name-new-uid" }, func(p *core.Pod) { p.Status.Phase = core.PodFailed }} {
		changed := pod.DeepCopy()
		mutate(changed)
		require.Error(t, dsgaOriginalPod(vm, changed, "node-a", string(pod.UID), pod.Status.PodIP))
	}
	changed := pod.DeepCopy()
	changed.Status.Phase = core.PodFailed
	_, e := s.kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, changed, metav1.UpdateOptions{})
	require.NoError(t, e)
	s.syncInitialDSGA(ctx, zap.NewNop(), []*vmv1.VirtualMachine{vm}, selected)
	require.Eventually(t, func() bool {
		original.status.mu.Lock()
		defer original.status.mu.Unlock()
		return original.status.endState != nil
	}, 2*time.Second, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	s.lock.Lock()
	require.Same(t, original, s.pods[key])
	s.lock.Unlock()
	s.Stop()
}
