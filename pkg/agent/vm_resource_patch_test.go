package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	vmtyped "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
)

func patchFixtureVM() *vmv1.VirtualMachine {
	// 此处只模拟API资源对象，不启动VM；未参与本模块的Guest/Status项保持真实零值。
	var guest vmv1.Guest
	guest.CPUs = vmv1.CPUs{Min: 1000, Use: 2000, Max: 4000}
	guest.MemorySlots = vmv1.MemorySlots{Min: 1, Use: 8, Max: 32}
	guest.MemorySlotSize = resource.MustParse("1Gi")
	var status vmv1.VirtualMachineStatus
	status.Phase = vmv1.VmRunning
	status.PodName = "runner-original"
	status.PodIP = "10.1.2.3"
	return &vmv1.VirtualMachine{
		TypeMeta:   metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "vm-name", UID: "vm-original", ResourceVersion: "10"},
		Spec:       vmv1.VirtualMachineSpec{Guest: guest, TargetRevision: nil},
		Status:     status,
	}
}

func patchFixtureIdentity() vmPatchIdentity {
	return vmPatchIdentity{
		UID: "vm-original", Namespace: "testing", Name: "vm-name",
		PodName: "runner-original", PodIP: "10.1.2.3", SlotBytes: 1 << 30,
	}
}

func patchCurrent() api.Resources { return api.Resources{VCPU: 2000, Mem: 8 << 30} }

func patchRevision(value int64) vmv1.RevisionWithTime {
	return (vmv1.Revision{Value: value, Flags: 0}).WithTime(time.Unix(12345, 0))
}

// 使用真正生成的clientset、HTTP和RFC6902应用，不借fake client自动接受Patch。
// 仅模拟API对象CAS/响应，不宣称真实apiserver admission或QMP/VM验收。
type patchAPIFixture struct {
	mu            sync.Mutex
	vm            *vmv1.VirtualMachine
	patchCalls    int
	beforePatch   func(int)
	loseResponse  bool
	alterResponse func(*vmv1.VirtualMachine)
	server        *httptest.Server
}

func patchAPI(t *testing.T, vm *vmv1.VirtualMachine) (*patchAPIFixture, vmtyped.VirtualMachineInterface) {
	t.Helper()
	f := &patchAPIFixture{
		mu: sync.Mutex{}, vm: vm, patchCalls: 0, beforePatch: nil,
		loseResponse: false, alterResponse: nil, server: nil,
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	config := new(rest.Config)
	config.Host = f.server.URL
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	return f, client.NeonvmV1().VirtualMachines("testing")
}

func (f *patchAPIFixture) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/vm-name" {
		http.Error(w, "unexpected resource", http.StatusNotFound)
		return
	}
	if r.Method == http.MethodPatch {
		f.mu.Lock()
		f.patchCalls++
		call := f.patchCalls
		f.mu.Unlock()
		if f.beforePatch != nil {
			f.beforePatch(call)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodPatch {
		if r.Header.Get("Content-Type") != "application/json-patch+json" {
			http.Error(w, "wrong patch type", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		operations, err := jsonpatch.DecodePatch(body)
		if err != nil {
			http.Error(w, "invalid patch", http.StatusBadRequest)
			return
		}
		original, err := json.Marshal(f.vm)
		if err != nil {
			http.Error(w, "fixture encode failed", http.StatusInternalServerError)
			return
		}
		updated, err := operations.Apply(original)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				ListMeta: metav1.ListMeta{SelfLink: "", ResourceVersion: "", Continue: "", RemainingItemCount: nil},
				Details:  nil,
				Status:   metav1.StatusFailure, Reason: metav1.StatusReasonInvalid, Message: "JSON Patch test failed", Code: 422,
			})
			return
		}
		var next vmv1.VirtualMachine
		if err := json.Unmarshal(updated, &next); err != nil {
			http.Error(w, "fixture decode failed", 500)
			return
		}
		version, _ := strconv.Atoi(f.vm.ResourceVersion)
		next.ResourceVersion = strconv.Itoa(version + 1)
		f.vm = &next
		if f.loseResponse {
			http.Error(w, "response lost after commit", http.StatusInternalServerError)
			return
		}
	} else if r.Method != http.MethodGet {
		http.Error(w, "wrong method", http.StatusMethodNotAllowed)
		return
	}
	response := f.vm.DeepCopy()
	if r.Method == http.MethodPatch && f.alterResponse != nil {
		f.alterResponse(response)
	}
	_ = json.NewEncoder(w).Encode(response)
}

func TestVMConditionalPatchUsesFixedUIDAndHandlesAbsentRevision(t *testing.T) {
	vm := patchFixtureVM()
	f, client := patchAPI(t, vm)
	target := api.Resources{VCPU: 3000, Mem: 4 << 30}
	require.NoError(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), target, patchRevision(5)))
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, 1, f.patchCalls)
	require.Equal(t, target.VCPU, f.vm.Spec.Guest.CPUs.Use)
	require.EqualValues(t, 4, f.vm.Spec.Guest.MemorySlots.Use)
	require.EqualValues(t, 5, f.vm.Spec.TargetRevision.Value)
}

func TestVMConditionalPatchRejectsChangedIdentityAndBoundsBeforeWrite(t *testing.T) {
	cases := []struct {
		name   string
		change func(*vmv1.VirtualMachine)
	}{
		{"new UID under same name", func(vm *vmv1.VirtualMachine) { vm.UID = "vm-recreated" }},
		{"wrong namespace", func(vm *vmv1.VirtualMachine) { vm.Namespace = "other" }},
		{"wrong name", func(vm *vmv1.VirtualMachine) { vm.Name = "other" }},
		{"missing resourceVersion", func(vm *vmv1.VirtualMachine) { vm.ResourceVersion = "" }},
		{"new runner name", func(vm *vmv1.VirtualMachine) { vm.Status.PodName = "new-runner" }},
		{"new runner IP", func(vm *vmv1.VirtualMachine) { vm.Status.PodIP = "10.1.2.4" }},
		{"migration", func(vm *vmv1.VirtualMachine) { vm.Status.Phase = vmv1.VmMigrating }},
		{"deletion", func(vm *vmv1.VirtualMachine) { value := metav1.Now(); vm.DeletionTimestamp = &value }},
		{"slot size changed", func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.MemorySlotSize = resource.MustParse("512Mi") }},
		{"minimum raised", func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.MemorySlots.Min = 5 }},
		{"maximum lowered", func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.MemorySlots.Max = 3 }},
		{"CPU maximum lowered", func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.CPUs.Max = 2000 }},
		{"CPU minimum raised", func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.CPUs.Min = 4000 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vm := patchFixtureVM()
			tc.change(vm)
			f, client := patchAPI(t, vm)
			err := patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(5))
			require.Error(t, err)
			require.Zero(t, f.patchCalls)
		})
	}
}

func TestVMConditionalPatchRejectsRoundingOverflowAndRevisionConflict(t *testing.T) {
	cases := []struct {
		name     string
		target   api.Resources
		revision vmv1.RevisionWithTime
	}{
		{"fractional slot", api.Resources{VCPU: 3000, Mem: 4<<30 + 1}, patchRevision(5)},
		{"zero memory", api.Resources{VCPU: 3000, Mem: 0}, patchRevision(5)},
		{"huge memory", api.Resources{VCPU: 3000, Mem: ^api.Bytes(0)}, patchRevision(5)},
		{"negative revision", api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(-1)},
		{"old revision", api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(3)},
		{"revision downgrade to zero", api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(0)},
		{"same revision with changed source snapshot", api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(4)},
		{"same revision changes flags", api.Resources{VCPU: 2000, Mem: 8 << 30}, vmv1.RevisionWithTime{Revision: vmv1.Revision{Value: 4, Flags: 1}, UpdatedAt: metav1.NewTime(time.Unix(12345, 0))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vm := patchFixtureVM()
			current := patchRevision(4)
			vm.Spec.TargetRevision = &current
			f, client := patchAPI(t, vm)
			currentResources := patchCurrent()
			if tc.name == "same revision with changed source snapshot" {
				currentResources.Mem = 2 << 30
			}
			require.Error(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), currentResources, tc.target, tc.revision))
			require.Zero(t, f.patchCalls)
		})
	}
}

func TestVMConditionalPatchDoesNotTrustModifiedServerResponse(t *testing.T) {
	for _, change := range []func(*vmv1.VirtualMachine){
		func(vm *vmv1.VirtualMachine) { vm.UID = "new-uid" },
		func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.CPUs.Use = 1000 },
		func(vm *vmv1.VirtualMachine) { vm.Spec.Guest.MemorySlots.Use = 7 },
		func(vm *vmv1.VirtualMachine) { vm.Spec.TargetRevision = nil },
	} {
		f, client := patchAPI(t, patchFixtureVM())
		f.alterResponse = change
		err := patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(5))
		require.ErrorContains(t, err, "unconfirmed")
		require.Equal(t, 1, f.patchCalls)
	}
}

func TestVMConditionalPatchLostResponseReconcilesWithoutResending(t *testing.T) {
	f, client := patchAPI(t, patchFixtureVM())
	f.loseResponse = true
	target := api.Resources{VCPU: 3000, Mem: 4 << 30}
	revision := patchRevision(5)
	require.ErrorContains(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), target, revision), "unconfirmed")
	// API实际已持久化，新GET确认同revision/目标；不应因原HTTP500再写一次。
	require.NoError(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), target, revision))
	require.Equal(t, 1, f.patchCalls)
}

func TestVMConditionalPatchConcurrentNewerRevisionCannotBeOverwritten(t *testing.T) {
	f, client := patchAPI(t, patchFixtureVM())
	oldReached := make(chan struct{})
	releaseOld := make(chan struct{})
	f.beforePatch = func(call int) {
		if call == 1 {
			close(oldReached)
			<-releaseOld
		}
	}
	oldResult := make(chan error, 1)
	go func() {
		oldResult <- patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(5))
	}()
	select {
	case <-oldReached:
	case <-time.After(5 * time.Second):
		t.Fatal("old request never reached real HTTP Patch")
	}
	err := patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), api.Resources{VCPU: 4000, Mem: 16 << 30}, patchRevision(6))
	close(releaseOld)
	require.NoError(t, err)
	oldErr := <-oldResult
	f.mu.Lock()
	defer f.mu.Unlock()
	require.EqualValues(t, 6, f.vm.Spec.TargetRevision.Value)
	require.EqualValues(t, 16, f.vm.Spec.Guest.MemorySlots.Use)
	require.ErrorContains(t, oldErr, "unconfirmed")
}

func TestVMConditionalPatchPartialApprovalsMayShareProgressRevision(t *testing.T) {
	vm := patchFixtureVM()
	revision := patchRevision(5)
	vm.Spec.TargetRevision = &revision
	f, client := patchAPI(t, vm)
	first := api.Resources{VCPU: 3000, Mem: 12 << 30}
	second := api.Resources{VCPU: 4000, Mem: 16 << 30}
	require.NoError(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), first, revision))
	// 同一目标第二次取得更多资源批准；Revision仍为5，但动作源快照已是第一批结果。
	require.NoError(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), first, second, revision))
	// 第一批的旧动作不能读取新对象后倒退回第一批目标。
	require.ErrorContains(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), first, revision), "planned")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, 2, f.patchCalls)
	require.EqualValues(t, 16, f.vm.Spec.Guest.MemorySlots.Use)
}

func TestVMConditionalPatchUIDTestProtectsRecreationAfterGet(t *testing.T) {
	f, client := patchAPI(t, patchFixtureVM())
	f.beforePatch = func(_ int) {
		f.mu.Lock()
		defer f.mu.Unlock()
		// 特意保留同RV来独立验证UID条件，而非依靠另一个条件碰巧拒绝。
		f.vm.UID = "vm-recreated"
	}
	require.ErrorContains(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(),
		api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(5)), "unconfirmed")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.EqualValues(t, 8, f.vm.Spec.Guest.MemorySlots.Use)
	require.Nil(t, f.vm.Spec.TargetRevision)
}

func TestVMConditionalPatchLegacyZeroRevisionStillUsesUIDAndCAS(t *testing.T) {
	vm := patchFixtureVM()
	revision := patchRevision(0)
	vm.Spec.TargetRevision = &revision
	f, client := patchAPI(t, vm)
	require.NoError(t, patchVMResources(context.Background(), client, patchFixtureIdentity(), patchCurrent(), api.Resources{VCPU: 3000, Mem: 4 << 30}, revision))
	require.Equal(t, 1, f.patchCalls)
}

func TestVMInfoCarriesVMUIDRatherThanPodUID(t *testing.T) {
	vm := patchFixtureVM()
	info, err := api.ExtractVmInfo(zap.NewNop(), vm)
	require.NoError(t, err)
	require.Equal(t, vm.UID, info.UID)
	resources, err := json.Marshal(vm.Spec.Resources())
	require.NoError(t, err)
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: vm.Namespace, Name: vm.Status.PodName, UID: "pod-uid",
		Labels:          map[string]string{vmv1.VirtualMachineNameLabel: vm.Name},
		Annotations:     map[string]string{vmv1.VirtualMachineResourcesAnnotation: string(resources)},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: nil}},
	}}
	info, err = api.ExtractVmInfoFromPod(zap.NewNop(), pod)
	require.NoError(t, err)
	require.Equal(t, vm.UID, info.UID)
	pod.OwnerReferences = nil
	info, err = api.ExtractVmInfoFromPod(zap.NewNop(), pod)
	require.NoError(t, err)
	require.Empty(t, info.UID)
	identity := patchFixtureIdentity()
	identity.UID = ""
	_, client := patchAPI(t, vm)
	require.ErrorContains(t, patchVMResources(context.Background(), client, identity, patchCurrent(), api.Resources{VCPU: 3000, Mem: 4 << 30}, patchRevision(5)), "lacks fixed execution identity")
}
