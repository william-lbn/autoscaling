package dsgatest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	coord "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// SchedulerAPI只模拟Kube HTTP、原UID/RV及响应丢失，不能证明真实apiserver、
// RBAC或物理内存。测试须在Mutate/Read回调内访问状态，禁止跨锁保留字段指针。
type SchedulerAPI struct {
	mu            sync.Mutex
	Node          core.Node
	Lease         coord.Lease
	Pods          map[string]*core.Pod
	VMs           map[string]*vmv1.VirtualMachine
	LoseNodeReply bool
	LoseVMReply   bool
	LosePodReply  bool
	NodeWrites    int
	VMWrites      int
	PodWrites     int
	version       int
	Core          *kubernetes.Clientset
	VM            *vmclient.Clientset
}

func NewSchedulerAPI(t *testing.T, holder string) (*SchedulerAPI, hostguard.Binding) {
	t.Helper()
	b := hostguard.Binding{NodeName: "node-one", NodeUID: "node-uid", Namespace: "system", LeaseName: "node-one-allocator", LeaseUID: "original-host-lease"}
	controller, seconds, transitions := true, int32(60), int32(0)
	stamp := metav1.NewMicroTime(time.Now())
	f := &SchedulerAPI{mu: sync.Mutex{}, Node: core.Node{ObjectMeta: metav1.ObjectMeta{Name: b.NodeName, UID: b.NodeUID, ResourceVersion: "1"}, Status: core.NodeStatus{Allocatable: core.ResourceList{core.ResourceMemory: resource.MustParse("8Gi")}}}, Lease: coord.Lease{TypeMeta: metav1.TypeMeta{Kind: "Lease", APIVersion: "coordination.k8s.io/v1"}, ObjectMeta: metav1.ObjectMeta{Name: b.LeaseName, Namespace: b.Namespace, UID: b.LeaseUID, ResourceVersion: "1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(b)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: b.NodeName, UID: b.NodeUID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: coord.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &seconds, AcquireTime: &stamp, RenewTime: &stamp, LeaseTransitions: &transitions, Strategy: nil, PreferredHolder: nil}}, Pods: make(map[string]*core.Pod), VMs: make(map[string]*vmv1.VirtualMachine), LoseNodeReply: false, LoseVMReply: false, LosePodReply: false, NodeWrites: 0, VMWrites: 0, PodWrites: 0, version: 1, Core: nil, VM: nil}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	c := new(rest.Config)
	c.Host = server.URL
	var err error
	f.Core, err = kubernetes.NewForConfig(c)
	require.NoError(t, err)
	f.VM, err = vmclient.NewForConfig(c)
	require.NoError(t, err)
	return f, b
}

func (f *SchedulerAPI) Mutate(fn func(*SchedulerAPI)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *SchedulerAPI) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	if strings.Contains(path, "/leases/") {
		if r.Method == http.MethodPut {
			var next coord.Lease
			if json.NewDecoder(r.Body).Decode(&next) != nil || next.UID != f.Lease.UID || next.ResourceVersion != f.Lease.ResourceVersion {
				http.Error(w, "Lease UID/RV conflict", http.StatusConflict)
				return
			}
			f.version++
			next.ResourceVersion = fmt.Sprint(f.version)
			f.Lease = next
		} else if r.Method != http.MethodGet {
			http.Error(w, "Lease recreation forbidden in fixture", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(f.Lease)
		return
	}
	if path == "/api/v1/pods" && r.Method == http.MethodGet {
		list := &core.PodList{TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"}}
		for _, pod := range f.Pods {
			if pod.Spec.NodeName != "" {
				list.Items = append(list.Items, *pod.DeepCopy())
			}
		}
		_ = json.NewEncoder(w).Encode(list)
		return
	}
	var object any
	var vm *vmv1.VirtualMachine
	var podObject *core.Pod
	if path == "/api/v1/nodes/node-one" {
		object = &f.Node
	} else if strings.HasPrefix(path, "/api/v1/namespaces/") && strings.Contains(path, "/pods/") {
		parts := strings.Split(path, "/")
		object = f.Pods[parts[4]+"/"+parts[6]]
		if object.(*core.Pod) == nil {
			http.NotFound(w, r)
			return
		}
		podObject = object.(*core.Pod)
	} else if strings.Contains(path, "/virtualmachines/") {
		parts := strings.Split(path, "/")
		vm = f.VMs[parts[5]+"/"+parts[7]]
		if vm == nil {
			http.NotFound(w, r)
			return
		}
		object = vm
	} else {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodPatch {
		raw, err := io.ReadAll(r.Body)
		if err != nil || r.Header.Get("Content-Type") != "application/json-patch+json" {
			http.Error(w, "invalid RFC6902 body", http.StatusBadRequest)
			return
		}
		ops, err := jsonpatch.DecodePatch(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		prior, err := json.Marshal(object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		raw, err = ops.Apply(prior)
		if err != nil {
			http.Error(w, "original UID/RV CAS conflict", http.StatusConflict)
			return
		}
		f.version++
		lose := false
		if podObject != nil {
			var next core.Pod
			if json.Unmarshal(raw, &next) != nil {
				http.Error(w, "invalid Pod", http.StatusBadRequest)
				return
			}
			next.ResourceVersion = fmt.Sprint(f.version)
			f.Pods[next.Namespace+"/"+next.Name], object = &next, &next
			f.PodWrites++
			lose, f.LosePodReply = f.LosePodReply, false
		} else if vm == nil {
			var next core.Node
			if json.Unmarshal(raw, &next) != nil {
				http.Error(w, "invalid Node", http.StatusBadRequest)
				return
			}
			next.ResourceVersion = fmt.Sprint(f.version)
			f.Node, object = next, &next
			f.NodeWrites++
			lose, f.LoseNodeReply = f.LoseNodeReply, false
		} else {
			var next vmv1.VirtualMachine
			if json.Unmarshal(raw, &next) != nil {
				http.Error(w, "invalid VM", http.StatusBadRequest)
				return
			}
			next.ResourceVersion = fmt.Sprint(f.version)
			f.VMs[next.Namespace+"/"+next.Name], object = &next, &next
			f.VMWrites++
			lose, f.LoseVMReply = f.LoseVMReply, false
		}
		if lose {
			http.Error(w, "fixture reply lost after commit", http.StatusServiceUnavailable)
			return
		}
	} else if r.Method != http.MethodGet {
		http.Error(w, "unsupported fixture method", http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(object)
}
