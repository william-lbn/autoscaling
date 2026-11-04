package hostscheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
)

const ordinarySourceAnnotation = "internal.autoscaling.neon.tech/dsga-ordinary-stop-source-v1"

func ordinaryTestHash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func ordinaryTestCanonical(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	var obj any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&obj))
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(obj))
	return ordinaryTestHash(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
}

// 使用公开原reconcile/Store/typedREST UID-RV CAS；新来源fixture为显式受信
// operator观察，不冒真实节点/RBAC/物理退出。旧生产代码也可编译此完整反例。
func ordinaryTrustedStopFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *kubefake.Clientset, *core.Pod, *core.ConfigMap) {
	t.Helper()
	s, f, n, p := retainedOrdinaryFixture(t)
	ctx := context.Background()
	d := s.domains[n.Binding.NodeName]
	snap, err := d.store.Load(ctx)
	require.NoError(t, err)
	row, err := snap.State().Placement(string(p.UID))
	require.NoError(t, err)
	names := []string{}
	for _, c := range p.Spec.InitContainers {
		names = append(names, "init/"+c.Name)
	}
	for _, c := range p.Spec.Containers {
		names = append(names, "app/"+c.Name)
	}
	facts := map[string]any{"namespace": p.Namespace, "name": p.Name, "uid": string(p.UID), "node_name": p.Spec.NodeName, "phase": p.Status.Phase, "spec_sha256": ordinaryTestCanonical(t, p.Spec), "status_sha256": ordinaryTestCanonical(t, p.Status)}
	liveUID := "12345678-1234-4234-8234-123456789abc"
	cid, sid := strings.Repeat("a", 64), strings.Repeat("b", 64)
	ns := map[string]any{"pid": "1", "mount": "2", "cgroup": "3"}
	nf := map[string]any{"name": n.Binding.NodeName, "uid": string(n.Binding.NodeUID), "boot_id": "captured-boot"}
	source := map[string]any{"protocol_version": 1, "scope": "ordinary-pod-node-stop-v1", "binding": n.Binding, "origin": snap.State().Origin, "placement": row, "before": facts, "after": facts, "container_names": names, "probe": map[string]any{
		"node_name": n.Binding.NodeName, "node_uid": string(n.Binding.NodeUID), "boot_before": "captured-boot", "boot_after": "captured-boot", "api_node_before": nf, "api_node_after": nf,
		"kubelet":  map[string]any{"pid": 2, "birth_before": "11", "birth_after": "11", "exe": "/usr/local/bin/kubelet", "exe_sha256": strings.Repeat("c", 64), "version": "Kubernetes v1.31.2", "endpoint": "unix:///run/containerd/containerd.sock", "resolved_socket": "/run/containerd/containerd.sock"},
		"self_pid": 1, "self_ns": ns, "init_ns": ns, "kubelet_ns": ns, "mount": map[string]any{"root": "/", "point": "/sys/fs/cgroup", "fs_type": "cgroup2"},
		"runtime":     map[string]any{"cri_container_exit": 0, "cri_sandbox_exit": 0, "ctr_exit": 0, "ctr_namespace": "k8s.io", "containers": []any{map[string]any{"id": cid, "pod_uid": liveUID, "sandbox_id": sid, "state": 1}}, "sandboxes": []any{map[string]any{"id": sid, "pod_uid": liveUID, "state": 0}}, "ctr_container_ids": []string{cid}},
		"cgroup_dirs": []string{"/", "/kubepods/pod" + liveUID}, "walk_errors": []string{}, "walk_truncated": false, "proc_enumerated": []int{1, 2, 3}, "processes": []any{map[string]any{"pid": 1, "cgroup_paths": []string{"/"}}, map[string]any{"pid": 2, "cgroup_paths": []string{"/"}}, map[string]any{"pid": 3, "cgroup_paths": []string{"/kubepods/pod" + liveUID}}}, "proc_errors": []string{}, "issues": []string{}, "live": map[string]any{"uid": liveUID, "container_id": cid, "sandbox_id": sid},
	}}
	raw, err := json.Marshal(source)
	require.NoError(t, err)
	name := "dsga-os-" + ordinaryTestHash([]byte(p.UID))[:55]
	yes := true
	cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: name, UID: "trusted-stop-source-uid", ResourceVersion: "1", Annotations: map[string]string{"internal.autoscaling.neon.tech/dsga-ordinary-stop-source-kind": "ordinary-pod-node-stop-v1"}, Finalizers: []string{"autoscaling.neon.tech/dsga-ordinary-stop-source-protection"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: n.Binding.NodeName, UID: n.Binding.NodeUID, Controller: &yes, BlockOwnerDeletion: &yes}}}, Immutable: &yes, Data: map[string]string{"ordinary-stop.json": string(raw)}}
	ref, _ := json.Marshal(map[string]any{"claim": map[string]string{"name": name, "uid": string(cm.UID)}, "sha256": ordinaryTestHash(raw), "spec_sha256": facts["spec_sha256"]})
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		q := f.Pods[p.Namespace+"/"+p.Name]
		if q.Annotations == nil {
			q.Annotations = map[string]string{}
		}
		q.Annotations[ordinarySourceAnnotation] = string(ref)
		q.ResourceVersion += "-source"
	})
	p, err = f.Core.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	require.NoError(t, err)
	claims := kubefake.NewSimpleClientset(cm)
	combined := retirementCore{CoreV1Interface: f.Core.CoreV1(), claims: claims.CoreV1()}
	s.core = combined
	d.store, err = hostguard.NewStore(combined, f.Core.CoordinationV1(), n.Binding, s.holder)
	require.NoError(t, err)
	s.domains[n.Binding.NodeName] = d
	return s, f, n, claims, p, cm
}
func TestSchedulerOrdinaryTrustedStopReleasesOnlyOriginalAllocation(t *testing.T) {
	s, f, n, _, p, _ := ordinaryTrustedStopFixture(t)
	ctx := context.Background()
	d := s.domains[n.Binding.NodeName]
	before, err := d.store.Load(ctx)
	require.NoError(t, err)
	old, err := before.State().Placement(string(p.UID))
	require.NoError(t, err)
	oldBytes, err := old.Bytes()
	require.NoError(t, err)
	require.NoError(t, s.reconcile(ctx, d))
	after, err := d.store.Load(ctx)
	require.NoError(t, err)
	_, err = after.State().Placement(string(p.UID))
	require.Error(t, err, "受信完整Node停止来源必须经原退休CAS释放原行")
	a, _ := after.State().UsedBytes()
	b, _ := before.State().UsedBytes()
	require.Equal(t, b-oldBytes, a)
	actual, err := f.Core.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, actual.Finalizers, hostguard.PodProtectionFinalizer)
	var intent map[string]any
	require.NoError(t, json.Unmarshal([]byte(actual.Annotations[hostguard.RetirementAnnotation]), &intent))
	require.EqualValues(t, 2, intent["protocol_version"])
	require.Error(t, hostguard.InspectRetiredPod(after.Node(), actual), "不读原Source的旧纯函数不能签v2完成")
	canonical, err := s.ObserveRetiredOrdinary(ctx, n.Binding.NodeName, actual)
	require.NoError(t, err)
	require.Equal(t, actual, canonical)
	require.NoError(t, s.reconcile(ctx, d))
	again, err := d.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, after.State(), again.State(), "同原source不得双释放")
}
func TestSchedulerOrdinaryStopUnknownCASAndHistoricalBootRecoverSameSource(t *testing.T) {
	for _, which := range []string{"lost_node_reply", "lost_intent_reply", "historical_boot"} {
		t.Run(which, func(t *testing.T) {
			s, f, n, _, p, cm := ordinaryTrustedStopFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.LoseNodeReply = which == "lost_node_reply"
				f.LosePodReply = which == "lost_intent_reply"
				if which == "historical_boot" {
					f.Node.Status.NodeInfo.BootID = "new-current-boot"
				}
			})
			err := s.retirePod(ctx, d, p)
			if which == "lost_node_reply" {
				var unknown *hostguard.UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					require.Contains(t, f.Pods[p.Namespace+"/"+p.Name].Finalizers, hostguard.PodProtectionFinalizer)
				})
				require.NoError(t, s.retirePod(ctx, d, p))
			} else {
				require.NoError(t, err)
			}
			actual, err := f.Core.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Contains(t, actual.Annotations[hostguard.RetirementAnnotation], string(cm.UID))
			require.NoError(t, s.retirePod(ctx, d, actual))
		})
	}
}
func TestSchedulerOrdinaryStopRejectsChangedOrUntrustedSourcesWithoutRelease(t *testing.T) {
	for _, which := range []string{"missing", "tenant_namespace", "source_uid", "source_body", "mutable", "unprotected", "owner", "owner_unprotected", "vm_owner", "migration_owner", "spec_drift", "expired_lease", "read_after_CAS_unknown"} {
		t.Run(which, func(t *testing.T) {
			s, f, n, claims, p, cm := ordinaryTrustedStopFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			if which == "missing" {
				require.NoError(t, claims.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name))
			}
			if which == "tenant_namespace" {
				require.NoError(t, claims.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name))
				cm.Namespace = p.Namespace
				require.NoError(t, claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			}
			if which == "source_uid" {
				cm.UID = types.UID("replacement-uid")
			}
			if which == "source_body" {
				cm.Data["ordinary-stop.json"] += " "
			}
			if which == "mutable" {
				no := false
				cm.Immutable = &no
			}
			if which == "unprotected" {
				cm.Finalizers = nil
			}
			if which == "owner" {
				cm.OwnerReferences[0].UID = "another-node"
			}
			if which == "owner_unprotected" {
				cm.OwnerReferences[0].BlockOwnerDeletion = nil
			}
			if which == "source_uid" || which == "source_body" || which == "mutable" || which == "unprotected" || which == "owner" || which == "owner_unprotected" {
				require.NoError(t, claims.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			}
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				q := f.Pods[p.Namespace+"/"+p.Name]
				if which == "vm_owner" {
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-uid"}}
				}
				if which == "migration_owner" {
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-uid"}}
				}
				if which == "spec_drift" {
					q.Spec.SchedulerName = "changed-scheduler"
				}
				if which == "expired_lease" {
					stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
					f.Lease.Spec.RenewTime = &stamp
				}
			})
			if which == "read_after_CAS_unknown" {
				reads := 0
				claims.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
					reads++
					if reads >= 3 {
						return true, nil, errors.New("explicit source GET unknown after CAS")
					}
					return false, nil, nil
				})
			}
			require.Error(t, s.retirePod(ctx, d, p))
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			if which != "read_after_CAS_unknown" {
				require.Equal(t, before.State(), after.State())
			}
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Contains(t, f.Pods[p.Namespace+"/"+p.Name].Finalizers, hostguard.PodProtectionFinalizer)
			})
		})
	}
}
