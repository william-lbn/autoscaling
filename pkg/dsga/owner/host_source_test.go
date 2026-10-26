package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	coord "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	kt "k8s.io/client-go/testing"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// actualHostPublisher执行生产Host Publisher/Node Store，只有Kube服务、Host
// 开销及域外资格是显式fixture。Node PATCH用锁内RFC6902 UID/RV测试、RV递增，
// 不把fake tracker默认的非原子Get/Update当apiserver CAS证明。未运行实际Scheduler。
func actualHostPublisher(t *testing.T, client *kubefake.Clientset, vmCoord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, faults ...string) (*hostexec.Publisher, *hostguard.Store) {
	t.Helper()
	ctx := context.Background()
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	node.Status.Allocatable = core.ResourceList{core.ResourceMemory: resource.MustParse("16Gi")}
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
	b := hostguard.Binding{NodeName: node.Name, NodeUID: node.UID, Namespace: "host-fixture-system", LeaseName: "node-one-allocator", LeaseUID: "original-host-lease-uid"}
	holder, transitions, duration, controller := "host-fixture-holder", int32(0), int32(60), true
	stamp := metav1.NewMicroTime(time.Now())
	lease := &coord.Lease{TypeMeta: metav1.TypeMeta{Kind: "Lease", APIVersion: "coordination.k8s.io/v1"}, ObjectMeta: metav1.ObjectMeta{Name: b.LeaseName, Namespace: b.Namespace, UID: b.LeaseUID, ResourceVersion: "host-lease-rv-1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(b)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: coord.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &stamp, RenewTime: &stamp, LeaseTransitions: &transitions, Strategy: nil, PreferredHolder: nil}}
	require.NoError(t, client.Tracker().Add(lease))
	var mu sync.Mutex
	version := 1
	losePublication := len(faults) == 1 && faults[0] == "publish-lost"
	client.PrependReactor("patch", "nodes", func(action kt.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		a := action.(kt.PatchAction)
		old, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("nodes"), "", a.GetName())
		if err != nil {
			return true, nil, err
		}
		ops, err := jsonpatch.DecodePatch(a.GetPatch())
		if err != nil {
			return true, nil, err
		}
		raw, err := json.Marshal(old)
		if err != nil {
			return true, nil, err
		}
		raw, err = ops.Apply(raw)
		if err != nil {
			return true, nil, fmt.Errorf("fixture Node UID/RV conflict: %w", err)
		}
		var next core.Node
		if err := json.Unmarshal(raw, &next); err != nil {
			return true, nil, err
		}
		version++
		next.ResourceVersion = fmt.Sprintf("host-node-rv-%d", version)
		if err := client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), &next, ""); err != nil {
			return true, nil, err
		}
		if losePublication {
			var m hostguard.Model
			if json.Unmarshal([]byte(next.Annotations[hostguard.LedgerAnnotation]), &m) == nil && len(m.Holds) == 1 && m.Holds[0].Claim != nil {
				losePublication = false
				return true, nil, fmt.Errorf("fixture lost publication CAS reply after commit")
			}
		}
		return true, &next, nil
	})
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), b, holder)
	require.NoError(t, err)
	_, err = allocator.Initialize(ctx, node.ResourceVersion, 1<<30)
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), vmCoord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	return publisher, allocator
}

// 真实Node/VM存储、Host生产者、Runner出生/文件WAL和Unix QMP读取参与。
// Kube服务、QEMU peer及资格上界仍为显式fixture，不声称已接生产Scheduler。
func TestHostPublisherOriginalSourcesAndLostWrites(t *testing.T) {
	for _, mode := range []string{"complete", "create-lost", "publish-lost", "published-source-missing", "same-name-new-uid", "outside-missing", "before-memory-drift", "VM-intent-changes", "consumer-start-lost", "consumer-record-lost", "consumer-source-gone", "consumer-waits-unpublished"} {
		t.Run(mode, func(t *testing.T) {
			api, store, vmCoord := newAPI(t)
			client := vmCore(t)
			api.mu.Lock()
			cpuMode := vmv1.CpuScalingModeSysfs
			api.vm.Spec.CpuScalingMode, api.vm.Spec.ServiceAccountName = &cpuMode, "dsga-runner-test"
			api.vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(256<<20, resource.BinarySI)
			api.vm.Spec.Guest.MemorySlots.Min, api.vm.Spec.Guest.MemorySlots.Max, api.vm.Spec.Guest.MemorySlots.Use = 2, 8, 2
			api.vm.Status.Node = "node-one"
			api.vm.Status.MemorySize = resource.NewQuantity(512<<20, resource.BinarySI)
			api.mu.Unlock()
			dir, err := os.MkdirTemp("/private/tmp", "dsga-host-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			runner, err := vmguard.Initialize(context.Background(), store, client.CoreV1(), vmCoord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
			require.NoError(t, err)
			peer := &vmQMPPeer{mu: sync.Mutex{}, uuid: runner.QMPArguments()[1], minimum: 512 << 20, maximum: 2 << 30, actual: 512 << 20, writes: 0, lose: false, onResize: func(uint64) { t.Error("Host publisher must never issue QMP set") }}
			vmPeer(t, filepath.Join(dir, "qmp.sock"), peer)
			elector, err := New(vmCoord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				req := pgRequest(t, session.Actor())
				id := resourceguard.Identity{VMUID: req.Key.Instance.VMUID, ComputeInstanceID: req.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3, SystemCgroupDevice: 0, SystemCgroupInode: 0}
				s, err := store.Load(ctx)
				require.NoError(t, err)
				s, err = store.BindResource(ctx, s, session.Actor(), id)
				require.NoError(t, err)
				s, err = store.BindPG(ctx, s, session.Actor(), req.Key.Instance)
				require.NoError(t, err)
				p, _ := preparedGuest(t, req, id)
				outside := budget.OutsideReservation{ProtocolVersion: 1, VMShrinkPermit: false, Request: req, Identity: id, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
				outRef := vmReservation(t, client, "outside-original", budgetclaims.OutsideKind, outside)
				p.OutsideClaim = &outRef
				_, _, err = store.Begin(ctx, s, session.Actor(), req, 1)
				require.NoError(t, err)
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
				require.NoError(t, runner.Tick(ctx))
				publisher, allocator := actualHostPublisher(t, client, vmCoord, store, session, mode)
				creates := 0
				client.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
					cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
					if cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.HostKind {
						return false, nil, nil
					}
					creates++
					// client-go fake在回调期间持有自身锁；回调内只能直接读
					// tracker，不能递归使用typed客户端而制造夹具锁重入。
					obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("nodes"), "", "node-one")
					require.NoError(t, err)
					var nodeModel hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(obj.(*core.Node).Annotations[hostguard.LedgerAnnotation]), &nodeModel))
					require.Len(t, nodeModel.Holds, 1, "CM写入前必须实际Node容量已持久")
					require.Nil(t, nodeModel.Holds[0].Claim, "CM创建本身尚未发布")
					if mode == "VM-intent-changes" {
						api.mu.Lock()
						api.vmVersion++
						api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
						api.mu.Unlock()
					}
					if mode != "create-lost" {
						return false, nil, nil
					}
					cm = cm.DeepCopy()
					cm.UID, cm.ResourceVersion = "original-lost-create-uid", "host-cm-rv-1"
					require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
					return true, nil, fmt.Errorf("fixture CREATE reply lost after commit")
				})
				if mode == "outside-missing" {
					require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, outRef.Name))
				}
				if mode == "before-memory-drift" {
					peer.mu.Lock()
					peer.actual += vmguard.VirtioBlockBytes
					peer.mu.Unlock()
					require.NoError(t, runner.Tick(ctx))
				}
				ref, err := publisher.Reconcile(ctx)
				if mode == "outside-missing" || mode == "before-memory-drift" || mode == "VM-intent-changes" {
					require.Error(t, err)
					require.Empty(t, ref)
					if mode != "VM-intent-changes" {
						require.Zero(t, creates)
					}
					return nil
				}
				if mode == "publish-lost" {
					require.Error(t, err)
					require.Empty(t, ref)
					ref, err = publisher.Reconcile(ctx)
				}
				require.NoError(t, err)
				require.Equal(t, 1, creates)
				n, err := allocator.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, ref, *n.State().Holds[0].Claim)
				version := n.ResourceVersion()
				if mode == "published-source-missing" || mode == "same-name-new-uid" {
					original, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
					require.NoError(t, err)
					require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, ref.Name))
					if mode == "same-name-new-uid" {
						original.UID = "replacement-source-uid"
						require.NoError(t, client.Tracker().Add(original))
					}
					_, err = publisher.Reconcile(ctx)
					require.Error(t, err)
				} else {
					replay, err := publisher.Reconcile(ctx)
					require.NoError(t, err)
					require.Equal(t, ref, replay)
				}
				n, err = allocator.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, version, n.ResourceVersion(), "只读恢复/丢源不得重建原持有")
				require.Equal(t, 1, creates, "发布后来源丢失绝不CREATE替代UID")
				if mode == "consumer-waits-unpublished" {
					claims, err := budgetclaims.New(client.CoreV1(), binding())
					require.NoError(t, err)
					consumer, err := hostexec.NewConsumer(store, claims, session.Actor())
					require.NoError(t, err)
					node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
					require.NoError(t, err)
					beforePublish := node.DeepCopy()
					model := n.State()
					model.ProtocolVersion, model.Holds[0].Claim = 1, nil
					raw, err := json.Marshal(model)
					require.NoError(t, err)
					beforePublish.Annotations[hostguard.LedgerAnnotation] = string(raw)
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), beforePublish, ""))
					_, err = consumer.Advance(ctx)
					require.ErrorIs(t, err, resourceguard.ErrUnknown)
					require.Equal(t, journal.Planned, budgetStep(t, store, journal.ReserveHost).Phase, "未发布窗口不能Start或误退出owner")
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
					progress, err := consumer.Advance(ctx)
					require.NoError(t, err)
					require.Equal(t, pgexec.Recorded, progress)
				}
				if mode == "consumer-start-lost" || mode == "consumer-record-lost" || mode == "consumer-source-gone" {
					claims, err := budgetclaims.New(client.CoreV1(), binding())
					require.NoError(t, err)
					consumer, err := hostexec.NewConsumer(store, claims, session.Actor())
					require.NoError(t, err)
					if mode == "consumer-start-lost" || mode == "consumer-source-gone" {
						api.mu.Lock()
						api.loseJournalReply = true
						api.mu.Unlock()
					} else {
						client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
							// 观察真实VM写前步骤，不在fake锁内递归用Core客户端。
							if action.(kt.GetAction).GetName() == ref.Name {
								snapshot, err := store.Load(ctx)
								require.NoError(t, err)
								for _, step := range snapshot.State().Active.Steps {
									if step.Action == journal.ReserveHost && step.Phase == journal.Unconfirmed {
										api.mu.Lock()
										api.loseJournalReply = true
										api.mu.Unlock()
									}
								}
							}
							return false, nil, nil
						})
					}
					_, err = consumer.Advance(ctx)
					require.Error(t, err, "原Start或Record回复丢失不能返回完成")
					original := budgetStep(t, store, journal.ReserveHost)
					if mode == "consumer-source-gone" {
						require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, ref.Name))
						_, err = consumer.Advance(ctx)
						require.Error(t, err)
						require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ReserveHost).Phase)
					} else {
						progress, err := consumer.Advance(ctx)
						require.NoError(t, err)
						require.True(t, progress == pgexec.Recorded || progress == pgexec.ExternalStep)
						final := budgetStep(t, store, journal.ReserveHost)
						require.Equal(t, journal.Settled, final.Phase)
						require.Equal(t, original.CommandSHA256, final.CommandSHA256, "恢复不重写原Host命令")
					}
					require.Equal(t, 1, creates)
				}
				return nil
			})
			require.NoError(t, err)
			peer.mu.Lock()
			require.Zero(t, peer.writes)
			peer.mu.Unlock()
		})
	}
}

func publishVMHost(t *testing.T, ctx context.Context, client *kubefake.Clientset, vmCoord coordclient.CoordinationV1Interface, store *journal.Store, session *Session) budget.HostReservation {
	t.Helper()
	publisher, _ := actualHostPublisher(t, client, vmCoord, store, session)
	ref, err := publisher.Reconcile(ctx)
	require.NoError(t, err)
	cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var h budget.HostReservation
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &h))
	require.EqualValues(t, 2, h.ProtocolVersion)
	return h
}
