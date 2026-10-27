package hostscheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// 仅替换LIST的分页发现及确定性取消点；实际Pod GET/PATCH、Node/Lease CAS
// 仍走原HTTP夹具。ctx取消不靠sleep竞争，证明出队后尾部能进入下一轮。
type workAPIState struct {
	mu         sync.Mutex
	pages      map[string]*core.PodList
	pageErrors map[string]error
	lists      []string
	gets       []string
	cancelName string
	cancel     context.CancelFunc
}

type workCore struct {
	coreclient.CoreV1Interface
	state *workAPIState
}

func (c *workCore) Pods(ns string) coreclient.PodInterface {
	return &workPods{PodInterface: c.CoreV1Interface.Pods(ns), state: c.state}
}

type workPods struct {
	coreclient.PodInterface
	state *workAPIState
}

func (p *workPods) List(ctx context.Context, opts metav1.ListOptions) (*core.PodList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	p.state.lists = append(p.state.lists, opts.Continue)
	if opts.Limit != workPageSize {
		return nil, fmt.Errorf("unbounded test discovery request")
	}
	if err := p.state.pageErrors[opts.Continue]; err != nil {
		delete(p.state.pageErrors, opts.Continue)
		return nil, err
	}
	list := p.state.pages[opts.Continue]
	if list == nil {
		return nil, fmt.Errorf("unexpected discovery cursor %q", opts.Continue)
	}
	return list.DeepCopy(), nil
}

func (p *workPods) Get(ctx context.Context, name string, opts metav1.GetOptions) (*core.Pod, error) {
	p.state.mu.Lock()
	p.state.gets = append(p.state.gets, name)
	if name == p.state.cancelName && p.state.cancel != nil {
		p.state.cancel()
		p.state.cancel = nil
		p.state.mu.Unlock()
		return nil, ctx.Err()
	}
	p.state.mu.Unlock()
	return p.PodInterface.Get(ctx, name, opts)
}

func useWorkPages(s *Service, pages map[string]*core.PodList) *workAPIState {
	w := &workAPIState{mu: sync.Mutex{}, pages: pages, pageErrors: make(map[string]error), lists: nil, gets: nil, cancelName: "", cancel: nil}
	s.core = &workCore{CoreV1Interface: s.core, state: w}
	return w
}

func TestSchedulerWorkRetainsUnattemptedTailAcrossDeadline(t *testing.T) {
	s, f, n := schedulerFixture(t)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	bad, good := boundedPod("a-slow", n.Binding.NodeName, "1Gi"), boundedPod("z-healthy", n.Binding.NodeName, "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/a-slow"], f.Pods["testing/z-healthy"] = bad, good })
	w := useWorkPages(s, map[string]*core.PodList{"": {Items: []core.Pod{*bad, *good}}})
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	w.cancelName, w.cancel = bad.Name, cancel
	require.ErrorIs(t, s.reconcile(call, s.domains[n.Binding.NodeName]), context.Canceled)
	require.Len(t, s.domains[n.Binding.NodeName].work.pending, 1)
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	w.mu.Lock()
	require.Equal(t, []string{bad.Name, good.Name, good.Name}, w.gets, "健康项包含原GET和保护PATCH后的真实GET确认，不回到慢项")
	require.Len(t, w.lists, 1)
	w.mu.Unlock()
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	_, err = snapshot.State().Placement(string(good.UID))
	require.NoError(t, err)
	_, err = snapshot.State().Placement(string(bad.UID))
	require.Error(t, err, "取消前未写容量，不补造成功")
}

func TestSchedulerWorkPagesClosedHistoryWithoutLedgerExhaustion(t *testing.T) {
	s, f, n := schedulerFixture(t)
	ctx := context.Background()
	done := boundedPod("done", n.Binding.NodeName, "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/done"] = done })
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		pod := f.Pods["testing/done"]
		now := metav1.Now()
		pod.Status.Phase = core.PodSucceeded
		pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", ContainerID: "containerd://done", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://done", StartedAt: now, FinishedAt: now}}}}
	})
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	var base *core.Pod
	f.Mutate(func(f *dsgatest.SchedulerAPI) { base = f.Pods["testing/done"].DeepCopy() })
	closed, err := hostguard.InspectRetirement(base)
	require.NoError(t, err)
	// 一个真实关闭路径产生v5，额外512条为显式历史协议夹具；不是512次
	// 物理退出验收。全部无活动行，合法关闭历史不应耗尽MaxPlacements。
	pages := map[string]*core.PodList{"": {}, "next-256": {}, "next-512": {}}
	for i := 0; i < 512; i++ {
		pod := base.DeepCopy()
		pod.Name = fmt.Sprintf("history-%03d", i)
		pod.UID = types.UID(pod.Name + "-uid")
		r := closed
		r.Placement.PodName, r.Placement.PodUID = pod.Name, string(pod.UID)
		raw, err := json.Marshal(r)
		require.NoError(t, err)
		pod.Annotations[hostguard.RetirementAnnotation] = string(raw)
		key := ""
		if i >= 256 {
			key = "next-256"
		}
		pages[key].Items = append(pages[key].Items, *pod)
	}
	pages[""].Continue = "next-256"
	pages["next-256"].Continue = "next-512"
	live := boundedPod("live", n.Binding.NodeName, "1Gi")
	pages["next-512"].Items = []core.Pod{*live}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/live"] = live })
	w := useWorkPages(s, pages)
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	w.mu.Lock()
	require.Len(t, w.lists, 2)
	require.Empty(t, w.gets)
	w.mu.Unlock()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	w.mu.Lock()
	require.Equal(t, []string{"", "next-256", "next-512"}, w.lists)
	require.Equal(t, []string{live.Name, live.Name}, w.gets, "只读取新活动Pod及保护写回确认，不重读512个关闭历史")
	w.mu.Unlock()
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, *snapshot.State().Placements, 1)
	_, err = snapshot.State().Placement(string(live.UID))
	require.NoError(t, err)
	broken := base.DeepCopy()
	delete(broken.Annotations, hostguard.RetirementAnnotation)
	require.False(t, closedHistory(snapshot, s.domains[n.Binding.NodeName], broken), "仅终态和缺行不能跳过未知原Pod")
}

func TestSchedulerWorkExpiredDiscoveryDoesNotResetOriginalLedger(t *testing.T) {
	s, _, n := schedulerFixture(t)
	ctx := context.Background()
	d := s.domains[n.Binding.NodeName]
	require.NoError(t, s.reconcile(ctx, d))
	before, err := d.store.Load(ctx)
	require.NoError(t, err)
	w := useWorkPages(s, map[string]*core.PodList{"": {ListMeta: metav1.ListMeta{Continue: "expired", SelfLink: "", ResourceVersion: "", RemainingItemCount: nil}}, "expired": {}})
	w.pageErrors["expired"] = apierrors.NewResourceExpired("fixture original discovery snapshot expired")
	require.Error(t, s.reconcile(ctx, d))
	require.Empty(t, d.work.continuation)
	require.NoError(t, s.reconcile(ctx, d))
	w.mu.Lock()
	require.Equal(t, []string{"", "expired", "", "expired"}, w.lists)
	require.Empty(t, w.gets)
	w.mu.Unlock()
	after, err := d.store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	require.Equal(t, before.ResourceVersion(), after.ResourceVersion())
}

func TestSettledScanCacheRequiresExactWatermarkAndRemainsBounded(t *testing.T) {
	req := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: "vm-uid", ComputeInstanceID: "compute", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 128, TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: "budget", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Hour).UnixMilli())}
	closed := hostguard.Settlement{Request: req, HostClaim: budget.ObjectRef{Name: "host", UID: "host-uid"}, Runner: budget.RunnerSources{BirthClaim: budget.ObjectRef{Name: "birth", UID: "birth-uid"}, StateClaim: budget.ObjectRef{Name: "state", UID: "state-uid"}, ReceiptClaim: budget.ObjectRef{Name: "receipt", UID: "receipt-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}, BudgetSequence: 1, FinalizationSHA: strings.Repeat("c", 64)}
	require.NoError(t, closed.Validate())
	p := hostguard.Placement{Namespace: "testing", PodName: "runner", PodUID: "runner-uid", VMName: "vm", VMUID: req.Key.Instance.VMUID, Incarnation: strings.Repeat("a", 32), Managed: true, MemoryBytes: req.TargetVMMemoryBytes, HostOverheadBytes: 64 << 20, Settled: &closed}
	for _, what := range []string{"same", "cold", "expired", "clock-backwards", "new-budget", "different-source", "unsettled"} {
		t.Run(what, func(t *testing.T) {
			w := newDomainWork()
			now := time.Now()
			w.rememberSettledScan(p, closed, now)
			expected, row, when := closed, p, now
			switch what {
			case "cold":
				w = newDomainWork()
			case "expired":
				when = now.Add(gcRescanInterval)
			case "clock-backwards":
				when = now.Add(-time.Second)
			case "new-budget":
				expected.BudgetSequence++
			case "different-source":
				expected.HostClaim.UID = "another-uid"
			case "unsettled":
				row.Settled = nil
			}
			require.Equal(t, what == "same", w.skipSettledScan(row, expected, when))
		})
	}
	w := newDomainWork()
	for i := 0; i < 2*hostguard.MaxPlacements; i++ {
		p.PodUID = fmt.Sprintf("pod-%d", i)
		w.rememberSettledScan(p, closed, time.Now())
	}
	require.LessOrEqual(t, len(w.gc), hostguard.MaxPlacements)
}
