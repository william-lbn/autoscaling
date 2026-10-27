package hostscheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

const (
	// 有界的是发现/尝试工作量，不是可超卖容量；原Host账本256条上限
	// 不变。实际动作仍受owner循环10秒ctx及最终来源/UID/RV检查约束。
	workPageSize     = 256
	maxWorkPages     = 2
	maxWorkPods      = 16
	gcRescanInterval = time.Minute
)

type settledScan struct {
	closed    hostguard.Settlement
	checkedAt time.Time
}

// domainWork只保存发现游标/待尝试队列和GC节流，不是容量或许可事实。进程
// 重启/换主可丢失它，原Node账本不变。单节点仅一个reconcile以原子标志进入，
// 不在网络间持Mutex/PluginState.mu；Framework Reserve不访问这些字段。
// pending最多一页，单轮有界，已尝试项在调用前出队。即使一个请求耗尽ctx，
// 未尝试的尾部仍留到下一轮，不从同一坏VM重新开始。失败项下一次发现再试。
type domainWork struct {
	running      atomic.Bool
	continuation string
	end          bool
	pending      []core.Pod
	gc           map[string]settledScan
}

func newDomainWork() *domainWork {
	return &domainWork{running: atomic.Bool{}, continuation: "", end: false, pending: nil, gc: make(map[string]settledScan)}
}

func (s *Service) assignedPage(ctx context.Context, node, cursor string) (*core.PodList, error) {
	list, err := s.core.Pods("").List(ctx, metav1.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.nodeName", node).String(), Limit: workPageSize, Continue: cursor})
	if err != nil {
		return nil, err
	}
	if list == nil || len(list.Items) > workPageSize || cursor != "" && cursor == list.Continue {
		return nil, fmt.Errorf("assigned Pod page is absent, oversized or did not advance")
	}
	for i := range list.Items {
		if list.Items[i].Spec.NodeName != node {
			return nil, fmt.Errorf("assigned Pod page changed original Node selector")
		}
	}
	return list, nil
}

// closedHistory只跳过原Node v5已确认的、已无保护的同一终态Pod；不会把
// “终态/404/缺行”单独当关闭。这里只消除发现热路径的重复API，不产生新容量。
func closedHistory(snapshot *hostguard.Snapshot, d domain, pod *core.Pod) bool {
	if pod == nil || pod.Status.Phase != core.PodSucceeded && pod.Status.Phase != core.PodFailed || slices.Contains(pod.Finalizers, hostguard.PodProtectionFinalizer) {
		return false
	}
	r, err := hostguard.InspectRetirement(pod)
	return err == nil && r.Binding == d.config.Binding && hostguard.InspectRetiredPod(snapshot.Node(), pod) == nil
}

func (s *Service) reconcileWork(ctx context.Context, d domain, snapshot *hostguard.Snapshot) error {
	w := d.work
	if len(w.pending) == 0 && w.end {
		w.end = false
	}
	var failures []error
	pages, attempted := 0, 0
	for attempted < maxWorkPods && ctx.Err() == nil {
		if len(w.pending) == 0 {
			if w.end || pages == maxWorkPages {
				break
			}
			list, err := s.assignedPage(ctx, d.config.Binding.NodeName, w.continuation)
			if err != nil {
				// API快照过期只重置发现游标。旧持有/floor/未知原动作
				// 不重置；下一轮重新发现，实际动作仍各自真实GET和CAS。
				if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
					w.continuation, w.end = "", false
				}
				return errors.Join(append(failures, err)...)
			}
			pages++
			w.continuation, w.end = list.Continue, list.Continue == ""
			for i := range list.Items {
				if !closedHistory(snapshot, d, &list.Items[i]) {
					w.pending = append(w.pending, list.Items[i])
				}
			}
			if len(w.pending) == 0 {
				continue
			}
		}
		pod := w.pending[0]
		w.pending[0] = core.Pod{}
		w.pending = w.pending[1:]
		attempted++
		var err error
		if pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed {
			err = s.retirePod(ctx, d, &pod)
		} else {
			var canonical *core.Pod
			var held hostguard.Placement
			canonical, held, err = s.Reserve(ctx, d.config.Binding.NodeName, &pod, 0)
			if err == nil && held.Managed {
				err = s.publish(ctx, d, canonical, held)
			}
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("original Pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	if ctx.Err() != nil {
		failures = append(failures, ctx.Err())
	}
	return errors.Join(failures...)
}

// GC缓存只推迟同一已结清预算的重复扫描，绝不授予执行/降低持有/替代来源
// 验证。精确水位变化立即失效；一分钟后仍重扫迟到垃圾。缓存丢失只多做验证。
func (w *domainWork) skipSettledScan(p hostguard.Placement, closed hostguard.Settlement, now time.Time) bool {
	last, ok := w.gc[p.PodUID]
	return p.Settled != nil && *p.Settled == closed && ok && last.closed == closed && !now.Before(last.checkedAt) && now.Sub(last.checkedAt) < gcRescanInterval
}

func (w *domainWork) rememberSettledScan(p hostguard.Placement, closed hostguard.Settlement, now time.Time) {
	if len(w.gc) >= hostguard.MaxPlacements {
		// 只淘汰调度优化，不能用缓存上限淘汰持久预算或旧Key水位。
		for key := range w.gc {
			delete(w.gc, key)
			break
		}
	}
	w.gc[p.PodUID] = settledScan{closed: closed, checkedAt: now}
}
