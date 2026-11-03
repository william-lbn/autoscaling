package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 只在本次原initial RunQEMU写launch之前使用。没有Lease不是许可；等待只允许
// 原bootstrap owner尚未被同一真实Elector接管的pristine窗口，不重试一般authorize错误。
func (r *Runner) initialOwnerPoll(ctx context.Context, fixedLeaseUID string) (bool, string, error) {
	if r == nil || r.initial == nil || r.birth.InitialMode == nil {
		return false, "", fmt.Errorf("explicit initial Runner required")
	}
	err := r.engine.withRecord(ctx, func(rec *record, s *lockedState) error {
		if rec.Sequence != 1 || rec.Pending != nil || rec.Last != nil || rec.HighWater != nil || rec.LastPublished != nil {
			return fmt.Errorf("initial owner wait has nonpristine local history")
		}
		raw, e := s.readProcessFile("process-launch.json")
		if e != nil {
			return e
		}
		if raw != nil {
			return fmt.Errorf("initial owner wait cannot adopt an existing launch")
		}
		return nil
	})
	if err != nil {
		return false, "", err
	}
	snapshot, err := r.store.Load(ctx)
	if err != nil {
		return false, "", err
	}
	m, vm := snapshot.State(), snapshot.VirtualMachine()
	if vm.DeletionTimestamp != nil || vm.Spec.DSGABootstrap == nil || !vm.Spec.DSGABootstrap.Ready || m.Active != nil || m.HighWater != nil || len(m.History) != 0 || m.ExpiredResults != 0 || m.ResourceIdentity != nil || m.PGInstance != nil || m.BudgetHeld != nil || m.CapacityHeld != nil || m.ServiceClosure != nil {
		return false, "", fmt.Errorf("initial owner wait lost pristine original VM")
	}
	pod, err := r.core.Pods(vm.Namespace).Get(ctx, r.pod, metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	if err := originalPod(pod, r.store.Binding(), r.pod, r.birth.Identity.RunnerPodUID, r.node); err != nil {
		return false, "", err
	}
	node, err := r.core.Nodes().Get(ctx, r.node, metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	if string(node.UID) != r.birth.Identity.NodeUID || node.DeletionTimestamp != nil || node.ResourceVersion == "" {
		return false, "", fmt.Errorf("initial owner wait original Node changed")
	}
	born, err := readClaim(ctx, r.core, r.store.Binding(), r.engine.binding, bindingKind, true)
	if err != nil {
		return false, "", err
	}
	var birth Birth
	if json.Unmarshal([]byte(born.Data[ClaimKey]), &birth) != nil || !sameBirth(birth, r.birth) {
		return false, "", fmt.Errorf("initial owner wait original Birth changed")
	}
	state, err := readClaim(ctx, r.core, r.store.Binding(), r.birth.StateClaim, stateKind, false)
	if err != nil {
		return false, "", err
	}
	var pub Publication
	if json.Unmarshal([]byte(state.Data[ClaimKey]), &pub) != nil || initialPublicationMatches(pub, r.birth, &r.engine.binding, true) != nil || pub.State != nil || pub.Actual != nil {
		return false, "", fmt.Errorf("initial owner wait publication is not pristine")
	}
	bootstrap := m.Owner.HolderID == "bootstrap-"+r.store.Binding().Incarnation && m.Owner.Epoch == 1
	lease, err := r.readLease(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) && bootstrap && fixedLeaseUID == "" {
			return false, "", nil
		}
		return false, "", err
	}
	uid := string(lease.UID)
	if fixedLeaseUID != "" && uid != fixedLeaseUID {
		return false, "", fmt.Errorf("initial owner wait original Lease UID changed")
	}
	// VM epoch只来自原journal CAS；client-go首次Lease transitions为0，
	// 真实Elector从bootstrap epoch1接管后journal已经是2，二者不能相加推导。
	if !bootstrap && *lease.Spec.HolderIdentity == m.Owner.HolderID {
		a, err := r.authorize(ctx) // 原完整Lease/journal/Lease及Birth/Pod/Node门仍为唯一许可。
		if err != nil {
			return false, "", err
		}
		if a.Owner != m.Owner || a.LeaseUID != uid {
			return false, "", fmt.Errorf("initial owner wait owner changed across original reads")
		}
		return true, uid, nil
	}
	if bootstrap {
		return false, uid, nil
	} // 真实Lease CREATE与原journal接管之间的唯一窗口。
	return false, "", fmt.Errorf("initial owner wait Lease and original owner differ")
}

func (r *Runner) awaitInitialOwner(ctx context.Context, timeout, interval time.Duration) error {
	if timeout <= 0 || interval <= 0 {
		return fmt.Errorf("finite initial owner wait required")
	}
	wait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	uid := ""
	for {
		if err := wait.Err(); err != nil {
			return err
		}
		ready, observed, err := r.initialOwnerPoll(wait, uid)
		if err != nil {
			return err
		}
		if observed != "" {
			uid = observed
		}
		if ready {
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-tick.C:
		}
	}
}
