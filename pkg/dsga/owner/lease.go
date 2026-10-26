package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const leaseBindingAnnotation = journal.LeaseBindingAnnotation

// LeaseName仅从可信Store固定绑定派生，包含VM UID/incarnation而非可复用名称。
// 摘要完整保留256位且总长69超出DNS label，取224位后总长61；原文另作强校验，
// 即使摘要碰撞也不能把另一个VM的Lease当作本VM领导权。
func LeaseName(binding journal.Binding) string {
	return journal.LeaseName(binding)
}

func leaseMarker(binding journal.Binding) string {
	return journal.LeaseMarker(binding)
}

// boundLease按client-go资源锁接口提供带VM绑定的真实Lease读/创建/条件更新。
// Get/Create/Update只由单个LeaderElector循环调用，不跨网络持mu；Session检查走
// 独立GET，不修改选主缓存。mu只保护首次确认后固定的Lease UID读取。
type boundLease struct {
	client  coordclient.LeaseInterface
	binding journal.Binding
	holder  string
	name    string
	lease   *coordination.Lease
	mu      sync.Mutex
	uid     types.UID
}

func (lock *boundLease) fixedUID() types.UID {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	return lock.uid
}

func validateLease(lease *coordination.Lease, binding journal.Binding, name string, expectedUID types.UID) error {
	return journal.ValidateLease(lease, binding, name, expectedUID)
}

func validRecord(record resourcelock.LeaderElectionRecord) bool {
	return control.ValidateID(record.HolderIdentity, "lease holder") == nil && record.LeaseDurationSeconds >= 1 &&
		record.LeaseDurationSeconds <= 86400 && record.LeaderTransitions >= 0 && record.LeaderTransitions <= math.MaxInt32 &&
		record.PreferredHolder == "" && record.Strategy == ""
}

func (lock *boundLease) accept(lease *coordination.Lease, expected *resourcelock.LeaderElectionRecord) error {
	if err := validateLease(lease, lock.binding, lock.name, lock.fixedUID()); err != nil {
		return err
	}
	if expected != nil {
		actual := resourcelock.LeaseSpecToLeaderElectionRecord(&lease.Spec)
		left, _ := json.Marshal(actual)
		right, _ := json.Marshal(expected)
		if string(left) != string(right) {
			return fmt.Errorf("DSGA lease response differs from requested record")
		}
	}
	lock.mu.Lock()
	if lock.uid == "" {
		lock.uid = lease.UID
	}
	lock.mu.Unlock()
	lock.lease = lease.DeepCopy()
	return nil
}

func (lock *boundLease) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	lease, err := lock.client.Get(ctx, lock.name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	if err := lock.accept(lease, nil); err != nil {
		return nil, nil, err
	}
	record := resourcelock.LeaseSpecToLeaderElectionRecord(&lease.Spec)
	raw, err := json.Marshal(record)
	return record, raw, err
}

func (lock *boundLease) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if !validRecord(record) {
		return fmt.Errorf("invalid DSGA lease record")
	}
	controller, blockDeletion := true, false
	lease, err := lock.client.Create(ctx, &coordination.Lease{
		TypeMeta: metav1.TypeMeta{APIVersion: coordination.SchemeGroupVersion.String(), Kind: "Lease"},
		ObjectMeta: metav1.ObjectMeta{
			Name: lock.name, Namespace: lock.binding.Namespace, Annotations: map[string]string{leaseBindingAnnotation: leaseMarker(lock.binding)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: lock.binding.Name, UID: lock.binding.VMUID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}},
		},
		Spec: resourcelock.LeaderElectionRecordToLeaseSpec(&record),
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return lock.accept(lease, &record)
}

func (lock *boundLease) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if lock.lease == nil || !validRecord(record) {
		return fmt.Errorf("DSGA lease update has no bound original record")
	}
	// 同一对象的UID/RV均随PUT提交。冲突、写成丢回复、错误返回对象都不确认
	// 新领导权；client-go只能经后续真实GET/条件更新再次确认，不能透明猜成功。
	next := lock.lease.DeepCopy()
	next.Spec = resourcelock.LeaderElectionRecordToLeaseSpec(&record)
	lease, err := lock.client.Update(ctx, next, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return lock.accept(lease, &record)
}

func (lock *boundLease) RecordEvent(string) {}
func (lock *boundLease) Describe() string   { return lock.binding.Namespace + "/" + lock.name }
func (lock *boundLease) Identity() string   { return lock.holder }

var _ resourcelock.Interface = (*boundLease)(nil)
