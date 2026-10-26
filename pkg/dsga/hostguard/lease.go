package hostguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"

	coord "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// LeaseLock把client-go选主严格绑定到启动时固定的原Lease UID。普通LeaseLock
// 会在404后自动CREATE同名对象，这会丢失原分配域的epoch历史；这里永不CREATE。
// 选主协程独占此对象，不与Store共享缓存或本地锁；容量写的权限仍由Store逐次
// 真实GET核验，选举成功本身不是任何PG/QMP动作的票据。
type LeaseLock struct {
	client  coordclient.CoordinationV1Interface
	binding Binding
	holder  string
	lease   *coord.Lease
}

var _ resourcelock.Interface = (*LeaseLock)(nil)

func NewLeaseLock(client coordclient.CoordinationV1Interface, b Binding, holder string) (*LeaseLock, error) {
	if client == nil || b.Validate() != nil || control.ValidateID(holder, "Host lease holder") != nil {
		return nil, fmt.Errorf("host election requires original bound Lease/client/holder")
	}
	return &LeaseLock{client: client, binding: b, holder: holder, lease: nil}, nil
}

func validateLeaseBinding(l *coord.Lease, b Binding) error {
	if l == nil || l.UID != b.LeaseUID || l.Name != b.LeaseName || l.Namespace != b.Namespace || l.ResourceVersion == "" || l.DeletionTimestamp != nil || l.Annotations[LeaseAnnotation] != LeaseMarker(b) || len(l.OwnerReferences) != 1 {
		return fmt.Errorf("original Host Lease identity/binding changed")
	}
	o := l.OwnerReferences[0]
	if o.APIVersion != "v1" || o.Kind != "Node" || o.Name != b.NodeName || o.UID != b.NodeUID || o.Controller == nil || !*o.Controller || l.Spec.Strategy != nil || l.Spec.PreferredHolder != nil {
		return fmt.Errorf("original Host Lease owner or election protocol changed")
	}
	return nil
}

func (l *LeaseLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	lease, err := l.client.Leases(l.binding.Namespace).Get(ctx, l.binding.LeaseName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	if err := validateLeaseBinding(lease, l.binding); err != nil {
		return nil, nil, err
	}
	record := resourcelock.LeaseSpecToLeaderElectionRecord(&lease.Spec)
	// 新域允许从空Spec首次选主；已有record必须完整，不能把损坏历史当空域。
	if lease.Spec.HolderIdentity == nil && (lease.Spec.LeaseTransitions != nil || lease.Spec.LeaseDurationSeconds != nil || lease.Spec.AcquireTime != nil || lease.Spec.RenewTime != nil) {
		return nil, nil, fmt.Errorf("partial original Host Lease cannot be adopted as pristine")
	}
	if lease.Spec.HolderIdentity != nil && (control.ValidateID(record.HolderIdentity, "Host holder") != nil || record.LeaderTransitions < 0 || record.LeaderTransitions == math.MaxInt32 || record.LeaseDurationSeconds < 1 || record.LeaseDurationSeconds > 86400 || record.AcquireTime.IsZero() || record.RenewTime.IsZero()) {
		return nil, nil, fmt.Errorf("original Host Lease election history is malformed or exhausted")
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, nil, err
	}
	l.lease = lease.DeepCopy()
	return record, raw, nil
}

func (l *LeaseLock) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return fmt.Errorf("original Host Lease disappeared; recreation/adoption forbidden")
}

func (l *LeaseLock) Update(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	if l.lease == nil || validateLeaseBinding(l.lease, l.binding) != nil || r.HolderIdentity != l.holder || r.LeaderTransitions < 0 || r.LeaderTransitions >= math.MaxInt32 || r.LeaseDurationSeconds < 1 || r.LeaseDurationSeconds > 86400 || r.Strategy != "" || r.PreferredHolder != "" || r.AcquireTime.IsZero() || r.RenewTime.IsZero() {
		return fmt.Errorf("invalid original Host election update")
	}
	old := resourcelock.LeaseSpecToLeaderElectionRecord(&l.lease.Spec)
	if r.LeaderTransitions < old.LeaderTransitions || r.HolderIdentity != old.HolderIdentity && r.LeaderTransitions <= old.LeaderTransitions {
		return fmt.Errorf("host election cannot reset original transitions")
	}
	next := l.lease.DeepCopy()
	next.Spec = resourcelock.LeaderElectionRecordToLeaseSpec(&r)
	got, err := l.client.Leases(l.binding.Namespace).Update(ctx, next, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	// apiserver将MicroTime规范化到微秒。比较实际JSON语义，不能把原本合法
	// 的纳秒截断误判成未知写，也不能仅比较holder忽略transitions/时间。
	wantRaw, marshalErr := json.Marshal(next.Spec)
	var gotRaw []byte
	if got != nil {
		gotRaw, err = json.Marshal(got.Spec)
	}
	if marshalErr != nil || err != nil || validateLeaseBinding(got, l.binding) != nil || !bytes.Equal(wantRaw, gotRaw) {
		return fmt.Errorf("host election reply did not confirm original complete record")
	}
	l.lease = got.DeepCopy()
	return nil
}

func (l *LeaseLock) Identity() string   { return l.holder }
func (l *LeaseLock) Describe() string   { return l.binding.Namespace + "/" + l.binding.LeaseName }
func (l *LeaseLock) RecordEvent(string) {}
