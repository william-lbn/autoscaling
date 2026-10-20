package owner

import (
	"context"
	"encoding/hex"
	"strings"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Ownership是可以传输的待核对身份，不是凭据或许可；复制/自报任何字段均不能
// 创建领导权。远程服务必须先认证调用Pod，再经自己的RBAC客户端VerifyCurrent。
// Epoch保留uint64十进制字符串编码，不能经过float64或LeaseTransitions推导。
type Ownership struct {
	Namespace   string         `json:"namespace"`
	VMName      string         `json:"vm_name"`
	VMUID       types.UID      `json:"vm_uid"`
	Incarnation string         `json:"incarnation"`
	LeaseUID    types.UID      `json:"lease_uid"`
	HolderID    string         `json:"holder_id"`
	IssuerEpoch control.Uint64 `json:"issuer_epoch"`
}

func (session *Session) Ownership() Ownership {
	return Ownership{
		Namespace: session.binding.Namespace, VMName: session.binding.Name,
		VMUID: session.binding.VMUID, Incarnation: session.binding.Incarnation,
		LeaseUID: session.leaseUID, HolderID: session.actor.HolderID, IssuerEpoch: session.actor.Epoch,
	}
}

// holder的随机部分固定为128位小写hex，后接不可复用Pod UID。限制UID字符和长度
// 保证整串满足journal现有128字节上限，并使解析只有一种表达；Pod本身的真实性
// 仍由TokenReview和真实Pod GET决定，不从该后缀猜测认证结果。
func validPodUID(uid types.UID) bool {
	if len(uid) == 0 || len(uid) > 90 {
		return false
	}
	for _, b := range []byte(uid) {
		switch {
		case (b >= 'a' && b <= 'z'), (b >= 'A' && b <= 'Z'), (b >= '0' && b <= '9'), b == '-', b == '_':
		default:
			return false
		}
	}
	return true
}

func HolderPodUID(holder string) (types.UID, bool) {
	const prefixBytes = len("dsga-") + 32
	if len(holder) <= prefixBytes+1 || !strings.HasPrefix(holder, "dsga-") || holder[prefixBytes] != '.' {
		return "", false
	}
	entropy := holder[len("dsga-"):prefixBytes]
	if _, err := hex.DecodeString(entropy); err != nil || strings.ToLower(entropy) != entropy {
		return "", false
	}
	uid := types.UID(holder[prefixBytes+1:])
	return uid, validPodUID(uid)
}

// VerifyCurrent复用唯一journal解码/绑定规则，供远程凭据签发器使用自己的可信
// Kubernetes客户端查询。调用者必须设置独立期限；本函数不引入隐藏重试/新日志。
// Lease→完整journal→Lease的二次读取封闭读取journal期间发生的换holder/重建，
// 允许同holder正常续约导致RV变化。最后读取后仍可换主，故结果不是跨对象原子
// 票据，也不能取代PG/Runner单调fence。远程签发还须核对Pod与续约新鲜度。
func VerifyCurrent(ctx context.Context, client coordclient.CoordinationV1Interface, store *journal.Store, claim Ownership) (*journal.Snapshot, *coordination.Lease, error) {
	if client == nil || store == nil {
		return nil, nil, ErrLeadershipLost
	}
	return verifyCurrent(ctx, client.Leases(store.Binding().Namespace), store, claim)
}

func verifyCurrent(ctx context.Context, client coordclient.LeaseInterface, store *journal.Store, claim Ownership) (*journal.Snapshot, *coordination.Lease, error) {
	binding := store.Binding()
	if claim.Namespace != binding.Namespace || claim.VMName != binding.Name || claim.VMUID != binding.VMUID || claim.Incarnation != binding.Incarnation ||
		control.ValidateID(string(claim.LeaseUID), "lease") != nil || control.ValidateID(claim.HolderID, "holder") != nil || claim.IssuerEpoch == 0 {
		return nil, nil, ErrLeadershipLost
	}
	readLease := func() (*coordination.Lease, error) {
		lease, err := client.Get(ctx, LeaseName(binding), metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if validateLease(lease, binding, LeaseName(binding), claim.LeaseUID) != nil || *lease.Spec.HolderIdentity != claim.HolderID {
			return nil, ErrLeadershipLost
		}
		return lease, nil
	}
	if _, err := readLease(); err != nil {
		return nil, nil, err
	}
	snapshot, err := store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	if snapshot.State().Owner != (journal.Owner{HolderID: claim.HolderID, Epoch: claim.IssuerEpoch}) || snapshot.VirtualMachine().DeletionTimestamp != nil {
		return nil, nil, ErrLeadershipLost
	}
	lease, err := readLease()
	if err != nil {
		return nil, nil, err
	}
	return snapshot, lease, nil
}
