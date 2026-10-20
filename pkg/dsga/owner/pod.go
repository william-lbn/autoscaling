package owner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	authentication "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	authclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const CredentialAudience = "neon-dsga-credential-broker"

var ErrCredentialsDenied = errors.New("DSGA credential subject or current ownership was not verified")

// PodVerifier只供可信的中心凭据服务使用。Agent传来的Bearer通过apiserver的
// TokenReview验证，不本地解析JWT、不把该Bearer当成访问apiserver的服务凭据。
// API客户端持有中心服务自己的RBAC身份；私钥始终留在中心签发器。
type PodVerifier struct {
	auth      authclient.AuthenticationV1Interface
	core      coreclient.CoreV1Interface
	coord     coordclient.CoordinationV1Interface
	namespace string
	account   string
	timeout   time.Duration
	clockSkew time.Duration
	now       func() time.Time
}

// NewPodVerifier要求显式允许的Agent ServiceAccount，不默认接受全namespace。
// timeout覆盖TokenReview、Pod/SA/Lease/VM整个检查链；clockSkew是时钟偏差的
// 保守上界，过旧/过于未来的续约均拒绝。时钟检查只能收紧签发，不能证明最终
// 执行权；短期JWT也可能跨换主重叠，PG/Runner仍必须执行单调epoch fence。
func NewPodVerifier(auth authclient.AuthenticationV1Interface, core coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, namespace, account string, timeout, clockSkew time.Duration) (*PodVerifier, error) {
	if auth == nil || core == nil || coord == nil || len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(account)) != 0 ||
		timeout <= 0 || timeout > 30*time.Second || clockSkew < 0 || clockSkew > 5*time.Second {
		return nil, fmt.Errorf("invalid DSGA credential verifier configuration")
	}
	return &PodVerifier{auth: auth, core: core, coord: coord, namespace: namespace, account: account, timeout: timeout, clockSkew: clockSkew, now: time.Now}, nil
}

func singleExtra(user authentication.UserInfo, name string) (string, bool) {
	values := user.Extra["authentication.kubernetes.io/"+name]
	if len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

// Verify在认证成功后直接读取真实Pod、ServiceAccount、完整journal和固定Lease。
// Pod名/SA名可复用，因此同时固定TokenReview中的UID；holder还必须包含同一
// Pod UID和每进程随机数。VM必须处于可派发生命周期且仍位于该Agent所在Node。
// 失败全部返回无秘密的统一错误，不把TokenReview错误、Bearer或Kube原始body
// 回显给调用方；不改变任何journal/预算/Lease，不在认证失败后自动接管。
func (verifier *PodVerifier) Verify(ctx context.Context, bearer string, store *journal.Store, claim Ownership) (*journal.Snapshot, error) {
	if verifier == nil || store == nil || len(bearer) == 0 || len(bearer) > 16384 {
		return nil, ErrCredentialsDenied
	}
	for _, b := range []byte(bearer) {
		if b < 0x21 || b > 0x7e {
			return nil, ErrCredentialsDenied
		}
	}
	podUID, bound := HolderPodUID(claim.HolderID)
	if !bound {
		return nil, ErrCredentialsDenied
	}
	callCtx, cancel := context.WithTimeout(ctx, verifier.timeout)
	defer cancel()
	request := new(authentication.TokenReview)
	request.TypeMeta = metav1.TypeMeta{APIVersion: authentication.SchemeGroupVersion.String(), Kind: "TokenReview"}
	request.Spec = authentication.TokenReviewSpec{Token: bearer, Audiences: []string{CredentialAudience}}
	review, err := verifier.auth.TokenReviews().Create(callCtx, request, metav1.CreateOptions{})
	if err != nil || review == nil || !review.Status.Authenticated || review.Status.Error != "" ||
		len(review.Status.Audiences) != 1 || review.Status.Audiences[0] != CredentialAudience ||
		review.Status.User.Username != "system:serviceaccount:"+verifier.namespace+":"+verifier.account || review.Status.User.UID == "" {
		return nil, ErrCredentialsDenied
	}
	user := review.Status.User
	podName, validName := singleExtra(user, "pod-name")
	reviewUID, validUID := singleExtra(user, "pod-uid")
	if !validName || !validUID || reviewUID != string(podUID) || len(validation.IsDNS1123Subdomain(podName)) != 0 {
		return nil, ErrCredentialsDenied
	}
	account, err := verifier.core.ServiceAccounts(verifier.namespace).Get(callCtx, verifier.account, metav1.GetOptions{})
	if err != nil || account == nil || account.Name != verifier.account || account.Namespace != verifier.namespace ||
		string(account.UID) != user.UID || account.DeletionTimestamp != nil || account.ResourceVersion == "" {
		return nil, ErrCredentialsDenied
	}
	pod, err := verifier.core.Pods(verifier.namespace).Get(callCtx, podName, metav1.GetOptions{})
	if err != nil || pod == nil || pod.Name != podName || pod.Namespace != verifier.namespace || pod.UID != podUID || pod.ResourceVersion == "" ||
		pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.ServiceAccountName != verifier.account || pod.Spec.NodeName == "" {
		return nil, ErrCredentialsDenied
	}
	// 某些Kubernetes版本额外返回node-name声明；一旦存在必须与Pod真实绑定
	// 一致，但不要求旧版本尚未提供的字段。Node UID不是Pod绑定字段，不在
	// 此处从名称猜测；最终节点资源动作仍须独立核验Node身份并fence。
	if node, present := user.Extra["authentication.kubernetes.io/node-name"]; present && (len(node) != 1 || node[0] != pod.Spec.NodeName) {
		return nil, ErrCredentialsDenied
	}
	snapshot, lease, err := VerifyCurrent(callCtx, verifier.coord, store, claim)
	if err != nil || !snapshot.Dispatchable() || snapshot.VirtualMachine().Status.Node != pod.Spec.NodeName {
		return nil, ErrCredentialsDenied
	}
	now := verifier.now()
	renew := lease.Spec.RenewTime.Time
	duration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
	if renew.IsZero() || renew.After(now.Add(verifier.clockSkew)) || !now.Add(verifier.clockSkew).Before(renew.Add(duration)) || callCtx.Err() != nil {
		return nil, ErrCredentialsDenied
	}
	// 不缓存正向授权。调用者签名必须紧接本次核验，且使用返回快照中同一
	// VM/incarnation/owner；不能将该指针长期缓存成独立执行许可。
	return snapshot, nil
}

// BearerToken只接受一个严格Bearer值，供HTTP入口在调用Verify前提取。凭据
// 不得出现在URL/日志/模型中；重复Authorization由HTTP入口另行拒绝。
func BearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) || len(header) <= len(prefix) {
		return "", false
	}
	token := strings.TrimPrefix(header, prefix)
	if strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	return token, true
}
