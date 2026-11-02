package qualificationissuer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

// Issuer 的 Kubernetes 身份只能由部署者授予独立资格发布权限；Agent 仍只读资格。
// 本对象无 Node 初始化、Root Apply、PG、QMP、预算释放或资格删除接口。
type Issuer struct {
	core    coreclient.CoreV1Interface
	coord   coordclient.CoordinationV1Interface
	vm      vmclient.Interface
	root    RootReader
	config  Config
	inspect func(context.Context, Report) (string, error)
}

func New(coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, vm vmclient.Interface, root RootReader, config Config) (*Issuer, error) {
	if coreClient == nil || coord == nil || vm == nil || root == nil || config.Validate() != nil {
		return nil, fmt.Errorf("qualification issuer requires fixed trusted clients and configuration")
	}
	issuer := &Issuer{core: coreClient, coord: coord, vm: vm, root: root, config: config}
	issuer.inspect = issuer.capture
	return issuer, nil
}

// Unconfirmed 不等于 NoEffect。CREATE/读回或写后身份变化可能已经留下原受保护
// 对象；调用方必须归档，不得删除、换名称或按当前同名 UID 自动收养。
type Unconfirmed struct {
	Claim budget.ObjectRef
	Err   error
}

func (e *Unconfirmed) Error() string {
	return "qualification publication unconfirmed; retain original protected object"
}
func (e *Unconfirmed) Unwrap() error { return e.Err }

type Result struct {
	Source                   budget.QualificationSource `json:"source"`
	ReportSHA256             string                     `json:"report_sha256"`
	LinuxArtifactSHA256      string                     `json:"linux_artifact_sha256"`
	ContinuousArtifactSHA256 string                     `json:"continuous_artifact_sha256"`
}

func (i *Issuer) Publish(ctx context.Context, bundle Bundle) (Result, error) {
	var zero Result
	ctx, cancel := context.WithTimeout(ctx, time.Duration(i.config.TimeoutSeconds)*time.Second)
	defer cancel()
	report, err := i.config.authenticate(bundle)
	if err != nil {
		return zero, err
	}
	// 全部认证和两次真实读取都在任何写入之前；没有平台报告不能生成测试 CM。
	before, err := i.inspect(ctx, report)
	if err != nil {
		return zero, err
	}
	after, err := i.inspect(ctx, report)
	if err != nil || before != after {
		return zero, fmt.Errorf("original platform sources changed before publication")
	}
	q := report.Qualification
	raw, err := json.Marshal(q)
	if err != nil || len(raw) > journal.MaxObservationBytes {
		return zero, fmt.Errorf("qualification body exceeds existing consumer contract")
	}
	yes := true
	name := "dsga-q-" + i.config.ReportSHA256[:56]
	annotations := map[string]string{ReportAnnotation: i.config.ReportSHA256, LinuxAnnotation: report.LinuxArtifactSHA256, ContinuousAnnotation: report.ContinuousArtifactSHA256}
	expected := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: i.config.Binding.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: i.config.Binding.Name, UID: i.config.Binding.VMUID, Controller: &yes, BlockOwnerDeletion: &yes}}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.QualificationKind}, Annotations: annotations}, Immutable: &yes, Data: map[string]string{budgetclaims.ClaimKey: string(raw)}}
	ref := budget.ObjectRef{Name: name, UID: i.config.ResumeClaimUID}
	if ref.UID == "" {
		made, createErr := i.core.ConfigMaps(expected.Namespace).Create(ctx, expected, metav1.CreateOptions{})
		if createErr != nil {
			return zero, &Unconfirmed{Claim: ref, Err: createErr}
		}
		if made == nil || made.UID == "" {
			return zero, &Unconfirmed{Claim: ref, Err: fmt.Errorf("qualification CREATE identity missing")}
		}
		ref.UID = string(made.UID)
		if err = checkCreated(made, expected, ref); err != nil {
			return zero, &Unconfirmed{Claim: ref, Err: err}
		}
	}
	got, err := i.core.ConfigMaps(expected.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return zero, &Unconfirmed{Claim: ref, Err: err}
	}
	if err = checkCreated(got, expected, ref); err != nil {
		return zero, &Unconfirmed{Claim: ref, Err: err}
	}
	// 写后也重新取原完整事实；不假装跨对象原子性，竞争时不返回可消费来源。
	final, err := i.inspect(ctx, report)
	if err != nil || before != final {
		return zero, &Unconfirmed{Claim: ref, Err: fmt.Errorf("original platform sources changed after publication")}
	}
	source := budget.QualificationSource{Claim: ref, SHA256: SHA256(raw)}
	reader, err := budgetclaims.New(i.core, i.config.Binding)
	if err != nil {
		return zero, &Unconfirmed{Claim: ref, Err: err}
	}
	checked, err := reader.Qualification(ctx, source)
	if err != nil || !reflect.DeepEqual(q, checked) {
		return zero, &Unconfirmed{Claim: ref, Err: fmt.Errorf("original qualification consumer readback unconfirmed")}
	}
	return Result{Source: source, ReportSHA256: i.config.ReportSHA256, LinuxArtifactSHA256: report.LinuxArtifactSHA256, ContinuousArtifactSHA256: report.ContinuousArtifactSHA256}, nil
}

func checkCreated(got, want *core.ConfigMap, ref budget.ObjectRef) error {
	if got == nil || got.Namespace != want.Namespace || got.Name != ref.Name || string(got.UID) != ref.UID || got.ResourceVersion == "" || got.DeletionTimestamp != nil || got.Immutable == nil || !*got.Immutable || len(got.Data) != 1 || len(got.BinaryData) != 0 || !reflect.DeepEqual(got.Data, want.Data) || got.Labels[budgetclaims.ClaimLabel] != budgetclaims.QualificationKind || !slices.Contains(got.Finalizers, budgetclaims.ProtectionFinalizer) {
		return fmt.Errorf("original protected qualification object changed")
	}
	owner := metav1.GetControllerOf(got)
	wanted := want.OwnerReferences[0]
	if len(got.OwnerReferences) != 1 || owner == nil || owner.APIVersion != wanted.APIVersion || owner.Kind != wanted.Kind || owner.Name != wanted.Name || owner.UID != types.UID(wanted.UID) || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return fmt.Errorf("qualification VM controller changed")
	}
	for k, v := range want.Annotations {
		if got.Annotations[k] != v {
			return fmt.Errorf("qualification provenance changed")
		}
	}
	return nil
}
