package vmguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const (
	ClaimKey            = "runner.json"
	ClaimLabel          = "autoscaling.neon.tech/dsga-runner"
	ProtectionFinalizer = "autoscaling.neon.tech/dsga-runner-protection"
	bindingKind         = "binding-v1"
	stateKind           = "state-v1"
	receiptKind         = "receipt-v1"
	// ReceiptKind供原Host结清后的来源清理选择已知版本；标签本身不
	// 认证回执，仍须严格解码、核原VM/出生/完整命令和内容寻址名。
	ReceiptKind = receiptKind
)

// Birth是启动QEMU之前一次性创建并读回的不可变来源。原Pod/boot/Node、几何和
// mutable状态对象UID均固定。同名Runner重启不自动收养此来源，必须另走受控恢复。
type Birth struct {
	ProtocolVersion uint32                `json:"protocol_version"`
	Identity        Identity              `json:"identity"`
	StateClaim      budget.ObjectRef      `json:"state_claim"`
	InitialMode     *InitialBootstrapMode `json:"initial_mode,omitempty"`
}

func (b Birth) Validate() error {
	if !initialVersionMatches(b.ProtocolVersion, b.InitialMode, b.Identity) || b.Identity.Validate() != nil || b.StateClaim.Validate() != nil {
		return fmt.Errorf("incomplete original Runner birth source")
	}
	return nil
}

// View保留完整原命令/floor/pending/结果，不将JSON对象本身当许可。消费者必须
// 用自己的Kube客户端从原Birth固定的State UID取得它，再核对不可变原回执。
type View record

func (v View) Validate() error { return record(v).validate() }

func (v *View) UnmarshalJSON(raw []byte) error {
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	*v = View(r)
	return nil
}

type Receipt receipt

func (r Receipt) Complete() bool { return r.Kind == completed }
func (r Receipt) NotSent() bool  { return r.Kind == notSent }

// CancelledBeforeSend是禁止原命令再重放的原域终态，区别于仍可重放的not_sent。
// 消费者还须核原State/不可变Receipt及当前实际before，不能单凭此布尔结算。
func (r Receipt) CancelledBeforeSend() bool { return r.Kind == cancelledBeforeSend }

func (r Receipt) Validate() error {
	key := r.Intent.Command.Request.Key
	// Receipt不含可变floor/Lease，下面仅复用完整字段关系校验。占位值不被
	// 序列化/返回，也不是认证来源；真实Lease另由当前View永久固定UID。
	probe := record{ProtocolVersion: 1, Identity: r.Intent.Command.Identity, BindingClaim: r.Intent.Command.BindingClaim, Sequence: 1, Floor: r.Intent.Owner, LeaseUID: "validation-only", HighWater: &key, Last: (*receipt)(&r), Pending: nil, LastPublished: nil}
	if r.Cancellation != nil {
		probe.ProtocolVersion, probe.Floor = 2, r.Cancellation.Owner
	}
	return probe.validate()
}

func (r *Receipt) UnmarshalJSON(raw []byte) error {
	var p receipt
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	next := Receipt(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*r = next
	return nil
}

type Publication struct {
	ProtocolVersion  uint32                       `json:"protocol_version"`
	Identity         Identity                     `json:"identity"`
	BindingClaim     *budget.ObjectRef            `json:"binding_claim"`
	State            *View                        `json:"state"`
	Actual           *Observation                 `json:"actual"`
	InitialBootstrap *InitialBootstrapPublication `json:"initial_bootstrap,omitempty"`
}

func (v *Birth) UnmarshalJSON(raw []byte) error {
	type plain Birth
	var p plain
	if err := strictDecode(raw, &p); err != nil {
		return err
	}
	next := Birth(p)
	if e := rejectLegacyInitialField(raw, next.ProtocolVersion, "initial_mode"); e != nil {
		return e
	}
	if err := next.Validate(); err != nil {
		return err
	}
	*v = next
	return nil
}

func (v *Publication) UnmarshalJSON(raw []byte) error {
	type plain Publication
	var p plain
	if err := strictDecode(raw, &p); err != nil {
		return err
	}
	if e := rejectLegacyInitialField(raw, p.ProtocolVersion, "initial_bootstrap"); e != nil {
		return e
	}
	*v = Publication(p)
	return nil
}

func SourceDigest(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

func sourceName(binding journal.Binding, prefix string) string {
	raw, _ := json.Marshal([]string{binding.Namespace, binding.Name, string(binding.VMUID), binding.Incarnation})
	h := sha256.Sum256(append([]byte("neon-dsga-runner-binding-v1\x00"), raw...))
	return prefix + hex.EncodeToString(h[:28])
}

func BindingName(binding journal.Binding) string { return sourceName(binding, "dsga-b-") }
func stateName(binding journal.Binding) string   { return sourceName(binding, "dsga-s-") }
func receiptName(raw []byte) string              { return "dsga-r-" + SourceDigest(raw)[:56] }

// ReceiptSourceName只复用原始回执字节的内容寻址规则，避免GC与生产者
// 各维护一套名称算法。它不校验JSON、不确认UID，也不赋予任何执行/回收许可。
func ReceiptSourceName(raw []byte) string { return receiptName(raw) }

func newClaim(binding journal.Binding, name, kind string, immutable bool, raw []byte) *core.ConfigMap {
	controller, block := true, true
	return &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: binding.Namespace, Labels: map[string]string{ClaimLabel: kind}, Finalizers: []string{ProtectionFinalizer},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding.Name, UID: binding.VMUID, Controller: &controller, BlockOwnerDeletion: &block}},
	}, Immutable: &immutable, Data: map[string]string{ClaimKey: string(raw)}}
}

func validClaim(cm *core.ConfigMap, binding journal.Binding, ref budget.ObjectRef, kind string, immutable bool) error {
	return validClaimState(cm, binding, ref, kind, immutable, false)
}

// allowDeleting仅供已退出来源的关闭读取；VM进入GC后，原UID且仍受保护的
// immutable Birth/Exit可以结清原成本。活动执行读取仍严格拒绝DeletionTimestamp。
func validClaimState(cm *core.ConfigMap, binding journal.Binding, ref budget.ObjectRef, kind string, immutable, allowDeleting bool) error {
	if ref.Validate() != nil || cm == nil || cm.Name != ref.Name || string(cm.UID) != ref.UID || cm.Namespace != binding.Namespace || cm.ResourceVersion == "" || !allowDeleting && cm.DeletionTimestamp != nil || cm.Immutable == nil || *cm.Immutable != immutable || cm.Labels[ClaimLabel] != kind || !slices.Contains(cm.Finalizers, ProtectionFinalizer) || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || len(cm.Data[ClaimKey]) == 0 || len(cm.Data[ClaimKey]) > journal.MaxObservationBytes {
		return fmt.Errorf("original Runner source is absent, replaced, malformed or releasing")
	}
	owner := metav1.GetControllerOf(cm)
	if len(cm.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != binding.Name || owner.UID != binding.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return fmt.Errorf("Runner source belongs to another original VM")
	}
	return nil
}

// createClaim只对这一次完整nonce/原文恢复CREATE丢回复。先Create再真实GET，
// 不把伪造成功对象当持久事实，也不在404/超时后盲目更换名称/nonce重建来源。
func createClaim(ctx context.Context, coreClient coreclient.CoreV1Interface, binding journal.Binding, expected *core.ConfigMap, kind string) (*core.ConfigMap, error) {
	_, createErr := coreClient.ConfigMaps(binding.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	got, err := coreClient.ConfigMaps(binding.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Runner source CREATE unconfirmed: %w", errors.Join(createErr, err))
	}
	if got == nil {
		return nil, fmt.Errorf("Runner source CREATE has no actual GET readback")
	}
	ref := budget.ObjectRef{Name: got.Name, UID: string(got.UID)}
	if validClaim(got, binding, ref, kind, *expected.Immutable) != nil || !reflect.DeepEqual(got.Data, expected.Data) {
		return nil, fmt.Errorf("Runner source CREATE conflicts with another original birth/result")
	}
	return got, nil
}

func readClaim(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, ref budget.ObjectRef, kind string, immutable bool) (*core.ConfigMap, error) {
	cm, err := client.ConfigMaps(binding.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := validClaim(cm, binding, ref, kind, immutable); err != nil {
		return nil, err
	}
	return cm, nil
}

// ReadPublished只从实际Kubernetes读取。固定name用于找到Birth，真实UID由调用
// 者在原ResizeVM/长期预算中持有；原ref非nil时绝不能同名收养新Birth。无ref的
// Controller状态展示不构成派发/预算许可，亦不能用此展示绕过最终执行者。
func ReadPublished(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, original *budget.ObjectRef) (Birth, budget.ObjectRef, Publication, error) {
	var birth Birth
	var p Publication
	if client == nil || binding.Namespace == "" || binding.Name == "" || binding.VMUID == "" || control.ValidateIncarnation(binding.Incarnation) != nil {
		return birth, budget.ObjectRef{}, p, fmt.Errorf("fixed Runner source client/VM required")
	}
	cm, err := client.ConfigMaps(binding.Namespace).Get(ctx, BindingName(binding), metav1.GetOptions{})
	if err != nil {
		return birth, budget.ObjectRef{}, p, err
	}
	birth, ref, err := InspectBirth(cm, binding, original)
	if err != nil {
		return birth, ref, p, err
	}
	if _, err := ReadProcessExit(ctx, client, binding, birth, ref); err == nil {
		return birth, ref, p, ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return birth, ref, p, err
	}
	// 无Wait时Scheduler的整Pod关闭来源同样永久封住旧发布。此处只拒绝
	// 活动授权，不作Node容量结清；坏来源/网络失败也不能降级到旧State。
	if stopped, err := client.ConfigMaps(binding.Namespace).Get(ctx, StoppedPodClaimName(binding, birth), metav1.GetOptions{}); err == nil {
		if _, err := InspectStoppedPodSource(stopped, binding, birth, ref); err != nil {
			return birth, ref, p, err
		}
		return birth, ref, p, ErrStopped
	} else if !apierrors.IsNotFound(err) {
		return birth, ref, p, err
	}
	state, err := readClaim(ctx, client, binding, birth.StateClaim, stateKind, false)
	if err != nil {
		return birth, ref, p, err
	}
	p, err = InspectPublication(state, binding, birth, ref)
	return birth, ref, p, err
}

// InspectBirth/InspectPublication供拥有直接APIReader的Controller复用唯一解码
// 规则。它们只做纯校验，不认证传入对象；生产调用者必须先真实GET相应原对象。
func InspectBirth(cm *core.ConfigMap, binding journal.Binding, original *budget.ObjectRef) (Birth, budget.ObjectRef, error) {
	return inspectBirthState(cm, binding, original, false)
}

// InspectBirthForExit只固定原关闭来源，不能用于活动派发或QMP状态读取。生产
// 调用者必须真实GET此CM，再以自己的客户端核原Exit/Pod/Node，不信请求自报。
func InspectBirthForExit(cm *core.ConfigMap, binding journal.Binding, original *budget.ObjectRef) (Birth, budget.ObjectRef, error) {
	return inspectBirthState(cm, binding, original, true)
}

func inspectBirthState(cm *core.ConfigMap, binding journal.Binding, original *budget.ObjectRef, allowDeleting bool) (Birth, budget.ObjectRef, error) {
	var birth Birth
	if cm == nil {
		return birth, budget.ObjectRef{}, fmt.Errorf("Runner Birth unavailable")
	}
	ref := budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
	if ref.Name != BindingName(binding) || original != nil && *original != ref {
		return birth, ref, fmt.Errorf("original Runner Birth name/UID changed")
	}
	if validClaimState(cm, binding, ref, bindingKind, true, allowDeleting) != nil || json.Unmarshal([]byte(cm.Data[ClaimKey]), &birth) != nil || birth.Identity.VMUID != string(binding.VMUID) || birth.Identity.Incarnation != binding.Incarnation {
		return birth, ref, fmt.Errorf("original Runner Birth malformed")
	}
	return birth, ref, nil
}

func InspectPublication(cm *core.ConfigMap, binding journal.Binding, birth Birth, ref budget.ObjectRef) (Publication, error) {
	var p Publication
	if birth.Validate() != nil || validClaim(cm, binding, birth.StateClaim, stateKind, false) != nil || json.Unmarshal([]byte(cm.Data[ClaimKey]), &p) != nil || initialPublicationMatches(p, birth, &ref, false) != nil || p.Identity != birth.Identity || p.BindingClaim == nil || *p.BindingClaim != ref || p.State == nil || p.State.Validate() != nil || p.State.Identity != birth.Identity || p.State.BindingClaim != ref || p.Actual == nil || p.Actual.validate(birth.Identity) != nil {
		return p, fmt.Errorf("original Runner has no complete private QMP publication")
	}
	return p, nil
}

// ReadReceipt固定原UID与完整命令，不用可变State中的字符串、HTTP 200或普通
// QMP回复生成Completed。新owner可读旧epoch结果，不能重写其owner/命令摘要。
func ReadReceipt(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, ref budget.ObjectRef, expected Command, journalSHA string) (Receipt, error) {
	var result Receipt
	if client == nil || expected.Validate() != nil || expected.Identity.VMUID != string(binding.VMUID) || expected.Identity.Incarnation != binding.Incarnation || !digestValid(journalSHA) {
		return result, fmt.Errorf("receipt reader requires fixed original VM/command/journal binding")
	}
	cm, err := readClaim(ctx, client, binding, ref, receiptKind, true)
	if err != nil {
		return result, err
	}
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &result) != nil || result.Intent.Command != expected || result.Intent.JournalSHA256 != journalSHA || ref.Name != receiptName([]byte(cm.Data[ClaimKey])) {
		return result, fmt.Errorf("original immutable Runner receipt differs")
	}
	return result, nil
}

func (r *Runner) publish(ctx context.Context) error {
	return r.engine.withRecord(ctx, func(state *record, s *lockedState) error {
		if state.Last != nil && state.LastPublished == nil {
			raw, err := json.Marshal(state.Last)
			if err != nil {
				return err
			}
			cm, err := createClaim(ctx, r.core, r.store.Binding(), newClaim(r.store.Binding(), receiptName(raw), receiptKind, true, raw), receiptKind)
			if err != nil {
				return err
			}
			state.LastPublished = &budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
			if err := saveNext(s, state); err != nil {
				return err
			}
		}
		if r.initial != nil {
			if err := r.initial.gate(s); err != nil {
				return err
			}
		}
		actual, err := r.engine.native.Read(ctx)
		if err != nil {
			return err
		}
		if err := actual.validate(r.birth.Identity); err != nil {
			return err
		}
		view := View(state.clone())
		publication := Publication{ProtocolVersion: r.birth.ProtocolVersion, Identity: r.birth.Identity, BindingClaim: &r.engine.binding, State: &view, Actual: &actual}
		if r.initial != nil {
			proof, e := r.initial.publication(s)
			if e != nil {
				return e
			}
			publication.InitialBootstrap = proof
		}
		raw, err := json.Marshal(publication)
		if err != nil || len(raw) > journal.MaxObservationBytes {
			return fmt.Errorf("Runner publication exceeds bounded state body")
		}
		cm, err := readClaim(ctx, r.core, r.store.Binding(), r.birth.StateClaim, stateKind, false)
		if err != nil {
			return err
		}
		var prior Publication
		if json.Unmarshal([]byte(cm.Data[ClaimKey]), &prior) != nil || prior.Identity != r.birth.Identity || initialPublicationMatches(prior, r.birth, &r.engine.binding, true) != nil || prior.State != nil && prior.State.Sequence > state.Sequence {
			return fmt.Errorf("Runner published state is malformed or newer than local durable state")
		}
		if cm.Data[ClaimKey] == string(raw) {
			return nil
		}
		cm.Data = map[string]string{ClaimKey: string(raw)}
		// Update自带原UID/RV，冲突或丢回复只在下一轮GET确认，不伪造完成。
		_, err = r.core.ConfigMaps(r.store.Binding().Namespace).Update(ctx, cm, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			return fmt.Errorf("Runner publication CAS conflict: %w", err)
		}
		return err
	})
}

func sameBirth(a, b Birth) bool {
	return a.ProtocolVersion == b.ProtocolVersion && a.Identity == b.Identity && a.StateClaim == b.StateClaim && sameInitialMode(a.InitialMode, b.InitialMode)
}
