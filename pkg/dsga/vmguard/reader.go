package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Reader只消费固定VM/Lease UID下的原Runner事实，不创建对象、不发QMP或重放。
// Lease UID来自已经确认的owner Session，不能从当前同名新Lease临时收养。
// 与普通Controller状态展示不同，本读者始终要求原floor、完整原命令和持久回执。
type Reader struct {
	core     coreclient.CoreV1Interface
	binding  journal.Binding
	leaseUID string
}

func NewReader(core coreclient.CoreV1Interface, binding journal.Binding, leaseUID string) (*Reader, error) {
	if core == nil || control.ValidateID(leaseUID, "owner Lease UID") != nil || binding.Namespace == "" || binding.Name == "" || control.ValidateID(string(binding.VMUID), "VM") != nil || control.ValidateIncarnation(binding.Incarnation) != nil {
		return nil, fmt.Errorf("Runner reader requires fixed original client/VM/Lease UID")
	}
	return &Reader{core: core, binding: binding, leaseUID: leaseUID}, nil
}

// Inspect用于派发前发现原Birth，也用于原命令的只读恢复。返回值是本次读取，
// 不赋予派发权限；调用者还须核实际Host/Root/PG，并以VM UID/RV保存原命令。
func (r *Reader) Inspect(ctx context.Context, original *budget.ObjectRef, actor journal.Owner) (Birth, budget.ObjectRef, Publication, error) {
	birth, ref, p, err := ReadPublished(ctx, r.core, r.binding, original)
	if err != nil {
		// Initialize先持久真实出生，再启动QEMU和首次完整发布。只有经过
		// 原Birth/State对象校验、且严格匹配初始化空发布的这一种状态可等待；
		// 丢对象、换UID、错误几何或畸形/半截发布不降级成暂态。
		if actor.Validate() == nil && birth.Validate() == nil && ref.Validate() == nil && p.ProtocolVersion == 1 && p.Identity == birth.Identity && p.BindingClaim == nil && p.State == nil && p.Actual == nil {
			return birth, ref, p, fmt.Errorf("%w: original Runner awaits first QMP publication", ErrUnknown)
		}
		return birth, ref, p, err
	}
	if actor.Validate() != nil || p.State.LeaseUID != "" && p.State.LeaseUID != r.leaseUID {
		return birth, ref, p, fmt.Errorf("Runner current durable floor or original Lease UID differs")
	}
	// 当前Session先取得新epoch，Runner自己的循环随后持久化它。落后的
	// floor或首次尚未固定Lease UID只允许等待，绝不赋派发权；同epoch换
	// holder、较新floor及同名新Lease仍作为真实身份冲突拒绝。
	if p.State.Floor.Epoch < actor.Epoch || p.State.Floor == actor && p.State.LeaseUID == "" {
		return birth, ref, p, fmt.Errorf("%w: original Runner has not acknowledged current owner floor", ErrUnknown)
	}
	if p.State.Floor != actor {
		return birth, ref, p, fmt.Errorf("Runner durable owner conflicts with current Session")
	}
	return birth, ref, p, nil
}

// Confirmed是读者刚从原Kube对象核验的完整结果副本，不是可缓存的永久VM许可。
type Confirmed struct {
	Sources budget.RunnerSources
	Birth   Birth
	Receipt Receipt
	Actual  Observation
}

func (r *Reader) confirm(ctx context.Context, sources budget.RunnerSources, c Command, actor journal.Owner) (Confirmed, error) {
	return r.confirmKind(ctx, sources, c, actor, completed)
}

func (r *Reader) confirmKind(ctx context.Context, sources budget.RunnerSources, c Command, actor journal.Owner, wanted resultKind) (Confirmed, error) {
	var out Confirmed
	if sources.Validate() != nil || c.Validate() != nil || c.Identity.VMUID != string(r.binding.VMUID) || c.Identity.Incarnation != r.binding.Incarnation || c.BindingClaim != sources.BirthClaim {
		return out, fmt.Errorf("Runner original source command differs from fixed VM")
	}
	birth, _, p, err := r.Inspect(ctx, &sources.BirthClaim, actor)
	if err != nil {
		return out, err
	}
	if birth.StateClaim != sources.StateClaim || birth.Identity != c.Identity || p.State.Pending != nil || p.State.Last == nil || p.State.LastPublished == nil || *p.State.LastPublished != sources.ReceiptClaim || p.State.Last.Intent.Command != c || p.State.Last.Intent.CommandSHA256 != sources.CommandSHA256 || p.State.Last.Intent.JournalSHA256 != sources.JournalSHA256 {
		return out, ErrUnknown
	}
	result, err := ReadReceipt(ctx, r.core, r.binding, sources.ReceiptClaim, c, sources.JournalSHA256)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(receipt(result), *p.State.Last) {
		return out, fmt.Errorf("Runner publication and immutable original receipt differ")
	}
	if result.CancelledBeforeSend() {
		if wanted == cancelledBeforeSend && p.State.ProtocolVersion == 2 && result.Actual == *p.Actual && p.Actual.RequestedBytes == c.BeforeBytes && p.Actual.ActualBytes == c.BeforeBytes {
			return Confirmed{Sources: sources, Birth: birth, Receipt: result, Actual: *p.Actual}, nil
		}
		return out, ErrUnknown
	}
	if result.NotSent() {
		if wanted == notSent && result.Intent.Owner == actor && p.Actual.RequestedBytes == c.BeforeBytes && p.Actual.ActualBytes == c.BeforeBytes && result.Actual == *p.Actual {
			return Confirmed{Sources: sources, Birth: birth, Receipt: result, Actual: *p.Actual}, nil
		}
		return out, ErrNotSent
	}
	if wanted != completed || !result.Complete() || p.Actual.RequestedBytes != c.Request.TargetVMMemoryBytes || p.Actual.ActualBytes != c.Request.TargetVMMemoryBytes {
		return out, ErrUnknown
	}
	return Confirmed{Sources: sources, Birth: birth, Receipt: result, Actual: *p.Actual}, nil
}

// ObserveOriginal先只读原域结果，缺PG/root/Host不能诱发重发。只有完整原回执已
// 发布、当前无pending且实际值吻合才返回；外层仍须重核预算后才能记录Completed。
func (r *Reader) ObserveOriginal(ctx context.Context, c Command, journalSHA string, actor journal.Owner) (Confirmed, error) {
	return r.observeOriginal(ctx, c, journalSHA, actor, false)
}

// ObserveAbortOriginal只供外层已经持久取消的原步骤对账。返回原动作Completed或
// 新增的不可再重放取消终态，调用方必须区分；普通not_sent仍保持等待，不能按
// 当前before或VM取消标志升级NoEffect。本读者不写VM/Runner，不取消物理动作。
func (r *Reader) ObserveAbortOriginal(ctx context.Context, c Command, journalSHA string, actor journal.Owner) (Confirmed, error) {
	return r.observeOriginal(ctx, c, journalSHA, actor, true)
}

func (r *Reader) observeOriginal(ctx context.Context, c Command, journalSHA string, actor journal.Owner, abort bool) (Confirmed, error) {
	var out Confirmed
	if c.Validate() != nil || !digestValid(journalSHA) {
		return out, fmt.Errorf("invalid original Runner query")
	}
	birth, _, p, err := r.Inspect(ctx, &c.BindingClaim, actor)
	if err != nil {
		return out, err
	}
	if p.State.Pending != nil || p.State.Last == nil || p.State.LastPublished == nil || p.State.Last.Intent.Command != c || p.State.Last.Intent.JournalSHA256 != journalSHA {
		return out, ErrUnknown
	}
	sources := budget.RunnerSources{BirthClaim: c.BindingClaim, StateClaim: birth.StateClaim, ReceiptClaim: *p.State.LastPublished, CommandSHA256: p.State.Last.Intent.CommandSHA256, JournalSHA256: journalSHA}
	wanted := completed
	if abort && p.State.Last.Kind == cancelledBeforeSend {
		wanted = cancelledBeforeSend
	}
	return r.confirmKind(ctx, sources, c, actor, wanted)
}

// CheckRetained用于Finalize/准入及长期预算复核。请求历史淘汰后，原不可变回执
// 仍含完整原命令；Prepared/Guest/Host摘要与持有的最终命令交叉绑定，不按同数值
// 换另一份完成。Grow没有未来GuestReady摘要，Shrink必须原前驱Guest摘要完全相同。
func (r *Reader) CheckRetained(ctx context.Context, sources budget.RunnerSources, request control.Request, preparedSHA, guestSHA string, host budget.ObjectRef, actor journal.Owner) (Confirmed, error) {
	var out Confirmed
	result, err := r.retainedReceipt(ctx, sources, request, preparedSHA, guestSHA, host)
	if err != nil {
		return out, err
	}
	return r.confirm(ctx, sources, result.Intent.Command, actor)
}

// retainedReceipt只认证不可变原回执及其完整来源，不能单独证明当前VM仍处于
// 原目标。正常CheckRetained还须当前State.Last完全相同；取消恢复另须同时核
// 新取消终态，不能通过抽出该函数放宽正常完成读者。
func (r *Reader) retainedReceipt(ctx context.Context, sources budget.RunnerSources, request control.Request, preparedSHA, guestSHA string, host budget.ObjectRef) (Receipt, error) {
	var out Receipt
	if sources.Validate() != nil || request.Validate() != nil || !digestValid(preparedSHA) || (!digestValid(guestSHA) && (request.Kind != control.Grow || guestSHA != "")) {
		return out, fmt.Errorf("incomplete retained VM source context")
	}
	cm, err := readClaim(ctx, r.core, r.binding, sources.ReceiptClaim, receiptKind, true)
	if err != nil {
		return out, err
	}
	var result Receipt
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &result) != nil {
		return out, fmt.Errorf("retained immutable Runner receipt malformed")
	}
	c := result.Intent.Command
	if c.Validate() != nil || c.BindingClaim != sources.BirthClaim || c.Identity.VMUID != string(r.binding.VMUID) || c.Identity.Incarnation != r.binding.Incarnation || c.Request != request || c.PreparedSHA256 != preparedSHA || c.HostClaim != host || result.Intent.CommandSHA256 != sources.CommandSHA256 || result.Intent.JournalSHA256 != sources.JournalSHA256 || (request.Kind == control.Shrink && c.GuestSHA256 != guestSHA) || (request.Kind == control.Grow && c.GuestSHA256 != "") {
		return out, fmt.Errorf("retained Runner receipt belongs to another original intent/source")
	}
	// 再用正式回执读者核内容寻址名和同一原UID，不能把标签正确但非原
	// 回执名称的对象用于取消恢复。普通路径也保持相同的完整认证。
	return ReadReceipt(ctx, r.core, r.binding, sources.ReceiptClaim, c, sources.JournalSHA256)
}
