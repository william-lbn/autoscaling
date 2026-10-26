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
		return birth, ref, p, err
	}
	if actor.Validate() != nil || p.State.Floor != actor || p.State.LeaseUID != r.leaseUID {
		return birth, ref, p, fmt.Errorf("Runner current durable floor or original Lease UID differs")
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
	if result.NotSent() {
		return out, ErrNotSent
	}
	if !result.Complete() || p.Actual.RequestedBytes != c.Request.TargetVMMemoryBytes || p.Actual.ActualBytes != c.Request.TargetVMMemoryBytes {
		return out, ErrUnknown
	}
	return Confirmed{Sources: sources, Birth: birth, Receipt: result, Actual: *p.Actual}, nil
}

// ObserveOriginal先只读原域结果，缺PG/root/Host不能诱发重发。只有完整原回执已
// 发布、当前无pending且实际值吻合才返回；外层仍须重核预算后才能记录Completed。
func (r *Reader) ObserveOriginal(ctx context.Context, c Command, journalSHA string, actor journal.Owner) (Confirmed, error) {
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
	return r.confirm(ctx, sources, c, actor)
}

// CheckRetained用于Finalize/准入及长期预算复核。请求历史淘汰后，原不可变回执
// 仍含完整原命令；Prepared/Guest/Host摘要与持有的最终命令交叉绑定，不按同数值
// 换另一份完成。Grow没有未来GuestReady摘要，Shrink必须原前驱Guest摘要完全相同。
func (r *Reader) CheckRetained(ctx context.Context, sources budget.RunnerSources, request control.Request, preparedSHA, guestSHA string, host budget.ObjectRef, actor journal.Owner) (Confirmed, error) {
	var out Confirmed
	if sources.Validate() != nil || request.Validate() != nil || !digestValid(preparedSHA) || !digestValid(guestSHA) {
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
	if c.Request != request || c.PreparedSHA256 != preparedSHA || c.HostClaim != host || result.Intent.CommandSHA256 != sources.CommandSHA256 || result.Intent.JournalSHA256 != sources.JournalSHA256 || (request.Kind == control.Shrink && c.GuestSHA256 != guestSHA) || (request.Kind == control.Grow && c.GuestSHA256 != "") {
		return out, fmt.Errorf("retained Runner receipt belongs to another original intent/source")
	}
	return r.confirm(ctx, sources, c, actor)
}
