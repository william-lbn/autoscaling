package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

const VMReplayAnnotation = "internal.autoscaling.neon.tech/dsga-vm-original-replay-v1"

// VMReplaySelection是受信协调入口的持久选择，不是新的命令或执行许可。原
// ResizeVM完整命令/摘要/owner不变；Runner还须独立核原not_sent回执、当前
// Lease/floor/预算/before，最终在原文件锁内核持久回执UID及nonce。
// 放在同一VM元数据以复用原UID/RV与既有Agent权限，不授予Agent预算CM写权限。
// 旧读者忽略该提示只会停在原not_sent，不会执行新动作；Model仍为既有v4。
type VMReplaySelection struct {
	ProtocolVersion uint32               `json:"protocol_version"`
	Key             control.Key          `json:"key"`
	Owner           Owner                `json:"owner"`
	Sources         budget.RunnerSources `json:"sources"`
	Nonce           string               `json:"nonce"`
}

func (v VMReplaySelection) Validate() error {
	if v.ProtocolVersion != 1 || v.Key.Validate() != nil || v.Owner.Validate() != nil || v.Key.IssuerEpoch != v.Owner.Epoch || v.Sources.Validate() != nil || control.ValidateIncarnation(v.Nonce) != nil {
		return fmt.Errorf("invalid original VM replay selection")
	}
	return nil
}

func (v *VMReplaySelection) UnmarshalJSON(raw []byte) error {
	type plain VMReplaySelection
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 2048, 128); err != nil {
		return err
	}
	*v = VMReplaySelection(p)
	return v.Validate()
}

// ParseVMReplaySelection在json.Unmarshal裁去外层空白之前限制整个注解字节数，
// 避免合法小对象前后附大量空白绕过持久选择的固定大小边界。
func ParseVMReplaySelection(raw string) (VMReplaySelection, error) {
	var v VMReplaySelection
	if len(raw) > 2048 {
		return v, fmt.Errorf("VM replay selection exceeds byte limit")
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return v, err
	}
	return v, nil
}

// SelectVMReplay只条件保存明确选择，不向Runner发送第二个命令，也不改变
// Model序列/原步骤或清任何pending。写回复丢失返回UnconfirmedWrite；调用方
// 先重读同一原回执/原选择，不能按网络错误换nonce自动重新授权一次。
func (store *Store) SelectVMReplay(ctx context.Context, snapshot *Snapshot, actor Owner, v VMReplaySelection) (*Snapshot, error) {
	if store.checked(snapshot) != nil || v.Validate() != nil || v.Owner != actor || snapshot.model.Owner != actor || !snapshot.Dispatchable() {
		return nil, fmt.Errorf("VM replay selection requires original dispatchable owner snapshot")
	}
	op := snapshot.model.Active
	// 服务关闭是独立的业务栅栏；来源恢复也不能创建或续用原重放选择。
	// Runner仍在最后写者处重复核实关闭，覆盖关闭前已持久的旧提示。
	if op == nil || snapshot.model.ServiceClosure != nil || op.Origin != actor || op.AbortRequested || op.Request.Key != v.Key {
		return nil, fmt.Errorf("VM replay selection differs from active original operation")
	}
	var found bool
	for _, step := range op.Steps {
		if step.Phase == Settled {
			if step.Observation == nil || step.Observation.Result != Completed {
				return nil, fmt.Errorf("VM replay has an unsettled predecessor")
			}
			continue
		}
		found = step.Action == ResizeVM && step.Phase == Unconfirmed && step.DispatchedBy != nil && *step.DispatchedBy == actor && step.CommandSHA256 == v.Sources.JournalSHA256
		break
	}
	if !found {
		return nil, fmt.Errorf("VM replay requires the complete original unconfirmed ResizeVM")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(raw) > 2048 {
		return nil, fmt.Errorf("VM replay selection exceeds byte limit")
	}
	if snapshot.vm.Annotations[VMReplayAnnotation] == string(raw) {
		// 已提交的原选择只GET确认同RV，不重复PATCH或生成新的选择。
		// apply的无模型变化分支仍拒绝过期快照，不拿旧读缓存当确认。
		return store.apply(ctx, snapshot, snapshot.model)
	}
	annotations := maps.Clone(snapshot.vm.Annotations)
	annotations[VMReplayAnnotation] = string(raw)
	encoded := annotations[control.JournalAnnotation]
	if len(encoded) > annotationBudget(annotations) {
		return nil, fmt.Errorf("VM replay selection exceeds original annotation budget")
	}
	operations := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: store.binding.VMUID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: snapshot.vm.ResourceVersion},
		{Op: patch.OpTest, Path: "/metadata/annotations/" + patch.PathEscape(control.ModeAnnotation), Value: "1:" + store.binding.Incarnation},
		{Op: patch.OpAdd, Path: "/metadata/annotations/" + patch.PathEscape(VMReplayAnnotation), Value: string(raw)},
	}
	result, err := store.write(ctx, operations, snapshot.model, encoded)
	if err != nil {
		return nil, err
	}
	if result.vm.Annotations[VMReplayAnnotation] != string(raw) {
		return nil, &UnconfirmedWrite{Err: fmt.Errorf("VM replay response differs from original selection")}
	}
	return result, nil
}
