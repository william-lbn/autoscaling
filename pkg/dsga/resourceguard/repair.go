package resourceguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// HeldRepairRequest只选择已经持久确认的原约束，没有调用者可填写的新限额。
// IssuerEpoch是本次修复的执行权；SourceSHA256绑定原Bootstrap/Held/Birth，
// 原命令内较老的epoch/revision一字不改。新owner也不能借修复覆盖未知命令。
type HeldRepairRequest struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	Identity        Identity       `json:"identity"`
	IssuerEpoch     control.Uint64 `json:"issuer_epoch"`
	SourceSHA256    string         `json:"source_sha256"`
}

func (v HeldRepairRequest) Validate() error {
	if v.ProtocolVersion != 1 || validateResourceContext(v.Identity, v.IssuerEpoch, "held-repair") != nil || len(v.SourceSHA256) != sha256.Size*2 {
		return fmt.Errorf("invalid original held repair selection")
	}
	b, err := hex.DecodeString(v.SourceSHA256)
	if err != nil || hex.EncodeToString(b) != v.SourceSHA256 {
		return fmt.Errorf("invalid original held repair digest")
	}
	return nil
}

func (v *HeldRepairRequest) UnmarshalJSON(raw []byte) error {
	type plain HeldRepairRequest
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := HeldRepairRequest(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*v = next
	return nil
}

// HeldSourceSHA256只给受信协调者生成选择摘要，不授予资源写权限。
// Actual/floor不进入摘要：它们是每次修复必须重新认证的当前事实；原出生和
// 完整来源进入摘要，防止同值不同预算/PG出生被当成原恢复对象。未知Original
// 或未完成Bootstrap没有可修复来源，必须继续其原Observe/Replay流程。
func HeldSourceSHA256(v View) (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	if !v.BootstrapApplied || v.Bootstrap.Limits.SystemMemoryMaxBytes != 0 && !v.SystemPlacementObserved || v.Original != nil && (!v.Applied || !reflect.DeepEqual(v.Original, v.Held)) {
		return "", fmt.Errorf("held repair cannot cross an unknown original resource intent")
	}
	if v.Original != nil && (v.Birth == nil || !v.Birth.matches(*v.Original)) {
		return "", fmt.Errorf("held repair lacks original Postmaster birth")
	}
	raw, err := json.Marshal([]any{v.Bootstrap, v.Held, v.Birth})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("neon-dsga-held-root-repair-v1\x00"), raw...))
	return hex.EncodeToString(hash[:]), nil
}

func heldLimits(v View) Limits {
	if v.Held != nil {
		return v.Held.limits()
	}
	return v.Bootstrap.Limits
}

// repairPreflight只允许从更紧/相等的实际memory.max和整HugeTLB池恢复到
// 原承诺上界。不能借瞬时RSS把“恢复”变成一次降限/排空，也不修复swap、
// surplus、pids或系统归组漂移。所有hstate必须完整同序且原预算仍放得进
// 当前Guest；任一不明确在第一项内核写之前拒绝。Linux写者须仍root独占。
func repairPreflight(e Evidence, id Identity, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if e.ProtocolVersion != 1 || e.VMShrinkPermit || e.Identity != id || e.MemoryMaxBytes > limits.MemoryMaxBytes || e.MemoryCurrentBytes > limits.MemoryMaxBytes || e.SwapMaxBytes != 0 || e.SwapCurrentBytes != 0 || !e.HugeOvercommitDisabled || e.HugeSurplusPages != 0 || len(e.HugePools) != len(limits.HugePools) {
		return fmt.Errorf("held repair requires monotonic limits and an unchanged constrained domain")
	}
	if err := e.matchesTasks(limits.PidsMaxTasks); err != nil {
		return err
	}
	if err := e.systemPlacementMatches(limits.SystemMemoryMaxBytes); err != nil {
		return err
	}
	total, err := limits.totalUpper()
	if err != nil || total > uint64(e.GuestMemTotalBytes) {
		return fmt.Errorf("original held upper no longer fits actual Guest")
	}
	var actualPool control.Uint64
	for i, actual := range e.HugePools {
		target := limits.HugePools[i]
		if actual.PageSizeKB != target.PageSizeKB || actual.Pages > target.Pages {
			return fmt.Errorf("held repair cannot lower or replace an actual HugeTLB hstate")
		}
		// target已由完整Limits.Validate/totalUpper防溢出；actual逐项不
		// 大于target，因此这里的乘法和累加也被同一个总上界约束。
		actualPool += actual.PageSizeKB * 1024 * actual.Pages
	}
	if actualPool != e.WholePoolUpperBytes {
		return fmt.Errorf("held repair whole pool accounting is incomplete")
	}
	return nil
}

// RepairHeld是显式、独立的原约束恢复入口。锁序仍为原资源文件锁→原日志/
// floor/出生→实际内核→原日志fsync→写→完整读回。普通Apply/Replay/轮询不
// 调用它。目标从原日志选择，完全不采纳请求中的新额度；新floor只授权本次
// 受限修复，不把老命令改成新epoch后重放一般资源动作。
//
// 修复全过程保持同一个已确认Held上界，没有释放/重分配或新逻辑目标。
// 写前再次fsync完整原约束，部分失败仍保留它；Applied记录原操作曾完成，
// 不能单独证明本次恢复成功。只有当前实际全量匹配的Evidence才可返回成功，
// 失败/丢回复后先ObserveHeldRepair，不自动重发；缺/坏日志永不在此重建。
func (g *Guard) RepairHeld(ctx context.Context, request HeldRepairRequest) (Evidence, error) {
	var evidence Evidence
	if err := request.Validate(); err != nil {
		return evidence, err
	}
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Identity != request.Identity || r.Floor != request.IssuerEpoch || r.Bootstrap == nil {
			return fmt.Errorf("original held repair is fenced or lacks bootstrap")
		}
		var sourceEvidence Evidence
		sourceEvidence.ProtocolVersion, sourceEvidence.Identity = 1, r.Identity
		v := View{ProtocolVersion: 1, VMShrinkPermit: false, Identity: r.Identity, IssuerFloorEpoch: r.Floor, Bootstrap: r.Bootstrap.Original, BootstrapApplied: r.Bootstrap.Applied, SystemPlacementObserved: r.Bootstrap.SystemPlacementObserved, Original: r.Original, Held: r.Held, Applied: r.Applied, Birth: r.Birth, Actual: sourceEvidence}
		digest, err := HeldSourceSHA256(v)
		if err != nil || digest != request.SourceSHA256 {
			return fmt.Errorf("original held repair source changed or is unconfirmed")
		}
		if r.Birth != nil {
			if err := g.kernel.VerifyProcess(*r.Birth); err != nil {
				return err
			}
		}
		limits := heldLimits(v)
		read := func() (Evidence, error) {
			if r.Held != nil {
				return g.kernel.Read(*r.Held)
			}
			actual, err := g.kernel.ReadBounds(r.Identity, limits)
			if err == nil && r.Birth != nil {
				err = g.kernel.VerifyProcess(*r.Birth)
			}
			return actual, err
		}
		evidence, err = read()
		if err != nil {
			return err
		}
		if r.Birth == nil && (evidence.CgroupPopulated || evidence.TaskLimit != nil && evidence.TaskLimit.CurrentTasks != 0) {
			return fmt.Errorf("held bootstrap repair cannot adopt an unrecorded process")
		}
		if err := repairPreflight(evidence, r.Identity, limits); err != nil {
			return err
		}
		if evidence.matchesLimits(r.Identity, limits) == nil {
			return nil // 已恢复只读，不因显式重复选择再次写同一组文件。
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.save(r); err != nil {
			return fmt.Errorf("%w: held repair persistence: %w", ErrUnknown, err)
		}
		if err := g.kernel.ApplyBounds(limits); err != nil {
			return fmt.Errorf("%w: held repair may have applied: %w", ErrUnknown, err)
		}
		evidence, err = read()
		if err != nil {
			return fmt.Errorf("%w: held repair readback: %w", ErrUnknown, err)
		}
		if err := evidence.matchesLimits(r.Identity, limits); err != nil {
			return fmt.Errorf("%w: held repair incomplete: %w", ErrUnknown, err)
		}
		return evidence.systemPlacementMatches(limits.SystemMemoryMaxBytes)
	})
	if err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}
