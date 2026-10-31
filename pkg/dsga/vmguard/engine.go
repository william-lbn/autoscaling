package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

var (
	ErrUnknown = errors.New("Runner memory action requires original readback; possible QMP send cannot be replayed")
	ErrNotSent = errors.New("Runner original action was durably prepared but not sent; explicit original replay required")
)

// nativeMemory只有本包私有Unix QMP适配器和测试夹具实现，不能向HTTP开放通用
// command/路径，也不能让调用者提供“执行成功”回调。Read是原QEMU的完整读回；
// Set只尝试一次，不得自行重连重发，超时前必须结束本地socket I/O才释放文件锁。
type nativeMemory interface {
	Read(context.Context) (Observation, error)
	Set(context.Context, control.Uint64) error
}

// authorization仅在本包固定Kubernetes读者完整复核原VM/Lease/Step和预算后形成。
// 它不是公开票据：CommandSHA/JournalSHA相等只能绑定原文，不能认证预算来源。
// 读回旧未知只需当前owner；缺少新派发资格仍允许提高floor并只读观察旧目标。
type authorization struct {
	Owner         journal.Owner
	LeaseUID      string
	Command       *Command
	JournalSHA256 string
	Dispatch      bool
	ReplayNonce   string
	ReplayReceipt budget.ObjectRef
	Denied        error
	// Abort只来自固定Kube读者确认的同一原VM/步骤取消，不是普通Denied。
	Abort *cancellationAuthority
}

type authority func(context.Context) (authorization, error)

type engine struct {
	dir         string
	id          Identity
	binding     budget.ObjectRef
	native      nativeMemory
	read        authority
	now         func() time.Time
	initialMode *InitialBootstrapMode
	initialGate func(*lockedState) error
}

func (e *engine) withRecord(ctx context.Context, fn func(*record, *lockedState) error) error {
	return e.withRecordState(ctx, false, fn)
}

// 原子进程退出文件是与原WAL同锁的永久关闭屏障。普通派发/读回/发布都先核
// 它，不能在QEMU已退出后继续使用旧QMP publication。唯一allowStopped调用
// 只允许补发已经持久的退出事实，不改变Pending/Last或把未知动作伪造为完成。
func (e *engine) withRecordState(ctx context.Context, allowStopped bool, fn func(*record, *lockedState) error) error {
	if e.id.Validate() != nil || e.binding.Validate() != nil || e.native == nil || e.read == nil || e.now == nil {
		return fmt.Errorf("Runner requires original binding, private native adapter and fixed authority")
	}
	s, err := lockState(ctx, e.dir)
	if err != nil {
		return err
	}
	defer s.close()
	r, err := s.load()
	if err != nil {
		return err
	}
	if r == nil || r.Identity != e.id || r.BindingClaim != e.binding {
		return fmt.Errorf("Runner original journal is missing or belongs to another birth; automatic reset forbidden")
	}
	if raw, err := s.readProcessFile("birth-abort.json"); err != nil {
		return err
	} else if raw != nil {
		var aborted failedBirth
		if json.Unmarshal(raw, &aborted) != nil || !initialVersionMatches(aborted.ProtocolVersion, aborted.InitialMode, e.id) || !sameInitialMode(aborted.InitialMode, e.initialMode) || aborted.Identity != e.id || aborted.Owner != r.Floor || r.Sequence != 1 || r.HighWater != nil || r.Pending != nil || r.Last != nil || r.LastPublished != nil {
			return fmt.Errorf("original failed birth barrier differs")
		}
		// 出生失败后的来源补齐只服务于NoStart关闭，不能借获得完整
		// state/binding对象重新取QEMU启动权。只允许原关闭事实补发。
		if !allowStopped {
			return ErrStopped
		}
	}
	raw, err := s.readProcessFile("process-exit.json")
	if err != nil {
		return err
	}
	if raw != nil {
		var exited ProcessExit
		body, err := json.Marshal(r)
		if err != nil || json.Unmarshal(raw, &exited) != nil || exited.Identity != e.id || exited.BirthClaim != e.binding || exited.RecordSequence != r.Sequence || exited.RecordSHA256 != SourceDigest(body) {
			return fmt.Errorf("original process exit file changed identity or retained WAL")
		}
		if !allowStopped {
			return ErrStopped
		}
	}
	return fn(r, s)
}

func saveNext(s *lockedState, r *record) error {
	if r.Sequence == control.Uint64(math.MaxUint64) {
		return fmt.Errorf("Runner journal sequence exhausted")
	}
	r.Sequence++
	return s.save(r)
}

func (e *engine) observe(ctx context.Context, r *record, s *lockedState) error {
	i := r.Pending
	if i == nil {
		return nil
	}
	actual, err := e.native.Read(ctx)
	if err != nil {
		return err
	}
	if err := actual.validate(e.id); err != nil {
		return err
	}
	kind := completed
	target := i.Command.Request.TargetVMMemoryBytes
	if i.Stage == prepared && i.Command.BeforeBytes != target {
		// prepared从未越过may_have_sent写前屏障。本次推进绝不自动发它；将
		// 真实读回保留为not_sent，允许外层显式选择原命令重放或取消结清。
		kind = notSent
	} else if actual.RequestedBytes != target || actual.ActualBytes != target {
		return ErrUnknown
	}
	r.Last = &receipt{Intent: *i, Kind: kind, Actual: actual, Cancellation: nil}
	r.LastPublished = nil
	r.Pending = nil
	if err := saveNext(s, r); err != nil {
		return err
	}
	if kind == notSent {
		return ErrNotSent
	}
	return nil
}

func (e *engine) advance(ctx context.Context) error {
	return e.withRecord(ctx, func(r *record, s *lockedState) error {
		if e.initialGate != nil {
			if err := e.initialGate(s); err != nil {
				return err
			}
		}
		a, err := e.read(ctx)
		if err != nil {
			return err
		}
		if a.Owner.Validate() != nil || control.ValidateID(a.LeaseUID, "Lease UID") != nil || (r.LeaseUID != "" && r.LeaseUID != a.LeaseUID) || a.Owner.Epoch < r.Floor.Epoch || (a.Owner.Epoch == r.Floor.Epoch && a.Owner != r.Floor) {
			return fmt.Errorf("stale or conflicting Runner owner")
		}
		if a.Owner != r.Floor || r.LeaseUID == "" {
			r.Floor = a.Owner
			r.LeaseUID = a.LeaseUID
			if err := saveNext(s, r); err != nil {
				return err
			}
		}
		if handled, err := e.cancelBeforeSend(ctx, r, s, a); handled {
			return err
		}
		// 新owner的floor持久后，旧pending仍先于一切新动作。接管/取消/租约
		// 过期不能清除它，也不能把旧request改成新epoch后再执行。
		if r.Pending != nil {
			return e.observe(ctx, r, s)
		}
		if a.Command == nil {
			return a.Denied
		}
		c := *a.Command
		hash, err := c.digest()
		if err != nil || c.Identity != e.id || c.BindingClaim != e.binding || !digestValid(a.JournalSHA256) {
			return fmt.Errorf("Runner source command or original journal binding differs")
		}
		key := c.Request.Key
		if r.HighWater != nil {
			if key.Instance != r.HighWater.Instance {
				return fmt.Errorf("Runner cannot adopt another PG boot while retaining original effects")
			}
			order := compareKey(key, *r.HighWater)
			if order < 0 {
				return fmt.Errorf("Runner request is below retained high water")
			}
			if order == 0 {
				last := r.Last
				if key != *r.HighWater || last == nil || last.Intent.Command != c || last.Intent.CommandSHA256 != hash || last.Intent.JournalSHA256 != a.JournalSHA256 {
					return fmt.Errorf("same Runner key changed its full original command")
				}
				if last.Kind == completed || last.Kind == cancelledBeforeSend {
					return nil
				}
				if a.ReplayNonce == "" {
					return ErrNotSent
				}
				// 选择只能消费这一次原not_sent回执。发布滞后、旧选择回灌
				// 或再一次prepared失败产生了新not_sent时，不得只靠“nonce
				// 与上次不同”轮换重放；必须再次有明确选择和原真实UID。
				if r.LastPublished == nil || a.ReplayReceipt != *r.LastPublished {
					return ErrUnknown
				}
				if control.ValidateIncarnation(a.ReplayNonce) != nil || a.ReplayNonce == last.Intent.ReplayNonce {
					return fmt.Errorf("original replay requires a fresh explicit 128-bit nonce")
				}
			} else if a.ReplayNonce != "" {
				return fmt.Errorf("replay cannot create a different Runner command")
			}
			if order > 0 && r.Last != nil && r.LastPublished == nil {
				return fmt.Errorf("original Runner result must be durably published before another command")
			}
		} else if a.ReplayNonce != "" {
			return fmt.Errorf("replay has no retained original Runner command")
		}
		if !a.Dispatch || a.Owner.Epoch != c.Request.Key.IssuerEpoch || c.Request.DeadlineUnixMillis <= control.Uint64(e.now().UnixMilli()) {
			return errors.Join(fmt.Errorf("Runner original command is not currently dispatchable"), a.Denied)
		}
		before, err := e.native.Read(ctx)
		if err != nil {
			return err
		}
		if before.validate(e.id) != nil || before.RequestedBytes != c.BeforeBytes || before.ActualBytes != c.BeforeBytes {
			return fmt.Errorf("Runner actual baseline differs from original command")
		}
		r.HighWater = &key
		r.Pending = &intent{Command: c, CommandSHA256: hash, JournalSHA256: a.JournalSHA256, Owner: a.Owner, Stage: prepared, ReplayNonce: a.ReplayNonce, Before: before}
		if err := saveNext(s, r); err != nil {
			return err
		}
		// 完整原意图已经持久。再读真实来源，确保前一轮预算/owner在写前未被
		// 更换；失败只留下prepared，不把失败变成自动重试或删除旧WAL。
		latest, err := e.read(ctx)
		if err != nil {
			return err
		}
		if latest.Command == nil || *latest.Command != c || latest.Owner != a.Owner || latest.LeaseUID != a.LeaseUID || latest.JournalSHA256 != a.JournalSHA256 || latest.ReplayNonce != a.ReplayNonce || latest.ReplayReceipt != a.ReplayReceipt || !latest.Dispatch || ctx.Err() != nil || c.Request.DeadlineUnixMillis <= control.Uint64(e.now().UnixMilli()) {
			return fmt.Errorf("Runner authority changed after original preparation")
		}
		if c.BeforeBytes == c.Request.TargetVMMemoryBytes {
			// no-op只能读回，绝不发迟到后可能覆盖后续目标的qom-set。
			return e.observe(ctx, r, s)
		}
		r.Pending.Stage = mayHaveSent
		if err := saveNext(s, r); err != nil {
			return err
		}
		if err := e.native.Set(ctx, c.Request.TargetVMMemoryBytes); err != nil {
			var absent *notSentError
			if errors.As(err, &absent) {
				// 只有私有适配器能产生此类型，且仅在开始写qom-set之前。
				// 不把零值、普通timeout、QMP错误或请求取消解释成未发送。
				r.Pending.Stage = prepared
				if saveErr := saveNext(s, r); saveErr != nil {
					return saveErr
				}
				return errors.Join(err, e.observe(ctx, r, s))
			}
			return errors.Join(ErrUnknown, err)
		}
		return e.observe(ctx, r, s)
	})
}
