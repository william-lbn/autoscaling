//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalSystemRoleJournalScope = "original-system-role-exec-journal-v1"

type normalSystemRoleJournalEntry struct {
	ProtocolVersion          uint32                              `json:"protocol_version"`
	Scope                    string                              `json:"scope"`
	Nonce                    string                              `json:"nonce"`
	Stage                    string                              `json:"stage"`
	PreviousSHA256           string                              `json:"previous_sha256,omitempty"`
	Role                     string                              `json:"role"`
	PlanSHA256               string                              `json:"plan_sha256"`
	Binding                  normalSystemLifecycleBinding        `json:"binding"`
	Namespace                []normalSystemRoleNamespaceIdentity `json:"namespace"`
	ResourceIdentity         Identity                            `json:"resource_identity"`
	ResourceFloor            control.Uint64                      `json:"resource_floor"`
	StateDevice              uint64                              `json:"state_device"`
	StateInode               uint64                              `json:"state_inode"`
	Task                     *normalSystemRoleObservation        `json:"task,omitempty"`
	OriginalTaskExitObserved bool                                `json:"original_task_exit_observed,omitempty"`
}
type normalSystemRoleJournal struct {
	v2       *normalSystemRoleV2Publication
	state    *lockedState
	pending  normalSystemRoleJournalEntry
	previous string
	stopped  bool
	seen     map[string]normalSystemRoleFileWitness
}
type normalSystemRoleBrokerComponent struct {
	mu         sync.Mutex
	guard      *Guard
	source     *normalSystemLifecycleSource
	namespaces *normalSystemRoleNamespaceSource
	runs       map[string]*normalSystemRoleOriginalRun
	byRole     map[string]*normalSystemRoleOriginalRun
	closed     bool
}
type normalSystemRoleOriginalRun struct {
	broker            *normalSystemRoleBrokerComponent
	child             *normalSystemRoleOriginalChild
	pending           normalSystemRoleJournalEntry
	previous          string
	terminal, unknown bool
	seen              map[string]normalSystemRoleFileWitness
}

// 每个阶段在成功file+dir fsync之后，另钉住实际读回的inode/ctime/字节摘要。
// 原文件被替换或可见但未持久的文件不能借同字节路径复用私有完成见证。
type normalSystemRoleFileWitness struct {
	Device, Inode uint64
	Size          int64
	CTime         unix.Timespec
	SHA256        string
}

func readNormalSystemRoleFileWitness(state *lockedState, name string) (normalSystemRoleFileWitness, error) {
	var zero normalSystemRoleFileWitness
	fd, e := unix.Openat(int(state.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return zero, e
	}
	f := os.NewFile(uintptr(fd), "original-role-durable-stage")
	defer f.Close()
	var before, after unix.Stat_t
	if unix.Fstat(fd, &before) != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Uid != 0 || before.Mode&0077 != 0 || before.Nlink != 1 || before.Size <= 0 || before.Size > 64<<10 {
		return zero, fmt.Errorf("original role stage file differs")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if e != nil || len(raw) > 64<<10 || unix.Fstat(fd, &after) != nil || before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Ctim != after.Ctim || before.Mtim != after.Mtim || before.Mode != after.Mode || before.Uid != after.Uid || before.Gid != after.Gid || before.Nlink != after.Nlink {
		return zero, fmt.Errorf("original role stage changed while reading")
	}
	return normalSystemRoleFileWitness{Device: after.Dev, Inode: after.Ino, Size: after.Size, CTime: after.Ctim, SHA256: physicalDigest(raw)}, nil
}
func newNormalSystemRoleBrokerComponent(g *Guard, s *normalSystemLifecycleSource, n *normalSystemRoleNamespaceSource) (*normalSystemRoleBrokerComponent, error) {
	if g == nil || s == nil || n == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original role broker sources missing"))
	}
	if e := normalSystemRoleCaller(); e != nil {
		return nil, e
	}
	if e := n.verify(-1); e != nil {
		return nil, e
	}
	return &normalSystemRoleBrokerComponent{guard: g, source: s, namespaces: n, runs: map[string]*normalSystemRoleOriginalRun{}, byRole: map[string]*normalSystemRoleOriginalRun{}}, nil
}
func normalSystemRoleStageName(nonce, stage string) string {
	return "system-role-" + nonce + "-" + stage + ".json"
}
func (j *normalSystemRoleJournal) append(ctx context.Context, stage string, task *normalSystemRoleObservation, exited bool) error {
	if j == nil || j.state == nil || ctx == nil || ctx.Err() != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original role durable stage unavailable"))
	}
	next := j.pending
	next.Stage = stage
	next.PreviousSHA256 = j.previous
	next.Task = task
	next.OriginalTaskExitObserved = exited
	raw, e := json.Marshal(next)
	if e != nil {
		return e
	}
	// O_EXCL、文件fsync和目录fsync都由原physical writer完成。失败即使文件可见
	// 也不更新本Root私有见证；无ordinary重读文件后猜已持久的恢复路径。
	if e = j.state.createPhysicalFile(normalSystemRoleStageName(next.Nonce, stage), raw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	w, e := readNormalSystemRoleFileWitness(j.state, normalSystemRoleStageName(next.Nonce, stage))
	if e != nil || w.SHA256 != physicalDigest(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("original role durable file readback differs"))
	}
	if j.seen == nil {
		j.seen = map[string]normalSystemRoleFileWitness{}
	}
	j.seen[normalSystemRoleStageName(next.Nonce, stage)] = w
	j.previous = physicalDigest(raw)
	return nil
}
func (j *normalSystemRoleJournal) persistOriginalSystemRoleStopped(ctx context.Context, o normalSystemRoleObservation, ns []normalSystemRoleNamespaceIdentity, plan string) error {
	if j == nil || j.stopped || o.Binding != j.pending.Binding || o.Role != j.pending.Role || plan != j.pending.PlanSHA256 || !o.OriginalTaskObserved || !o.FixedArgvEnvironmentObserved {
		return errors.Join(ErrUnknown, fmt.Errorf("original stopped role binding differs"))
	}
	a, _ := json.Marshal(ns)
	b, _ := json.Marshal(j.pending.Namespace)
	if string(a) != string(b) {
		return fmt.Errorf("original stopped role namespace differs")
	}
	if e := j.append(ctx, "exec-stopped", &o, false); e != nil {
		return e
	}
	if e := j.append(ctx, "released", &o, false); e != nil {
		return e
	}
	j.stopped = true
	return nil
}
func (b *normalSystemRoleBrokerComponent) rejectUnknownHistoryLocked(state *lockedState) error {
	fd, e := unix.Openat(int(state.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	scan := os.NewFile(uintptr(fd), "original-System-role-history")
	defer scan.Close()
	names, e := scan.Readdirnames(4097)
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if len(names) > 4096 {
		return errors.Join(ErrUnknown, fmt.Errorf("original role history exceeds bounded inventory"))
	}
	for _, name := range names {
		if !strings.HasPrefix(name, "system-role-") {
			continue
		}
		parts := strings.TrimPrefix(name, "system-role-")
		if len(parts) < 65 {
			return errors.Join(ErrUnknown, fmt.Errorf("unknown System role history"))
		}
		nonce := parts[:64]
		stage := parts[64:]
		if _, e := nativeBirthDigest(nonce); e != nil {
			return ErrUnknown
		}
		if stage != "-pending.json" && stage != "-exec-stopped.json" && stage != "-released.json" && stage != "-exited.json" {
			return errors.Join(ErrUnknown, fmt.Errorf("unknown System role stage"))
		}
		run := b.runs[nonce]
		if run == nil || run.unknown || run.pending.Binding != b.source.binding {
			return errors.Join(ErrUnknown, fmt.Errorf("role history lacks original current broker witness"))
		}
		// 同Root已经完成的真正fsync见证只覆盖原task；它不宣称其fork出来的任意
		// 后代都退出，也不替代后继固定服务/配置调用图的持续writer来源。
		wanted, ok := run.seen[name]
		if !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("role stage lacks original durable file witness"))
		}
		got, e := readNormalSystemRoleFileWitness(state, name)
		if e != nil || got != wanted {
			return errors.Join(ErrUnknown, e, fmt.Errorf("original role durable file identity differs"))
		}
		if run.terminal {
			continue
		}
		if run.child == nil {
			return errors.Join(ErrUnknown, fmt.Errorf("original role spawn incomplete"))
		}
		if e := arrayRegistryPIDFDAlive(int(run.child.pidfd.Fd())); e != nil {
			return errors.Join(ErrUnknown, e)
		}
	}
	for _, run := range b.runs {
		if run.unknown {
			return errors.Join(ErrUnknown, fmt.Errorf("original role history incomplete"))
		}
		for name, wanted := range run.seen {
			got, e := readNormalSystemRoleFileWitness(state, name)
			if e != nil || got != wanted {
				return errors.Join(ErrUnknown, e, fmt.Errorf("original role durable prefix missing or replaced"))
			}
		}
	}
	return nil
}

// 原resource flock从intent起一直覆盖原exec-stop与released fsync/真实detach。
// 不改共享state.json/Held；未完成/Root重启只Unknown，不能新造nonce补签旧role。
func (b *normalSystemRoleBrokerComponent) spawn(ctx context.Context, role string) (*normalSystemRoleOriginalRun, error) {
	if b == nil {
		return nil, ErrUnknown
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrUnknown
	}
	if old := b.byRole[role]; old != nil && !old.terminal {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original role already active or unknown"))
	}
	plan, e := normalSystemRoleGateLaunchPlan(ctx, b.source, role)
	if e != nil {
		return nil, e
	}
	planSHA, e := plan.referenceSHA256()
	if e != nil {
		return nil, e
	}
	ns, e := b.namespaces.snapshot()
	if e != nil {
		return nil, e
	}
	var run *normalSystemRoleOriginalRun
	e = b.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || r.Original != nil && !r.Applied {
			return errors.Join(ErrUnknown, fmt.Errorf("original resource stage cannot start a System role"))
		}
		source, e := sealedRuntimeSource()
		if e != nil {
			return e
		}
		if e = sameBootstrapSource(source, r.Bootstrap.Original); e != nil {
			return e
		}
		if e = b.rejectUnknownHistoryLocked(state); e != nil {
			return e
		}
		nonce, e := newNativeBirthNonce()
		if e != nil {
			return e
		}
		var st unix.Stat_t
		if e = unix.Fstat(int(state.dir.Fd()), &st); e != nil {
			return e
		}
		pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: role, PlanSHA256: planSHA, Binding: plan.binding, Namespace: ns, ResourceIdentity: r.Identity, ResourceFloor: r.Floor, StateDevice: st.Dev, StateInode: st.Ino}
		j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
		if plan.roleSourceProtocol == 2 {
			runtime, e := originalNormalSystemRoleV2Runtime(b)
			if e != nil {
				return e
			}
			j.v2, e = runtime.prepareOriginalRoleLocked(ctx, state, r, plan, j)
			if e != nil {
				return e
			}
		}
		run = &normalSystemRoleOriginalRun{broker: b, pending: pending, unknown: true, seen: j.seen}
		b.runs[nonce] = run
		b.byRole[role] = run
		if e = j.append(ctx, "pending", nil, false); e != nil {
			return e
		}
		child, e := spawnNormalSystemRoleOriginal(ctx, b.source, b.namespaces, role, j)
		run.previous = j.previous
		if e != nil {
			return e
		}
		run.child = child
		run.unknown = false
		return nil
	})
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	return run, nil
}

// 单一Wait/cancel所有者；真正原task退出后再取原flock持久exited。不以锁释放、
// pid消失或counter变小推断物理free/全部后代退出；全Source仍需其它生产者。
func (r *normalSystemRoleOriginalRun) finish(ctx context.Context, cancel bool) error {
	if r == nil || r.broker == nil {
		return ErrUnknown
	}
	b := r.broker
	b.mu.Lock()
	if r.terminal || r.unknown || r.child == nil {
		b.mu.Unlock()
		return ErrUnknown
	}
	r.unknown = true
	b.mu.Unlock()
	var exitErr error
	if cancel {
		exitErr = r.child.cancelAndWait()
	} else {
		exitErr = r.child.wait()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !r.child.waited {
		return errors.Join(ErrUnknown, exitErr)
	}
	e := b.guard.withRecord(ctx, func(rec *record, state *lockedState) error {
		if rec.Identity != r.pending.ResourceIdentity {
			return errors.Join(ErrUnknown, fmt.Errorf("original role resource identity changed"))
		}
		var st unix.Stat_t
		if unix.Fstat(int(state.dir.Fd()), &st) != nil || st.Dev != r.pending.StateDevice || st.Ino != r.pending.StateInode {
			return ErrUnknown
		}
		j := normalSystemRoleJournal{state: state, pending: r.pending, previous: r.previous, seen: r.seen}
		if e := j.append(ctx, "exited", &r.child.observed.Task, true); e != nil {
			return e
		}
		if r.child.installedMain != nil {
			if e := r.child.installedMain.publication.completeOriginalWaitLocked(ctx, state, rec); e != nil {
				return e
			}
			if e := r.child.installedMain.stdio.completeOriginalWaitLocked(ctx, state); e != nil {
				return e
			}
		}
		r.previous = j.previous
		r.terminal = true
		r.unknown = false
		return nil
	})
	return errors.Join(exitErr, e)
}
