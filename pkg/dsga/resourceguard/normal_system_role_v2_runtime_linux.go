//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"sync"
)

// 这里只保存本R2原Guard/typedFS/真实四或五FD ExecProof，不提供从JSON/路径恢复入口。
// 创建工厂可早于COMMIT；prepare角色和租约发布必须同原resource锁且已COMMIT。
type normalSystemRoleV2Runtime struct {
	authority   *normalSystemResourceAuthority
	broker      *normalSystemRoleBrokerComponent
	stdio       *normalSystemRoleStdioFactory
	stock       *normalOrdinaryUdevV2
	actions     *normalOrdinaryActionsV2
	closeLayout func()
	closed      bool
}

var normalSystemRoleV2Runtimes = struct {
	sync.Mutex
	byBroker map[*normalSystemRoleBrokerComponent]*normalSystemRoleV2Runtime
}{byBroker: map[*normalSystemRoleBrokerComponent]*normalSystemRoleV2Runtime{}}

func installNormalSystemRoleV2BrokerRuntime(ctx context.Context, g *Guard, system *normalSystemLifecycleSource, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof, b *normalSystemRoleBrokerComponent) (func(), error) {
	version, e := normalSystemRoleOriginalControlProtocol(ctx, system)
	if e != nil {
		return nil, e
	}
	if version == 1 {
		return func() {}, nil
	}
	if version != 2 || g == nil || b == nil || b.guard != g || b.source != system || filesystem == nil || proof == nil {
		return nil, ErrUnknown
	}
	a := lookupNormalSystemAuthority(g)
	if a == nil || a.source != system || a.declaration == nil || a.declaration.declaration.ProtocolVersion != 3 {
		return nil, ErrUnknown
	}
	f, e := newNormalSystemRoleStdioFactory(ctx, g, filesystem, proof)
	if e != nil {
		return nil, e
	}
	var stock *normalOrdinaryUdevV2
	var actions *normalOrdinaryActionsV2
	var closeLayout func()
	// 只原RO System3明确选stock的controls2进入；旧ordinary1及无stock组件2原样。
	if normalSystemStockUdevSelected(system.manifest) {
		closeLayout, e = installNormalSystemOrdinaryV2DirectoryLayout(ctx, g, system, filesystem, proof)
		if e != nil {
			f.close()
			return nil, e
		}
		e = g.withRecord(ctx, func(rec *record, state *lockedState) error {
			var err error
			stock, err = g.prepareNormalOrdinaryUdevV2Locked(ctx, state, rec, filesystem, proof, a)
			if err != nil {
				return err
			}
			actions, err = g.prepareNormalOrdinaryActionsV2Locked(ctx, state, rec, filesystem, proof, a)
			if err != nil {
				stock.close()
			}
			return err
		})
		if e != nil {
			closeLayout()
			f.close()
			return nil, errors.Join(ErrUnknown, e)
		}
	}
	v := &normalSystemRoleV2Runtime{authority: a, broker: b, stdio: f, stock: stock, actions: actions, closeLayout: closeLayout}
	normalSystemRoleV2Runtimes.Lock()
	if normalSystemRoleV2Runtimes.byBroker[b] != nil || len(normalSystemRoleV2Runtimes.byBroker) >= 64 {
		normalSystemRoleV2Runtimes.Unlock()
		if actions != nil {
			actions.close()
		}
		if stock != nil {
			stock.close()
		}
		if closeLayout != nil {
			closeLayout()
		}
		f.close()
		return nil, ErrUnknown
	}
	normalSystemRoleV2Runtimes.byBroker[b] = v
	normalSystemRoleV2Runtimes.Unlock()
	watchCtx, stopWatch := context.WithCancel(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		var stockDone, actionDone <-chan error
		if stock != nil {
			stockDone = stock.done
		}
		if actions != nil {
			actionDone = actions.done
		}
		select {
		case <-watchCtx.Done():
			return
		case <-stockDone:
			a.admission.fail()
		case <-actionDone:
			a.admission.fail()
		}
	}()
	var closeOnce sync.Once
	return func() {
		closeOnce.Do(func() {
			stopWatch()
			<-watchDone
			normalSystemRoleV2Runtimes.Lock()
			v.closed = true
			normalSystemRoleV2Runtimes.Unlock()
			if actions != nil {
				actions.close()
			}
			if stock != nil {
				stock.close()
			}
			if closeLayout != nil {
				closeLayout()
			}
			f.close()
		})
	}, nil
}
func originalNormalSystemRoleV2Runtime(b *normalSystemRoleBrokerComponent) (*normalSystemRoleV2Runtime, error) {
	normalSystemRoleV2Runtimes.Lock()
	defer normalSystemRoleV2Runtimes.Unlock()
	v := normalSystemRoleV2Runtimes.byBroker[b]
	if v == nil || v.closed || v.broker != b || v.authority == nil || v.authority.guard != b.guard || v.stdio == nil {
		return nil, ErrUnknown
	}
	return v, nil
}

type normalSystemRoleV2Publication struct {
	checkpoint func(string, *normalSystemRoleV2Publication) error // 仅原私有DEV ctx，生产nil。
	runtime    *normalSystemRoleV2Runtime
	journal    *normalSystemRoleJournal
	record     *record
	endpoints  *normalSystemRoleStdio
	installed  *normalSystemRoleInstalledMain
	candidate  *normalSystemRoleLeaseCandidate
	compute    *normalSystemRoleV2ComputePublication
}

func (v *normalSystemRoleV2Runtime) prepareOriginalRoleLocked(ctx context.Context, state *lockedState, rec *record, plan normalSystemRoleLaunchSpec, j *normalSystemRoleJournal) (*normalSystemRoleV2Publication, error) {
	if v == nil || j == nil || j.state != state || j.pending.PlanSHA256 == "" || plan.roleSourceProtocol != 2 || v.authority == nil || v.authority.source != v.broker.source {
		return nil, ErrUnknown
	}
	if normalSystemStockUdevSelected(v.authority.source.manifest) {
		if v.stock == nil {
			return nil, ErrUnknown
		}
		if e := requireNormalSystemOrdinaryV2DirectoryLayoutLocked(ctx, v.authority, state, rec); e != nil {
			return nil, e
		}
		if e := v.stock.checkLocked(ctx, state, rec); e != nil {
			return nil, e
		}
		if plan.role == "compute" {
			// broker.mu→resource已持有；只fresh核，不在事务内等待。
			if e := v.stock.computeStartupLocked(ctx, state, rec, v.broker); e != nil {
				return nil, errors.Join(ErrUnknown, e)
			}
		}
	}
	endpoints, e := v.stdio.prepareOriginalLocked(ctx, state, rec, plan.role)
	if e != nil {
		return nil, e
	}
	p := &normalSystemRoleV2Publication{runtime: v, journal: j, record: rec, endpoints: endpoints}
	p.checkpoint, _ = ctx.Value(normalSystemRoleV2CheckpointKey{}).(func(string, *normalSystemRoleV2Publication) error)
	return p, nil
}
func (p *normalSystemRoleV2Publication) spawnFiles() ([3]*os.File, error) {
	if p == nil || p.endpoints == nil || p.endpoints.objects == nil {
		return [3]*os.File{}, ErrUnknown
	}
	return p.endpoints.objects.spawnFiles()
}

// 此接口只有真实第二exec-stop生产者实现。普通2为完整BPF/cap/NNP+原Gate安装序；
// Compute保NNP0，其证据仅固定非Root原exec身份，不能借此宣称已安装普通控制。
type normalSystemRoleV2ControlWitness interface {
	recheckOriginalRoleControlHistory(context.Context) error
	recheckOriginalRoleControlLive(context.Context) error
	close()
}

func (p *normalSystemRoleV2Publication) prepareOriginalStopped(ctx context.Context, control *nativeTraceOriginalControl, task *normalSystemOriginalTask, child *normalSystemRoleOriginalChild, plan normalSystemRoleLaunchSpec, tid int) error {
	if p == nil || p.runtime == nil || p.journal == nil || p.record == nil || !p.journal.stopped || child == nil || child.task != task {
		return ErrUnknown
	}
	j := p.journal
	a := p.runtime.authority
	if normalSystemStockUdevSelected(a.source.manifest) {
		if p.runtime.stock == nil {
			return ErrUnknown
		}
		if e := requireNormalSystemOrdinaryV2DirectoryLayoutLocked(ctx, a, j.state, p.record); e != nil {
			return e
		}
		if e := p.runtime.stock.checkLocked(ctx, j.state, p.record); e != nil {
			return e
		}
	}
	controls, e := inspectNormalSystemRoleV2ControlsAtStop(ctx, a.source, task, plan, control, tid)
	if e != nil {
		return e
	}
	stdio, e := p.endpoints.inspectOriginalStoppedTargetLocked(ctx, j.state, p.record, control, task)
	if e != nil {
		controls.close()
		return e
	}
	original, e := duplicateNormalSystemOriginalTask(task)
	if e != nil {
		controls.close()
		stdio.close()
		return e
	}
	creator, e := child.pinOriginalCreatorForLease()
	if e != nil {
		original.close()
		controls.close()
		stdio.close()
		return e
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(j.state.dir.Fd()), &dir) != nil || unix.Fstat(int(j.state.lock.Fd()), &lock) != nil {
		creator.close()
		original.close()
		controls.close()
		stdio.close()
		return ErrUnknown
	}
	var prefixes [3]normalSystemRoleFileWitness
	for n, stage := range []string{"pending", "exec-stopped", "released"} {
		expected, ok := j.seen[normalSystemRoleStageName(j.pending.Nonce, stage)]
		actual, e := readNormalSystemRoleFileWitness(j.state, normalSystemRoleStageName(j.pending.Nonce, stage))
		if !ok || e != nil || actual != expected {
			creator.close()
			original.close()
			controls.close()
			stdio.close()
			return errors.Join(ErrUnknown, e)
		}
		prefixes[n] = actual
	}
	sha, e := a.source.referenceSHA256()
	if e != nil {
		creator.close()
		original.close()
		controls.close()
		stdio.close()
		return e
	}
	i := &normalSystemRoleInstalledMain{phase: normalSystemRoleInstalledStopped, g: a.guard, system: a.source, sourceSHA: sha, role: plan.role, nonce: j.pending.Nonce, binding: plan.binding, identity: p.record.Identity, floor: p.record.Floor, stateDevice: dir.Dev, stateInode: dir.Ino, lockDevice: lock.Dev, lockInode: lock.Ino, prefixes: prefixes, child: child, task: original, controls: controls, stdio: stdio, creator: creator}
	p.installed = i
	i.publication = p
	child.installedMain = i
	p.endpoints.factory.mu.Lock()
	p.endpoints.installed = i
	p.endpoints.factory.mu.Unlock()
	candidate, e := a.prepareOriginalRoleMainLeaseLocked(ctx, j.state, p.record, i)
	if e != nil {
		i.markOriginalRoleUnknown()
		return e
	}
	p.candidate = candidate
	if plan.role == "compute" {
		if e = p.prepareOriginalComputeStopped(ctx); e != nil {
			return e
		}
	}
	return normalSystemRoleV2CheckedCheckpoint(ctx, "stopped", p)
}
func (p *normalSystemRoleV2Publication) originalDetachSucceeded(ctx context.Context) error {
	if p == nil || p.installed == nil || p.candidate == nil {
		return ErrUnknown
	}
	if e := p.installed.originalDetachSucceeded(); e != nil {
		return e
	}
	if e := p.candidate.activateOriginalRoleMainLocked(ctx, p.journal.state, p.record); e != nil {
		if !errors.Is(e, errNormalSystemRoleLeasePendingOriginalExit) {
			return e
		}
		// 短命main已真实Detach但尚未原Wait确认：只保不可授权pending-exit。
		if p.compute != nil {
			if e = p.compute.originalExitPending(ctx, p.journal.state, p.record); e != nil {
				return e
			}
		}
		return normalSystemRoleV2CheckedCheckpoint(ctx, "pending-exit", p)
	}
	if p.compute != nil {
		if e := p.compute.publishOriginalDetached(ctx, p.journal.state, p.record); e != nil {
			return e
		}
	}
	return normalSystemRoleV2CheckedCheckpoint(ctx, "active", p)
}
func (p *normalSystemRoleV2Publication) markUnknown(cause error) {
	if p == nil {
		return
	}
	if p.compute != nil {
		p.compute.markUnknown()
	}
	if p.candidate != nil {
		p.candidate.markUnknownLocked(cause)
	} else if p.installed != nil {
		p.installed.markOriginalRoleUnknown()
	}
}

func (i *normalSystemRoleInstalledMain) recheckOriginalInstalledSourcesLocked(ctx context.Context, state *lockedState) error {
	if i.controls == nil || i.stdio == nil || i.creator == nil || i.child == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original installed controls/stdio/creator absent"))
	}
	if e := i.controls.recheckOriginalRoleControlHistory(ctx); e != nil {
		return e
	}
	select {
	case <-i.child.done:
		if _, e := i.child.originalRoleWaitConfirmed(i.creator); e != nil {
			return e
		}
		return i.stdio.recheckOriginalHistoryLocked(ctx, state)
	default:
		if e := i.controls.recheckOriginalRoleControlLive(ctx); e != nil {
			return e
		}
		return i.stdio.recheckOriginalLiveLocked(ctx, state)
	}
}

// 原run已唯一Wait/creator退役，调用者仍持当前record的resource锁。
// 后端只能由真实Wait转KnownExited；stdio Root写端在此之后再关闭以观全部EOF。
func (p *normalSystemRoleV2Publication) completeOriginalWaitLocked(ctx context.Context, state *lockedState, r *record) error {
	if p == nil || p.candidate == nil || p.installed == nil || state == nil || r == nil || p.runtime == nil || p.runtime.authority == nil {
		return ErrUnknown
	}
	a := p.runtime.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	return p.candidate.retireOriginalRoleMainLocked(ctx, state, r)
}
