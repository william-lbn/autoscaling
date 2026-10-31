//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"golang.org/x/sys/unix"
)

type initialBootCompleteEvidence struct {
	Preboot bootprotocol.InitialPrebootReceipt `json:"preboot"`
	Movable bootprotocol.InitialMovableReceipt `json:"movable"`
	Actual  Observation                        `json:"actual"`
}
type linuxInitialLifecycle struct {
	r            *Runner
	config       InitialBootstrapConfig
	q            *nativeQMP
	mu           sync.Mutex
	processGuard sync.RWMutex
	source       *initialBootSource
	process      *initialBootProcess
	ready        *InitialBootstrapPublication
	stopped      bool
}

func newInitialLifecycle(r *Runner, c *InitialBootstrapConfig) (initialLifecycle, error) {
	if r == nil || r.engine == nil || r.birth.InitialMode == nil || c == nil {
		return nil, fmt.Errorf("original explicit initial mode required")
	}
	q, ok := r.engine.native.(*nativeQMP)
	if !ok {
		return nil, fmt.Errorf("original fixed QMP constructor required")
	}
	m, e := initialModeForConfig(Config{StateDir: r.engine.dir, InitialBootstrap: c})
	if e != nil || !sameInitialMode(m, r.birth.InitialMode) {
		return nil, fmt.Errorf("initial configuration differs from original Birth")
	}
	return &linuxInitialLifecycle{r: r, q: q, config: InitialBootstrapConfig{RuntimeJSON: append([]byte(nil), c.RuntimeJSON...), OriginalBaseBytes: c.OriginalBaseBytes}}, nil
}
func (l *linuxInitialLifecycle) arguments() []string {
	return []string{"-chardev", "socket,id=dsga_boot,path=" + filepath.Join(l.r.engine.dir, initialBootSocketName) + ",server=on,wait=off", "-device", "virtio-serial-pci,id=dsga_boot_bus", "-device", "virtserialport,bus=dsga_boot_bus.0,chardev=dsga_boot,name=" + initialBootPortName}
}
func (l *linuxInitialLifecycle) freeze(ctx context.Context, cmd *exec.Cmd) error {
	if e := validateInitialBootLaunch(cmd, l.r, l.arguments()); e != nil {
		return e
	}
	ref, e := l.r.freezeInitialBootstrap(ctx, l.config.RuntimeJSON, cmd, control.Uint64(l.config.OriginalBaseBytes))
	if e != nil {
		return e
	}
	return l.r.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
		entries, e := initialBootRead(s)
		if e != nil {
			return e
		}
		if len(entries) != 1 || initialBootReference(entries) != ref || entries[0].Source.OriginalBase != l.r.birth.InitialMode.OriginalBaseBytes {
			return fmt.Errorf("initial source already progressed or differs")
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.source != nil || l.process != nil || l.stopped {
			return fmt.Errorf("initial lifecycle cannot freeze twice")
		}
		v := entries[0].Source
		l.source = &v
		return nil
	})
}

type linuxInitialRun struct {
	lifecycle *linuxInitialLifecycle
	cancel    context.CancelFunc
	done      <-chan error
	once      sync.Once
	result    error
}

func (l *linuxInitialLifecycle) started(ctx context.Context, cmd *exec.Cmd) (initialLifecycleRun, error) {
	l.mu.Lock()
	source := l.source
	if source == nil || l.process != nil || l.stopped {
		l.mu.Unlock()
		return nil, fmt.Errorf("initial pre-Start source missing or already consumed")
	}
	l.mu.Unlock()
	// 同一个RunQEMU尚未进入唯一Wait；这里从原Start句柄捕获，不能按远端PID补绑。
	process, e := captureInitialBootProcess(cmd, *source)
	if e != nil {
		return nil, e
	}
	l.mu.Lock()
	l.process = process
	l.mu.Unlock()
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	done := make(chan error, 1)
	run := &linuxInitialRun{lifecycle: l, cancel: cancel, done: done}
	go func() {
		defer cancel()
		e := l.drive(runCtx)
		if e != nil {
			// 首次启动尚未公布完整容量；失败仅终止同原pidfd，不杀可复用numeric PID。
			// 即使已经may-have-sent也保留WAL/Host预留，真实Wait稍后另记退出事实。
			killErr := unix.PidfdSendSignal(process.pidfd, unix.SIGKILL, nil, 0)
			if killErr != nil && !errors.Is(killErr, unix.ESRCH) {
				e = errors.Join(e, killErr)
			}
		}
		done <- e
		close(done)
	}()
	return run, nil
}
func (run *linuxInitialRun) stopAndWait() error {
	run.once.Do(func() {
		run.cancel()
		// 必须等实际worker终态，不能ctx到期后遗留QMP/Guest写者。
		run.result = <-run.done
		l := run.lifecycle
		l.mu.Lock()
		l.stopped = true
		l.mu.Unlock()
		// 普通Read/Set也持RLock；先join工作者，再独占关闭原句柄，避免读/close数据竞争。
		l.processGuard.Lock()
		l.mu.Lock()
		p := l.process
		l.process = nil
		l.mu.Unlock()
		if p != nil {
			p.close()
		}
		l.processGuard.Unlock()
	})
	return run.result
}
func (l *linuxInitialLifecycle) sourceAndProcessLocked() (initialBootSource, *initialBootProcess, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.source == nil || l.process == nil || l.stopped {
		return initialBootSource{}, nil, errInitialBootUnavailable
	}
	return *l.source, l.process, nil
}
func (l *linuxInitialLifecycle) drive(ctx context.Context) error {
	l.processGuard.RLock()
	defer l.processGuard.RUnlock()
	source, process, e := l.sourceAndProcessLocked()
	if e != nil {
		return e
	}
	return l.r.engine.withRecord(ctx, func(rec *record, s *lockedState) error {
		entries, e := initialBootRead(s)
		if e != nil {
			return e
		}
		if len(entries) != 1 || entries[0].SourceSHA256 != initialBootSHA(initialBootCanonical(source)) {
			return fmt.Errorf("initial worker cannot replay any progressed or foreign chain")
		}
		ref := initialBootReference(entries)
		probe := &initialBaseQMP{source: source, q: l.q, process: process}
		if e = waitInitialQMPSocket(ctx, l.q.path); e != nil {
			return e
		}
		base, e := probe.inspect(ctx)
		if e != nil {
			return e
		}
		if base.RequestedBytes != source.OriginalBase || base.ActualBytes != source.OriginalBase {
			return fmt.Errorf("original QEMU did not start base-only")
		}
		hardware, e := readInitialBootHardware(ctx, l.q, process, source)
		if e != nil {
			return e
		}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(struct {
			Geometry originalMemoryGeometry `json:"geometry"`
			Hardware initialBootHardware    `json:"hardware"`
		}{base, hardware}))
		if e != nil {
			return e
		}
		challenge, e := initialBootChallenge(source, ref)
		if e != nil {
			return e
		}
		channel, e := openInitialBootChannel(ctx, l.r.engine.dir, process, source)
		if e != nil {
			return e
		}
		defer channel.close()
		prepared, e := channel.prepare(ctx, challenge)
		if e != nil {
			return e
		}
		if e = prepared.MatchChallenge(challenge); e != nil {
			return e
		}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(prepared))
		if e != nil {
			return e
		}
		if e = l.recheckInitialAuthority(ctx, source); e != nil {
			return e
		}
		target := source.Identity.MinimumBytes
		offset, e := initialBootTargetOffset(source, target)
		if e != nil {
			return e
		}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(struct {
			Target control.Uint64 `json:"target_bytes"`
			Offset control.Uint64 `json:"requested_size"`
		}{target, offset}))
		if e != nil {
			return e
		}
		// 连接/实际原base和peer先核，再同步may-have-sent；只可能在这一处写一次vm0。
		c, e := l.q.connect(ctx)
		if e != nil {
			return e
		}
		sentErr := func() error {
			defer c.close()
			if e := process.checkPeer(c, source); e != nil {
				return e
			}
			in, e := initialBootGeometryRead(c)
			if e != nil {
				return e
			}
			before, e := decodeOriginalMemoryGeometry(source.Identity, source.OriginalBase, in)
			if e != nil {
				return e
			}
			if before.RequestedBytes != source.OriginalBase || before.ActualBytes != source.OriginalBase {
				return fmt.Errorf("initial QMP changed before sole grow")
			}
			if e = l.recheckInitialAuthority(ctx, source); e != nil {
				return e
			}
			ref, e = initialBootAppend(s, ref, initialBootCanonical(struct {
				PreparedSHA256 string         `json:"prepared_sha256"`
				Target         control.Uint64 `json:"target_bytes"`
			}{ref.TailSHA256, target}))
			if e != nil {
				return e
			}
			if target == source.OriginalBase {
				return nil
			} // 明确无写，仍不合成Resize回执。
			_, e = c.execute("qom-set", struct {
				Path     string `json:"path"`
				Property string `json:"property"`
				Value    uint64 `json:"value"`
			}{"/machine/peripheral/vm0", "requested-size", uint64(offset)})
			return e // 任何错误均Unknown；不重连/重发原写。
		}()
		if sentErr != nil {
			return sentErr
		}
		var actual originalMemoryGeometry
		for {
			actual, e = probe.inspect(ctx)
			if e != nil {
				return e
			}
			if actual.RequestedBytes == target && actual.ActualBytes == target {
				break
			}
			if e = initialBootPause(ctx); e != nil {
				return e
			}
		}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(actual))
		if e != nil {
			return e
		}
		if e = l.recheckInitialAuthority(ctx, source); e != nil {
			return e
		}
		movableChallenge := bootprotocol.InitialMovableChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialMovableScope, Preboot: prepared, HostStageSHA256: ref.TailSHA256, TargetBytes: target}
		moved, e := channel.movable(ctx, movableChallenge)
		if e != nil {
			return e
		}
		if e = moved.MatchChallenge(movableChallenge); e != nil {
			return e
		}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(moved))
		if e != nil {
			return e
		}
		// 完成只是首次启动事实，不签Root/PG/normal许可。实际容量+Movable已被两侧核完。
		if e = l.recheckInitialAuthority(ctx, source); e != nil {
			return e
		}
		evidence := initialBootCompleteEvidence{prepared, moved, Observation{actual.RequestedBytes, actual.ActualBytes}}
		ref, e = initialBootAppend(s, ref, initialBootCanonical(evidence))
		if e != nil {
			return e
		}
		movedSHA, e := moved.SHA256()
		if e != nil {
			return e
		}
		complete := bootprotocol.InitialCompletion{ProtocolVersion: 1, Scope: bootprotocol.InitialCompletionScope, Challenge: movableChallenge, MovableReceiptSHA256: movedSHA, HostStageSHA256: ref.TailSHA256}
		tail, e := initialTail(s)
		if e != nil {
			return e
		}
		proof := &InitialBootstrapPublication{ProtocolVersion: 2, Scope: initialBootScope, CompletedEntryJSON: initialBootCanonical(tail), Completion: complete}
		if e = validateInitialLifecyclePublication(l.r.birth, l.r.engine.binding, *proof); e != nil {
			return e
		}
		if e = channel.complete(ctx, complete); e != nil {
			return e
		}
		l.mu.Lock()
		l.ready = proof
		l.mu.Unlock()
		return nil
	})
}
func initialTail(s *lockedState) (initialBootEntry, error) {
	entries, e := initialBootRead(s)
	if e != nil {
		return initialBootEntry{}, e
	}
	if len(entries) != len(initialBootStages) {
		return initialBootEntry{}, fmt.Errorf("complete original initial chain missing")
	}
	return entries[len(entries)-1], nil
}
func waitInitialQMPSocket(ctx context.Context, path string) error {
	for {
		st, e := os.Lstat(path)
		if e == nil {
			if st.Mode()&os.ModeSocket == 0 {
				return fmt.Errorf("fixed initial QMP socket replaced")
			}
			return nil
		}
		if !os.IsNotExist(e) {
			return e
		}
		if e = initialBootPause(ctx); e != nil {
			return e
		}
	}
}
func initialBootPause(ctx context.Context) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (l *linuxInitialLifecycle) recheckInitialAuthority(ctx context.Context, source initialBootSource) error {
	current, e := l.r.readInitialBootReservation(ctx)
	if e != nil {
		return e
	}
	if current.Owner != source.Owner || current.LeaseUID != source.LeaseUID || current.Host.Binding != source.HostBinding || current.Host.Origin != source.HostOrigin || current.Host.Profile == nil || current.Host.Profile.ID != source.HostProfile || current.Placement.MemoryBytes < source.Identity.MinimumBytes || current.Placement.HostOverheadBytes != source.HostOverheadBytes || initialBootSHA(initialBootCanonical(current.Placement)) != source.PlacementSHA256 {
		return fmt.Errorf("original initial Host/owner reservation changed")
	}
	return nil
}
func (l *linuxInitialLifecycle) gate(s *lockedState) error {
	l.processGuard.RLock()
	defer l.processGuard.RUnlock()
	return l.gateLocked(s)
}
func (l *linuxInitialLifecycle) gateLocked(s *lockedState) error {
	l.mu.Lock()
	ready, p, stopped := l.ready, l.process, l.stopped
	l.mu.Unlock()
	if ready == nil || p == nil || stopped {
		return errInitialBootUnavailable
	}
	if e := p.alive(); e != nil {
		return e
	}
	entries, e := initialBootRead(s)
	if e != nil {
		return e
	}
	if len(entries) != len(initialBootStages) || !bytes.Equal(initialBootCanonical(entries[len(entries)-1]), ready.CompletedEntryJSON) {
		return fmt.Errorf("current original initial completion WAL changed")
	}
	return validateInitialLifecyclePublication(l.r.birth, l.r.engine.binding, *ready)
}
func (l *linuxInitialLifecycle) publication(s *lockedState) (*InitialBootstrapPublication, error) {
	if e := l.gate(s); e != nil {
		return nil, e
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	v := *l.ready
	v.CompletedEntryJSON = append([]byte(nil), v.CompletedEntryJSON...)
	return &v, nil
}

// Native接口只能从原engine的同flock调用；额外自行核原完整WAL和进程，
// 不递归取得同一flock。processGuard保证Wait后的清理不会关闭活跃读写句柄。
func (l *linuxInitialLifecycle) nativeGateLocked() error {
	fd, e := unix.Open(l.r.engine.dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "initial-native-state")
	defer f.Close()
	if e = privateFile(fd, unix.S_IFDIR); e != nil {
		return e
	}
	return l.gateLocked(&lockedState{dir: f})
}
func (l *linuxInitialLifecycle) Read(ctx context.Context) (Observation, error) {
	l.processGuard.RLock()
	defer l.processGuard.RUnlock()
	if e := l.nativeGateLocked(); e != nil {
		return Observation{}, e
	}
	source, p, e := l.sourceAndProcessLocked()
	if e != nil {
		return Observation{}, e
	}
	v, e := (&initialBaseQMP{source: source, q: l.q, process: p}).inspect(ctx)
	if e != nil {
		return Observation{}, e
	}
	result := Observation{v.RequestedBytes, v.ActualBytes}
	if e = result.validate(source.Identity); e != nil {
		return Observation{}, e
	}
	return result, nil
}
func (l *linuxInitialLifecycle) Set(ctx context.Context, target control.Uint64) error {
	l.processGuard.RLock()
	defer l.processGuard.RUnlock()
	if e := l.nativeGateLocked(); e != nil {
		return e
	}
	source, p, e := l.sourceAndProcessLocked()
	if e != nil {
		return e
	}
	offset, e := initialBootTargetOffset(source, target)
	if e != nil {
		return &notSentError{e}
	}
	c, e := l.q.connect(ctx)
	if e != nil {
		return &notSentError{e}
	}
	defer c.close()
	if e = p.checkPeer(c, source); e != nil {
		return &notSentError{e}
	}
	in, e := initialBootGeometryRead(c)
	if e != nil {
		return &notSentError{e}
	}
	v, e := decodeOriginalMemoryGeometry(source.Identity, source.OriginalBase, in)
	if e != nil {
		return &notSentError{e}
	}
	if (Observation{v.RequestedBytes, v.ActualBytes}).validate(source.Identity) != nil {
		return &notSentError{fmt.Errorf("current business geometry is below original minimum")}
	}
	_, e = c.execute("qom-set", struct {
		Path     string `json:"path"`
		Property string `json:"property"`
		Value    uint64 `json:"value"`
	}{"/machine/peripheral/vm0", "requested-size", uint64(offset)})
	return e
}
func validateInitialLifecyclePublication(birth Birth, ref budget.ObjectRef, proof InitialBootstrapPublication) error {
	if birth.ProtocolVersion != 2 || birth.InitialMode == nil || birth.InitialMode.validate(birth.Identity) != nil || ref.Validate() != nil || proof.ProtocolVersion != 2 || proof.Scope != initialBootScope || len(proof.CompletedEntryJSON) == 0 || len(proof.CompletedEntryJSON) > 16384 {
		return fmt.Errorf("initial publication lacks explicit original mode")
	}
	var entry initialBootEntry
	if control.UnmarshalObject(proof.CompletedEntryJSON, &entry) != nil || !bytes.Equal(initialBootCanonical(entry), proof.CompletedEntryJSON) || entry.ProtocolVersion != 2 || entry.Scope != initialBootScope || entry.Stage != "complete_non_authorizing" || entry.Sequence != 8 || entry.Source.validate() != nil || entry.SourceSHA256 != initialBootSHA(initialBootCanonical(entry.Source)) || !initialBootNonce(entry.Nonce) || !digestValid(entry.PreviousSHA256) {
		return fmt.Errorf("original initial completion entry malformed")
	}
	source := entry.Source
	if !initialSourceMatchesMode(source, birth, ref) {
		return fmt.Errorf("initial complete Source changed original static Birth or UID")
	}
	var evidence initialBootCompleteEvidence
	if control.UnmarshalObject(entry.Evidence, &evidence) != nil || !bytes.Equal(initialBootCanonicalEvidence(evidence), entry.Evidence) {
		return fmt.Errorf("initial completion evidence is not typed actual/Guest proof")
	}
	c := evidence.Preboot.Challenge
	if evidence.Preboot.Validate() != nil || c.SourceSHA256 != entry.SourceSHA256 || c.Nonce != entry.Nonce || c.Identity != bootprotocol.RunnerIdentity(source.Identity) || c.RuntimeSHA256 != source.RuntimeSHA256 || c.ResourceSHA256 != source.ResourceSHA256 || c.ConfigSecretUID != source.ConfigSecretUID || c.KernelBuildSHA256 != source.KernelBuildSHA256 || c.OriginalBase != source.OriginalBase || c.BusinessMinimum != source.Identity.MinimumBytes || c.Maximum != source.Identity.MaximumBytes || evidence.Movable.Challenge.Preboot != evidence.Preboot || evidence.Movable.Validate() != nil || evidence.Actual.RequestedBytes != source.Identity.MinimumBytes || evidence.Actual.ActualBytes != source.Identity.MinimumBytes || proof.Completion.MatchReceipt(evidence.Movable) != nil || proof.Completion.HostStageSHA256 != initialBootSHA(proof.CompletedEntryJSON) {
		return fmt.Errorf("initial completion Guest/actual/source binding differs")
	}
	return nil
}

func initialSourceMatchesMode(source initialBootSource, birth Birth, ref budget.ObjectRef) bool {
	mode := birth.InitialMode
	return mode != nil && source.Identity == birth.Identity && source.BirthClaim == ref && source.StateClaim == birth.StateClaim && source.OriginalBase == mode.OriginalBaseBytes && source.RuntimeSHA256 == mode.RuntimeSHA256 && source.ResourceSHA256 == mode.ResourceSHA256 && source.ConfigSecretUID == mode.ConfigSecretUID && source.KernelBuildSHA256 == mode.KernelBuildSHA256
}

func initialBootCanonicalEvidence(v any) []byte {
	var fields map[string]json.RawMessage
	raw, e := json.Marshal(v)
	if e != nil {
		return nil
	}
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil
	}
	return initialBootCanonical(fields)
}
