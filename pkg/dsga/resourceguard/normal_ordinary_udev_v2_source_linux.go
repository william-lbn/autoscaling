//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type normalOrdinaryUdevV2Slot struct{ source *normalOrdinaryUdevV2 }

var normalOrdinaryUdevV2Slots sync.Map

type normalOrdinaryUdevV2 struct {
	mu                   sync.Mutex
	closed               bool
	g                    *Guard
	filesystem           *normalFilesystemOriginSource
	proof                *normalSystemRoleBrokerExecProof
	authority            *normalSystemResourceAuthority
	originalFS           normalFilesystemOriginReference
	current              nativeBirthConsumer
	kernel               normalKernelPrepV2HandoffReference
	memory               normalMemoryOriginConfirmedReference
	prepared             normalSystemRoleFileWitness
	parent               *normalSystemOriginalTask
	pid1                 *normalFilesystemTask
	namespaces           *normalSystemRoleNamespaceSource
	monitor              *normalOrdinaryUdevV2KernelMonitor
	emitter              int
	catalog              *normalOrdinaryUdevV2Catalog
	ledger               normalOrdinaryUdevV2Ledger
	cold                 *normalOrdinaryUdevV2ColdTrigger
	done                 chan error
	changed              chan struct{}
	once                 sync.Once
	closeOnce            sync.Once
	preparation          normalOrdinaryUdevV2Preparation
	preparationReference normalOrdinaryUdevV2PreparationReference
}
type normalOrdinaryUdevV2ColdTrigger struct {
	source     *normalOrdinaryUdevV2
	uuid       string
	peer       *os.File
	credential unix.Ucred
	next       int
	pending    map[string]bool
	finished   bool
}

// 唯一首次阶段：原5FD→FS2→KernelR2→MemoryR2后，authority prepared且未COMMIT。
func (g *Guard) prepareNormalOrdinaryUdevV2Locked(ctx context.Context, state *lockedState, r *record, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof, authority *normalSystemResourceAuthority) (out *normalOrdinaryUdevV2, err error) {
	fail := func(e error) (*normalOrdinaryUdevV2, error) { return nil, errors.Join(ErrUnknown, e) }
	if g == nil || ctx == nil || ctx.Err() != nil || state == nil || r == nil || filesystem == nil || proof == nil || authority == nil || filesystem.g != g || authority.guard != g || authority.source != filesystem.system || authority.admission == nil || filesystem.kernelConsumer == nil || filesystem.memoryConsumer == nil {
		return fail(fmt.Errorf("udev original typed prepared inputs absent"))
	}
	if e := normalKernelPrepV2R2Caller(); e != nil {
		return fail(e)
	}
	slot := &normalOrdinaryUdevV2Slot{}
	if _, loaded := normalOrdinaryUdevV2Slots.LoadOrStore(g, slot); loaded {
		return fail(fmt.Errorf("udev same-Guard stage is single-use; no recapture"))
	}
	q := &normalOrdinaryUdevV2{g: g, filesystem: filesystem, proof: proof, authority: authority, emitter: -1, done: make(chan error, 1), changed: make(chan struct{}, 1)}
	slot.source = q
	good := false
	defer func() {
		if !good {
			q.close()
		}
	}()
	if e := authority.matchOriginalRecord(r, state); e != nil {
		return fail(e)
	}
	if authority.admission.enabled.Load() || authority.admission.failed.Load() || lookupNormalSystemAuthority(g) != authority || authority.reference.PreparedSHA256 == "" {
		return fail(fmt.Errorf("udev capture must precede first authority COMMIT"))
	}
	authority.producerMu.Lock()
	virgin := authority.compute == nil && authority.attachCount == 0 && len(authority.attachments) == 0
	authority.producerMu.Unlock()
	if !virgin || r.Original != nil || r.Held != nil || r.Birth != nil {
		return fail(fmt.Errorf("udev prepared phase already has actor history"))
	}
	raw, e := json.Marshal(r)
	if e != nil || physicalDigest(raw) != proof.transfer.Origin.ResourceStageSHA256 {
		return fail(errors.Join(e, fmt.Errorf("udev initial resource stage differs")))
	}
	q.prepared, e = readNormalSystemRoleFileWitness(state, normalSystemAuthorityStageName(authority.reference.InstanceNonce, "prepared"))
	if e != nil || q.prepared != authority.prepared {
		return fail(errors.Join(e, fmt.Errorf("udev original prepared durability witness differs")))
	}
	q.originalFS, e = filesystem.referenceLocked(ctx, state)
	if e != nil {
		return fail(e)
	}
	if q.originalFS.ProtocolVersion != 2 || filesystem.intent.Memory == nil || filesystem.intent.Memory.InitialWALFamily != normalInitialWALFamilyKernelPrepV2 {
		return fail(ErrUnknown)
	}
	q.kernel, e = filesystem.kernelConsumer.referenceLocked(ctx, state)
	if e != nil {
		return fail(e)
	}
	if e = filesystem.memoryConsumer.recheckLocked(ctx, state, g); e != nil {
		return fail(e)
	}
	q.memory = filesystem.memoryConsumer.reference
	q.current, e = nativeBirthCurrentConsumerIdentity()
	if e != nil || q.current != filesystem.current {
		return fail(errors.Join(e, ErrUnknown))
	}
	proof.mu.Lock()
	e = proof.verifyLocked(ctx, state, filesystem.system)
	if e == nil && proof.parent != nil {
		e = proof.parent.verify()
		if e == nil {
			q.parent, e = duplicateNormalSystemOriginalTask(proof.parent)
		}
	} else if e == nil {
		e = ErrUnknown
	}
	proof.mu.Unlock()
	if e != nil {
		return fail(e)
	}
	if q.parent.source.PID != proof.transfer.Origin.Parent.RootConsumer.PID || q.parent.source.StartTicks != uint64(proof.transfer.Origin.Parent.RootConsumer.StartTicks) {
		return fail(ErrUnknown)
	}
	q.pid1, e = normalFilesystemPinInitialTask(true)
	if e != nil {
		return fail(e)
	}
	q.namespaces, e = captureNormalSystemRoleNamespaces()
	if e != nil {
		return fail(e)
	}
	if e = normalSystemRoleServiceVerifyTask(ctx, proof.declaration, q.namespaces, q.parent, []string{normalSystemRoleBrokerExecutable, "initial-boot"}); e != nil {
		if e = normalSystemRoleServiceVerifyTask(ctx, proof.declaration, q.namespaces, q.parent, []string{normalSystemRoleBrokerExecutable, "recover-initial-boot"}); e != nil {
			return fail(e)
		}
	}
	if e = normalOrdinaryUdevV2InitialActors(q.parent, q.current, q.pid1); e != nil {
		return fail(e)
	}
	empty, e := preSystemRecursivePGEmpty()
	if e != nil || !empty {
		return fail(errors.Join(ErrUnknown, e))
	}
	// 仅以出现过普通role WAL作拒绝，不从后继目录扫描重建任何来源。
	names, e := state.dir.Readdirnames(normalOrdinaryUdevV2CatalogMaximum + 1)
	if e != nil && !errors.Is(e, io.EOF) {
		return fail(e)
	}
	if len(names) > normalOrdinaryUdevV2CatalogMaximum {
		return fail(ErrUnknown)
	}
	for _, name := range names {
		if strings.HasPrefix(name, "system-role-") {
			return fail(fmt.Errorf("udev original phase contains prior ordinary role history"))
		}
	}
	preparationReader, ok := any(g).(normalOrdinaryUdevV2PreparationReader)
	if !ok {
		return fail(fmt.Errorf("original fixed stock-preparation opaque producer absent"))
	}
	// 先接管原opaque，失败时原q.close仍能释放它。reference只来自factory同栈
	// 最后一次完整校验；中间没有可变回调，非跨调用/生命周期缓存。
	q.preparation, q.preparationReference, e = preparationReader.normalOrdinaryUdevV2PreparationLocked(ctx, state, r, filesystem, proof, authority)
	if e != nil || q.preparation == nil {
		return fail(errors.Join(ErrUnknown, e))
	}
	stockPreparation, ok := q.preparation.(*normalOrdinaryStockUdevPreparation)
	if !ok || stockPreparation.q != q {
		return fail(ErrUnknown)
	}
	if e = stockPreparation.handoffLive(ctx); e != nil {
		return fail(e)
	}
	fsRaw, e := json.Marshal(q.originalFS)
	if e != nil {
		return fail(e)
	}
	prep := q.preparationReference
	if prep.ProtocolVersion != 2 || prep.InstanceNonce == "" || prep.OriginalR2 != q.current || prep.Identity != r.Identity || prep.OriginalInitialWAL != q.memory.InitialWAL || prep.AuthorityPrepared != q.prepared || prep.FilesystemSHA256 != physicalDigest(fsRaw) {
		return fail(fmt.Errorf("original stock preparation stage differs"))
	}
	for _, digest := range []string{prep.ManifestSHA256, prep.PackageLockSHA256, prep.RulesSHA256, prep.ProjectionSHA256, prep.WaitSHA256} {
		if !physicalDigestValid(digest) {
			return fail(ErrUnknown)
		}
	}
	q.monitor, e = openNormalOrdinaryUdevV2KernelMonitor()
	if e != nil {
		return fail(e)
	}
	q.monitor.start(q.receive, q.fail, q.idle)
	rules, ok := q.preparation.(normalOrdinaryUdevV2OriginalRuleReader)
	if !ok {
		return fail(fmt.Errorf("original stock-preparation rule FD producer absent"))
	}
	catalog, e := captureNormalOrdinaryUdevV2Catalog(ctx, filesystem.system, filesystem.memoryConsumer, rules)
	if e != nil {
		return fail(e)
	}
	q.mu.Lock()
	q.catalog = catalog
	q.mu.Unlock()
	if e = catalog.installPreparationLocked(ctx, state, q.preparation); e != nil {
		return fail(e)
	}
	q.emitter, e = unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
	if e != nil {
		return fail(e)
	}
	if e = unix.Bind(q.emitter, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); e != nil {
		return fail(e)
	}
	if e = normalOrdinaryUdevV2InitialActors(q.parent, q.current, q.pid1); e != nil {
		return fail(e)
	}
	if e = q.parent.verify(); e != nil {
		return fail(e)
	}
	if authority.admission.enabled.Load() || authority.admission.failed.Load() {
		return fail(ErrUnknown)
	}
	q.mu.Lock()
	for _, ticket := range q.ledger.entries {
		if _, e = q.catalog.originalTicket(ticket); e != nil {
			break
		}
	}
	failed := q.ledger.failed
	q.mu.Unlock()
	if e != nil || failed != nil {
		return fail(errors.Join(e, failed))
	}
	if len(q.catalog.unresolved) > 0 {
		return fail(fmt.Errorf("udev original catalog captured but complete readonly rule semantics unresolved: %s", q.catalog.unresolved[0]))
	}
	// 原初始catalog逐目标只读sealed资料；全部安装后再完整重核一次，
	// 避免O(目标数*全部ELF)重hash，同时不省略准备末尾的原来源/持久见证。
	finalPreparation, e := q.preparation.referenceLocked(ctx, state)
	if e != nil || finalPreparation != q.preparationReference {
		return fail(errors.Join(ErrUnknown, e))
	}
	good = true
	return q, nil
}

// userspace只允许原PID1、活原R1与本原R2；真实kernel线程由内核PF_KTHREAD单独识别。
func normalOrdinaryUdevV2InitialActors(parent *normalSystemOriginalTask, current nativeBirthConsumer, pid1 *normalFilesystemTask) error {
	if parent == nil || pid1 == nil || current.PID <= 1 || parent.source.PID <= 1 || parent.source.PID == current.PID {
		return ErrUnknown
	}
	if e := parent.verify(); e != nil {
		return e
	}
	if e := pid1.verify(); e != nil {
		return e
	}
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return e
	}
	if len(entries) > 65536 {
		return ErrUnknown
	}
	allowed := map[int]bool{1: true, parent.source.PID: true, current.PID: true}
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil {
			continue
		}
		if allowed[pid] {
			continue
		}
		fd, e := unix.Open("/proc/"+entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e != nil {
			return errors.Join(ErrUnknown, e)
		}
		text, e := nativeTraceProcText(fd, "status")
		if e != nil {
			unix.Close(fd)
			return e
		}
		kernel, e := nativeTraceStatusValues(text, "Kthread", 1)
		if e != nil || kernel[0] != 1 {
			unix.Close(fd)
			return errors.Join(ErrUnknown, fmt.Errorf("udev prepared phase contains unapproved userspace actor %d", pid))
		}
		stat, e := nativeTraceProcText(fd, "stat")
		unix.Close(fd)
		if e != nil {
			return e
		}
		right := strings.LastIndexByte(stat, ')')
		if right < 0 {
			return ErrUnknown
		}
		fields := strings.Fields(stat[right+1:])
		if len(fields) < 7 {
			return ErrUnknown
		}
		flags, e := strconv.ParseUint(fields[6], 10, 64)
		if e != nil || flags&0x200000 == 0 {
			return ErrUnknown
		}
	}
	return parent.verify()
}
func (q *normalOrdinaryUdevV2) notify() {
	select {
	case q.changed <- struct{}{}:
	default:
	}
}
func (q *normalOrdinaryUdevV2) fail(e error) {
	if q == nil {
		return
	}
	q.mu.Lock()
	failure := q.ledger.fail(e)
	q.mu.Unlock()
	q.once.Do(func() {
		if q.authority != nil && q.authority.admission != nil {
			q.authority.admission.fail()
		}
		q.done <- failure
	})
	q.notify()
}
func (q *normalOrdinaryUdevV2) idle() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	return q.ledger.expire(time.Now())
}
func (q *normalOrdinaryUdevV2) receive(p normalOrdinaryUdevV2KernelPacket) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrUnknown
	}
	if e := q.ledger.add(p.raw, p.properties, p.sequence, time.Now()); e != nil {
		return e
	}
	if q.catalog != nil {
		if _, e := q.catalog.originalTicket(q.ledger.entries[p.sequence]); e != nil {
			return e
		}
	}
	q.notify()
	return nil
}

// 必须已持resource flock；只读稳定引用，不要求原R1仍活或PGEmpty/旧floor。
func (q *normalOrdinaryUdevV2) checkLocked(ctx context.Context, state *lockedState, r *record) error {
	if q == nil || ctx == nil || ctx.Err() != nil || state == nil || r == nil {
		return ErrUnknown
	}
	q.mu.Lock()
	closed, failed := q.closed, q.ledger.failed
	q.mu.Unlock()
	if closed || failed != nil {
		return errors.Join(ErrUnknown, failed)
	}
	v, ok := normalOrdinaryUdevV2Slots.Load(q.g)
	if !ok || v.(*normalOrdinaryUdevV2Slot).source != q {
		return ErrUnknown
	}
	if !q.authority.admission.businessAllowed() {
		return ErrUnknown
	}
	if e := normalKernelPrepV2R2Caller(); e != nil {
		return e
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != q.current {
		return errors.Join(ErrUnknown, e)
	}
	ref, e := q.filesystem.referenceLocked(ctx, state)
	if e != nil || ref != q.originalFS {
		return errors.Join(ErrUnknown, e)
	}
	kernel, e := q.filesystem.kernelConsumer.referenceLocked(ctx, state)
	if e != nil || kernel != q.kernel {
		return errors.Join(ErrUnknown, e)
	}
	if e = q.filesystem.memoryConsumer.recheckLocked(ctx, state, q.g); e != nil {
		return e
	}
	if q.filesystem.memoryConsumer.reference != q.memory {
		return ErrUnknown
	}
	if e = q.monitor.verify(); e != nil {
		return e
	}
	if e = q.namespaces.verify(-1); e != nil {
		return e
	}
	if e = q.catalog.verify(); e != nil {
		return e
	}
	prep, e := q.preparation.referenceLocked(ctx, state)
	if e != nil || prep != q.preparationReference {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (q *normalOrdinaryUdevV2) originalMainLocked(ctx context.Context, state *lockedState, r *record, role string, credential *unix.Ucred, peer *os.File) error {
	reader, ok := any(q.g).(normalSystemOriginalRoleMainLeaseReader)
	if !ok {
		return ErrUnknown
	}
	lease, e := reader.normalSystemOriginalRoleMainLeaseLocked(ctx, state, r, role)
	if e != nil {
		return e
	}
	defer lease.close()
	return lease.matchOriginalMainPacket(ctx, credential, peer)
}
func (q *normalOrdinaryUdevV2) emitOriginalMainLocked(ctx context.Context, state *lockedState, r *record, credential *unix.Ucred, peer *os.File, body []byte) error {
	if e := q.checkLocked(ctx, state, r); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e := q.originalMainLocked(ctx, state, r, "udev", credential, peer); e != nil {
		return e
	}
	request, e := decodeNormalOrdinaryUdevV2Request(body)
	if e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrUnknown
	}
	ticket, e := q.ledger.take(request.Sequence, time.Now())
	if e != nil {
		return e
	}
	if e = q.catalog.processed(ticket, request); e != nil {
		return q.ledger.fail(e)
	}
	packet, e := normalOrdinaryUdevV2PublicPacket(request.Properties)
	if e != nil {
		return q.ledger.fail(e)
	}
	if e = normalOrdinaryUdevV2Broadcast(q.emitter, packet); e != nil {
		return q.ledger.fail(e)
	}
	if e = q.ledger.commit(ticket); e != nil {
		return e
	}
	if q.cold != nil && ticket.uuid == q.cold.uuid {
		if _, exists := q.cold.pending[ticket.properties["DEVPATH"]]; exists {
			q.cold.pending[ticket.properties["DEVPATH"]] = true
		}
	}
	q.notify()
	return nil
}
func (q *normalOrdinaryUdevV2) beginColdTriggerLocked(ctx context.Context, state *lockedState, r *record, credential *unix.Ucred, peer *os.File) (*normalOrdinaryUdevV2ColdTrigger, error) {
	if e := q.checkLocked(ctx, state, r); e != nil {
		return nil, e
	}
	if e := q.originalMainLocked(ctx, state, r, "udev-init", credential, peer); e != nil {
		return nil, e
	}
	if credential == nil || peer == nil {
		return nil, ErrUnknown
	}
	uuid, e := normalOrdinaryUdevV2NewTriggerUUID()
	if e != nil {
		return nil, e
	}
	fd, e := unix.FcntlInt(peer.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	t := &normalOrdinaryUdevV2ColdTrigger{source: q, uuid: uuid, peer: os.NewFile(uintptr(fd), "original cold-trigger init main peer"), credential: *credential}
	q.mu.Lock()
	if q.closed || q.cold != nil {
		q.mu.Unlock()
		t.peer.Close()
		return nil, ErrUnknown
	}
	q.cold = t
	q.mu.Unlock()
	if e = q.advanceColdLocked(ctx, state, r, t); e != nil {
		q.fail(e)
		return nil, e
	}
	return t, nil
}
func (q *normalOrdinaryUdevV2) advanceColdLocked(ctx context.Context, state *lockedState, r *record, t *normalOrdinaryUdevV2ColdTrigger) error {
	if t == nil || t.source != q {
		return ErrUnknown
	}
	if e := q.checkLocked(ctx, state, r); e != nil {
		return e
	}
	if e := q.originalMainLocked(ctx, state, r, "udev-init", &t.credential, t.peer); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.cold != t || q.ledger.failed != nil {
		return ErrUnknown
	}
	for _, processed := range t.pending {
		if !processed {
			return ErrUnknown
		}
	}
	t.pending = map[string]bool{}
	end := t.next + normalOrdinaryUdevV2ColdBatch
	if end > len(q.catalog.ordered) {
		end = len(q.catalog.ordered)
	}
	for ; t.next < end; t.next++ {
		entry := q.catalog.ordered[t.next]
		object, e := normalFilesystemFDObject(int(entry.uevent.Fd()))
		if e != nil || object != entry.object {
			return errors.Join(ErrUnknown, e)
		}
		t.pending[entry.path] = false
		literal := []byte("add " + t.uuid + "\n")
		n, e := unix.Pwrite(int(entry.uevent.Fd()), literal, 0)
		if e != nil || n != len(literal) {
			return errors.Join(ErrUnknown, e, fmt.Errorf("original catalog fixed synthetic trigger not confirmed"))
		}
	}
	return nil
}

// 调用方不得持resource/producer锁；每批至多8，后继批显式g.withRecord重新验原init租约。
func (t *normalOrdinaryUdevV2ColdTrigger) wait(ctx context.Context) error {
	if t == nil || t.source == nil || ctx == nil {
		return ErrUnknown
	}
	q := t.source
	call, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	defer t.peer.Close()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		q.mu.Lock()
		e := q.ledger.expire(time.Now())
		ready := true
		for _, done := range t.pending {
			ready = ready && done
		}
		all := t.next == len(q.catalog.ordered)
		if e == nil && ready && all {
			t.finished = true
		}
		q.mu.Unlock()
		if e != nil {
			q.fail(e)
			return e
		}
		if ready && all {
			return nil
		}
		if ready {
			e = q.g.withRecord(call, func(r *record, state *lockedState) error { return q.advanceColdLocked(call, state, r, t) })
			if e != nil {
				q.fail(e)
				return e
			}
			continue
		}
		select {
		case <-call.Done():
			q.fail(call.Err())
			return errors.Join(ErrUnknown, call.Err())
		case <-q.changed:
		case <-ticker.C:
		}
	}
}
func (q *normalOrdinaryUdevV2) finishColdTriggerLocked(ctx context.Context, state *lockedState, r *record, credential *unix.Ucred, peer *os.File, t *normalOrdinaryUdevV2ColdTrigger) error {
	if t == nil || t.source != q {
		return ErrUnknown
	}
	if e := q.checkLocked(ctx, state, r); e != nil {
		return e
	}
	if e := q.originalMainLocked(ctx, state, r, "udev-init", credential, peer); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.cold != t || !t.finished || q.ledger.failed != nil {
		return ErrUnknown
	}
	return nil
}
func (q *normalOrdinaryUdevV2) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("udev kernel event source does not close Root writers or normal"))
}
func (q *normalOrdinaryUdevV2) close() {
	if q == nil {
		return
	}
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		if q.monitor != nil {
			q.monitor.close()
		}
		if q.emitter >= 0 {
			unix.Close(q.emitter)
		}
		if q.catalog != nil {
			q.catalog.close()
		}
		// 外层可能仍持resource flock；这里只释放原prep对象，不可回g/FS/System/resource。
		// runtime monitor已停、catalog原dup已关，且未持q.mu。
		if preparation, ok := q.preparation.(interface{ close() }); ok {
			preparation.close()
		}
		if q.parent != nil {
			q.parent.close()
		}
		if q.pid1 != nil {
			q.pid1.close()
		}
		if q.namespaces != nil {
			q.namespaces.close()
		}
		if q.cold != nil && q.cold.peer != nil {
			q.cold.peer.Close()
		}
	})
}
