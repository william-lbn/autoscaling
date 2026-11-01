//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type normalOrdinaryStockPreparationCheckpointKey struct{}
type normalOrdinaryStockPreparationStageKey struct{}
type normalOrdinaryStockPreparationFailureKey struct{}

func normalOrdinaryStockPreparationStage(ctx context.Context, stage string) {
	if f, ok := ctx.Value(normalOrdinaryStockPreparationStageKey{}).(func(string)); ok {
		f(stage)
	}
}

const normalOrdinaryStockUdevPreparationScope = "original-same-guard-stock-udev-preparation-v2"
const normalOrdinaryStockUdevPreparationIntentName = "ordinary-stock-udev-preparation.intent.json"
const normalOrdinaryStockUdevPreparationCompleteName = "ordinary-stock-udev-preparation.complete.json"

// 单Guard单次原准备；失败对象也永久留槽。没有按新nonce/新epoch自动重跑入口，
// 新Root同字节WAL也不能重建本进程原monitor、Wait或sealed projection。
var normalOrdinaryStockUdevPreparations sync.Map

type normalOrdinaryStockUdevPreparation struct {
	mu                            sync.Mutex // resource→本producer；调用FS/Kernel/Memory前不持此锁。
	closed, complete              bool
	q                             *normalOrdinaryUdevV2
	inputs                        *normalOrdinaryStockUdevInputs
	dirs                          *normalOrdinaryStockUdevRuntimeDirs
	kmod                          *normalOrdinaryStockUdevKmodInputs
	monitor                       *normalOrdinaryStockUdevMonitor
	catalog                       *normalOrdinaryUdevV2Catalog
	programs                      *normalOrdinaryStockUdevPrograms
	seal                          *normalOrdinaryStockUdevProjectionSeal
	reference                     normalOrdinaryUdevV2PreparationReference
	inputSHA, kmodSHA, monitorSHA string
	waitRaw                       []byte
	stockLog                      []byte // 仅有界私有诊断；WAL/引用永远只记SHA，不输出环境或凭据。
	history                       [2]normalSystemRoleFileWitness
}

type normalOrdinaryStockUdevPreparationIntent struct {
	ProtocolVersion       uint32                                   `json:"protocol_version"`
	Scope                 string                                   `json:"scope"`
	Reference             normalOrdinaryUdevV2PreparationReference `json:"original_preparation"`
	KernelReferenceSHA256 string                                   `json:"kernel_reference_sha256"`
	MemoryReferenceSHA256 string                                   `json:"memory_reference_sha256"`
	InputReferenceSHA256  string                                   `json:"input_reference_sha256"`
}
type normalOrdinaryStockUdevPreparationCompletion struct {
	ProtocolVersion      uint32                                   `json:"protocol_version"`
	Scope                string                                   `json:"scope"`
	IntentSHA256         string                                   `json:"intent_sha256"`
	Reference            normalOrdinaryUdevV2PreparationReference `json:"original_preparation"`
	KmodReferenceSHA256  string                                   `json:"kmod_reference_sha256"`
	MonitorReceiptSHA256 string                                   `json:"monitor_receipt_sha256"`
	Targets              uint32                                   `json:"targets"`
	Waits                json.RawMessage                          `json:"original_waits"`
}

func originalNormalOrdinaryStockUdevPreparationSource(g *Guard, fs *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof, a *normalSystemResourceAuthority) (*normalOrdinaryUdevV2, error) {
	if g == nil || fs == nil || proof == nil || a == nil || fs.g != g || a.guard != g || a.source != fs.system || proof.system != fs.system {
		return nil, ErrUnknown
	}
	v, yes := normalOrdinaryUdevV2Slots.Load(g)
	if !yes {
		return nil, ErrUnknown
	}
	slot, yes := v.(*normalOrdinaryUdevV2Slot)
	if !yes || slot == nil || slot.source == nil {
		return nil, ErrUnknown
	}
	q := slot.source
	if q.g != g || q.filesystem != fs || q.proof != proof || q.authority != a || q.preparation != nil || q.parent == nil || q.pid1 == nil || q.namespaces == nil {
		return nil, ErrUnknown
	}
	return q, nil
}
func normalOrdinaryStockUdevReservePreparation(q *normalOrdinaryUdevV2) (*normalOrdinaryStockUdevPreparation, error) {
	if q == nil || q.g == nil {
		return nil, ErrUnknown
	}
	p := &normalOrdinaryStockUdevPreparation{q: q}
	if _, used := normalOrdinaryStockUdevPreparations.LoadOrStore(q.g, p); used {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original stock preparation is single-use"))
	}
	return p, nil
}
func normalOrdinaryStockUdevPreparationRejectHistory(s *lockedState) error {
	if s == nil || s.dir == nil || s.lock == nil {
		return ErrUnknown
	}
	for _, name := range []string{normalOrdinaryStockUdevPreparationIntentName, normalOrdinaryStockUdevPreparationCompleteName} {
		if _, exists, e := s.readPhysicalFile(name); e != nil || exists {
			return errors.Join(ErrUnknown, e, fmt.Errorf("stock preparation history exists or is unreadable"))
		}
	}
	return nil
}

// 构造仅由正在原q.prepare里的同g调用。ctx不携来源选择/路径/ready标记。
// 调用者已经持resource flock；不反取flock，不持q.mu/Runtime.mu跨外部命令Wait。
func (g *Guard) normalOrdinaryUdevV2PreparationLocked(ctx context.Context, s *lockedState, r *record, fs *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof, a *normalSystemResourceAuthority) (normalOrdinaryUdevV2Preparation, normalOrdinaryUdevV2PreparationReference, error) {
	if ctx == nil || ctx.Err() != nil || s == nil || r == nil {
		return nil, normalOrdinaryUdevV2PreparationReference{}, ErrUnknown
	}
	q, e := originalNormalOrdinaryStockUdevPreparationSource(g, fs, proof, a)
	if e != nil {
		return nil, normalOrdinaryUdevV2PreparationReference{}, e
	}
	p, e := normalOrdinaryStockUdevReservePreparation(q)
	if e != nil {
		return nil, normalOrdinaryUdevV2PreparationReference{}, e
	}
	good := false
	defer func() {
		if !good {
			p.close()
		}
	}()
	fail := func(stage string, e error) (normalOrdinaryUdevV2Preparation, normalOrdinaryUdevV2PreparationReference, error) {
		failure := errors.Join(ErrUnknown, fmt.Errorf("original stock preparation %s", stage), e)
		// 仅私有DEV在原失败已确定后观察；不改返回、清理、路径或任何授权。
		if observe, ok := ctx.Value(normalOrdinaryStockPreparationFailureKey{}).(func(string, error, *normalOrdinaryStockUdevPreparation)); ok {
			observe(stage, failure, p)
		}
		return nil, normalOrdinaryUdevV2PreparationReference{}, failure
	}
	if e = p.checkOriginalLocked(ctx, s, true); e != nil {
		return fail("initial source", e)
	}
	if e = normalOrdinaryStockUdevPreparationRejectHistory(s); e != nil {
		return fail("existing history", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "inputs")
	p.inputs, e = pinNormalOrdinaryStockUdevInputs(ctx, a.source)
	if e != nil {
		return fail("readonly inputs", e)
	}
	input, _, e := p.inputs.snapshot()
	if e != nil {
		return fail("input reference", e)
	}
	inputRaw, e := json.Marshal(input)
	if e != nil {
		return fail("input bytes", e)
	}
	p.inputSHA = physicalDigest(inputRaw)
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return fail("nonce", e)
	}
	fsRaw, _ := json.Marshal(q.originalFS)
	p.reference = normalOrdinaryUdevV2PreparationReference{ProtocolVersion: 2, InstanceNonce: nonce, OriginalR2: q.current, Identity: r.Identity, OriginalInitialWAL: q.memory.InitialWAL, AuthorityPrepared: q.prepared, FilesystemSHA256: physicalDigest(fsRaw), ManifestSHA256: input.ManifestSHA256, PackageLockSHA256: input.PackageLockSHA256, RulesSHA256: input.RulesSHA256}
	kernelRaw, _ := json.Marshal(q.kernel)
	memoryRaw, _ := json.Marshal(q.memory)
	intent := normalOrdinaryStockUdevPreparationIntent{2, normalOrdinaryStockUdevPreparationScope, p.reference, physicalDigest(kernelRaw), physicalDigest(memoryRaw), p.inputSHA}
	intentRaw, e := json.Marshal(intent)
	if e != nil {
		return fail("intent bytes", e)
	}
	// Runtime.pin有mkdir副作用，必须在它之前把原输入/原准备意图file+dir fsync。
	normalOrdinaryStockPreparationStage(ctx, "intent")
	if e = s.createPhysicalFile(normalOrdinaryStockUdevPreparationIntentName, intentRaw); e != nil {
		return fail("intent durable", e)
	}
	p.history[0], e = readNormalSystemRoleFileWitness(s, normalOrdinaryStockUdevPreparationIntentName)
	if e != nil {
		return fail("intent identity", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "runtime")
	p.dirs, e = pinNormalOrdinaryStockUdevRuntimeDirs(ctx)
	if e != nil {
		return fail("runtime pins", e)
	}
	// 固定管理TLS空目录也属于同一原intent失败边界，不由请求handler补造。
	if e = p.dirs.recheck(ctx); e != nil {
		return fail("original run before TLS", e)
	}
	p.dirs.mu.Lock()
	runFD, dupErr := unix.FcntlInt(p.dirs.run.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	p.dirs.mu.Unlock()
	if dupErr != nil {
		return fail("original run duplicate", dupErr)
	}
	originalRun := os.NewFile(uintptr(runFD), "stock-original-run-TLS-preparation")
	tlsErr := prepareNormalOrdinaryManagementTLSAtRun(ctx, originalRun)
	closeErr := originalRun.Close()
	if tlsErr != nil || closeErr != nil {
		return fail("fixed management TLS preparation", errors.Join(tlsErr, closeErr))
	}
	normalOrdinaryStockPreparationStage(ctx, "kmod")
	p.kmod, e = pinNormalOrdinaryStockUdevKmodInputs(ctx, p.inputs, p.dirs)
	if e != nil {
		return fail("kmod pins", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "pre-stock-recheck")
	if e = p.checkOriginalLocked(ctx, s, true); e != nil {
		return fail("pre-stock actors/kernel", e)
	}
	if e = normalOrdinaryStockUdevNoNSCD(); e != nil {
		return fail("nscd protection", e)
	}
	// 先原连续OFD monitor，再拍sysfs目标；整个legacyRoot运行/Wait期间不留收件空窗。
	normalOrdinaryStockPreparationStage(ctx, "monitor")
	p.monitor, e = openNormalOrdinaryStockUdevMonitor()
	if e != nil {
		return fail("processed monitor", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "catalog")
	p.catalog, e = captureNormalOrdinaryUdevV2Catalog(ctx, a.source, fs.memoryConsumer, p)
	if e != nil {
		return fail("original targets", e)
	}
	targets, e := normalOrdinaryStockUdevPreparationTargets(p.catalog)
	if e != nil {
		return fail("target inventory", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "programs")
	p.programs, e = pinNormalOrdinaryStockUdevPrograms(a.source)
	if e != nil {
		return fail("program pins", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "run-wait")
	waits, log, e := p.programs.run(ctx)
	p.mu.Lock()
	p.stockLog = append([]byte(nil), log...)
	p.mu.Unlock()
	if e != nil {
		return fail("original commands/descendants Wait", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "Wait-complete")
	p.waitRaw, e = normalOrdinaryStockUdevPreparationWaitBytes(waits, log)
	if e != nil {
		return fail("original Wait receipt", e)
	}
	if e = p.checkOriginalLocked(ctx, s, true); e != nil {
		return fail("post-stock actors/kernel", e)
	}
	if e = normalOrdinaryStockUdevNoNSCD(); e != nil {
		return fail("post-stock nscd", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "drain")
	receipt, e := p.monitor.finishAfterOriginalWritersWait(ctx, targets)
	if e != nil {
		return fail("complete processed drain", e)
	}
	receiptRaw, e := json.Marshal(receipt)
	if e != nil {
		return fail("monitor receipt bytes", e)
	}
	p.monitorSHA = physicalDigest(receiptRaw)
	// 数据库函数自取Runtime.mu；它强制每个原目标均有matched add，缺/空DB不补假I。
	normalOrdinaryStockPreparationStage(ctx, "database")
	projections, e := captureNormalOrdinaryStockUdevDatabase(ctx, p.dirs, p.catalog, receipt)
	if e != nil {
		return fail("original database", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "seal")
	p.seal, e = sealNormalOrdinaryStockUdevProjection(ctx, projections)
	if e != nil {
		return fail("sealed projection", e)
	}
	p.reference.ProjectionSHA256, e = p.seal.digest()
	if e != nil {
		return fail("projection readback", e)
	}
	p.reference.WaitSHA256 = physicalDigest(p.waitRaw)
	normalOrdinaryStockPreparationStage(ctx, "handoff")
	if e = p.dirs.handoffAfterOriginalWritersWait(ctx); e != nil {
		return fail("runtime handoff", e)
	}
	if e = p.checkOriginalLocked(ctx, s, true); e != nil {
		return fail("final actors/kernel", e)
	}
	if e = p.recheckPinsLocked(ctx); e != nil {
		return fail("final original pins", e)
	}
	kref, e := p.kmod.snapshot()
	if e != nil {
		return fail("final kmod reference", e)
	}
	kraw, _ := json.Marshal(kref)
	p.kmodSHA = physicalDigest(kraw)
	done := normalOrdinaryStockUdevPreparationCompletion{2, normalOrdinaryStockUdevPreparationScope, p.history[0].SHA256, p.reference, p.kmodSHA, p.monitorSHA, uint32(len(targets)), json.RawMessage(p.waitRaw)}
	doneRaw, e := json.Marshal(done)
	if e != nil {
		return fail("completion bytes", e)
	}
	normalOrdinaryStockPreparationStage(ctx, "complete")
	if e = s.createPhysicalFile(normalOrdinaryStockUdevPreparationCompleteName, doneRaw); e != nil {
		return fail("completion durable", e)
	}
	p.history[1], e = readNormalSystemRoleFileWitness(s, normalOrdinaryStockUdevPreparationCompleteName)
	if e != nil {
		return fail("completion identity", e)
	}
	if e = p.verifyHistoryLocked(s); e != nil {
		return fail("durable original prefix", e)
	}
	p.mu.Lock()
	p.complete = true
	p.mu.Unlock()
	// 私有DEV回调可能改变对象；它必须位于最后完整校验之前。
	// 返回fresh tuple之后至caller立即接管之间不再执行任何外部回调。
	if observe, ok := ctx.Value(normalOrdinaryStockPreparationCheckpointKey{}).(func(context.Context, *lockedState, *normalOrdinaryStockUdevPreparation) error); ok {
		if e = observe(ctx, s, p); e != nil {
			return fail("original observation", e)
		}
	}
	reference, e := p.referenceLocked(ctx, s)
	if e != nil {
		return fail("final reference", e)
	}
	if e = p.handoffLive(ctx); e != nil {
		return fail("fresh reference handoff", e)
	}
	good = true
	return p, reference, nil
}

// 仅同栈移交前后的cheap状态门，不验证来源、不给Source，也不缓存完整校验。
// 两把锁分开取得，避免与projection已有的p→q顺序形成反向并持。
func (p *normalOrdinaryStockUdevPreparation) handoffLive(ctx context.Context) error {
	if p == nil || p.q == nil || ctx == nil || ctx.Err() != nil || p.q.authority == nil || p.q.authority.admission == nil {
		return ErrUnknown
	}
	p.mu.Lock()
	closed, complete := p.closed, p.complete
	p.mu.Unlock()
	if closed || !complete {
		return ErrUnknown
	}
	q := p.q
	q.mu.Lock()
	closed, failed := q.closed, q.ledger.failed
	q.mu.Unlock()
	if closed || failed != nil || q.authority.admission.failed.Load() || q.authority.admission.enabled.Load() || ctx.Err() != nil {
		return errors.Join(ErrUnknown, failed, ctx.Err())
	}
	return nil
}

// 所有来源均为原q、原ExecProof/Initial8/FS2/KernelR2/MemoryR2。仅initial=true
// 才要求原R1活着和原三actor；运行期不按旧完整record/floor或R1存活锁死合法推进。
func (p *normalOrdinaryStockUdevPreparation) checkOriginalLocked(ctx context.Context, s *lockedState, initial bool) error {
	if p == nil || p.q == nil || ctx == nil || ctx.Err() != nil || s == nil {
		return ErrUnknown
	}
	q := p.q
	a := q.authority
	if a == nil || a.admission == nil || a.admission.failed.Load() || lookupNormalSystemAuthority(q.g) != a || q.filesystem == nil || q.filesystem.g != q.g || q.proof == nil || q.filesystem.kernelConsumer == nil || q.filesystem.memoryConsumer == nil {
		return ErrUnknown
	}
	v, yes := normalOrdinaryUdevV2Slots.Load(q.g)
	if !yes {
		return ErrUnknown
	}
	slot, ok := v.(*normalOrdinaryUdevV2Slot)
	if !ok || slot == nil || slot.source != q {
		return ErrUnknown
	}
	v, yes = normalOrdinaryStockUdevPreparations.Load(q.g)
	if !yes || v != p {
		return ErrUnknown
	}
	q.mu.Lock()
	closed, failed := q.closed, q.ledger.failed
	q.mu.Unlock()
	if closed || failed != nil {
		return errors.Join(ErrUnknown, failed)
	}
	if e := normalKernelPrepV2R2Caller(); e != nil {
		return e
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != q.current {
		return errors.Join(ErrUnknown, e)
	}
	r, e := s.load()
	if e != nil || r == nil || r.Identity != q.proof.transfer.Origin.ResourceIdentity || r.Floor < q.proof.transfer.Origin.ResourceFloor || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved {
		return errors.Join(ErrUnknown, e)
	}
	witness, e := readNormalSystemRoleFileWitness(s, normalSystemAuthorityStageName(a.reference.InstanceNonce, "prepared"))
	if e != nil || witness != q.prepared || witness != a.prepared {
		return errors.Join(ErrUnknown, e)
	}
	fsref, e := q.filesystem.referenceLocked(ctx, s)
	if e != nil || fsref != q.originalFS || fsref.ProtocolVersion != 2 {
		return errors.Join(ErrUnknown, e)
	}
	kernel, e := q.filesystem.kernelConsumer.referenceLocked(ctx, s)
	if e != nil || kernel != q.kernel {
		return errors.Join(ErrUnknown, e)
	}
	// KernelR2.referenceLocked实际核modules_disabled=1、空modules、原userns RO FD；不自造scalar。
	if e = q.filesystem.memoryConsumer.recheckLocked(ctx, s, q.g); e != nil {
		return e
	}
	if q.filesystem.memoryConsumer.reference != q.memory || q.memory.InitialWALFamily != normalInitialWALFamilyKernelPrepV2 {
		return ErrUnknown
	}
	task, e := q.proof.originalTaskForFilesystemLocked(ctx, s, a.source)
	if e != nil {
		return e
	}
	task.close()
	if e = q.namespaces.verify(-1); e != nil {
		return e
	}
	if initial {
		if a.admission.enabled.Load() || q.parent == nil || q.pid1 == nil || r.Original != nil || r.Birth != nil || r.Held != nil {
			return ErrUnknown
		}
		raw, e := json.Marshal(r)
		if e != nil || physicalDigest(raw) != q.proof.transfer.Origin.ResourceStageSHA256 {
			return ErrUnknown
		}
		if e = normalOrdinaryUdevV2InitialActors(q.parent, q.current, q.pid1); e != nil {
			return e
		}
	}
	return normalOrdinaryStockUdevNoNSCD()
}
func normalOrdinaryStockUdevNoNSCD() error {
	for _, name := range []string{"/run/nscd/socket", "/var/run/nscd/socket"} {
		if _, e := os.Lstat(name); !errors.Is(e, os.ErrNotExist) {
			return errors.Join(ErrUnknown, e, fmt.Errorf("stock nscd socket/lookup must be absent"))
		}
	}
	return nil
}

// 检查原四个typed cmd.Wait记录；只做有界资料核验，传入这些值本身不能造opaque。
func normalOrdinaryStockUdevPreparationWaitBytes(waits []normalOrdinaryStockUdevWait, log []byte) ([]byte, error) {
	if len(waits) != 4 || len(log) > 32<<10 {
		return nil, ErrUnknown
	}
	stages := map[normalOrdinaryStockUdevStage]bool{}
	objects := map[[2]uint64]bool{}
	for _, w := range waits {
		if w.Stage < normalOrdinaryStockUdevDaemon || w.Stage > normalOrdinaryStockUdevSettle || stages[w.Stage] || w.PID <= 1 || w.PIDFSDevice == 0 || w.PIDFSInode == 0 || w.ExitCode != 0 || w.WaitError != "" {
			return nil, ErrUnknown
		}
		key := [2]uint64{w.PIDFSDevice, w.PIDFSInode}
		if objects[key] {
			return nil, ErrUnknown
		}
		objects[key] = true
		stages[w.Stage] = true
	}
	return json.Marshal(struct {
		Waits     []normalOrdinaryStockUdevWait `json:"original_waits"`
		LogBytes  uint32                        `json:"log_bytes"`
		LogSHA256 string                        `json:"log_sha256"`
	}{waits, uint32(len(log)), physicalDigest(log)})
}
func normalOrdinaryStockUdevPreparationTargets(c *normalOrdinaryUdevV2Catalog) ([]normalOrdinaryStockUdevMonitorTarget, error) {
	if c == nil || len(c.ordered) == 0 || len(c.ordered) > normalOrdinaryUdevV2CatalogMaximum || len(c.entries) != len(c.ordered) {
		return nil, ErrUnknown
	}
	out := make([]normalOrdinaryStockUdevMonitorTarget, 0, len(c.ordered))
	seen := map[string]bool{}
	for _, e := range c.ordered {
		if e == nil || e.uevent == nil || c.entries[e.path] != e || seen[e.path] {
			return nil, ErrUnknown
		}
		seen[e.path] = true
		props := map[string]string{}
		for k, v := range e.properties {
			if k != "DEVPATH" && k != "SUBSYSTEM" {
				props[k] = v
			}
		}
		out = append(out, normalOrdinaryStockUdevMonitorTarget{Devpath: e.path, Subsystem: e.subsystem, KernelProperties: props})
	}
	return out, nil
}

// 配置不在System的ELF-only map。只从Inputs的真实原RO pins dup，核当前只读
// 原inode/字节后再移交catalog；原OFD读取用ReadAt，不与其它消费者争共享offset。
func (p *normalOrdinaryStockUdevPreparation) duplicateOriginalRules(ctx context.Context) (out []normalOrdinaryUdevV2RuleSource, err error) {
	if p == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.inputs == nil {
		return nil, ErrUnknown
	}
	if e := p.inputs.recheck(ctx); e != nil {
		return nil, e
	}
	i := p.inputs
	i.mu.Lock()
	defer i.mu.Unlock()
	held := []*os.File{}
	good := false
	defer func() {
		if !good {
			for _, f := range held {
				f.Close()
			}
		}
	}()
	for _, f := range i.reference.Files {
		if f.Kind != "rule" && f.Kind != "acl-override" {
			continue
		}
		if !strings.HasSuffix(f.Path, ".rules") || (!strings.HasPrefix(f.Path, "/lib/udev/rules.d/") && !strings.HasPrefix(f.Path, "/etc/udev/rules.d/")) {
			return nil, ErrUnknown
		}
		original := i.pins[f.Path]
		if original == nil {
			return nil, ErrUnknown
		}
		fd, e := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 3)
		if e != nil {
			return nil, e
		}
		pin := os.NewFile(uintptr(fd), "original-stock-rules-catalog-dup")
		flags, e := unix.FcntlInt(pin.Fd(), unix.F_GETFL, 0)
		if e != nil || flags&unix.O_ACCMODE != unix.O_RDONLY || flags&unix.O_PATH != 0 {
			pin.Close()
			return nil, ErrUnknown
		}
		fact, e := normalOrdinaryStockUdevFileFact(pin, f.Path, f.CanonicalPath, f.Kind, f.Code)
		raw, re := normalOrdinaryStockUdevReadAt(pin, 256<<10)
		if e != nil || re != nil || fact.Object != f.Object || !fact.Object.Readonly || fact.Mode != f.Mode || fact.UID != f.UID || fact.GID != f.GID || fact.Ctime != f.Ctime || fact.Mtime != f.Mtime || uint64(len(raw)) != f.Bytes || physicalDigest(raw) != f.SHA256 {
			pin.Close()
			return nil, errors.Join(ErrUnknown, e, re)
		}
		held = append(held, pin)
		out = append(out, normalOrdinaryUdevV2RuleSource{path: f.Path, sha: f.SHA256, raw: raw, original: pin})
	}
	if len(out) == 0 {
		return nil, ErrUnknown
	}
	sort.Slice(out, func(a, b int) bool { return out[a].path < out[b].path })
	good = true
	return out, nil
}
func (p *normalOrdinaryStockUdevPreparation) recheckPinsLocked(ctx context.Context) error {
	if p.inputs == nil || p.dirs == nil || p.kmod == nil || p.catalog == nil || p.programs == nil || p.seal == nil || p.monitor == nil {
		return ErrUnknown
	}
	if e := p.inputs.recheck(ctx); e != nil {
		return e
	}
	if e := p.kmod.recheck(ctx); e != nil {
		return e
	}
	if e := p.dirs.recheck(ctx); e != nil {
		return e
	}
	p.dirs.mu.Lock()
	phase := p.dirs.phase
	p.dirs.mu.Unlock()
	if phase != 3 {
		return ErrUnknown
	}
	p.monitor.mu.Lock()
	monitorErr := p.monitor.verify()
	monitorGood := p.monitor.finished && !p.monitor.closed && p.monitor.err == nil
	p.monitor.mu.Unlock()
	if monitorErr != nil || !monitorGood {
		return errors.Join(ErrUnknown, monitorErr)
	}
	if e := p.catalog.verify(); e != nil {
		return e
	}
	if e := p.programs.verify(); e != nil {
		return e
	}
	sha, e := p.seal.digest()
	if e != nil || sha != p.reference.ProjectionSHA256 || physicalDigest(p.waitRaw) != p.reference.WaitSHA256 {
		return errors.Join(ErrUnknown, e)
	}
	return ctx.Err()
}
func (p *normalOrdinaryStockUdevPreparation) verifyHistoryLocked(s *lockedState) error {
	for i, name := range []string{normalOrdinaryStockUdevPreparationIntentName, normalOrdinaryStockUdevPreparationCompleteName} {
		got, e := readNormalSystemRoleFileWitness(s, name)
		if e != nil || got != p.history[i] {
			return errors.Join(ErrUnknown, e, fmt.Errorf("stock original persistent prefix changed"))
		}
	}
	return nil
}
func (p *normalOrdinaryStockUdevPreparation) referenceLocked(ctx context.Context, s *lockedState) (normalOrdinaryUdevV2PreparationReference, error) {
	var zero normalOrdinaryUdevV2PreparationReference
	if e := p.checkOriginalLocked(ctx, s, false); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.complete {
		return zero, ErrUnknown
	}
	if e := p.recheckPinsLocked(ctx); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	if e := p.verifyHistoryLocked(s); e != nil {
		return zero, e
	}
	input, _, e := p.inputs.snapshot()
	if e != nil {
		return zero, e
	}
	raw, _ := json.Marshal(input)
	kmod, e := p.kmod.snapshot()
	if e != nil {
		return zero, e
	}
	kraw, _ := json.Marshal(kmod)
	if physicalDigest(raw) != p.inputSHA || physicalDigest(kraw) != p.kmodSHA {
		return zero, ErrUnknown
	}
	return p.reference, nil
}
func (p *normalOrdinaryStockUdevPreparation) projectionForOriginalDeviceLocked(ctx context.Context, s *lockedState, entry *normalOrdinaryUdevV2CatalogEntry) (normalOrdinaryUdevV2DeviceProjection, error) {
	var zero normalOrdinaryUdevV2DeviceProjection
	// 仅同步安装原初始catalog。q.prepare已经完整取过reference，末尾再全核；
	// 此getter只提供sealed资料，不授Source/运行许可，不能在COMMIT后重拍设备。
	if p == nil || p.q == nil || ctx == nil || ctx.Err() != nil || s == nil || p.q.authority == nil || p.q.authority.admission == nil || p.q.authority.admission.enabled.Load() || p.q.authority.admission.failed.Load() {
		return zero, ErrUnknown
	}
	if e := normalKernelPrepV2R2Caller(); e != nil {
		return zero, e
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != p.q.current {
		return zero, ErrUnknown
	}
	v, yes := normalOrdinaryStockUdevPreparations.Load(p.q.g)
	if !yes || v != p {
		return zero, ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.complete || entry == nil || p.catalog == nil || p.seal == nil {
		return zero, ErrUnknown
	}
	if e := p.verifyHistoryLocked(s); e != nil {
		return zero, e
	}
	p.q.mu.Lock()
	closed, failed := p.q.closed, p.q.ledger.failed
	p.q.mu.Unlock()
	if closed || failed != nil {
		return zero, ErrUnknown
	}
	old := p.catalog.entries[entry.path]
	if old == nil || old.subsystem != entry.subsystem || old.object != entry.object || !reflect.DeepEqual(old.properties, entry.properties) {
		return zero, ErrUnknown
	}
	sealed, e := p.seal.projection(entry.path)
	if e != nil {
		return zero, e
	}
	if sealed.Subsystem != entry.subsystem || sealed.UeventObject != entry.object {
		return zero, ErrUnknown
	}
	return sealed, nil
}

// q.close只对原实例调用一次。本清理只关自己拥有的FD，不回g/FS/proof/System，
// 不删WAL/数据库，不把close当Wait或回收证明，也不重跑已退出Rootwriter。
func (p *normalOrdinaryStockUdevPreparation) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.monitor != nil {
		p.monitor.close()
	}
	if p.catalog != nil {
		p.catalog.close()
	}
	if p.programs != nil {
		p.programs.close()
	}
	if p.seal != nil {
		p.seal.close()
	}
	if p.kmod != nil {
		p.kmod.close()
	}
	if p.dirs != nil {
		p.dirs.close()
	}
	if p.inputs != nil {
		p.inputs.close()
	}
}
