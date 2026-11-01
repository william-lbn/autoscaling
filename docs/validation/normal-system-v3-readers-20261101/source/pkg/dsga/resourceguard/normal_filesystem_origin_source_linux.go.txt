//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalFilesystemOriginIntentName = "normal-filesystem-origin.intent.json"
const normalFilesystemOriginCompleteName = "normal-filesystem-origin.complete.json"

type normalFilesystemMountFact struct {
	Mount  normalFilesystemMount  `json:"mount"`
	Object normalFilesystemObject `json:"object"`
}
type normalFilesystemOriginIntent struct {
	StateDevice          uint64                           `json:"state_device"`
	StateInode           uint64                           `json:"state_inode"`
	LockDevice           uint64                           `json:"lock_device"`
	LockInode            uint64                           `json:"lock_inode"`
	LayoutVersion        uint32                           `json:"layout_version"`
	ProtocolVersion      uint32                           `json:"protocol_version"`
	Scope                string                           `json:"scope"`
	Nonce                string                           `json:"nonce"`
	System               normalSystemLifecycleBinding     `json:"original_system_source"`
	SystemSourceSHA256   string                           `json:"system_source_sha256"`
	VminitSHA256         string                           `json:"vminit_sha256"`
	InitialReceiptSHA256 string                           `json:"initial_receipt_sha256"`
	InitialPrefixSHA256  [8]string                        `json:"initial_prefix_sha256"`
	ResourceStageSHA256  string                           `json:"resource_stage_sha256"`
	PID1StartTicks       uint64                           `json:"pid1_start_ticks"`
	Namespace            normalFilesystemObject           `json:"namespace"`
	Root                 normalFilesystemObject           `json:"root"`
	Mounts               []normalFilesystemMountFact      `json:"mounts"`
	Tasks                [2]normalFilesystemTaskInventory `json:"initial_task_inventory"`
}
type normalFilesystemOriginComplete struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Scope           string `json:"scope"`
	IntentSHA256    string `json:"intent_sha256"`
}
type normalFilesystemOriginObservation struct {
	Scope                       string                      `json:"scope"`
	IntentSHA256                string                      `json:"intent_sha256"`
	CompletionSHA256            string                      `json:"completion_sha256"`
	OriginalRoot                nativeBirthConsumer         `json:"original_root"`
	CurrentRoot                 nativeBirthConsumer         `json:"current_root"`
	GuestBootID                 string                      `json:"guest_boot_id"`
	VminitSHA256                string                      `json:"vminit_sha256"`
	Mounts                      []normalFilesystemMountFact `json:"mounts"`
	OriginalFDInventoryCaptured bool                        `json:"original_fd_inventory_captured"`
	DurabilityConfirmed         bool                        `json:"durability_confirmed"`
	ContinuousWritersClosed     bool                        `json:"continuous_writers_closed"`
	NormalDecreaseAuthorized    bool                        `json:"normal_decrease_authorized"`
	MissingSources              []string                    `json:"missing_sources"`
}
type normalFilesystemOriginWitnessKey struct {
	Guard    *Guard
	Consumer nativeBirthConsumer
	Prefix   initialPrebootGuestWitnessKey
	Files    [2]arrayPopulateHistoryIdentity
	SHA256   [2]string
}

// 每Guard+真实state/lock+当前Root+全部10原文件inode/ctime/摘要的进程私有见证。
// visible complete、新Guard、新Root、同字节换inode都不能共享完成许可。
var normalFilesystemOriginWitnesses sync.Map

type normalFilesystemOriginSource struct {
	mu         sync.Mutex
	closed     bool
	g          *Guard
	system     *normalSystemLifecycleSource
	pid1, self *normalFilesystemTask
	intent     normalFilesystemOriginIntent
	witness    normalFilesystemOriginWitnessKey
	current    nativeBirthConsumer
}

func (o *normalFilesystemOriginSource) close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.closed {
		o.self.close()
		o.pid1.close()
		o.closed = true
	}
}
func (o *normalFilesystemOriginSource) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("filesystem future mount/FD/control-writer and original role lifecycle qualification missing"))
}

func normalFilesystemSourceMetadata(system *normalSystemLifecycleSource) (normalSystemLifecycleBinding, string, string, error) {
	var zero normalSystemLifecycleBinding
	if system == nil {
		return zero, "", "", fmt.Errorf("original opaque System source unavailable")
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return zero, "", "", e
	}
	o, e := system.snapshot()
	if e != nil || o.Binding.RootConsumer != consumer {
		return zero, "", "", fmt.Errorf("filesystem current System producer differs")
	}
	var systemStat unix.Stat_t
	if unix.Stat(systemCgroup, &systemStat) != nil || systemStat.Dev != o.Binding.SystemDevice || systemStat.Ino != o.Binding.SystemInode {
		return zero, "", "", fmt.Errorf("filesystem original System domain identity differs")
	}
	currentSystem, err := splitReadNode(systemCgroup)
	if err != nil || !currentSystem.RootControlled || uint64(currentSystem.MemoryMaxBytes) != o.Binding.SystemMemoryMaxBytes || currentSystem.SwapMaxBytes != 0 || !currentSystem.MemoryHighUnlimited {
		return zero, "", "", fmt.Errorf("filesystem original System controls differ")
	}
	// 原引用每次都对应固定实际文件字节，缓存snapshot本身不是当前文件证明。
	for _, item := range []struct {
		path    string
		maximum int64
		want    string
		iso     bool
	}{
		{FixedRuntimePath, 64 << 10, o.Binding.RuntimeDocumentSHA256, true},
		{fixedNativeBirthPolicyPath, 16 << 10, o.Binding.NativePolicySHA256, true},
		{fixedNativeBirthBuildPath, 32 << 10, o.Binding.NativeBuildSHA256, false},
		{NormalSystemSourceManifestPath, 1 << 20, o.Binding.SystemManifestSHA256, false},
	} {
		var raw []byte
		var err error
		if item.iso {
			raw, err = nativeBirthReadISO(item.path, item.maximum)
		} else {
			f, openErr := nativeBirthOpenRO(item.path)
			err = openErr
			if err == nil {
				raw, err = nativeBirthReadBounded(f, item.maximum)
				f.Close()
			}
		}
		if err != nil || physicalDigest(raw) != item.want {
			return zero, "", "", fmt.Errorf("filesystem original fixed source bytes differ")
		}
	}
	if _, err := sealedRuntimeSource(); err != nil {
		return zero, "", "", err
	}
	ref, e := system.referenceSHA256()
	if e != nil {
		return zero, "", "", e
	}
	system.mu.Lock()
	defer system.mu.Unlock()
	if system.closed || system.manifest.ProtocolVersion != 2 && system.manifest.ProtocolVersion != 3 {
		return zero, "", "", fmt.Errorf("filesystem requires original System v2/v3 source")
	}
	vminit := ""
	for _, f := range system.manifest.Files {
		if f.Path == "/neonvm/bin/vminit" && f.CanonicalPath == f.Path && f.Kind == "script" {
			vminit = f.SHA256
		}
	}
	if !physicalDigestValid(vminit) {
		return zero, "", "", fmt.Errorf("filesystem vminit original member missing")
	}
	f, err := nativeBirthOpenRO("/neonvm/bin/vminit")
	if err != nil {
		return zero, "", "", err
	}
	actual, err := nativeBirthFileHash(f)
	f.Close()
	if err != nil || actual != vminit {
		return zero, "", "", fmt.Errorf("filesystem original vminit bytes differ")
	}
	return o.Binding, ref, vminit, nil
}
func normalFilesystemSourceSame(a, b normalSystemLifecycleBinding) bool {
	// 原producer实例永远保存在intent，新的broker实例另绑定。不能把新Root nonce
	// 写成旧producer；只有其余原ISO/build/System/boot/cgroup字段允许逐字比较。
	a.RootConsumer, b.RootConsumer = nativeBirthConsumer{}, nativeBirthConsumer{}
	return a == b
}
func normalFilesystemTaskExecutable(t *normalFilesystemTask, system *normalSystemLifecycleSource, name string) error {
	status, e := normalFilesystemReadAt(int(t.procfd.Fd()), "status", 64<<10)
	if e != nil {
		return e
	}
	for _, key := range []string{"Uid", "Gid"} {
		ids, e := nativeTraceStatusValues(string(status), key, 4)
		if e != nil {
			return e
		}
		for _, id := range ids {
			if id != 0 {
				return fmt.Errorf("filesystem initial task is not original root identity")
			}
		}
	}
	group, e := normalFilesystemReadAt(int(t.procfd.Fd()), "cgroup", 64<<10)
	if e != nil || strings.TrimSpace(string(group)) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fmt.Errorf("filesystem original task is not in fixed System domain")
	}
	actual, e := normalFilesystemOpenAt(int(t.procfd.Fd()), "exe", unix.O_RDONLY)
	if e != nil {
		return e
	}
	defer actual.Close()
	system.mu.Lock()
	defer system.mu.Unlock()
	if system.closed {
		return ErrUnknown
	}
	expected := system.files[system.logical[name]]
	if expected == nil {
		return fmt.Errorf("filesystem task executable absent from original System closure")
	}
	var a, b unix.Stat_t
	if unix.Fstat(int(actual.Fd()), &a) != nil || unix.Fstat(int(expected.Fd()), &b) != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return fmt.Errorf("filesystem task executable original inode differs")
	}
	got, e := nativeBirthFileHash(actual)
	if e != nil {
		return e
	}
	want, e := nativeBirthFileHash(expected)
	originalSHA := ""
	for _, member := range system.observed.ELFs {
		if member.Path == system.logical[name] {
			originalSHA = member.SHA256
		}
	}
	if e != nil || got != want || got != originalSHA {
		return fmt.Errorf("filesystem task executable bytes differ")
	}
	return nil
}

func normalFilesystemCurrentGraph(ctx context.Context, ns, root normalFilesystemObject) ([]normalFilesystemMountFact, map[uint64]bool, error) {
	// mount namespace/cwd可以按OS线程不同；在同一M读取实际namespace、mountinfo和
	// 每个O_PATH对象，不能用leader一次status代替实际执行线程。
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	n, e := unix.Open("/proc/thread-self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, nil, e
	}
	actual, e := normalFilesystemFDObject(n)
	unix.Close(n)
	if e != nil || actual.Device != ns.Device || actual.Inode != ns.Inode {
		return nil, nil, fmt.Errorf("filesystem current namespace differs from original")
	}
	n, e = unix.Open("/", unix.O_PATH|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, nil, e
	}
	actual, e = normalFilesystemFDObject(n)
	unix.Close(n)
	if e != nil || actual != root {
		return nil, nil, fmt.Errorf("filesystem current root differs from PID1 original")
	}
	raw, e := os.ReadFile("/proc/thread-self/mountinfo")
	if e != nil {
		return nil, nil, e
	}
	mounts, e := normalFilesystemParseMounts(raw)
	if e != nil {
		return nil, nil, e
	}
	if e = normalFilesystemValidateGraph(mounts); e != nil {
		return nil, nil, e
	}
	sort.Slice(mounts, func(i, j int) bool { return mounts[i].Point < mounts[j].Point })
	facts := make([]normalFilesystemMountFact, 0, len(mounts))
	allowed := map[uint64]bool{}
	for _, m := range mounts {
		if e = ctx.Err(); e != nil {
			return nil, nil, e
		}
		canonical, e := filepath.EvalSymlinks(m.Point)
		if e != nil || canonical != m.Point {
			return nil, nil, fmt.Errorf("filesystem fixed mount logical/canonical path differs")
		}
		fd, e := unix.Open(m.Point, unix.O_PATH|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if e != nil {
			return nil, nil, e
		}
		object, e := normalFilesystemFDObject(fd)
		unix.Close(fd)
		if e != nil {
			return nil, nil, e
		}
		fsTypes := map[string]int64{"ext4": unix.EXT4_SUPER_MAGIC, "tmpfs": unix.TMPFS_MAGIC, "proc": unix.PROC_SUPER_MAGIC, "sysfs": unix.SYSFS_MAGIC, "devtmpfs": unix.TMPFS_MAGIC, "devpts": unix.DEVPTS_SUPER_MAGIC, "cgroup2": unix.CGROUP2_SUPER_MAGIC, "iso9660": unix.ISOFS_SUPER_MAGIC, "securityfs": unix.SECURITYFS_MAGIC}
		if object.MountID != m.ID || object.Filesystem != fsTypes[m.FSType] || object.Readonly != m.Options["ro"] || fmt.Sprintf("%d:%d", unix.Major(object.Device), unix.Minor(object.Device)) != m.Device {
			return nil, nil, fmt.Errorf("filesystem O_PATH/statfs/mountID differs from actual graph")
		}
		if m.FSType == "ext4" && m.Point != "/" {
			allowed[m.ID] = true
		}
		facts = append(facts, normalFilesystemMountFact{m, object})
	}
	// 所有静态逻辑与canonical祖先均由System原reader验证；这里独立核固定孔的
	// 每一祖先。任何额外RW覆盖已被17mount图拒绝，不用路径字符串冒充mount对象。
	for _, hole := range normalFilesystemPersistentHoles {
		for at := filepath.Dir(hole); ; at = filepath.Dir(at) {
			var fs unix.Statfs_t
			if e := unix.Statfs(at, &fs); e != nil || fs.Type != unix.EXT4_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
				return nil, nil, fmt.Errorf("filesystem persistent-hole ancestor is not original readonly ext4")
			}
			if at == "/" {
				break
			}
		}
	}
	return facts, allowed, nil
}

func normalFilesystemDecodeRecord(raw []byte, out any) error {
	if e := control.UnmarshalObject(raw, out); e != nil {
		return e
	}
	b, e := json.Marshal(out)
	if e != nil || !bytes.Equal(raw, b) {
		return fmt.Errorf("filesystem original stage is not exact canonical producer output")
	}
	return nil
}
func normalFilesystemInitialPrefix(s *lockedState, system normalSystemLifecycleBinding, capture bool) (initialPrebootGuestIntent, initialPrebootGuestComplete, initialPrebootGuestWitnessKey, error) {
	var in initialPrebootGuestIntent
	var done initialPrebootGuestComplete
	var empty initialPrebootGuestWitnessKey
	for name, target := range map[string]any{initialPrebootGuestIntentName: &in, initialPrebootGuestCompleteName: &done} {
		raw, exists, e := s.readPhysicalFile(name)
		if e != nil || !exists {
			return in, done, empty, errors.Join(ErrUnknown, fmt.Errorf("filesystem original initial preparation missing"), e)
		}
		if e = normalFilesystemDecodeRecord(raw, target); e != nil {
			return in, done, empty, e
		}
	}
	if in.ProtocolVersion != 1 || in.Scope != initialPrebootGuestScope || in.Challenge.Validate() != nil || in.Challenge.RuntimeSHA256 != system.RuntimeDocumentSHA256 || in.GuestBootID != system.GuestBootID || in.Challenge.Identity.VMUID != system.VMUID || done.PreparedPhysical.Identity.ComputeInstanceID != system.ComputeInstanceID || done.ProtocolVersion != 1 || done.Scope != initialPrebootGuestScope || done.PreparedPhysical.Identity.GuestBootID != system.GuestBootID {
		return in, done, empty, fmt.Errorf("filesystem original initial source binding differs")
	}
	state, e := initialPrebootStateIdentity(s)
	if e != nil || in.StateDevice != control.Uint64(state.Dev) || in.StateInode != control.Uint64(state.Ino) {
		return in, done, empty, fmt.Errorf("filesystem original preparation state inode differs")
	}
	key, e := initialPrebootGuestWitnessLocked(s, in.Challenge)
	if e != nil {
		return in, done, empty, e
	}
	if done.IntentSHA256 != key.SHA256[6] {
		return in, done, empty, fmt.Errorf("filesystem original aggregate prefix differs")
	}
	if capture {
		if done.IssuingRoot != system.RootConsumer {
			return in, done, empty, fmt.Errorf("filesystem first capture cannot acquire another Root preparation")
		}
		if _, ok := initialPrebootGuestWitnesses.Load(key); !ok {
			return in, done, empty, fmt.Errorf("filesystem first capture lacks original initial durability witness")
		}
	}
	return in, done, key, nil
}
func normalFilesystemWitnessLocked(g *Guard, s *lockedState, c nativeBirthConsumer, prefix initialPrebootGuestWitnessKey) (normalFilesystemOriginWitnessKey, error) {
	k := normalFilesystemOriginWitnessKey{Guard: g, Consumer: c, Prefix: prefix}
	for i, name := range []string{normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName} {
		raw, stamp, e := arrayPopulateHistoryRaw(s, name)
		if e != nil {
			return k, e
		}
		k.Files[i], k.SHA256[i] = stamp, physicalDigest(raw)
	}
	return k, nil
}

// 最后完成必须同步原8WAL+本2WAL后重读所有inode/ctime/准确字节。同步失败后
// 即便complete可见也不登记；普通Observe从不调用本函数或任何Sync。
func normalFilesystemConfirmPrefix(ctx context.Context, syncOne func(string) error, fingerprint func() (normalFilesystemOriginWitnessKey, error)) (normalFilesystemOriginWitnessKey, error) {
	before, e := fingerprint()
	if e != nil {
		return before, e
	}
	names := append(append([]string(nil), initialPrebootGuestFiles[:]...), normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName)
	for _, name := range names {
		if e = ctx.Err(); e != nil {
			return before, e
		}
		if e = syncOne(name); e != nil {
			return before, e
		}
	}
	after, e := fingerprint()
	if e != nil || before != after {
		return before, errors.Join(fmt.Errorf("filesystem original prefix changed during sync"), e)
	}
	return after, nil
}

func (g *Guard) captureNormalFilesystemOrigin(ctx context.Context, system *normalSystemLifecycleSource) (*normalFilesystemOriginSource, error) {
	if g == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	s, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return nil, e
	}
	defer s.close()
	r, e := s.load()
	if e != nil {
		return nil, e
	}
	return g.captureNormalFilesystemOriginLocked(ctx, s, r, system)
}

// 初始协议调用者已持原resource flock时用此变体，绝不再调用公开Observe/重入flock。
// 捕获时点是Source seal、原8准备WAL和可选正式Bootstrap之后、PID1启动其它角色之前。
func (g *Guard) captureNormalFilesystemOriginLocked(ctx context.Context, s *lockedState, r *record, system *normalSystemLifecycleSource) (_ *normalFilesystemOriginSource, err error) {
	fail := func(e error) (*normalFilesystemOriginSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 || os.Getppid() != 1 {
		return fail(fmt.Errorf("filesystem first capture requires original initial-boot Root"))
	}
	argv, e := normalFilesystemReadSelfArgv()
	if e != nil || !reflect.DeepEqual(argv, []string{"/neonvm/bin/dsga-resource-guard", "initial-boot"}) {
		return fail(fmt.Errorf("filesystem first capture requires fixed original initial-boot entry"))
	}
	binding, reference, vminit, e := normalFilesystemSourceMetadata(system)
	if e != nil {
		return fail(e)
	}
	in, done, prefix, e := normalFilesystemInitialPrefix(s, binding, true)
	if e != nil {
		return fail(e)
	}
	sealed, e := sealedRuntimeSource()
	if e != nil || sealed.Config != g.config {
		return fail(fmt.Errorf("filesystem sealed resource source differs"))
	}
	if r != nil && (r.Bootstrap == nil || sameBootstrapSource(sealed, r.Bootstrap.Original) != nil) {
		return fail(fmt.Errorf("filesystem original Bootstrap source differs"))
	}
	loaded, e := s.load()
	if e != nil || !reflect.DeepEqual(loaded, r) {
		return fail(fmt.Errorf("filesystem capture record changed or is not original lock value"))
	}
	if r != nil && (r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth != nil || r.Original != nil || r.validate(done.PreparedPhysical.Identity) != nil) {
		return fail(fmt.Errorf("filesystem first capture cannot acquire workload/history state"))
	}
	for _, name := range []string{normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName} {
		_, exists, e := s.readPhysicalFile(name)
		if e != nil || exists {
			return fail(errors.Join(fmt.Errorf("filesystem origin history exists; observe original opaque source or explicitly recover complete prefix"), e))
		}
	}
	o := &normalFilesystemOriginSource{g: g, system: system, current: binding.RootConsumer}
	ok := false
	defer func() {
		if !ok {
			o.close()
		}
	}()
	if o.pid1, e = normalFilesystemPinInitialTask(true); e != nil {
		return fail(e)
	}
	if o.self, e = normalFilesystemPinInitialTask(false); e != nil {
		return fail(e)
	}
	if o.self.ns.Device != o.pid1.ns.Device || o.self.ns.Inode != o.pid1.ns.Inode || o.self.rootObject != o.pid1.rootObject {
		return fail(fmt.Errorf("filesystem Root/PID1 namespace or root differs"))
	}
	for name, task := range map[string]*normalFilesystemTask{"/neonvm/bin/busybox": o.pid1, "/neonvm/bin/dsga-resource-guard": o.self} {
		if e = normalFilesystemTaskExecutable(task, system, name); e != nil {
			return fail(e)
		}
	}
	if e = normalFilesystemRejectOtherInitialUsers(); e != nil {
		return fail(e)
	}
	mounts, allowed, e := normalFilesystemCurrentGraph(ctx, o.pid1.ns, o.pid1.rootObject)
	if e != nil {
		return fail(e)
	}
	var inventories [2]normalFilesystemTaskInventory
	if inventories[0], e = o.pid1.inspect(ctx, o.pid1.ns, o.pid1.rootObject, allowed); e != nil {
		return fail(e)
	}
	if inventories[1], e = o.self.inspect(ctx, o.pid1.ns, o.pid1.rootObject, allowed); e != nil {
		return fail(e)
	}
	if e = normalFilesystemRejectOtherInitialUsers(); e != nil {
		return fail(e)
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return fail(e)
	}
	receipt, e := initialPrebootOriginalReceipt(in, done)
	if e != nil {
		return fail(e)
	}
	rawReceipt, _ := json.Marshal(receipt)
	rawStage, _ := json.Marshal(r)
	o.intent = normalFilesystemOriginIntent{StateDevice: prefix.Device, StateInode: prefix.Inode, LockDevice: prefix.LockDevice, LockInode: prefix.LockInode, LayoutVersion: normalFilesystemLayoutVersion, ProtocolVersion: 1, Scope: normalFilesystemOriginScope, Nonce: nonce, System: binding, SystemSourceSHA256: reference, VminitSHA256: vminit, InitialReceiptSHA256: physicalDigest(rawReceipt), InitialPrefixSHA256: prefix.SHA256, ResourceStageSHA256: physicalDigest(rawStage), PID1StartTicks: o.pid1.start, Namespace: o.pid1.ns, Root: o.pid1.rootObject, Mounts: mounts, Tasks: inventories}
	if e = normalFilesystemValidateIntent(o.intent); e != nil {
		return fail(e)
	}
	raw, e := json.Marshal(o.intent)
	if e != nil {
		return fail(e)
	}
	if e = s.createPhysicalFile(normalFilesystemOriginIntentName, raw); e != nil {
		return fail(e)
	}
	current, _, e := normalFilesystemCurrentGraph(ctx, o.intent.Namespace, o.intent.Root)
	if e != nil || !reflect.DeepEqual(current, mounts) {
		return fail(fmt.Errorf("filesystem graph changed before completion"))
	}
	complete, _ := json.Marshal(normalFilesystemOriginComplete{1, normalFilesystemOriginScope, physicalDigest(raw)})
	if e = s.createPhysicalFile(normalFilesystemOriginCompleteName, complete); e != nil {
		return fail(e)
	}
	fingerprint := func() (normalFilesystemOriginWitnessKey, error) {
		_, _, p, e := normalFilesystemInitialPrefix(s, binding, false)
		if e != nil {
			return normalFilesystemOriginWitnessKey{}, e
		}
		return normalFilesystemWitnessLocked(g, s, binding.RootConsumer, p)
	}
	o.witness, e = normalFilesystemConfirmPrefix(ctx, s.syncPhysicalFile, fingerprint)
	if e != nil {
		return fail(e)
	}
	normalFilesystemOriginWitnesses.Store(o.witness, struct{}{})
	ok = true
	return o, nil
}

func normalFilesystemReadSelfArgv() ([]string, error) {
	fd, e := unix.Open("/proc/self", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	defer unix.Close(fd)
	return normalSystemReadVector(fd, "cmdline")
}

func (o *normalFilesystemOriginSource) observe(ctx context.Context) (normalFilesystemOriginObservation, error) {
	var zero normalFilesystemOriginObservation
	if o == nil || ctx == nil || ctx.Err() != nil {
		return zero, ErrUnknown
	}
	// 所有入口统一resource flock→FS.mu→System.mu→Task.mu。
	// 不能先持FS.mu等flock，否则同锁StartBroker/reference会反序死锁。
	s, e := lockState(ctx, o.g.config.StateDir)
	if e != nil {
		return zero, e
	}
	defer s.close()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return zero, ErrUnknown
	}
	return o.observeLocked(ctx, s)
}
func (o *normalFilesystemOriginSource) observeLocked(ctx context.Context, s *lockedState) (normalFilesystemOriginObservation, error) {
	var zero normalFilesystemOriginObservation
	if e := normalFilesystemReferenceMatches(o.system, o.intent); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	bind, _, vminit, e := normalFilesystemSourceMetadata(o.system)
	if e != nil || !normalFilesystemSourceSame(o.intent.System, bind) || bind.RootConsumer != o.current || vminit != o.intent.VminitSHA256 {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("filesystem original source changed"), e)
	}
	_, _, prefix, e := normalFilesystemInitialPrefix(s, bind, false)
	if e != nil || prefix.SHA256 != o.intent.InitialPrefixSHA256 || prefix.Device != o.intent.StateDevice || prefix.Inode != o.intent.StateInode || prefix.LockDevice != o.intent.LockDevice || prefix.LockInode != o.intent.LockInode {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("filesystem original preparation prefix changed"), e)
	}
	key, e := normalFilesystemWitnessLocked(o.g, s, o.current, prefix)
	if e != nil || key != o.witness {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("filesystem original file identity changed"), e)
	}
	if _, ok := normalFilesystemOriginWitnesses.Load(key); !ok {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("filesystem original prefix has no current Guard durability witness"))
	}
	if e = o.pid1.verify(); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	if e = o.self.verify(); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	for name, task := range map[string]*normalFilesystemTask{"/neonvm/bin/busybox": o.pid1, "/neonvm/bin/dsga-resource-guard": o.self} {
		if e = normalFilesystemTaskExecutable(task, o.system, name); e != nil {
			return zero, errors.Join(ErrUnknown, e)
		}
	}
	mounts, _, e := normalFilesystemCurrentGraph(ctx, o.intent.Namespace, o.intent.Root)
	if e != nil || !reflect.DeepEqual(mounts, o.intent.Mounts) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("filesystem original graph changed"), e)
	}
	// 运行期只复核原引用和实际namespace/root/graph。不重新要求PGEmpty或两task，
	// 更不会把当前FD清单回填成原出生历史。未来writer/角色来源仍明确缺失。
	return normalFilesystemOriginObservation{Scope: normalFilesystemOriginScope, IntentSHA256: key.SHA256[0], CompletionSHA256: key.SHA256[1], OriginalRoot: o.intent.System.RootConsumer, CurrentRoot: o.current, GuestBootID: bind.GuestBootID, VminitSHA256: vminit, Mounts: mounts, OriginalFDInventoryCaptured: true, DurabilityConfirmed: true, MissingSources: []string{"continuous-original-namespace-mount-control-writers", "future-role-spawn-FD-and-SCM_RIGHTS-source-closure"}}, nil
}

func normalFilesystemValidateIntent(in normalFilesystemOriginIntent) error {
	if in.StateDevice == 0 || in.StateInode == 0 || in.LockDevice != in.StateDevice || in.LockInode == 0 || in.ProtocolVersion != 1 || in.LayoutVersion != normalFilesystemLayoutVersion || in.Scope != normalFilesystemOriginScope || !physicalDigestValid(in.Nonce) || !validGuestBoot(in.System.GuestBootID) || in.System.RootConsumer.PID <= 1 || in.System.RootConsumer.StartTicks == 0 || !physicalDigestValid(in.System.RootConsumer.InstanceNonce) || in.PID1StartTicks == 0 || in.Namespace.Device == 0 || in.Namespace.Inode == 0 || in.Root.Device == 0 || in.Root.Inode == 0 || in.Root.MountID == 0 || in.Root.Filesystem != unix.EXT4_SUPER_MAGIC || !in.Root.Readonly {
		return fmt.Errorf("filesystem original intent identity/geometry invalid")
	}
	for _, h := range append(append([]string{in.SystemSourceSHA256, in.VminitSHA256, in.InitialReceiptSHA256, in.ResourceStageSHA256}, in.InitialPrefixSHA256[:]...), in.System.RuntimeDocumentSHA256, in.System.NativePolicySHA256, in.System.NativeBuildSHA256, in.System.SystemManifestSHA256) {
		if !physicalDigestValid(h) {
			return fmt.Errorf("filesystem original intent digest missing")
		}
	}
	if in.Tasks[0].PID != 1 || in.Tasks[0].StartTicks != in.PID1StartTicks || in.Tasks[1].PID != in.System.RootConsumer.PID || in.Tasks[1].StartTicks != uint64(in.System.RootConsumer.StartTicks) || in.Tasks[0].Threads == 0 || in.Tasks[1].Threads == 0 {
		return fmt.Errorf("filesystem original task inventory differs")
	}
	mounts := make([]normalFilesystemMount, len(in.Mounts))
	previous := ""
	for i, f := range in.Mounts {
		if f.Mount.Point <= previous || f.Object.MountID != f.Mount.ID || f.Object.Inode == 0 || f.Object.Device == 0 || f.Object.Readonly != f.Mount.Options["ro"] {
			return fmt.Errorf("filesystem original mount facts are not ordered/matching")
		}
		previous = f.Mount.Point
		mounts[i] = f.Mount
	}
	return normalFilesystemValidateGraph(mounts)
}
func normalFilesystemReferenceMatches(system *normalSystemLifecycleSource, in normalFilesystemOriginIntent) error {
	o, e := system.snapshot()
	if e != nil {
		return e
	}
	if !normalFilesystemSourceSame(o.Binding, in.System) {
		return fmt.Errorf("filesystem original System metadata/boot/cgroup differs")
	}
	// 仅重算原不可变metadata投影；原producer nonce不被替换成当前Root。
	o.Binding.RootConsumer = in.System.RootConsumer
	b, e := json.Marshal(o)
	if e != nil || physicalDigest(b) != in.SystemSourceSHA256 {
		return fmt.Errorf("filesystem original System snapshot reference differs")
	}
	return nil
}
func normalFilesystemReadComplete(s *lockedState) (normalFilesystemOriginIntent, normalFilesystemOriginComplete, error) {
	var in normalFilesystemOriginIntent
	var complete normalFilesystemOriginComplete
	var rawIntent []byte
	for name, target := range map[string]any{normalFilesystemOriginIntentName: &in, normalFilesystemOriginCompleteName: &complete} {
		raw, exists, e := s.readPhysicalFile(name)
		if e != nil || !exists {
			return in, complete, errors.Join(ErrUnknown, fmt.Errorf("filesystem complete original prefix missing"), e)
		}
		if e = normalFilesystemDecodeRecord(raw, target); e != nil {
			return in, complete, e
		}
		if name == normalFilesystemOriginIntentName {
			rawIntent = raw
		}
	}
	if e := normalFilesystemValidateIntent(in); e != nil {
		return in, complete, e
	}
	if complete.ProtocolVersion != 1 || complete.Scope != normalFilesystemOriginScope || complete.IntentSHA256 != physicalDigest(rawIntent) {
		return in, complete, fmt.Errorf("filesystem original completion prefix differs")
	}
	return in, complete, nil
}

func (g *Guard) recoverNormalFilesystemOrigin(ctx context.Context, system *normalSystemLifecycleSource, currentRole *normalSystemOriginalTask) (*normalFilesystemOriginSource, error) {
	if g == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	s, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return nil, e
	}
	defer s.close()
	return g.recoverNormalFilesystemOriginLocked(ctx, s, system, currentRole)
}

// 显式恢复只对完整已验证10WAL同步，不创/删/改文件，也不重拍原FD/两task。
// 新Root必须由原角色broker交入当前task的原pidfd/proc，不按旧Root PID重开。
func (g *Guard) recoverNormalFilesystemOriginLocked(ctx context.Context, s *lockedState, system *normalSystemLifecycleSource, currentRole *normalSystemOriginalTask) (_ *normalFilesystemOriginSource, err error) {
	fail := func(e error) (*normalFilesystemOriginSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("filesystem recovery requires bounded original Root context"))
	}
	binding, _, vminit, e := normalFilesystemSourceMetadata(system)
	if e != nil {
		return fail(e)
	}
	in, _, e := normalFilesystemReadComplete(s)
	if e != nil {
		return fail(e)
	}
	if vminit != in.VminitSHA256 {
		return fail(fmt.Errorf("filesystem original vminit bytes changed"))
	}
	if e = normalFilesystemReferenceMatches(system, in); e != nil {
		return fail(e)
	}
	_, _, prefix, e := normalFilesystemInitialPrefix(s, binding, false)
	if e != nil || prefix.SHA256 != in.InitialPrefixSHA256 || prefix.Device != in.StateDevice || prefix.Inode != in.StateInode || prefix.LockDevice != in.LockDevice || prefix.LockInode != in.LockInode {
		return fail(fmt.Errorf("filesystem original initial prefix changed"))
	}
	if binding.RootConsumer == in.System.RootConsumer {
		argv, e := normalFilesystemReadSelfArgv()
		if e != nil || !reflect.DeepEqual(argv, []string{"/neonvm/bin/dsga-resource-guard", "initial-boot"}) {
			return fail(fmt.Errorf("filesystem original producer recovery entry differs"))
		}
	} else {
		if currentRole == nil || currentRole.source.PID != os.Getpid() || currentRole.source.GuestBootID != binding.GuestBootID {
			return fail(fmt.Errorf("filesystem new Root needs original role-spawn task source"))
		}
		argv, _ := normalSystemFixedRoleArgv("resource")
		if _, e = system.inspectOriginalTask(ctx, currentRole, "resource", argv, []string{"PATH=/neonvm/bin"}); e != nil {
			return fail(e)
		}
	}
	o := &normalFilesystemOriginSource{g: g, system: system, intent: in, current: binding.RootConsumer}
	ok := false
	defer func() {
		if !ok {
			o.close()
		}
	}()
	if o.pid1, e = normalFilesystemPinInitialTask(true); e != nil {
		return fail(e)
	}
	if o.self, e = normalFilesystemPinInitialTask(false); e != nil {
		return fail(e)
	}
	if o.pid1.start != in.PID1StartTicks || o.pid1.ns.Device != in.Namespace.Device || o.pid1.ns.Inode != in.Namespace.Inode || o.self.ns.Device != in.Namespace.Device || o.self.ns.Inode != in.Namespace.Inode || o.pid1.rootObject != in.Root || o.self.rootObject != in.Root {
		return fail(fmt.Errorf("filesystem original namespace/PID1/root changed"))
	}
	if currentRole != nil {
		currentRole.mu.Lock()
		e = arrayMatchPIDFD(int(o.self.pidfd.Fd()), currentRole.pidfd)
		currentRole.mu.Unlock()
		if e != nil {
			return fail(e)
		}
	}
	for name, task := range map[string]*normalFilesystemTask{"/neonvm/bin/busybox": o.pid1, "/neonvm/bin/dsga-resource-guard": o.self} {
		if e = normalFilesystemTaskExecutable(task, system, name); e != nil {
			return fail(e)
		}
	}
	mounts, _, e := normalFilesystemCurrentGraph(ctx, in.Namespace, in.Root)
	if e != nil || !reflect.DeepEqual(mounts, in.Mounts) {
		return fail(fmt.Errorf("filesystem original graph changed"))
	}
	fingerprint := func() (normalFilesystemOriginWitnessKey, error) {
		_, _, p, e := normalFilesystemInitialPrefix(s, binding, false)
		if e != nil {
			return normalFilesystemOriginWitnessKey{}, e
		}
		return normalFilesystemWitnessLocked(g, s, binding.RootConsumer, p)
	}
	o.witness, e = normalFilesystemConfirmPrefix(ctx, s.syncPhysicalFile, fingerprint)
	if e != nil {
		return fail(e)
	}
	normalFilesystemOriginWitnesses.Store(o.witness, struct{}{})
	ok = true
	return o, nil
}

// 此引用只从持活原FD/namespace的opaque对象经实读产生；JSON本身不可重建该对象，
// 不含qualified/permit。StartBroker后继应持原spawn task并另绑定current broker。
type normalFilesystemOriginReference struct {
	LayoutVersion         uint32                 `json:"layout_version"`
	ProtocolVersion       uint32                 `json:"protocol_version"`
	IntentSHA256          string                 `json:"intent_sha256"`
	CompletionSHA256      string                 `json:"completion_sha256"`
	OriginalRoot          nativeBirthConsumer    `json:"original_root"`
	GuestBootID           string                 `json:"guest_boot_id"`
	RuntimeDocumentSHA256 string                 `json:"runtime_document_sha256"`
	NativeBuildSHA256     string                 `json:"native_build_sha256"`
	SystemManifestSHA256  string                 `json:"system_manifest_sha256"`
	VminitSHA256          string                 `json:"vminit_sha256"`
	Namespace             normalFilesystemObject `json:"namespace"`
	Root                  normalFilesystemObject `json:"root"`
}

func (o *normalFilesystemOriginSource) referenceLocked(ctx context.Context, s *lockedState) (normalFilesystemOriginReference, error) {
	var zero normalFilesystemOriginReference
	if o == nil {
		return zero, ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return zero, ErrUnknown
	}
	observed, e := o.observeLocked(ctx, s)
	if e != nil {
		return zero, e
	}
	return normalFilesystemReferenceFromIntent(o.intent, observed.IntentSHA256, observed.CompletionSHA256), nil
}

// 原始引用、FD移交和显式重绑定必须共用同一版本化投影。这里的intent已由调用者
// 核验为当前17mount布局；漏填LayoutVersion会把FD路径降成0，与只读引用不一致。
func normalFilesystemReferenceFromIntent(in normalFilesystemOriginIntent, intentSHA, completeSHA string) normalFilesystemOriginReference {
	return normalFilesystemOriginReference{LayoutVersion: normalFilesystemLayoutVersion, ProtocolVersion: 1, IntentSHA256: intentSHA, CompletionSHA256: completeSHA, OriginalRoot: in.System.RootConsumer, GuestBootID: in.System.GuestBootID, RuntimeDocumentSHA256: in.System.RuntimeDocumentSHA256, NativeBuildSHA256: in.System.NativeBuildSHA256, SystemManifestSHA256: in.System.SystemManifestSHA256, VminitSHA256: in.VminitSHA256, Namespace: in.Namespace, Root: in.Root}
}

// 调用者持原resource flock，且未持System.mu/Task.mu。两个返回FD由调用者负责关闭，
// 固定broker继承原同namespace/root对象；Source close不会提前释放已移交的pin。
func (o *normalFilesystemOriginSource) pinOriginalNamespaceLocked(ctx context.Context, s *lockedState) (normalFilesystemOriginReference, *os.File, *os.File, error) {
	var zero normalFilesystemOriginReference
	if o == nil {
		return zero, nil, nil, ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return zero, nil, nil, ErrUnknown
	}
	observed, e := o.observeLocked(ctx, s)
	if e != nil {
		return zero, nil, nil, e
	}
	n, e := unix.FcntlInt(o.pid1.namespace.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return zero, nil, nil, e
	}
	namespace := os.NewFile(uintptr(n), "filesystem-original-namespace-handoff")
	n, e = unix.FcntlInt(o.pid1.root.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		namespace.Close()
		return zero, nil, nil, e
	}
	root := os.NewFile(uintptr(n), "filesystem-original-root-handoff")
	return normalFilesystemReferenceFromIntent(o.intent, observed.IntentSHA256, observed.CompletionSHA256), namespace, root, nil
}

// 仅旧resource/serve原角色路径的重绑定。专有broker还须其真实exec opaque proof，
// 在该producer闭合前不能用raw role枚举/自报verified绕过这里的旧角色核验。
func (g *Guard) rebindNormalFilesystemOriginLocked(ctx context.Context, s *lockedState, system *normalSystemLifecycleSource, currentRole *normalSystemOriginalTask, reference normalFilesystemOriginReference, namespace, root *os.File) (*normalFilesystemOriginSource, error) {
	fail := func(e error) (*normalFilesystemOriginSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if namespace == nil || root == nil {
		return fail(fmt.Errorf("filesystem original namespace/root handoff missing"))
	}
	ns, e := normalFilesystemDuplicateFile(namespace)
	if e != nil {
		return fail(e)
	}
	rt, e := normalFilesystemDuplicateFile(root)
	if e != nil {
		ns.Close()
		return fail(e)
	}
	owned := false
	defer func() {
		if !owned {
			ns.Close()
			rt.Close()
		}
	}()
	in, _, e := normalFilesystemReadComplete(s)
	if e != nil {
		return fail(e)
	}
	a, _, e := s.readPhysicalFile(normalFilesystemOriginIntentName)
	if e != nil {
		return fail(e)
	}
	b, _, e := s.readPhysicalFile(normalFilesystemOriginCompleteName)
	if e != nil {
		return fail(e)
	}
	expected := normalFilesystemReferenceFromIntent(in, physicalDigest(a), physicalDigest(b))
	if reference != expected {
		return fail(fmt.Errorf("filesystem transferred original reference differs"))
	}
	n, e := normalFilesystemFDObject(int(ns.Fd()))
	if e != nil || n != in.Namespace {
		return fail(fmt.Errorf("filesystem transferred namespace object differs"))
	}
	f, e := normalFilesystemFDObject(int(rt.Fd()))
	if e != nil || f != in.Root {
		return fail(fmt.Errorf("filesystem transferred root object differs"))
	}
	// 全部输入拒绝和dup先于任何Sync。正确source仍须原role/task/boot/引用门禁。
	o, e := g.recoverNormalFilesystemOriginLocked(ctx, s, system, currentRole)
	if e != nil {
		return nil, e
	}
	o.pid1.namespace.Close()
	o.pid1.root.Close()
	o.pid1.namespace = ns
	o.pid1.root = rt
	owned = true
	return o, nil
}

func normalFilesystemDuplicateFile(f *os.File) (*os.File, error) {
	if f == nil {
		return nil, fmt.Errorf("filesystem original FD missing")
	}
	raw, e := f.SyscallConn()
	if e != nil {
		return nil, e
	}
	duplicate := -1
	var inner error
	e = raw.Control(func(fd uintptr) { duplicate, inner = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0) })
	if e != nil {
		return nil, e
	}
	if inner != nil {
		return nil, inner
	}
	if duplicate < 0 {
		return nil, fmt.Errorf("filesystem original FD duplication failed")
	}
	return os.NewFile(uintptr(duplicate), "filesystem-original-owned-duplicate"), nil
}
