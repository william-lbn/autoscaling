//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type normalPGMemoryControlStdio struct {
	FD                  int `json:"fd"`
	Device, Inode, Rdev uint64
	Filesystem          int64  `json:"filesystem"`
	Mode                uint32 `json:"mode"`
	Flags               int    `json:"flags"`
	Kind                string `json:"kind"`
}

// 这是结构性审计，不把任何regular文件/pipe凭类型升级为原日志来源。
// 完整typed stdio必须由原System role/PID1出生链补齐；本批opaque仍拒continuous资格。
func normalPGMemoryControlInspectStdio(fd int) (normalPGMemoryControlStdio, error) {
	out := normalPGMemoryControlStdio{FD: fd}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if fd < 0 || unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil {
		return out, fmt.Errorf("PG stdio descriptor unavailable")
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return out, err
	}
	out.Device, out.Inode, out.Rdev, out.Filesystem, out.Mode, out.Flags = st.Dev, st.Ino, st.Rdev, fs.Type, st.Mode, flags
	switch fs.Type {
	case unix.PROC_SUPER_MAGIC, unix.SYSFS_MAGIC, unix.CGROUP_SUPER_MAGIC, unix.CGROUP2_SUPER_MAGIC, unix.SECURITYFS_MAGIC:
		return out, fmt.Errorf("PG stdio uses forbidden control filesystem")
	}
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFBLK, unix.S_IFSOCK, unix.S_IFDIR:
		return out, fmt.Errorf("PG stdio is a device/socket/directory authority")
	case unix.S_IFCHR:
		major, minor := unix.Major(st.Rdev), unix.Minor(st.Rdev)
		if major == 1 && minor == 3 {
			out.Kind = "dev-null"
		} else if major == 5 && minor == 1 {
			out.Kind = "console-structural-only"
		} else {
			return out, fmt.Errorf("PG stdio character device is not fixed null/console")
		}
	case unix.S_IFIFO:
		if fs.Type != 0x50495045 {
			return out, fmt.Errorf("PG stdio FIFO is not anonymous pipe")
		}
		out.Kind = "pipe-unbound-original"
	case unix.S_IFREG:
		if fs.Type != unix.EXT4_SUPER_MAGIC && fs.Type != unix.TMPFS_MAGIC {
			return out, fmt.Errorf("PG stdio regular filesystem is unsupported")
		}
		out.Kind = "regular-unbound-original"
	default:
		return out, fmt.Errorf("PG stdio unknown object class")
	}
	return out, nil
}

// 只能在最终exec的lockedM调用。UNSHARE复制当前线程fd表，CLOEXEC只标记>=3，
// 不关闭Go其它线程的共享表；成功exec时才由内核原子清除所有外部继承性。
// 此时198尚未存在；原C main在exec之后才connect/dup198，不留Go白名单漏洞。
func normalPGMemoryControlPrepareExecDescriptors() ([3]normalPGMemoryControlStdio, error) {
	var observations [3]normalPGMemoryControlStdio
	for fd := 0; fd < 3; fd++ {
		o, err := normalPGMemoryControlInspectStdio(fd)
		if err != nil {
			return observations, err
		}
		observations[fd] = o
	}
	if err := unix.CloseRange(3, uint(^uint32(0)), unix.CLOSE_RANGE_UNSHARE|unix.CLOSE_RANGE_CLOEXEC); err != nil {
		return observations, fmt.Errorf("PG private descriptor table CLOEXEC seal: %w", err)
	}
	return observations, nil
}

type normalPGMemoryControlSource struct {
	mu         sync.Mutex
	guard      *Guard
	original   *arrayRegistryOriginal
	generation *arrayRegistryGeneration
	reference  normalPGMemoryControlReference
	files      [4]*os.File
	bundle     nativeFilterAuditBundle
	stageFiles [4]arrayPopulateHistoryIdentity
	closed     bool
}

func (o *normalPGMemoryControlSource) close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.closed {
		o.closed = true
		for _, f := range o.files {
			if f != nil {
				f.Close()
			}
		}
	}
}

// 唯一生产入口只从原Guard私有真实binding读取，caller不能传PID/已可信bool/hash。
// 原resource flock→本source.mu→原array.mu；不SQL回调、不新造birth、不Sync补签。
func (g *Guard) captureNormalPGMemoryControlSource(ctx context.Context) (*normalPGMemoryControlSource, error) {
	if g == nil || ctx == nil {
		return nil, ErrUnknown
	}
	var out *normalPGMemoryControlSource
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		var err error
		out, err = g.captureNormalPGMemoryControlSourceLocked(ctx, s, r)
		return err
	})
	return out, err
}
func (g *Guard) captureNormalPGMemoryControlSourceLocked(ctx context.Context, s *lockedState, r *record) (*normalPGMemoryControlSource, error) {
	fail := func(err error) (*normalPGMemoryControlSource, error) { return nil, errors.Join(ErrUnknown, err) }
	if g == nil || s == nil || r == nil || ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("original Root PG control context missing"))
	}
	value, ok := arrayGuardBindings.Load(g)
	if !ok {
		return fail(fmt.Errorf("original PG registration pin unavailable"))
	}
	binding, ok := value.(*arrayGuardBinding)
	if !ok || binding == nil || binding.original == nil {
		return fail(fmt.Errorf("original PG private binding invalid"))
	}
	if err := g.checkNormalQuotaResourceLocked(s, r); err != nil {
		return fail(err)
	}
	if err := g.rejectArrayPendingLocked(s, r); err != nil {
		return fail(err)
	}
	original := binding.original
	original.mu.Lock()
	defer original.mu.Unlock()
	if err := original.verifyOriginal(); err != nil {
		return fail(err)
	}
	if r.validate(r.Identity) != nil || r.Birth == nil || *r.Birth != original.pending.Original || r.Bootstrap == nil || !r.Bootstrap.Applied || r.Original != nil && !r.Applied {
		return fail(fmt.Errorf("original PG budget/Birth unconfirmed"))
	}
	current := original.current
	if current == nil || current.retiring {
		return fail(fmt.Errorf("original PG four-array generation unavailable"))
	}
	for _, ready := range current.startup {
		if !ready {
			return fail(fmt.Errorf("original PG startup prefix not complete"))
		}
	}
	source, pg, err := readNativeBirthOriginalSource()
	if err != nil {
		return fail(err)
	}
	defer pg.Close()
	abi, err := nativeBirthABIForPG(source.binding.Policy.PGmajor)
	if err != nil || source.binding != original.pending.Source || !reflect.DeepEqual(source.resource.Config, g.config) {
		return fail(fmt.Errorf("original fixed PG source differs"))
	}
	stage, err := observeNativeBirthStageLocked(s, original.birthRef)
	if err != nil || stage.Pending != original.pending || stage.Phase != nativeBirthStageReleased || len(stage.Entries) != 3 {
		return fail(fmt.Errorf("original NativeFinal/released missing: %w", err))
	}
	transcript := stage.Entries[2].Transcript
	if transcript.Request != original.reservation || !transcript.FinalVerified || !transcript.FinalPersisted || !transcript.DetachedNonAuthorizing {
		return fail(fmt.Errorf("retained original NativeFinal evidence differs"))
	}
	raw, exists, err := s.readPhysicalFile(nativeBirthStageName(original.birthRef.Nonce, "released"))
	if err != nil || !exists || physicalDigest(raw) != original.releasedSHA {
		return fail(fmt.Errorf("original released prefix changed"))
	}
	// 固定同一只读PG ELF的纯audit，完整程序必须与原真实Kernel读回的摘要相同。
	// 不把新编译输出或PG自报摘要冒充一次新的内核安装观察。
	compiler := &nativeBirthTraceKernel{binary: pg}
	bundle, err := compiler.compile(ctx, original.reservation)
	if err != nil {
		return fail(err)
	}
	if bundle.EncodedSHA256 != transcript.AuditSHA256 {
		return fail(fmt.Errorf("full PG BPF differs from original Kernel-audited bytes"))
	}
	if err = normalPGMemoryControlReviewBundle(bundle); err != nil {
		return fail(err)
	}
	resourceRef, err := arrayAuthorityRecordReference(*r)
	if err != nil {
		return fail(err)
	}
	ref := normalPGMemoryControlReference{ProtocolVersion: 1, Scope: abi.memoryControlScope, Birth: original.birthRef, ReleasedSHA256: original.releasedSHA, AuditSHA256: hex.EncodeToString(bundle.EncodedSHA256[:]), PGELFSHA256: source.binding.BinarySHA256, ResourceReferenceSHA256: resourceRef, RegistrationNonce: hex.EncodeToString(current.nonce[:]), MappingGeneration: current.request.MappingGeneration, PGBootID: current.request.PGBoot, Files: current.identities}
	out := &normalPGMemoryControlSource{guard: g, original: original, generation: current, reference: ref, bundle: bundle}
	good := false
	defer func() {
		if !good {
			out.close()
		}
	}()
	for i, p := range bundle.Programs {
		out.reference.ProgramInstructions[i] = len(p)
		out.bundle.Programs[i] = append([]nativeFilterInstruction(nil), p...)
	}
	for i, f := range current.files {
		if err := normalPGMemoryControlOriginalFile(f, current.identities[i]); err != nil {
			return fail(err)
		}
		d, err := normalFilesystemDuplicateFile(f)
		if err != nil {
			return fail(err)
		}
		out.files[i] = d
	}
	for i, phase := range []string{"pending", "base", "final", "released"} {
		_, id, err := arrayPopulateHistoryRaw(s, nativeBirthStageName(original.birthRef.Nonce, phase))
		if err != nil {
			return fail(err)
		}
		out.stageFiles[i] = id
	}
	if err := normalPGMemoryControlStatus(original); err != nil {
		return fail(err)
	}
	if _, err := g.kernel.ReadBounds(r.Identity, func() Limits {
		if r.Held != nil {
			return r.Held.limits()
		}
		return r.Bootstrap.Original.Limits
	}()); err != nil {
		return fail(err)
	}
	if err := original.verifyOriginal(); err != nil {
		return fail(err)
	}
	good = true
	return out, nil
}
func normalPGMemoryControlOriginalFile(f *os.File, want arrayPopulateFile) error {
	if f == nil {
		return fmt.Errorf("original PG array descriptor missing")
	}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(int(f.Fd()), &st) != nil || unix.Fstatfs(int(f.Fd()), &fs) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || fs.Type != unix.TMPFS_MAGIC || st.Dev != uint64(want.Device) || st.Ino != uint64(want.Inode) || st.Size < 0 || uint64(st.Size) != uint64(want.FileBytes) {
		return fmt.Errorf("original PG array inode changed")
	}
	return nil
}
func normalPGMemoryControlStatus(o *arrayRegistryOriginal) error {
	raw, err := nativeTraceProcText(o.procfd, "status")
	if err != nil {
		return err
	}
	for field, want := range map[string]uint64{"NoNewPrivs": 1, "Seccomp": 2} {
		v, err := nativeTraceStatusValues(raw, field, 1)
		if err != nil || v[0] != want {
			return fmt.Errorf("original PG %s enforcement differs", field)
		}
	}
	for _, field := range []string{"CapEff:", "CapPrm:", "CapInh:", "CapAmb:"} {
		count := 0
		for _, line := range strings.Split(raw, "\n") {
			if strings.HasPrefix(line, field) {
				count++
				f := strings.Fields(line)
				if len(f) != 2 {
					return ErrUnknown
				}
				n, err := strconv.ParseUint(f[1], 16, 64)
				if err != nil || n != 0 {
					return fmt.Errorf("original PG retains privilege %s", field)
				}
			}
		}
		if count != 1 {
			return ErrUnknown
		}
	}
	return o.verifyOriginal()
}
func (o *normalPGMemoryControlSource) referenceLocked(ctx context.Context, s *lockedState, r *record) (normalPGMemoryControlReference, error) {
	var zero normalPGMemoryControlReference
	if o == nil {
		return zero, ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || ctx == nil || ctx.Err() != nil || r == nil || r.Birth == nil {
		return zero, ErrUnknown
	}
	value, ok := arrayGuardBindings.Load(o.guard)
	if !ok {
		return zero, ErrUnknown
	}
	binding, ok := value.(*arrayGuardBinding)
	if !ok || binding.original != o.original {
		return zero, ErrUnknown
	}
	original := o.original
	original.mu.Lock()
	defer original.mu.Unlock()
	if original.current != o.generation || original.current == nil || original.current.retiring || original.current.request.MappingGeneration != o.reference.MappingGeneration || original.current.request.PGBoot != o.reference.PGBootID || original.current.identities != o.reference.Files || *r.Birth != original.pending.Original {
		return zero, ErrUnknown
	}
	if err := o.guard.checkNormalQuotaResourceLocked(s, r); err != nil {
		return zero, err
	}
	if err := normalPGMemoryControlStatus(original); err != nil {
		return zero, err
	}
	ref, err := arrayAuthorityRecordReference(*r)
	if err != nil || ref != o.reference.ResourceReferenceSHA256 {
		return zero, ErrUnknown
	}
	for i, phase := range []string{"pending", "base", "final", "released"} {
		_, id, err := arrayPopulateHistoryRaw(s, nativeBirthStageName(original.birthRef.Nonce, phase))
		if err != nil || id != o.stageFiles[i] {
			return zero, ErrUnknown
		}
	}
	for i, f := range o.files {
		if err := normalPGMemoryControlOriginalFile(f, o.reference.Files[i]); err != nil {
			return zero, err
		}
	}
	return o.reference, nil
}

// 外域缺项没有bool设置器；Source本身不是normal资格。后继只准Root组合真实typed
// stdio/System/FS/Kernel opaque来源，未拼接前不得删MissingSources。
func (o *normalPGMemoryControlSource) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("PG control source still requires original typed stdio, FS/System and kernel controls; runtime PM plugin birth manifest remains separate"))
}
