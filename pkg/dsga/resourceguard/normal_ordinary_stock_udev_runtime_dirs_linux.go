//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const normalOrdinaryStockUdevRuntimePath = "/run/udev"
const normalOrdinaryStockUdevRuntimeUID = 60004

// 仅固定目录FD/权限原语。Root factory必须另证明原R2、准备阶段及所有原writer
// 真Wait/投影seal；方法名不是该证明，不接受ready bool或数字PID补签。
type normalOrdinaryStockUdevRuntimeDirs struct {
	mu                                 sync.Mutex
	closed                             bool
	phase                              uint8 // 1 prepared，2 changing，3 handed off，4 unknown不可重试。
	pid1                               *normalFilesystemTask
	run, root, rules                   *os.File
	runObject, rootObject, rulesObject normalFilesystemObject
}

func pinNormalOrdinaryStockUdevRuntimeDirs(ctx context.Context) (*normalOrdinaryStockUdevRuntimeDirs, error) {
	fail := func(e error) (*normalOrdinaryStockUdevRuntimeDirs, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("stock runtime directories require original root context"))
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	p, e := normalFilesystemPinInitialTask(true)
	if e != nil {
		return fail(e)
	}
	d := &normalOrdinaryStockUdevRuntimeDirs{pid1: p}
	ok := false
	defer func() {
		if !ok {
			d.close()
		}
	}()
	// 同PID1 namespace/root、真实17mount及每个mountID/statfs；不是从tmpfs类型单独认领。
	facts, _, e := normalFilesystemCurrentGraph(ctx, p.ns, p.rootObject)
	if e != nil {
		return fail(e)
	}
	var runFact normalFilesystemObject
	for _, f := range facts {
		if f.Mount.Point == "/run" && f.Mount.FSType == "tmpfs" && !f.Object.Readonly {
			runFact = f.Object
		}
	}
	if runFact.Inode == 0 {
		return fail(fmt.Errorf("stock runtime original run tmpfs absent"))
	}
	canonical, e := filepath.EvalSymlinks("/run")
	if e != nil || canonical != "/run" {
		return fail(fmt.Errorf("stock run directory alias unsupported"))
	}
	d.run, e = normalFilesystemOpenAt(unix.AT_FDCWD, "/run", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
	if e != nil {
		return fail(e)
	}
	d.runObject, e = normalOrdinaryStockUdevRuntimeDirFact(d.run, 0, 0, 0755)
	if e != nil || d.runObject != runFact {
		return fail(fmt.Errorf("stock run directory differs from original graph"))
	}
	d.root, d.rules, e = normalOrdinaryStockUdevRuntimePrepareAt(ctx, d.run)
	if e != nil {
		return fail(e)
	}
	d.rootObject, e = normalOrdinaryStockUdevRuntimeDirFact(d.root, 0, 0, 0755)
	if e != nil {
		return fail(e)
	}
	d.rulesObject, e = normalOrdinaryStockUdevRuntimeDirFact(d.rules, 0, 0, 0755)
	if e != nil {
		return fail(e)
	}
	if d.rootObject.MountID != runFact.MountID || d.rulesObject.MountID != runFact.MountID {
		return fail(fmt.Errorf("stock runtime directory has unexpected submount"))
	}
	d.phase = 1
	ok = true
	return d, nil
}

// 只在原run dirFD中创建字面udev/rules.d。U只传本测试新建临时run FD，
// 生产唯一路径由上方固定入口打开；此helper不产opaque FS/角色/Source。
func normalOrdinaryStockUdevRuntimePrepareAt(ctx context.Context, run *os.File) (root, rules *os.File, err error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrUnknown
	}
	if _, e := normalOrdinaryStockUdevRuntimeDirFact(run, 0, 0, 0755); e != nil {
		return nil, nil, e
	}
	makeDir := func(parent *os.File, name string) (*os.File, error) {
		created := false
		if e := unix.Mkdirat(int(parent.Fd()), name, 0755); e == nil {
			created = true
		} else if e != unix.EEXIST {
			return nil, e
		}
		f, e := normalFilesystemOpenAt(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
		if e != nil {
			return nil, e
		}
		// 仅刚由本调用mkdir的准确FD可修原umask造成的mode；既有对象不修补/不接管。
		if created {
			if e = unix.Fchmod(int(f.Fd()), 0755); e != nil {
				f.Close()
				return nil, e
			}
		}
		if _, e = normalOrdinaryStockUdevRuntimeDirFact(f, 0, 0, 0755); e != nil {
			f.Close()
			return nil, e
		}
		return f, nil
	}
	root, err = makeDir(run, "udev")
	if err != nil {
		return nil, nil, err
	}
	createdRoot := root
	var createdRules *os.File
	ok := false
	defer func() {
		if !ok {
			createdRoot.Close()
			if createdRules != nil {
				createdRules.Close()
			}
		}
	}()
	names, e := normalFilesystemReadDirAt(int(root.Fd()), ".", 8)
	if e != nil {
		return nil, nil, e
	}
	for _, n := range names {
		if n != "rules.d" {
			return nil, nil, fmt.Errorf("stock runtime contains preexisting state")
		}
	}
	rules, err = makeDir(root, "rules.d")
	if err != nil {
		return nil, nil, err
	}
	createdRules = rules
	names, e = normalFilesystemReadDirAt(int(rules.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		return nil, nil, fmt.Errorf("stock runtime rules must be original empty directory")
	}
	ok = true
	return root, rules, nil
}
func normalOrdinaryStockUdevRuntimeDirFact(f *os.File, uid, gid uint32, mode uint32) (normalFilesystemObject, error) {
	var st unix.Stat_t
	if f == nil || unix.Fstat(int(f.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uid || st.Gid != gid || st.Mode&07777 != mode {
		return normalFilesystemObject{}, fmt.Errorf("stock runtime directory owner/mode/type differs")
	}
	obj, e := normalFilesystemFDObject(int(f.Fd()))
	if e != nil {
		return normalFilesystemObject{}, e
	}
	return obj, nil
}
func normalOrdinaryStockUdevRuntimeSameAt(parent *os.File, name string, pin *os.File) error {
	f, e := normalFilesystemOpenAt(int(parent.Fd()), name, unix.O_PATH|unix.O_NOFOLLOW)
	if e != nil {
		return e
	}
	defer f.Close()
	a, e := normalFilesystemFDObject(int(f.Fd()))
	if e != nil {
		return e
	}
	b, e := normalFilesystemFDObject(int(pin.Fd()))
	if e != nil || a != b {
		return fmt.Errorf("stock runtime original directory replaced")
	}
	return nil
}
func (d *normalOrdinaryStockUdevRuntimeDirs) recheckLocked(ctx context.Context) error {
	if d.closed || d.pid1 == nil || d.run == nil || d.root == nil || d.rules == nil || (d.phase != 1 && d.phase != 3) {
		return ErrUnknown
	}
	if e := arrayRegistryPIDFDAlive(int(d.pid1.pidfd.Fd())); e != nil {
		return e
	}
	// 不调用signal0；从原procFD复核PID1 namespace/root，不按新数字PID重开角色。
	for name, want := range map[string]normalFilesystemObject{"ns/mnt": d.pid1.ns, "root": d.pid1.rootObject} {
		f, e := normalFilesystemOpenAt(int(d.pid1.procfd.Fd()), name, unix.O_PATH)
		if e != nil {
			return e
		}
		got, e := normalFilesystemFDObject(int(f.Fd()))
		f.Close()
		if e != nil || got != want {
			return fmt.Errorf("stock runtime PID1 namespace/root changed")
		}
	}
	if _, _, e := normalFilesystemCurrentGraph(ctx, d.pid1.ns, d.pid1.rootObject); e != nil {
		return e
	}
	got, e := normalOrdinaryStockUdevRuntimeDirFact(d.run, 0, 0, 0755)
	if e != nil || got != d.runObject {
		return fmt.Errorf("stock original run directory changed")
	}
	f, e := normalFilesystemOpenAt(unix.AT_FDCWD, "/run", unix.O_PATH|unix.O_NOFOLLOW)
	if e != nil {
		return e
	}
	actual, e := normalFilesystemFDObject(int(f.Fd()))
	f.Close()
	if e != nil || actual != d.runObject {
		return fmt.Errorf("stock logical run directory changed")
	}
	if e = normalOrdinaryStockUdevRuntimeSameAt(d.run, "udev", d.root); e != nil {
		return e
	}
	if e = normalOrdinaryStockUdevRuntimeSameAt(d.root, "rules.d", d.rules); e != nil {
		return e
	}
	gid, mode := uint32(0), uint32(0755)
	if d.phase == 3 {
		gid = normalOrdinaryStockUdevRuntimeUID
		mode = 01771
	}
	got, e = normalOrdinaryStockUdevRuntimeDirFact(d.root, 0, gid, mode)
	if e != nil || got != d.rootObject {
		return fmt.Errorf("stock protected runtime parent changed")
	}
	got, e = normalOrdinaryStockUdevRuntimeDirFact(d.rules, 0, 0, 0755)
	if e != nil || got != d.rulesObject {
		return fmt.Errorf("stock protected runtime rules directory changed")
	}
	names, e := normalFilesystemReadDirAt(int(d.rules.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		return fmt.Errorf("stock runtime rules shadow is not empty")
	}
	return ctx.Err()
}
func (d *normalOrdinaryStockUdevRuntimeDirs) recheck(ctx context.Context) error {
	if d == nil || ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	d.mu.Lock()
	defer d.mu.Unlock()
	return normalOrdinaryStockUdevRuntimeError(d.recheckLocked(ctx))
}

// 保持nil成功；所有失败为Unknown，不从观察补签或自动修复mode。
func normalOrdinaryStockUdevRuntimeError(e error) error {
	if e == nil {
		return nil
	}
	return errors.Join(ErrUnknown, e)
}

type normalOrdinaryStockUdevRuntimeEntry struct {
	f              *os.File
	object         normalFilesystemObject
	mode, uid, gid uint32
	size           int64
	nlink          uint64
	ctime, mtime   unix.Timespec
}

func normalOrdinaryStockUdevRuntimeEntryFact(f *os.File) (normalOrdinaryStockUdevRuntimeEntry, error) {
	var st unix.Stat_t
	if f == nil || unix.Fstat(int(f.Fd()), &st) != nil {
		return normalOrdinaryStockUdevRuntimeEntry{}, ErrUnknown
	}
	o, e := normalFilesystemFDObject(int(f.Fd()))
	if e != nil {
		return normalOrdinaryStockUdevRuntimeEntry{}, e
	}
	return normalOrdinaryStockUdevRuntimeEntry{f: f, object: o, mode: st.Mode, uid: st.Uid, gid: st.Gid, size: st.Size, nlink: st.Nlink, ctime: st.Ctim, mtime: st.Mtim}, nil
}
func normalOrdinaryStockUdevRuntimeName(s string) bool {
	if s == "" || len(s) > 255 || s == "." || s == ".." || strings.HasPrefix(s, ".") {
		return false
	}
	for _, c := range []byte(s) {
		if c < 33 || c > 126 || c == '/' {
			return false
		}
	}
	return true
}
func normalOrdinaryStockUdevRuntimeNumber(s string, positive bool) bool {
	v, e := strconv.ParseUint(s, 10, 32)
	return e == nil && v <= 0x7fffffff && (!positive || v != 0) && strconv.FormatUint(v, 10) == s
}
func normalOrdinaryStockUdevRuntimeDeviceID(s string) bool {
	if !normalOrdinaryStockUdevRuntimeName(s) || strings.HasSuffix(s, ".tmp") {
		return false
	}
	if s[0] == 'n' {
		return normalOrdinaryStockUdevRuntimeNumber(s[1:], true)
	}
	if s[0] == 'b' || s[0] == 'c' {
		a, b, ok := strings.Cut(s[1:], ":")
		return ok && normalOrdinaryStockUdevRuntimeNumber(a, true) && normalOrdinaryStockUdevRuntimeNumber(b, false)
	}
	if s[0] == '+' {
		a, b, ok := strings.Cut(s[1:], ":")
		return ok && normalOrdinaryStockUdevRuntimeName(a) && normalOrdinaryStockUdevRuntimeName(b)
	}
	return false
}

// 只有原writer真Wait之后Root factory才可调用。此helper只枚举固定有限类型，
// 不删control/queue/watch.old或.tmp，不凭当前PID/socket外观认领旧writer对象。
func normalOrdinaryStockUdevRuntimeCollect(ctx context.Context, root, rules *os.File) ([]normalOrdinaryStockUdevRuntimeEntry, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	base, e := normalOrdinaryStockUdevRuntimeDirFact(root, 0, 0, 0755)
	if e != nil {
		return nil, e
	}
	if _, e = normalOrdinaryStockUdevRuntimeDirFact(rules, 0, 0, 0755); e != nil {
		return nil, e
	}
	if e = normalOrdinaryStockUdevRuntimeSameAt(root, "rules.d", rules); e != nil {
		return nil, e
	}
	names, e := normalFilesystemReadDirAt(int(rules.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		return nil, fmt.Errorf("stock runtime rules must remain empty")
	}
	entries := []normalOrdinaryStockUdevRuntimeEntry{}
	ok := false
	defer func() {
		if !ok {
			for _, x := range entries {
				x.f.Close()
			}
		}
	}()
	var total int64
	var walk func(*os.File, string, int) error
	walk = func(dir *os.File, kind string, depth int) error {
		names, e := normalFilesystemReadDirAt(int(dir.Fd()), ".", 8193)
		if e != nil {
			return e
		}
		sort.Strings(names)
		for _, name := range names {
			if e = ctx.Err(); e != nil {
				return e
			}
			if !normalOrdinaryStockUdevRuntimeName(name) || len(entries) >= 8192 {
				return fmt.Errorf("stock runtime name/count exceeds supported bound")
			}
			if kind == "root" && name == "rules.d" {
				continue
			}
			if kind == "root" && name != "data" && name != "links" && name != "tags" && name != "watch" && name != "static_node-tags" {
				return fmt.Errorf("stock runtime has unknown or uncleared control/queue state")
			}
			f, e := normalFilesystemOpenAt(int(dir.Fd()), name, unix.O_PATH|unix.O_NOFOLLOW)
			if e != nil {
				return e
			}
			fact, e := normalOrdinaryStockUdevRuntimeEntryFact(f)
			if e != nil {
				f.Close()
				return e
			}
			entries = append(entries, fact)
			if fact.object.MountID != base.MountID || fact.object.Device != base.Device || fact.object.Filesystem != base.Filesystem || fact.uid != 0 || fact.gid != 0 {
				return fmt.Errorf("stock runtime child is not original same-filesystem root object")
			}
			typ := fact.mode & unix.S_IFMT
			isDir := kind == "root" || (depth == 1 && (kind == "links" || kind == "tags" || kind == "static_node-tags"))
			if isDir {
				if typ != unix.S_IFDIR || fact.mode&07000 != 0 || fact.mode&0022 != 0 || fact.mode&0700 != 0700 {
					return fmt.Errorf("stock runtime index directory type/mode differs")
				}
				nextKind := kind
				if kind == "root" {
					nextKind = name
				}
				readFD, e := normalFilesystemOpenAt(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
				if e != nil {
					return e
				}
				obj, e := normalFilesystemFDObject(int(readFD.Fd()))
				if e != nil || obj != fact.object {
					readFD.Close()
					return fmt.Errorf("stock runtime directory changed before enumeration")
				}
				e = walk(readFD, nextKind, depth+1)
				readFD.Close()
				if e != nil {
					return e
				}
				continue
			}
			validID := normalOrdinaryStockUdevRuntimeDeviceID(name)
			switch kind {
			case "data":
				if depth != 1 || !validID || typ != unix.S_IFREG || fact.nlink != 1 || fact.size < 0 || fact.size > 1<<20 || fact.mode&0600 != 0600 || fact.mode&06133 != 0 {
					return fmt.Errorf("stock runtime database object differs")
				}
				total += fact.size
				if total > 64<<20 {
					return fmt.Errorf("stock runtime database total exceeds bound")
				}
			case "links", "tags":
				if depth != 2 || !validID || typ != unix.S_IFREG || fact.nlink != 1 || fact.size != 0 || (fact.mode&07777 != 0444 && fact.mode&07777 != 0400) {
					return fmt.Errorf("stock runtime empty index object differs")
				}
			case "watch", "static_node-tags":
				if typ != unix.S_IFLNK || fact.nlink != 1 || fact.size <= 0 || fact.size > 4096 {
					return fmt.Errorf("stock runtime symbolic index type differs")
				}
				buf := make([]byte, 4097)
				n, e := unix.Readlinkat(int(dir.Fd()), name, buf)
				if e != nil || n <= 0 || n > 4096 {
					return fmt.Errorf("stock runtime symbolic index unreadable")
				}
				target := string(buf[:n])
				if kind == "watch" {
					if depth != 1 || !normalOrdinaryStockUdevRuntimeNumber(name, false) || !normalOrdinaryStockUdevRuntimeDeviceID(target) {
						return fmt.Errorf("stock watch id target differs")
					}
				} else if depth != 2 || !strings.HasPrefix(target, "/dev/") || filepath.Clean(target) != target || strings.ContainsAny(target, "\x00\r\n\t ") || target == "/dev/" {
					return fmt.Errorf("stock static node tag target differs")
				}
			default:
				return fmt.Errorf("stock runtime hierarchy differs")
			}
		}
		return nil
	}
	if e = walk(root, "root", 0); e != nil {
		return nil, e
	}
	ok = true
	return entries, nil
}
func normalOrdinaryStockUdevRuntimeHandoffAt(ctx context.Context, root, rules *os.File) error {
	entries, e := normalOrdinaryStockUdevRuntimeCollect(ctx, root, rules)
	if e != nil {
		return e
	}
	defer func() {
		for _, x := range entries {
			x.f.Close()
		}
	}()
	// 在任何权限写之前完成全枚举；每次写仍对同原pin重验，不跟随symlink到/dev或外部。
	for _, old := range entries {
		if e = ctx.Err(); e != nil {
			return e
		}
		current, e := normalOrdinaryStockUdevRuntimeEntryFact(old.f)
		if e != nil || current != old {
			return fmt.Errorf("stock runtime child changed before handoff")
		}
		if e = unix.Fchownat(int(old.f.Fd()), "", normalOrdinaryStockUdevRuntimeUID, normalOrdinaryStockUdevRuntimeUID, unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW); e != nil {
			return e
		}
	}
	if _, e = normalOrdinaryStockUdevRuntimeDirFact(rules, 0, 0, 0755); e != nil {
		return e
	}
	names, e := normalFilesystemReadDirAt(int(rules.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		return fmt.Errorf("stock rules changed before parent handoff")
	}
	if e = unix.Fchown(int(root.Fd()), 0, normalOrdinaryStockUdevRuntimeUID); e != nil {
		return e
	}
	if e = unix.Fchmod(int(root.Fd()), 01771); e != nil {
		return e
	}
	if _, e = normalOrdinaryStockUdevRuntimeDirFact(root, 0, normalOrdinaryStockUdevRuntimeUID, 01771); e != nil {
		return e
	}
	return nil
}
func (d *normalOrdinaryStockUdevRuntimeDirs) handoffAfterOriginalWritersWait(ctx context.Context) error {
	if d == nil || ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.phase != 1 {
		return ErrUnknown
	}
	if e := d.recheckLocked(ctx); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	d.phase = 2
	if e := normalOrdinaryStockUdevRuntimeHandoffAt(ctx, d.root, d.rules); e != nil {
		d.phase = 4
		return errors.Join(ErrUnknown, e)
	}
	d.phase = 3
	if e := d.recheckLocked(ctx); e != nil {
		d.phase = 4
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (d *normalOrdinaryStockUdevRuntimeDirs) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		for _, f := range []*os.File{d.rules, d.root, d.run} {
			if f != nil {
				f.Close()
			}
		}
		if d.pid1 != nil {
			d.pid1.close()
		}
		d.closed = true
	}
}
