//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
)

const normalOrdinaryV2LayoutScope = "fixed-ordinary-v2-directory-preparation-component-only"

var normalOrdinaryV2LayoutStages = [...]string{"intent", "identity", "complete"}

func normalOrdinaryV2LayoutStageName(stage string) string {
	return "ordinary-v2-layout-" + stage + ".json"
}

type normalOrdinaryV2PathFact struct {
	Path   string                 `json:"path"`
	Object normalFilesystemObject `json:"object"`
	UID    uint32                 `json:"uid"`
	GID    uint32                 `json:"gid"`
	Mode   uint32                 `json:"mode"`
	Links  uint64                 `json:"links"`
}
type normalOrdinaryV2HeldPath struct {
	spec normalOrdinaryV2PathSpec
	f    *os.File
	fact normalOrdinaryV2PathFact
}
type normalOrdinaryV2DirectoryPins struct {
	root    *os.File
	anchors map[string]*os.File
	paths   []normalOrdinaryV2HeldPath
}

func (p *normalOrdinaryV2DirectoryPins) close() {
	if p == nil {
		return
	}
	for _, x := range p.paths {
		if x.f != nil {
			x.f.Close()
		}
	}
	for _, f := range p.anchors {
		f.Close()
	}
	if p.root != nil {
		p.root.Close()
	}
}
func readNormalOrdinaryV2PathFact(fd int, name string) (normalOrdinaryV2PathFact, error) {
	var st unix.Stat_t
	if e := unix.Fstat(fd, &st); e != nil {
		return normalOrdinaryV2PathFact{}, e
	}
	obj, e := normalFilesystemFDObject(fd)
	if e != nil {
		return normalOrdinaryV2PathFact{}, e
	}
	links := uint64(st.Nlink)
	// 目录可由原角色合法创建子目录；不把nlink变化误当原目录身份替换。
	if st.Mode&unix.S_IFMT == unix.S_IFDIR {
		links = 0
	}
	return normalOrdinaryV2PathFact{name, obj, st.Uid, st.Gid, st.Mode, links}, nil
}

// openat2逐个组件拒alias/magiclink；固定anchor跨已认证挂载，anchor下拒额外挂载。
// 不先os.Open FIFO，不允许当前path静态检查与后续打开之间被替换。
func normalOrdinaryV2OpenAt(fd int, rel string, directory, sameMount bool) (*os.File, error) {
	if rel == "" || strings.HasPrefix(rel, "/") || path.Clean(rel) != rel || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, ErrUnknown
	}
	flags := unix.O_RDONLY | unix.O_NONBLOCK | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if directory {
		flags |= unix.O_DIRECTORY
	}
	resolve := uint64(unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS)
	if sameMount {
		resolve |= unix.RESOLVE_NO_XDEV
	}
	fd2, e := unix.Openat2(fd, rel, &unix.OpenHow{Flags: uint64(flags), Resolve: resolve})
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(fd2), "ordinary-v2-original-path"), nil
}
func normalOrdinaryV2ExpectedInitial(f *os.File, spec normalOrdinaryV2PathSpec) error {
	a, e := readNormalOrdinaryV2PathFact(int(f.Fd()), spec.path)
	if e != nil {
		return e
	}
	if a.Object.Readonly || a.UID != 0 || a.GID != 0 || a.Mode&07000 != 0 || a.Mode&0022 != 0 {
		return fmt.Errorf("ordinary2 initial path is not protected original Root object: %s", spec.path)
	}
	if spec.file {
		var st unix.Stat_t
		if unix.Fstat(int(f.Fd()), &st) != nil || a.Mode&unix.S_IFMT != unix.S_IFREG || a.Links != 1 || st.Size < 0 || st.Size > 64<<10 {
			return fmt.Errorf("ordinary2 DNS file type/size/link bound")
		}
	} else {
		if a.Mode&unix.S_IFMT != unix.S_IFDIR {
			return ErrUnknown
		}
		names, e := normalFilesystemReadDirAt(int(f.Fd()), ".", 1)
		if e != nil || len(names) != 0 {
			return errors.Join(ErrUnknown, fmt.Errorf("ordinary2 initial directory is nonempty: %s", spec.path), e)
		}
	}
	return nil
}
func (p *normalOrdinaryV2DirectoryPins) pathParent(spec normalOrdinaryV2PathSpec) (*os.File, string, error) {
	if spec.path == spec.anchor {
		return nil, "", ErrUnknown
	}
	anchor := p.anchors[spec.anchor]
	if anchor == nil {
		return nil, "", ErrUnknown
	}
	rel := strings.TrimPrefix(spec.path, spec.anchor+"/")
	parent := path.Dir(rel)
	f, e := normalOrdinaryV2OpenAt(int(anchor.Fd()), parent, true, true)
	return f, path.Base(rel), e
}
func (p *normalOrdinaryV2DirectoryPins) openSpec(spec normalOrdinaryV2PathSpec) (*os.File, error) {
	if spec.path == spec.anchor {
		return normalOrdinaryV2OpenAt(int(p.root.Fd()), strings.TrimPrefix(spec.path, "/"), !spec.file, false)
	}
	parent, name, e := p.pathParent(spec)
	if e != nil {
		return nil, e
	}
	defer parent.Close()
	return normalOrdinaryV2OpenAt(int(parent.Fd()), name, !spec.file, true)
}
func (p *normalOrdinaryV2DirectoryPins) verify() error {
	for _, name := range []string{"/run", "/var/log"} {
		current, e := normalOrdinaryV2OpenAt(int(p.root.Fd()), strings.TrimPrefix(name, "/"), true, false)
		if e != nil {
			return e
		}
		now, e := readNormalOrdinaryV2PathFact(int(current.Fd()), name)
		current.Close()
		held, he := readNormalOrdinaryV2PathFact(int(p.anchors[name].Fd()), name)
		if e != nil || he != nil || now != held || now.UID != 0 || now.GID != 0 || now.Mode&07777 != 0755 {
			return ErrUnknown
		}
	}

	for _, x := range p.paths {
		now, e := p.openSpec(x.spec)
		if e != nil {
			return e
		}
		fact, e := readNormalOrdinaryV2PathFact(int(now.Fd()), x.spec.path)
		now.Close()
		held, he := readNormalOrdinaryV2PathFact(int(x.f.Fd()), x.spec.path)
		if e != nil || he != nil || fact != x.fact || held != x.fact {
			return errors.Join(ErrUnknown, fmt.Errorf("ordinary2 path identity/mode drift: %s", x.spec.path), e, he)
		}
	}
	return nil
}

// 此低层函数只执行固定FD/文件系统原语。root和anchorFacts的真正来源由下方
// opaque constructor核验；直接单测调用本函数不构成原FS/角色/normal资格。
// append必须原O_EXCL+file及dir fsync。任一步失败不删前缀/目录，不隐式重放。
func prepareNormalOrdinaryV2PathsAt(ctx context.Context, root *os.File, anchorFacts map[string]normalFilesystemObject, appendStage func(string, []byte) error) (out *normalOrdinaryV2DirectoryPins, err error) {
	if ctx == nil || ctx.Err() != nil || root == nil || appendStage == nil {
		return nil, ErrUnknown
	}
	n, e := unix.FcntlInt(root.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	out = &normalOrdinaryV2DirectoryPins{root: os.NewFile(uintptr(n), "ordinary2-original-root"), anchors: map[string]*os.File{}}
	owned := out
	ok := false
	defer func() {
		if !ok {
			owned.close()
		}
	}()
	for _, name := range []string{"/run", "/var/log", "/var/lib/chrony", "/etc/resolv.conf"} {
		f, e := normalOrdinaryV2OpenAt(n, strings.TrimPrefix(name, "/"), name != "/etc/resolv.conf", false)
		if e != nil {
			return nil, e
		}
		out.anchors[name] = f
		obj, e := normalFilesystemFDObject(int(f.Fd()))
		want, exists := anchorFacts[name]
		if e != nil || !exists || obj != want || obj.Readonly {
			return nil, errors.Join(ErrUnknown, fmt.Errorf("ordinary2 original RW anchor drift: %s", name), e)
		}
		var st unix.Stat_t
		if unix.Fstat(int(f.Fd()), &st) != nil || st.Uid != 0 || st.Gid != 0 || st.Mode&0022 != 0 {
			return nil, ErrUnknown
		}
		if (name == "/run" || name == "/var/log") && st.Mode&07777 != 0755 {
			return nil, fmt.Errorf("ordinary2 original parent must be Root0755: %s", name)
		}
	}
	// 先验证所有已有对象。marker不能从旧启动/未知Root任务继承，原目录须空。
	for _, spec := range normalOrdinaryV2Paths() {
		f, e := out.openSpec(spec)
		if errors.Is(e, unix.ENOENT) && spec.path != spec.anchor {
			continue
		}
		if e != nil {
			return nil, e
		}
		e = normalOrdinaryV2ExpectedInitial(f, spec)
		f.Close()
		if e != nil {
			return nil, e
		}
	}
	intent, _ := json.Marshal(struct {
		Scope   string                            `json:"scope"`
		Anchors map[string]normalFilesystemObject `json:"anchors"`
	}{Scope: normalOrdinaryV2LayoutScope, Anchors: anchorFacts})
	if e = appendStage("intent", intent); e != nil {
		return nil, e
	}
	for _, spec := range normalOrdinaryV2Paths() {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		f, e := out.openSpec(spec)
		if errors.Is(e, unix.ENOENT) && spec.path != spec.anchor {
			parent, name, pe := out.pathParent(spec)
			if pe != nil {
				return nil, pe
			}
			e = unix.Mkdirat(int(parent.Fd()), name, 0700)
			if e == nil {
				e = unix.Fsync(int(parent.Fd()))
			}
			parent.Close()
			if e != nil {
				return nil, e
			}
			f, e = out.openSpec(spec)
		}
		if e != nil {
			return nil, e
		}
		if e = normalOrdinaryV2ExpectedInitial(f, spec); e != nil {
			f.Close()
			return nil, e
		}
		fact, e := readNormalOrdinaryV2PathFact(int(f.Fd()), spec.path)
		if e != nil {
			f.Close()
			return nil, e
		}
		out.paths = append(out.paths, normalOrdinaryV2HeldPath{spec, f, fact})
	}
	before := make([]normalOrdinaryV2PathFact, 0, len(out.paths))
	for _, x := range out.paths {
		before = append(before, x.fact)
	}
	raw, _ := json.Marshal(before)
	// 全部原inode先有持久身份，才授普通UID目录权。可见但未fsync的identity永不继续。
	if e = appendStage("identity", raw); e != nil {
		return nil, e
	}
	for i := range out.paths {
		x := &out.paths[i]
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		current, e := readNormalOrdinaryV2PathFact(int(x.f.Fd()), x.spec.path)
		if e != nil || current != x.fact {
			return nil, ErrUnknown
		}
		if e = unix.Fchown(int(x.f.Fd()), int(x.spec.uid), int(x.spec.gid)); e != nil {
			return nil, e
		}
		if e = unix.Fchmod(int(x.f.Fd()), x.spec.mode); e != nil {
			return nil, e
		}
		if e = unix.Fsync(int(x.f.Fd())); e != nil {
			return nil, e
		}
		x.fact, e = readNormalOrdinaryV2PathFact(int(x.f.Fd()), x.spec.path)
		if e != nil || x.fact.UID != x.spec.uid || x.fact.GID != x.spec.gid || x.fact.Mode&07777 != x.spec.mode {
			return nil, ErrUnknown
		}
	}
	for _, f := range out.anchors {
		if e = unix.Fsync(int(f.Fd())); e != nil {
			return nil, e
		}
	}
	if e = out.verify(); e != nil {
		return nil, e
	}
	after := make([]normalOrdinaryV2PathFact, 0, len(out.paths))
	for _, x := range out.paths {
		after = append(after, x.fact)
	}
	raw, _ = json.Marshal(after)
	if e = appendStage("complete", raw); e != nil {
		return nil, e
	}
	ok = true
	return out, nil
}

// 此来源只表明本Root在原prepared窗口落实了固定目录，不证明未来writer全闭。
// ordinary gate后继还须显式消费此对象；没有从bool/path/JSON补签的构造器。
type normalSystemOrdinaryV2LayoutSource struct {
	mu         sync.Mutex
	authority  *normalSystemResourceAuthority
	filesystem *normalFilesystemOriginSource
	proof      *normalSystemRoleBrokerExecProof
	originalFS normalFilesystemOriginReference
	pins       *normalOrdinaryV2DirectoryPins
	history    [3]normalSystemRoleFileWitness
	closed     bool
}

func (o *normalSystemOrdinaryV2LayoutSource) close() {
	if o != nil {
		o.mu.Lock()
		defer o.mu.Unlock()
		if !o.closed {
			o.closed = true
			o.pins.close()
		}
	}
}

// 调用者持原resource flock。锁序始终resource→FS/proof→producerMu，不反入broker锁。
func prepareNormalSystemOrdinaryV2LayoutLocked(ctx context.Context, g *Guard, s *lockedState, r *record, system *normalSystemLifecycleSource, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) (out *normalSystemOrdinaryV2LayoutSource, err error) {
	if ctx == nil || ctx.Err() != nil || g == nil || s == nil || r == nil || system == nil || filesystem == nil || proof == nil {
		return nil, ErrUnknown
	}
	if e := normalSystemRoleServiceCaller(); e != nil {
		return nil, e
	}
	mode, e := normalSystemRoleOriginalControlProtocol(ctx, system)
	if e != nil || mode != 2 {
		return nil, errors.Join(ErrUnknown, e)
	}
	a := lookupNormalSystemAuthority(g)
	if a == nil || a.source != system || a.admission.failed.Load() || a.admission.businessAllowed() || a.reference.validate() != nil || filesystem.g != g || a.declaration == nil || a.declaration.declaration.ProtocolVersion != 3 || a.origin.ProtocolVersion != 2 {
		return nil, ErrUnknown
	}
	if e = a.matchOriginalRecord(r, s); e != nil {
		return nil, e
	}
	prepared, e := readNormalSystemRoleFileWitness(s, normalSystemAuthorityStageName(a.reference.InstanceNonce, "prepared"))
	if e != nil || prepared != a.prepared {
		return nil, ErrUnknown
	}
	fsref, e := filesystem.referenceLocked(ctx, s)
	if e != nil || !reflect.DeepEqual(fsref, a.origin.Filesystem) {
		return nil, errors.Join(ErrUnknown, e)
	}
	task, e := proof.originalTaskForFilesystemLocked(ctx, s, system)
	if e != nil {
		return nil, e
	}
	task.close()
	anchors := map[string]normalFilesystemObject{}
	filesystem.mu.Lock()
	for _, f := range filesystem.intent.Mounts {
		switch f.Mount.Point {
		case "/run", "/var/log", "/var/lib/chrony", "/etc/resolv.conf":
			anchors[f.Mount.Point] = f.Object
		}
	}
	filesystem.mu.Unlock()
	root, e := normalFilesystemOpenAt(unix.AT_FDCWD, "/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
	if e != nil {
		return nil, e
	}
	defer root.Close()
	obj, e := normalFilesystemFDObject(int(root.Fd()))
	if e != nil || obj != fsref.Root {
		return nil, ErrUnknown
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if e = normalOrdinaryV2LayoutRejectHistoryLocked(s); e != nil {
		return nil, e
	}
	out = &normalSystemOrdinaryV2LayoutSource{authority: a, filesystem: filesystem, proof: proof, originalFS: fsref}
	origin := struct {
		Scope     string                         `json:"scope"`
		Origin    normalSystemRoleServiceOrigin  `json:"origin"`
		Authority normalSystemAuthorityReference `json:"authority"`
	}{normalOrdinaryV2LayoutScope, a.origin, a.reference}
	originRaw, _ := json.Marshal(origin)
	var previous string
	out.pins, e = prepareNormalOrdinaryV2PathsAt(ctx, root, anchors, func(stage string, raw []byte) error {
		b, me := json.Marshal(struct {
			ProtocolVersion uint32          `json:"protocol_version"`
			OriginSHA       string          `json:"origin_sha256"`
			Stage           string          `json:"stage"`
			Previous        string          `json:"previous_sha256,omitempty"`
			Facts           json.RawMessage `json:"facts"`
		}{1, physicalDigest(originRaw), stage, previous, raw})
		if me != nil {
			return me
		}
		if me = s.createPhysicalFile(normalOrdinaryV2LayoutStageName(stage), b); me != nil {
			return me
		}
		previous = physicalDigest(b)
		return nil
	})
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	owned := out
	ok := false
	defer func() {
		if !ok {
			owned.close()
		}
	}()
	for i, stage := range normalOrdinaryV2LayoutStages {
		out.history[i], e = readNormalSystemRoleFileWitness(s, normalOrdinaryV2LayoutStageName(stage))
		if e != nil {
			return nil, e
		}
	}
	ok = true
	return out, nil
}

// 普通Observe只有读取，不fsync、不补identity、不修owner/mode、不认领新Root。
func (o *normalSystemOrdinaryV2LayoutSource) referenceLocked(ctx context.Context, s *lockedState, r *record) ([3]string, error) {
	var out [3]string
	if o == nil || ctx == nil || ctx.Err() != nil || s == nil || r == nil || o.authority == nil {
		return out, ErrUnknown
	}
	a := o.authority
	if a.admission.failed.Load() || r.Identity != a.origin.ResourceIdentity || r.Floor < a.origin.ResourceFloor {
		return out, ErrUnknown
	}
	ref, e := o.filesystem.referenceLocked(ctx, s)
	if e != nil || !reflect.DeepEqual(ref, o.originalFS) {
		return out, ErrUnknown
	}
	task, e := o.proof.originalTaskForFilesystemLocked(ctx, s, a.source)
	if e != nil {
		return out, e
	}
	task.close()
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.pins == nil {
		return out, ErrUnknown
	}
	for i, stage := range normalOrdinaryV2LayoutStages {
		got, e := readNormalSystemRoleFileWitness(s, normalOrdinaryV2LayoutStageName(stage))
		if e != nil || got != o.history[i] {
			return out, ErrUnknown
		}
		out[i] = got.SHA256
	}
	if e = o.pins.verify(); e != nil {
		return [3]string{}, errors.Join(ErrUnknown, e)
	}
	return out, nil
}

// readPhysicalFile的不存在合同是exists=false/err=nil；不能把nil误当读失败。
// 可见partial/complete或任何读/类型/权限错误仍一律Unknown，不重放/不删除。
func normalOrdinaryV2LayoutRejectHistoryLocked(s *lockedState) error {
	if s == nil || s.dir == nil || s.lock == nil {
		return ErrUnknown
	}
	for _, stage := range normalOrdinaryV2LayoutStages {
		if _, exists, e := s.readPhysicalFile(normalOrdinaryV2LayoutStageName(stage)); e != nil || exists {
			return errors.Join(ErrUnknown, fmt.Errorf("ordinary2 directory history exists or is unreadable"), e)
		}
	}
	return nil
}
