//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalSystemRoleBuildPlanPath = "/neonvm/config/dsga-normal-build-plan.json"

// 私有计划只能由原只读System来源构造；socket请求只允许角色枚举，不传路径、
// 参数、环境或namespace。向量只保留在内存，持久/输出一律使用摘要。
type normalSystemRoleLaunchSpec struct {
	role      string
	argv, env []string
	uid, gid  uint32
	binding   normalSystemLifecycleBinding
	sourceSHA string
}
type normalSystemRoleNamespaceIdentity struct {
	Name   string `json:"name"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}
type normalSystemRoleNamespaceSource struct {
	mu         sync.Mutex
	files      map[string]*os.File
	identities []normalSystemRoleNamespaceIdentity
	closed     bool
}

// 钉住当前原namespace对象，不从请求路径打开另一个视图；每次出生都必须
// 对比本线程及原child的实际namespace。持有FD防inode身份被生命周期重用。
func captureNormalSystemRoleNamespaces() (*normalSystemRoleNamespaceSource, error) {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return nil, fmt.Errorf("System role namespaces require real root")
	}
	n := &normalSystemRoleNamespaceSource{files: map[string]*os.File{}}
	ok := false
	defer func() {
		if !ok {
			n.close()
		}
	}()
	for _, name := range []string{"mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time"} {
		fd, e := unix.Open("/proc/thread-self/ns/"+name, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if e != nil {
			return nil, e
		}
		f := os.NewFile(uintptr(fd), "original-System-namespace")
		n.files[name] = f
		var st unix.Stat_t
		var fs unix.Statfs_t
		if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.NSFS_MAGIC || st.Ino == 0 {
			return nil, fmt.Errorf("original System namespace source differs")
		}
		n.identities = append(n.identities, normalSystemRoleNamespaceIdentity{Name: name, Device: st.Dev, Inode: st.Ino})
	}
	ok = true
	return n, nil
}
func (n *normalSystemRoleNamespaceSource) close() {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		for _, f := range n.files {
			f.Close()
		}
		n.closed = true
	}
}
func (n *normalSystemRoleNamespaceSource) verify(procfd int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return fmt.Errorf("original System namespace source closed")
	}
	for _, want := range n.identities {
		var fd int
		var e error
		if procfd < 0 {
			fd, e = unix.Open("/proc/thread-self/ns/"+want.Name, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		} else {
			fd, e = unix.Openat(procfd, "ns/"+want.Name, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		}
		if e != nil {
			return e
		}
		var st unix.Stat_t
		e = unix.Fstat(fd, &st)
		unix.Close(fd)
		if e != nil || st.Dev != want.Device || st.Ino != want.Inode {
			return fmt.Errorf("original System namespace differs")
		}
	}
	return nil
}
func (n *normalSystemRoleNamespaceSource) snapshot() ([]normalSystemRoleNamespaceIdentity, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("original namespace source closed")
	}
	return append([]normalSystemRoleNamespaceIdentity(nil), n.identities...), nil
}

type normalSystemRoleBuildPlan struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	ComputeUID      uint32   `json:"compute_uid"`
	ComputeGID      uint32   `json:"compute_gid"`
	ComputeArgv     []string `json:"compute_argv"`
	ComputeEnv      []string `json:"compute_env"`
	LoaderPaths     []string `json:"loader_paths"`
}

func decodeNormalSystemRoleBuildPlan(raw []byte) (normalSystemRoleBuildPlan, error) {
	var p normalSystemRoleBuildPlan
	if len(raw) == 0 || len(raw) > 64<<10 {
		return p, fmt.Errorf("fixed role build plan exceeds bound")
	}
	if e := control.UnmarshalObject(raw, &p); e != nil {
		return p, fmt.Errorf("fixed role build plan malformed")
	}
	if p.ProtocolVersion != 1 || p.ComputeUID == 0 || p.ComputeGID == 0 || p.ComputeUID > 1<<31-1 || p.ComputeGID > 1<<31-1 || p.ComputeArgv == nil || len(p.ComputeArgv) == 0 || len(p.ComputeArgv) > 64 || p.ComputeArgv[0] != "/usr/local/bin/compute_ctl" || p.ComputeEnv == nil || p.LoaderPaths == nil {
		return p, fmt.Errorf("fixed role build plan unsupported")
	}
	if _, e := normalSystemVectorSHA(p.ComputeArgv); e != nil {
		return p, e
	}
	if e := normalSystemCleanEnvironment(p.ComputeEnv); e != nil {
		return p, e
	}
	return p, nil
}
func newNormalSystemRoleLaunchSpec(ctx context.Context, s *normalSystemLifecycleSource, role string) (normalSystemRoleLaunchSpec, error) {
	var out normalSystemRoleLaunchSpec
	if ctx == nil || ctx.Err() != nil || s == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return out, errors.Join(ErrUnknown, fmt.Errorf("original role source unavailable"))
	}
	digest, e := s.referenceSHA256()
	if e != nil {
		return out, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return out, ErrUnknown
	}
	out = normalSystemRoleLaunchSpec{role: role, binding: s.binding, sourceSHA: digest, env: []string{"PATH=/neonvm/bin"}}
	if role != "compute" {
		out.argv, e = normalSystemFixedRoleArgv(role)
		if e != nil {
			return normalSystemRoleLaunchSpec{}, e
		}
		return out, nil
	}
	canonical := s.logical[normalSystemRoleBuildPlanPath]
	if canonical == "" {
		return normalSystemRoleLaunchSpec{}, errors.Join(ErrUnknown, fmt.Errorf("original fixed compute build plan absent from source"))
	}
	f, e := nativeBirthOpenRO(canonical)
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	defer f.Close()
	raw, e := nativeBirthReadBounded(f, 64<<10)
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	var wanted string
	for _, entry := range s.manifest.Files {
		if entry.Path == normalSystemRoleBuildPlanPath {
			wanted = entry.SHA256
		}
	}
	if wanted == "" || physicalDigest(raw) != wanted {
		return normalSystemRoleLaunchSpec{}, fmt.Errorf("original compute plan bytes differ")
	}
	p, e := decodeNormalSystemRoleBuildPlan(raw)
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	ah, _ := normalSystemVectorSHA(p.ComputeArgv)
	eh, _ := normalSystemVectorSHA(p.ComputeEnv)
	if ah != s.manifest.ComputeArgvSHA256 || eh != s.manifest.ComputeEnvironmentSHA256 {
		return normalSystemRoleLaunchSpec{}, fmt.Errorf("original compute vector digest differs")
	}
	account, e := user.Lookup("postgres")
	if e != nil {
		return normalSystemRoleLaunchSpec{}, e
	}
	uid, e := strconv.ParseUint(account.Uid, 10, 32)
	if e != nil || uint32(uid) != p.ComputeUID {
		return normalSystemRoleLaunchSpec{}, fmt.Errorf("original compute UID differs")
	}
	gid, e := strconv.ParseUint(account.Gid, 10, 32)
	if e != nil || uint32(gid) != p.ComputeGID {
		return normalSystemRoleLaunchSpec{}, fmt.Errorf("original compute GID differs")
	}
	out.argv = append([]string(nil), p.ComputeArgv...)
	out.env = append([]string{}, p.ComputeEnv...)
	out.uid = p.ComputeUID
	out.gid = p.ComputeGID
	return out, nil
}
func (p normalSystemRoleLaunchSpec) referenceSHA256() (string, error) {
	ah, e := normalSystemVectorSHA(p.argv)
	if e != nil {
		return "", e
	}
	eh, e := normalSystemVectorSHA(p.env)
	if e != nil {
		return "", e
	}
	raw, e := json.Marshal(struct {
		Role                       string
		UID, GID                   uint32
		ArgvSHA, EnvSHA, SourceSHA string
		Binding                    normalSystemLifecycleBinding
	}{p.role, p.uid, p.gid, ah, eh, p.sourceSHA, p.binding})
	if e != nil {
		return "", e
	}
	return physicalDigest(raw), nil
}
