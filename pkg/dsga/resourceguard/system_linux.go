//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const systemCgroup = "/sys/fs/cgroup/neon-system"

// 固定路径、root独占domain组；系统服务不能修改额度、迁移入口或创建子组。只在
// 原只读Config明确开启时读取，不把缺失目录自动收养成一个新的原Guest身份。
func systemCgroupIdentity(pgDevice uint64) (uint64, uint64, error) {
	if err := secureParents(systemCgroup); err != nil {
		return 0, 0, err
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(systemCgroup, &fs); err != nil {
		return 0, 0, err
	}
	var st unix.Stat_t
	if err := unix.Stat(systemCgroup, &st); err != nil {
		return 0, 0, err
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC || uint64(st.Dev) != pgDevice || st.Ino == 0 {
		return 0, 0, fmt.Errorf("system cgroup is not on original unified hierarchy")
	}
	kind, err := kernelText(filepath.Join(systemCgroup, "cgroup.type"))
	if err != nil || kind != "domain" {
		return 0, 0, fmt.Errorf("system bound requires domain cgroup")
	}
	for _, name := range []string{"memory.max", "memory.swap.max", "cgroup.procs", "cgroup.threads"} {
		f, err := rootFile(filepath.Join(systemCgroup, name), false)
		if err != nil {
			return 0, 0, err
		}
		f.Close()
	}
	return uint64(st.Dev), st.Ino, nil
}

// /proc对象属各进程UID，不能使用只接受root文件的kernelText。这里仅访问固定
// procfs下规范PID的stat/cgroup，O_NOFOLLOW且有界读；没有来自外部请求的路径。
func bootProcText(pid int, name string) (string, error) {
	if pid < 1 || (name != "stat" && name != "cgroup") {
		return "", fmt.Errorf("invalid fixed boot observation")
	}
	path := "/proc/" + strconv.Itoa(pid) + "/" + name
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var fs unix.Statfs_t
	if err = unix.Fstatfs(fd, &fs); err != nil {
		return "", err
	}
	if fs.Type != unix.PROC_SUPER_MAGIC {
		return "", fmt.Errorf("boot observation is not procfs")
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return "", err
	}
	if len(data) > 64*1024 {
		return "", fmt.Errorf("boot observation exceeds bound")
	}
	return strings.TrimSpace(string(data)), nil
}
func bootProcGone(err error) bool { return errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESRCH) }
func bootProcGroup(pid int) (string, error) {
	text, err := bootProcText(pid, "cgroup")
	if err != nil {
		return "", err
	}
	// 只接受统一层级，不能在混合层级里挑一条看似匹配的路径。
	if !strings.HasPrefix(text, "0::/") || strings.Contains(text, "\n") {
		return "", fmt.Errorf("unexpected boot cgroup hierarchy")
	}
	return strings.TrimPrefix(text, "0::"), nil
}
func bootProc(pid int) (systemProcess, bool, error) {
	text, err := bootProcText(pid, "stat")
	if err != nil {
		return systemProcess{}, false, err
	}
	return bootProcessStat(text, pid)
}
func groupPIDs(path string) ([]int, error) {
	text, err := kernelText(filepath.Join(path, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	set := make(map[int]bool)
	var result []int
	for _, s := range strings.Fields(text) {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil || n == 0 || n > uint64(MaxPidsTasks) || s != strconv.FormatUint(n, 10) {
			return nil, fmt.Errorf("invalid boot cgroup process list")
		}
		if !set[int(n)] {
			set[int(n)] = true
			result = append(result, int(n))
		}
	}
	return result, nil
}
func (k *linuxKernel) rootUserspace() ([]systemProcess, error) {
	pids, err := groupPIDs("/sys/fs/cgroup")
	if err != nil {
		return nil, err
	}
	var result []systemProcess
	for _, pid := range pids {
		proc, kthread, err := bootProc(pid)
		if bootProcGone(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if kthread {
			continue
		}
		group, err := bootProcGroup(pid)
		if bootProcGone(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if group == "/neon-system" {
			continue
		}
		if group != "/" {
			return nil, fmt.Errorf("boot process changed to unexpected cgroup")
		}
		result = append(result, proc)
	}
	return result, nil
}
func (k *linuxKernel) moveSystem(original systemProcess) error {
	current, kthread, err := bootProc(original.pid)
	if bootProcGone(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != original || kthread {
		return fmt.Errorf("boot process changed before migration")
	}
	group, err := bootProcGroup(original.pid)
	if bootProcGone(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if group == "/neon-system" {
		return nil
	}
	if group != "/" {
		return fmt.Errorf("only root userspace may enter system cgroup during sysinit")
	}
	// cgroup.procs迁入整个线程组。数值PID写不是pidfd原子事务，前后starttime只能
	// 检出竞争，不能消除它；因此PG未出生且唯一root迁移者是本sysinit这一前提必需。
	// 若竞态/部分生效失败，不迁回根组、不当作NoEffect，保留未确认状态阻止PG出生。
	if err = writeNumber(filepath.Join(systemCgroup, "cgroup.procs"), uint64(original.pid)); err != nil {
		if bootProcGone(err) {
			return nil
		}
		return err
	}
	current, kthread, err = bootProc(original.pid)
	if bootProcGone(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != original || kthread {
		return fmt.Errorf("boot process changed during migration")
	}
	group, err = bootProcGroup(original.pid)
	if bootProcGone(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if group != "/neon-system" {
		return fmt.Errorf("boot process migration not observed")
	}
	return nil
}
func (k *linuxKernel) readSystemBound() (*SystemEvidence, error) {
	s := &SystemEvidence{Scope: SystemChargedScope}
	for _, entry := range []struct {
		name      string
		out       *control.Uint64
		unlimited bool
	}{
		{"memory.max", &s.MemoryMaxBytes, true}, {"memory.current", &s.MemoryCurrentBytes, false}, {"memory.swap.max", &s.SwapMaxBytes, true}, {"memory.swap.current", &s.SwapCurrentBytes, false},
	} {
		n, err := readNumber(filepath.Join(systemCgroup, entry.name), entry.unlimited)
		if err != nil {
			return nil, err
		}
		*entry.out = control.Uint64(n)
	}
	pids, err := groupPIDs(systemCgroup)
	if err != nil {
		return nil, err
	}
	s.ProcessCount = control.Uint64(len(pids))
	group, err := bootProcGroup(1)
	if err != nil {
		return nil, err
	}
	if group != "/" && group != "/neon-system" {
		return nil, fmt.Errorf("PID1 outside original boot scope")
	}
	s.PID1Member = group == "/neon-system"
	root, err := k.rootUserspace()
	if err != nil {
		return nil, err
	}
	s.RootUserspaceCount = control.Uint64(len(root))
	return s, nil
}
