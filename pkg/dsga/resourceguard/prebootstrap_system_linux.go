//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const FixedPrebootstrapSystemPolicyPath = "/neonvm/runtime/" + RuntimeISOPreSystemAssetName
const PrebootstrapSystemKernelArg = "neonvm.dsga.prebootstrap_system=normal-v1"
const PrebootstrapSystemNormalSplitKernelArg = "neonvm.dsga.prebootstrap_system=normal-split-v1"

// 三入口只读固定ISO来源，无policy/PID/path参数。当前尚未接sysinit/Runner；
// 缺marker、准确normal-v1 Origin或持久state盘均失败，绝不生成legacy成功。
func (g *Guard) PrepareSystemBeforeBootstrap(ctx context.Context) (PrebootstrapSystemResult, error) {
	in, err := g.prebootstrapSystemInputs(ctx)
	if err != nil {
		return PrebootstrapSystemResult{}, err
	}
	return g.prebootstrapSystem(ctx, in, preSystemPrepare)
}
func (g *Guard) ObserveSystemBeforeBootstrap(ctx context.Context) (PrebootstrapSystemResult, error) {
	in, err := g.prebootstrapSystemInputs(ctx)
	if err != nil {
		return PrebootstrapSystemResult{}, err
	}
	return g.prebootstrapSystem(ctx, in, preSystemObserve)
}
func (g *Guard) ReplaySystemBeforeBootstrap(ctx context.Context) (PrebootstrapSystemResult, error) {
	in, err := g.prebootstrapSystemInputs(ctx)
	if err != nil {
		return PrebootstrapSystemResult{}, err
	}
	return g.prebootstrapSystem(ctx, in, preSystemReplay)
}

func (g *Guard) prebootstrapSystemInputs(ctx context.Context) (prebootstrapSystemInputs, error) {
	var in prebootstrapSystemInputs
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return in, fmt.Errorf("pre-bootstrap System phase requires real root")
	}
	source, runtimeRaw, err := runtimeSource()
	if err != nil {
		return in, err
	}
	if source.Config != g.config {
		return in, fmt.Errorf("pre-bootstrap original resource configuration differs")
	}
	cmdline, err := kernelText("/proc/cmdline")
	if err != nil {
		return in, err
	}
	f, err := rootFile(FixedPrebootstrapSystemPolicyPath, false)
	if err != nil {
		return in, err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return in, err
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return in, fmt.Errorf("pre-bootstrap System policy requires root read-only runtime ISO")
	}
	policyRaw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return in, err
	}
	if len(policyRaw) > 64*1024 {
		return in, fmt.Errorf("pre-bootstrap System policy exceeds bound")
	}
	var policy prebootstrapSystemPolicy
	if err := json.Unmarshal(policyRaw, &policy); err != nil {
		return in, err
	}
	if err := preSystemBootMarker(cmdline, policy); err != nil {
		return in, err
	}
	if policy.RuntimeDocumentSHA256 != physicalDigest(runtimeRaw) {
		return in, fmt.Errorf("pre-bootstrap System policy differs from original runtime bytes")
	}
	// 必须在Origin捕获前由显式sysinit准备securityfs/sysctl；此只读loader不mount，
	// 不调优、不补捕获，不把当前可见值追溯写成首次启动事实。
	origin, err := g.ObservePhysicalKernelOrigin(ctx)
	if err != nil {
		return in, err
	}
	if origin.Policy.ProfileID != policy.PhysicalProfileID {
		return in, fmt.Errorf("old component Origin cannot enroll normal-v1 System phase")
	}
	ref, err := origin.ReferenceSHA256()
	if err != nil {
		return in, err
	}
	var document RuntimeDocument
	if err := json.Unmarshal(runtimeRaw, &document); err != nil {
		return in, err
	}
	in = prebootstrapSystemInputs{source: source, sourceRaw: append([]byte(nil), document.SourceJSON...), policy: policy, policyRaw: append([]byte(nil), policyRaw...), policySHA: physicalDigest(policyRaw), origin: origin, originSHA: ref}
	if err := in.validate(g.config); err != nil {
		return prebootstrapSystemInputs{}, err
	}
	return in, nil
}

// cgroup写仅固定System memory/swap；不施PG额度、不动Huge池、不把未出生PG移组。
// 原空System的max或同原cap才可继续，任意另一有限cap/已有swap都不是可收养来源。
func (k *linuxKernel) prebootstrapSystemApply(cap control.Uint64) error {
	if cap == 0 || uint64(cap)%(64<<10) != 0 {
		return fmt.Errorf("invalid original pre-bootstrap System cap")
	}
	max, err := readNumber(systemCgroup+"/memory.max", true)
	if err != nil {
		return err
	}
	swap, err := readNumber(systemCgroup+"/memory.swap.max", true)
	if err != nil {
		return err
	}
	current, err := readNumber(systemCgroup+"/memory.current", false)
	if err != nil {
		return err
	}
	swapCurrent, err := readNumber(systemCgroup+"/memory.swap.current", false)
	if err != nil {
		return err
	}
	if max != math.MaxUint64 && max != uint64(cap) || swap != math.MaxUint64 && swap != 0 || current > uint64(cap) || swapCurrent != 0 {
		return fmt.Errorf("pre-bootstrap System existing bound differs from original intent")
	}
	if swap != 0 {
		if err := writeNumber(systemCgroup+"/memory.swap.max", 0); err != nil {
			return err
		}
	}
	if max != uint64(cap) {
		return writeNumber(systemCgroup+"/memory.max", uint64(cap))
	}
	return nil
}

// /proc初始PID命名空间内所有TGID/线程做实际有界census，确认pid_max调低前
// 没有遗留高PID；任务消失最多完整重读4轮，不将读不到的条目算成0任务。
// 严格早期wrapper保留给KernelPrep，不能让PG任务进入原准备阶段。
func preSystemTaskCensus(pidMax uint32) (control.Uint64, control.Uint64, error) {
	return preSystemTaskCensusProfile(pidMax, "")
}

// 读取阶段中立：仅允许适配器声明的角色。首次/pending是否可有PG另由recursive
// PGEmpty门禁控制；完成后还须同源正式Bootstrap.Applied及SystemObserved，绝不补出生。
func preSystemTaskCensusProfile(pidMax uint32, profile string) (control.Uint64, control.Uint64, error) {
	if profile != "" && !preSystemPolicyVersionMatches(profile, 1) && !preSystemPolicyVersionMatches(profile, 2) {
		return 0, 0, fmt.Errorf("unrecognized System census profile")
	}
	for attempt := 0; attempt < 4; attempt++ {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return 0, 0, err
		}
		seen := make(map[uint64]bool)
		var maximum uint64
		churn := false
		for _, entry := range entries {
			if entry.Name() == "" || entry.Name()[0] < '0' || entry.Name()[0] > '9' {
				continue
			}
			pid, err := canonicalUint(entry.Name())
			if err != nil || pid == 0 || pid >= uint64(pidMax) {
				return 0, 0, fmt.Errorf("pre-bootstrap task exceeds original global PID bound")
			}
			tasks, err := os.ReadDir("/proc/" + entry.Name() + "/task")
			if bootProcGone(err) {
				churn = true
				break
			}
			if err != nil {
				return 0, 0, err
			}
			for _, task := range tasks {
				tid, err := canonicalUint(task.Name())
				if err != nil || tid == 0 || tid >= uint64(pidMax) {
					return 0, 0, fmt.Errorf("pre-bootstrap thread exceeds original global PID bound")
				}
				_, kthread, err := bootProc(int(tid))
				if bootProcGone(err) {
					churn = true
					break
				}
				if err != nil {
					return 0, 0, err
				}
				if !kthread {
					group, err := bootProcGroup(int(tid))
					if bootProcGone(err) {
						churn = true
						break
					}
					if err != nil {
						return 0, 0, err
					}
					if !preSystemTaskGroupAllowed(profile, group) {
						return 0, 0, fmt.Errorf("ordinary task outside original pre-bootstrap domains")
					}
				}
				seen[tid] = true
				if tid > maximum {
					maximum = tid
				}
			}
			if churn {
				break
			}
		}
		if churn {
			continue
		}
		if len(seen) == 0 || len(seen) >= int(pidMax) {
			return 0, 0, fmt.Errorf("invalid pre-bootstrap global task census")
		}
		return control.Uint64(len(seen)), control.Uint64(maximum), nil
	}
	return 0, 0, fmt.Errorf("%w: pre-bootstrap task census did not settle", ErrUnknown)
}

func preSystemNamespaces() ([4]control.Uint64, error) {
	var result [4]control.Uint64
	for i, name := range []string{"pid", "mnt", "ipc", "user"} {
		var self, init unix.Stat_t
		if err := unix.Stat("/proc/self/ns/"+name, &self); err != nil {
			return result, err
		}
		if err := unix.Stat("/proc/1/ns/"+name, &init); err != nil {
			return result, err
		}
		if self.Dev != init.Dev || self.Ino != init.Ino || self.Ino == 0 {
			return result, fmt.Errorf("pre-bootstrap process differs from original PID1 namespace")
		}
		result[i] = control.Uint64(self.Ino)
	}
	return result, nil
}

func (k *linuxKernel) prebootstrapSystemRead() (prebootstrapSystemSnapshot, error) {
	var s prebootstrapSystemSnapshot
	id, err := k.Identity()
	if err != nil {
		return s, err
	}
	s.Physical, err = readPhysicalKernelComponent(id)
	if err != nil {
		return s, err
	}
	system, err := k.readSystemBound()
	if err != nil {
		return s, err
	}
	s.System = *system
	s.Namespaces, err = preSystemNamespaces()
	if err != nil {
		return s, err
	}
	var fs unix.Statfs_t
	var st unix.Stat_t
	if err := unix.Statfs(k.config.StateDir, &fs); err != nil {
		return s, err
	}
	if err := unix.Stat(k.config.StateDir, &st); err != nil {
		return s, err
	}
	// 首profile只允许独立持久ext4 state盘；tmpfs/overlay/只读FS不能保存原WAL。
	// 容量/允许写者/完整F_state仍须FSProfile另证，本检查不自报完整文件预算。
	if fs.Type != unix.EXT4_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY != 0 || st.Ino == 0 {
		return s, fmt.Errorf("pre-bootstrap state requires writable persistent ext4 filesystem")
	}
	var rootFS unix.Statfs_t
	var rootStat unix.Stat_t
	if err := unix.Statfs("/", &rootFS); err != nil {
		return s, err
	}
	if err := unix.Stat("/", &rootStat); err != nil {
		return s, err
	}
	if st.Dev == rootStat.Dev || rootFS.Flags&unix.ST_RDONLY == 0 {
		return s, fmt.Errorf("pre-bootstrap requires a separate persistent state volume and read-only RootFS")
	}
	s.RootFilesystemReadonly = true
	s.StateDevice = control.Uint64(st.Dev)
	s.StateInode = control.Uint64(st.Ino)
	profile, err := physicalKernelObservationBuild(s.Physical.KernelRelease, s.Physical.KernelNotesSHA256)
	if err != nil || !preSystemPolicyVersionMatches(profile, 1) && !preSystemPolicyVersionMatches(profile, 2) {
		return s, fmt.Errorf("unsupported pre-System physical profile")
	}
	s.TaskCount, s.MaximumTaskID, err = preSystemTaskCensusProfile(s.Physical.PIDMax, profile)
	if err != nil {
		return s, err
	}
	s.PGEmpty, err = preSystemRecursivePGEmpty()
	if err != nil {
		return s, err
	}
	s.MemoryHighUnlimited = true
	for _, group := range []string{pgCgroup, systemCgroup} {
		high, err := readNumber(group+"/memory.high", true)
		if err != nil {
			return s, err
		}
		if high != math.MaxUint64 {
			s.MemoryHighUnlimited = false
		}
	}
	entries, err := os.ReadDir("/sys/kernel/mm/hugepages")
	if err != nil {
		return s, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "hugepages-") || !strings.HasSuffix(name, "kB") {
			return s, fmt.Errorf("unknown whole HugeTLB pool")
		}
		value := strings.TrimSuffix(strings.TrimPrefix(name, "hugepages-"), "kB")
		if _, err := strconv.ParseUint(value, 10, 64); err != nil {
			return s, err
		}
		for _, counter := range []string{"nr_hugepages", "surplus_hugepages"} {
			pages, err := readNumber(filepath.Join("/sys/kernel/mm/hugepages", name, counter), false)
			if err != nil {
				return s, err
			}
			if pages != 0 {
				return s, fmt.Errorf("normal-v1 pre-bootstrap requires zero whole HugeTLB pools")
			}
		}
	}
	return s, nil
}

// 已锁目录与内核backend解析路径必须是同一对象，封闭rename后的另一state盘。
func preSystemLockedDirectory(s *lockedState, current prebootstrapSystemSnapshot) error {
	var st unix.Stat_t
	if s == nil || s.dir == nil {
		return fmt.Errorf("original locked state directory unavailable")
	}
	if err := unix.Fstat(int(s.dir.Fd()), &st); err != nil {
		return err
	}
	if current.StateDevice != control.Uint64(st.Dev) || current.StateInode != control.Uint64(st.Ino) {
		return fmt.Errorf("pre-bootstrap state path differs from original locked directory")
	}
	return nil
}

func preSystemBootMarker(cmdline string, p prebootstrapSystemPolicy) error {
	if !preSystemPolicyVersionMatches(p.PhysicalProfileID, p.ProtocolVersion) {
		return fmt.Errorf("unsupported original System policy version")
	}
	want := PrebootstrapSystemKernelArg
	if p.PhysicalProfileID == NormalSplitV1PhysicalKernelProfileID {
		want = PrebootstrapSystemNormalSplitKernelArg
	}
	count := 0
	for _, token := range strings.Fields(cmdline) {
		key, _, _ := strings.Cut(token, "=")
		if key != "neonvm.dsga.prebootstrap_system" {
			continue
		}
		count++
		if token != want {
			return fmt.Errorf("original System boot marker differs from policy version")
		}
	}
	if count != 1 {
		return fmt.Errorf("original System boot marker missing or duplicated")
	}
	return nil
}

func preSystemTaskGroupAllowed(profile, group string) bool {
	if group == "/" || group == "/neon-system" {
		return true
	}
	switch profile {
	case NormalV1PhysicalKernelProfileID:
		return group == "/neon-postgres"
	case NormalSplitV1PhysicalKernelProfileID:
		return group == "/neon-postgres/private" || group == "/neon-postgres/arrays"
	default:
		return false
	}
}

// parent的procs/threads空不能代表split叶子空。先核真实递归populated，随后遍历
// 所有目录逐个核procs/threads；未知/超界/符号链接读错误拒绝，不采样RSS作资格。
func preSystemRecursivePGEmpty() (bool, error) {
	events, err := kernelText(pgCgroup + "/cgroup.events")
	if err != nil {
		return false, err
	}
	empty, err := preSystemRecursiveEmptyEvents(events)
	if err != nil {
		return false, err
	}
	pending := []string{pgCgroup}
	count := 0
	for len(pending) > 0 {
		path := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		count++
		if count > 4096 || strings.Count(strings.TrimPrefix(path, pgCgroup), "/") > 16 {
			return false, fmt.Errorf("pre-System PG subtree exceeds reviewed bound")
		}
		for _, name := range []string{"cgroup.procs", "cgroup.threads"} {
			v, e := kernelText(path + "/" + name)
			if e != nil {
				return false, e
			}
			if v != "" {
				empty = false
			}
		}
		entries, e := os.ReadDir(path)
		if e != nil {
			return false, e
		}
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 {
				return false, fmt.Errorf("pre-System PG subtree symlink")
			}
			if entry.IsDir() {
				pending = append(pending, path+"/"+entry.Name())
			}
		}
	}
	return empty, nil
}
func preSystemRecursiveEmptyEvents(text string) (bool, error) {
	found := false
	empty := false
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) > 0 && f[0] == "populated" {
			if found || len(f) != 2 || f[1] != "0" && f[1] != "1" {
				return false, fmt.Errorf("invalid recursive PG populated fact")
			}
			found = true
			empty = f[1] == "0"
		}
	}
	if !found {
		return false, fmt.Errorf("recursive PG populated fact missing")
	}
	return empty, nil
}
