package resourceguard

import (
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// AddressSpaceEvidence核验原PG计费域中所有已观察到的进程。MaxHardBytes和
// MaxVirtualBytes均为各进程最大值，不是累加RSS；不能由本事实扣除共享映射、
// PROT_NONE占位、page cache或内核开销，更不授权normal降限/VM收缩。
// 空组允许全部0，仅描述尚无进程；真正PG出生由InspectProcess另行确认。
type AddressSpaceEvidence struct {
	Processes       control.Uint64 `json:"processes"`
	MaxHardBytes    control.Uint64 `json:"max_hard_bytes"`
	MaxVirtualBytes control.Uint64 `json:"max_virtual_bytes"`
	Irreversible    bool           `json:"irreversible"`
}

func (e *AddressSpaceEvidence) UnmarshalJSON(raw []byte) error {
	type plain AddressSpaceEvidence
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*e = AddressSpaceEvidence(next)
	return nil
}

func validateAddressSpaceLimit(limit, tasks control.Uint64) error {
	if limit != 0 && (tasks == 0 || limit > math.MaxInt64 || uint64(limit)%(64<<10) != 0) {
		return fmt.Errorf("finite address space requires aligned original task and byte limits")
	}
	return nil
}

func (r *record) addressSpaceLimit() control.Uint64 {
	if r.Bootstrap == nil {
		return 0
	}
	return r.Bootstrap.Original.Limits.AddressSpaceMaxBytes
}

func (e Evidence) matchesAddressSpace(l Limits) error {
	a := e.AddressSpace
	if l.AddressSpaceMaxBytes == 0 {
		if a != nil {
			return fmt.Errorf("unexpected address space evidence for legacy source")
		}
		return nil
	}
	activity := processActivity{populated: e.CgroupPopulated, tasks: e.TaskLimit}
	if validateAddressSpaceLimit(l.AddressSpaceMaxBytes, l.PidsMaxTasks) != nil || a == nil || !a.Irreversible || a.Processes > l.PidsMaxTasks || a.MaxHardBytes > l.AddressSpaceMaxBytes || a.MaxVirtualBytes > a.MaxHardBytes || !activity.covers(a) {
		return fmt.Errorf("original process address space is missing, reversible or exceeded")
	}
	if a.Processes == 0 && (a.MaxHardBytes != 0 || a.MaxVirtualBytes != 0) || a.Processes != 0 && (a.MaxHardBytes == 0 || a.MaxVirtualBytes == 0) {
		return fmt.Errorf("inconsistent empty process address space facts")
	}
	return nil
}

// Linux /proc/<pid>/limits使用规范十进制字节；不接受unlimited、单位替换、
// 重复项或小于既有VmSize的事后降限。后者只能限制未来增长，不能回收已映射页。
func processAddressSpace(limits, status string, ceiling control.Uint64) (hard, virtual control.Uint64, err error) {
	if validateAddressSpaceLimit(ceiling, 1) != nil || ceiling == 0 {
		return 0, 0, fmt.Errorf("invalid original address space ceiling")
	}
	seen := false
	for _, line := range strings.Split(limits, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "Max" || fields[1] != "address" || fields[2] != "space" {
			continue
		}
		if seen || len(fields) != 6 || fields[5] != "bytes" {
			return 0, 0, fmt.Errorf("invalid process address space limits")
		}
		seen = true
		soft, parseErr := canonicalUint(fields[3])
		n, hardErr := canonicalUint(fields[4])
		if parseErr != nil || hardErr != nil || soft > n || n == 0 || n > uint64(ceiling) {
			return 0, 0, fmt.Errorf("process address space ceiling changed or is unlimited")
		}
		hard = control.Uint64(n)
	}
	if !seen {
		return 0, 0, fmt.Errorf("process address space limits missing")
	}
	// NNP会跨fork/exec继承且不能撤回，原init-userns的全部可用能力为0。
	// bounding非零本身不授予能力；NNP阻止SUID/file capability重新获权。
	want := map[string]bool{"NoNewPrivs:": false, "CapInh:": false, "CapPrm:": false, "CapEff:": false, "CapAmb:": false, "VmSize:": false}
	for _, line := range strings.Split(status, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		// 进程可在两次/proc读取间退出为僵尸；它已不保有VA，不把缺VmSize
		// 伪装成0成功，也不把正常退出误诊为可逆硬限，整轮有界重读。
		if fields[0] == "State:" && len(fields) >= 2 && (fields[1] == "Z" || fields[1] == "X") {
			return 0, 0, errAddressSpaceChurn
		}
		found, relevant := want[fields[0]]
		if !relevant {
			continue
		}
		if found {
			return 0, 0, fmt.Errorf("duplicate process privilege or address space fact")
		}
		want[fields[0]] = true
		switch fields[0] {
		case "NoNewPrivs:":
			if len(fields) != 2 || fields[1] != "1" {
				return 0, 0, fmt.Errorf("process can gain new privileges")
			}
		case "VmSize:":
			if len(fields) != 3 || fields[2] != "kB" {
				return 0, 0, fmt.Errorf("invalid process virtual size unit")
			}
			n, parseErr := canonicalUint(fields[1])
			if parseErr != nil || n > uint64(hard)/1024 {
				return 0, 0, fmt.Errorf("existing process virtual size exceeds hard ceiling")
			}
			virtual = control.Uint64(n * 1024)
		default:
			if len(fields) != 2 || len(fields[1]) != 16 || fields[1] != "0000000000000000" {
				return 0, 0, fmt.Errorf("process retained privilege to bypass original ceiling")
			}
		}
	}
	for field, found := range want {
		if field != "VmSize:" && !found {
			return 0, 0, fmt.Errorf("incomplete process privilege or address space facts")
		}
	}
	if virtual == 0 {
		return 0, 0, errAddressSpaceMappingUnavailable
	}
	return hard, virtual, nil
}

var errAddressSpaceMappingUnavailable = errors.New("process address space was not observed")

func canonicalUint(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s {
		return 0, fmt.Errorf("noncanonical process counter")
	}
	return n, nil
}

// cgroup.procs列出线程组ID；有限pids.current另计线程总数。只有原root可信迁入
// 入口可加入该组，枚举期间的新fork只能继承既有不可逆硬限；消失/PID复用保守
// 拒绝本次观察。重复/空行内空白不投影去重，避免掩盖迁入/枚举未知。
func addressSpacePIDs(raw string, tasks control.Uint64) ([]int, error) {
	if tasks == 0 || tasks > MaxPidsTasks || len(raw) > int(tasks)*12 {
		return nil, fmt.Errorf("invalid bounded process list")
	}
	if raw == "" {
		return nil, nil
	}
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if uint64(len(lines)) > uint64(tasks) {
		return nil, fmt.Errorf("process list exceeds original task limit")
	}
	seen := make(map[int]bool, len(lines))
	pids := make([]int, 0, len(lines))
	for _, line := range lines {
		n, err := canonicalUint(line)
		if err != nil || n <= 1 || n > 1<<22 {
			return nil, fmt.Errorf("invalid, duplicate or reused process list identity")
		}
		if seen[int(n)] {
			return nil, errAddressSpaceChurn
		}
		seen[int(n)] = true
		pids = append(pids, int(n))
	}
	slices.Sort(pids)
	return pids, nil
}

// 原生PG只把该启动值当作待落实约束，实际执行凭据仍来自root Birth与
// /proc核验。无新来源时也删除调用者遗留的同名值，不从环境升级旧0模式。
const addressSpaceEnvironmentKey = "NEON_DSGA_ADDRESS_SPACE_MAX_BYTES"

func addressSpaceEnvironment(original []string, limit control.Uint64) []string {
	result := make([]string, 0, len(original)+1)
	for _, entry := range original {
		key, _, _ := strings.Cut(entry, "=")
		if key != addressSpaceEnvironmentKey {
			result = append(result, entry)
		}
	}
	if limit != 0 {
		result = append(result, addressSpaceEnvironmentKey+"="+strconv.FormatUint(uint64(limit), 10))
	}
	return result
}

// processAddressSample仅在同一轮读取中使用，不持久化PID采样来授权未来写。
// starttime封闭PID复用；硬限/VA均逐进程核验，不把多个采样max相加成RSS。
type processAddressSample struct {
	start   control.Uint64
	hard    control.Uint64
	virtual control.Uint64
}

var errAddressSpaceChurn = errors.New("process membership changed during address space inspection")

type processActivity struct {
	populated bool
	tasks     *TaskLimitEvidence
}

func (p processActivity) covers(a *AddressSpaceEvidence) bool {
	return p.tasks != nil && p.populated == (a.Processes != 0) && p.tasks.CurrentTasks >= a.Processes
}

const processBoundsAttempts = 8

// 进程地址空间与前后两份 task 活动读回属于同一轮。失败轮的证据全部丢弃，
// 避免用一轮的 pids.current 给另一轮的 PID 集合签字。
func readProcessBounds(readActivity func() (processActivity, error), readSpaces func() (*AddressSpaceEvidence, error)) (processActivity, *AddressSpaceEvidence, error) {
	var lastChurn error
	for attempt := 0; attempt < processBoundsAttempts; attempt++ {
		if attempt != 0 {
			// 同一 Guard 锁内错开短时 fork/exit；七次计划退避合计 39 ms。
			// 持续变动仍返回未知，由原 Owner 的安全关闭路径处理。
			time.Sleep(time.Millisecond << min(attempt-1, 3))
		}
		before, err := readActivity()
		if err != nil {
			return processActivity{}, nil, err
		}
		if before.tasks == nil || before.tasks.CurrentTasks > before.tasks.MaxTasks {
			return processActivity{}, nil, unconfirmedProcessTaskBound()
		}
		spaces, err := readSpaces()
		if err != nil {
			if errors.Is(err, errAddressSpaceChurn) {
				lastChurn = err
				continue
			}
			return processActivity{}, nil, err
		}
		after, err := readActivity()
		if err != nil {
			return processActivity{}, nil, err
		}
		if after.tasks == nil || before.tasks.MaxTasks != after.tasks.MaxTasks || after.tasks.CurrentTasks > after.tasks.MaxTasks {
			return processActivity{}, nil, unconfirmedProcessTaskBound()
		}
		if before.covers(spaces) && after.covers(spaces) {
			return after, spaces, nil
		}
		lastChurn = &kernelBoundsSourceError{step: "address_task_membership", err: errAddressSpaceChurn}
	}
	return processActivity{}, nil, errors.Join(ErrUnknown, lastChurn)
}

func unconfirmedProcessTaskBound() error {
	return &kernelBoundsSourceError{step: "cgroup_task_activity", err: fmt.Errorf("%w: task bound missing, changed or exceeded during address space inspection", ErrUnknown)}
}

// 原资源锁不阻止 PG fork/exit。每轮仍要求两份成员集合相同，逐 PID 两次
// starttime 一致。新进程继承硬限和 NNP；特权迁入和子组创建由平台隔离。
func sampleAddressSpaces(l Limits, readPIDs func() (string, error), inspect func(int) (processAddressSample, error)) (_ *AddressSpaceEvidence, err error) {
	step := "address_initial_membership"
	defer func() {
		if err != nil && kernelBoundsSource(err) == "" {
			err = &kernelBoundsSourceError{step: step, err: err}
		}
	}()
	raw, err := readPIDs()
	if err != nil {
		return nil, err
	}
	pids, err := addressSpacePIDs(raw, l.PidsMaxTasks)
	if err != nil {
		return nil, err
	}
	step = "address_initial_process_sample"
	samples := make(map[int]processAddressSample, len(pids))
	for _, pid := range pids {
		sample, err := inspect(pid)
		if err != nil {
			return nil, addressSpaceSampleError(err)
		}
		samples[pid] = sample
	}
	step = "address_final_membership"
	raw, err = readPIDs()
	if err != nil {
		return nil, err
	}
	after, err := addressSpacePIDs(raw, l.PidsMaxTasks)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(pids, after) {
		return nil, errAddressSpaceChurn
	}
	step = "address_final_process_sample"
	result := &AddressSpaceEvidence{Processes: 0, MaxHardBytes: 0, MaxVirtualBytes: 0, Irreversible: true}
	for _, pid := range after {
		sample, err := inspect(pid)
		if err != nil {
			return nil, addressSpaceSampleError(err)
		}
		if sample.start != samples[pid].start {
			return nil, errAddressSpaceChurn
		}
		if sample.start == 0 || sample.hard == 0 || sample.hard > l.AddressSpaceMaxBytes || sample.virtual == 0 || sample.virtual > sample.hard {
			return nil, fmt.Errorf("invalid process address space sample")
		}
		result.Processes++
		result.MaxHardBytes = max(result.MaxHardBytes, sample.hard)
		result.MaxVirtualBytes = max(result.MaxVirtualBytes, sample.virtual)
	}
	return result, nil
}

func addressSpaceSampleError(err error) error {
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return errors.Join(errAddressSpaceChurn, err)
	}
	return err
}
