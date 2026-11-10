//go:build linux && amd64 && dsga_native_stock_real_roles && dsga_native_stock_preparation && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// 显式DEV机制探针：所有PID1客户端进入同生产API，既不吞角色也不造完成/租约。
// 初始stock组件仍保旧tag；此后继直到第一个真实拒点，有缺payload/旧Cproducer便FAIL。
func init() {
	prior := normalRoleServiceNativeBrokerContext
	normalRoleServiceNativeBrokerContext = func(ctx context.Context) context.Context {
		ctx = prior(ctx)
		startup := &stockRealRolesDEVStartup{changed: make(chan struct{}, 1), candidates: map[string]*normalSystemRoleLeaseCandidate{}}
		diagnostic := &normalSystemRoleDiagnostic{changed: make(chan struct{}, 1)}
		ctx = context.WithValue(ctx, normalSystemRoleDiagnosticKey{}, diagnostic)
		// changed永不close；生产仅nonblocking通知，不携callback或权限。
		go func() {
			restartPrinted := false
			for range diagnostic.changed {
				snapshot := diagnostic.snapshot()
				kill := false
				for _, event := range snapshot.Events {
					if event.Stage >= normalRoleDiagKillAcceptedWrite && event.Stage <= normalRoleDiagCreatorUnexpected {
						kill = true
					}
				}
				if !restartPrinted {
					for _, event := range snapshot.Events {
						if event.Stage == normalRoleDiagClientPollRestarted {
							raw, _ := json.Marshal(snapshot)
							fmt.Printf("REAL_ROLES_DEV_PIDFD_RESTART_DIAGNOSTIC_JSON %s\n", raw)
							restartPrinted = true
							break
						}
					}
				}
				if kill {
					raw, _ := json.Marshal(snapshot)
					fmt.Printf("REAL_ROLES_DEV_KILL_DIAGNOSTIC_JSON %s\n", raw)
					return
				}
			}
		}()
		ctx = context.WithValue(ctx, stockPreparationDEVAfterR1Key{}, func() {
			fmt.Println("REAL_ROLES_DEV_ORIGINAL_R1_EXIT_CONFIRMED_COMPONENT_ONLY")
			startup.once.Do(func() { go startup.observe(ctx) })
		})
		started := time.Now()
		return context.WithValue(ctx, normalSystemRoleV2CheckpointKey{}, func(stage string, p *normalSystemRoleV2Publication) error {
			role := "absent"
			pid := 0
			if p != nil && p.endpoints != nil {
				role = p.endpoints.role
			}
			if p != nil && p.installed != nil && p.installed.task != nil {
				pid = p.installed.task.source.PID
			}
			startup.publication(stage, p)
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CHECKPOINT elapsed_ms=%d stage=%s role=%s pid=%d normal=false\n", time.Since(started).Milliseconds(), stage, role, pid)
			if stage == "wait-confirmed" && p != nil && p.installed != nil {
				i := p.installed
				wait, e := i.child.originalRoleWaitConfirmed(i.creator)
				if e != nil {
					fmt.Printf("REAL_ROLES_DEV_ORIGINAL_WAIT_ERROR role=%s error=%v\n", role, e)
				} else {
					status := syscall.WaitStatus(wait.Status)
					fmt.Printf("REAL_ROLES_DEV_ORIGINAL_WAIT role=%s raw=%d exited=%t exit_status=%d signaled=%t signal=%d\n", role, uint32(status), status.Exited(), status.ExitStatus(), status.Signaled(), int(status.Signal()))
					if !status.Exited() || status.ExitStatus() != 0 {
						raw, _ := json.Marshal(diagnostic.snapshot())
						fmt.Printf("REAL_ROLES_DEV_WAIT_DIAGNOSTIC_JSON %s\n", raw)
						stockRealRolesDEVFailureCounters("original-nonzero-wait")
						if p.runtime != nil && p.runtime.stock != nil {
							q := p.runtime.stock
							q.mu.Lock()
							closed, failure := q.closed, q.ledger.failed
							q.mu.Unlock()
							fmt.Printf("REAL_ROLES_DEV_STOCK_FAILURE_AFTER_WAIT closed=%t error=%v\n", closed, failure)
						}
					}
				}
			}
			return nil // 只观察，不选来源/预算/路径，不构造成功。
		})
	}
	normalRoleServiceStockPreparationDEVDispatch = stockRealRolesDEVDispatch
}

// 仅原失败已决定后读取固定Kernel counters，不读env/argv/凭据，不影响任何判定。
func stockRealRolesDEVFailureCounters(stage string) {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return
	}
	for _, name := range []string{"memory.events", "memory.current", "memory.max", "memory.peak", "pids.events"} {
		raw, e := os.ReadFile(systemCgroup + "/" + name)
		if e == nil && len(raw) <= 1024 {
			fmt.Printf("REAL_ROLES_DEV_FAILURE_CGROUP stage=%s file=%s value=%q\n", stage, name, string(raw))
		} else {
			fmt.Printf("REAL_ROLES_DEV_FAILURE_CGROUP_READ stage=%s file=%s error=%v\n", stage, name, e)
		}
	}
}
func stockRealRolesDEVFail(stage string, e error) {
	fmt.Printf("REAL_ROLES_DEV_FIRST_FAILURE stage=%s error=%v\n", stage, e)
	fmt.Println("STOCK_PREPARATION_GUEST_EXIT=1")
	if os.Getuid() == 0 && os.Geteuid() == 0 {
		stockRealRolesDEVFailureCounters(stage)
		unix.Sync()
		_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	}
	os.Exit(47) // 普通目标不为DEV诊断尝试额外Root syscall，原creator仍真实Wait。
}
func stockRealRolesDEVDispatch(m *testing.M) bool {
	if len(os.Args) > 1 && os.Args[0] == normalSystemRoleBrokerExecutable {
		if os.Args[1] == "exec-system-role" {
			if len(os.Args) != 3 {
				stockRealRolesDEVFail("client-argument-shape", ErrUnknown)
			}
			role := os.Args[2]
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_CLIENT_ENTRY", role)
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CLIENT_PID phase=entry role=%s pid=%d\n", role, os.Getpid())
			code, e := RunNormalSystemRoleClient(context.Background(), role)
			if e != nil {
				stockRealRolesDEVFail("production-client-"+role, e)
			}
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CLIENT_RETURN role=%s code=%d\n", role, code)
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CLIENT_PID phase=return role=%s pid=%d\n", role, os.Getpid())
			if code != 0 {
				stockRealRolesDEVFail("production-client-nonzero-"+role, fmt.Errorf("actual fixed role exit code %d", code))
			}
			os.Exit(code)
		}
		if os.Args[1] == "ordinary-v2-device-check" {
			if len(os.Args) != 2 {
				stockRealRolesDEVFail("device-check-argument-shape", ErrUnknown)
			}
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_DEVICE_CHECK_ENTRY")
			if e := RunNormalSystemOrdinaryV2DeviceCheck(context.Background()); e != nil {
				stockRealRolesDEVFail("production-device-check", e)
			}
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_DEVICE_CHECK_RETURN")
			os.Exit(0)
		}
	}
	return stockPreparationDEVDispatch(m) // initial/broker/Gate保持原真实生产链。
}

// 仅原生产checkpoint唤醒DEV观察，不用阶段通知签来源。真正终态始终从同实例原对象fresh核。
var stockRealRolesDEVOnceRoles = []string{"allow-start", "kernel-version", "udev-init"}
var stockRealRolesDEVActiveRoles = []string{"network", "udev", "acpi", "logs", "clock", "daemon", "compute"}

type stockRealRolesDEVStartup struct {
	mu         sync.Mutex
	once       sync.Once
	changed    chan struct{}
	runtime    *normalSystemRoleV2Runtime
	candidates map[string]*normalSystemRoleLeaseCandidate
	failed     error
}

func stockRealRolesDEVContains(list []string, role string) bool {
	for _, r := range list {
		if r == role {
			return true
		}
	}
	return false
}
func (s *stockRealRolesDEVStartup) publication(stage string, p *normalSystemRoleV2Publication) {
	if p == nil || p.runtime == nil {
		return
	}
	s.mu.Lock()
	if s.runtime == nil {
		s.runtime = p.runtime
	} else if s.runtime != p.runtime {
		s.failed = ErrUnknown
	}
	role := ""
	if p.endpoints != nil {
		role = p.endpoints.role
	}
	expected := stage == "active" && stockRealRolesDEVContains(stockRealRolesDEVActiveRoles, role) || stage == "wait-confirmed" && stockRealRolesDEVContains(stockRealRolesDEVOnceRoles, role)
	if expected {
		if p.candidate == nil {
			s.failed = ErrUnknown
		} else if old := s.candidates[role]; old != nil && old != p.candidate {
			s.failed = ErrUnknown
		} else {
			s.candidates[role] = p.candidate
		}
	}
	// 持续服务退出不能用下一代的active事件洗掉；原client同时保原失败路径。
	if stage == "wait-confirmed" && stockRealRolesDEVContains(stockRealRolesDEVActiveRoles, role) {
		s.failed = ErrUnknown
	}
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
func (s *stockRealRolesDEVStartup) snapshot() (*normalSystemRoleV2Runtime, map[string]*normalSystemRoleLeaseCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := map[string]*normalSystemRoleLeaseCandidate{}
	for k, v := range s.candidates {
		m[k] = v
	}
	return s.runtime, m, s.failed
}

type stockRealRolesDEVRoleReceipt struct {
	Role                string `json:"role"`
	Nonce               string `json:"nonce"`
	PID                 int    `json:"pid"`
	WaitZero            bool   `json:"original_unique_Wait_zero"`
	ExitedWitnessSHA256 string `json:"exited_witness_sha256,omitempty"`
}
type stockRealRolesDEVTerminal struct {
	listener                        *stockRealRolesDEVComputeListener
	attachment                      *normalSystemResourceAttachment
	Roles                           []stockRealRolesDEVRoleReceipt `json:"original_roles"`
	ResourceAttachmentNonce         string                         `json:"original_resource_attachment_nonce"`
	ResourceAttachmentWitnessSHA256 string                         `json:"original_resource_attachment_witness_sha256"`
	MetricsSHA256                   string                         `json:"compute_empty_metrics_sha256"`
	ComputeEmpty                    bool                           `json:"actual_compute_empty"`
	PostgresServerStarted           bool                           `json:"postgres_server_started"`
	PGVersionProbeOnly              bool                           `json:"pg_version_probe_only"`
	SourceQualified                 bool                           `json:"full_Source_qualified"`
	Normal                          bool                           `json:"normal"`
	E                               int                            `json:"E"`
}

// caller持broker.mu→resource；只读原同candidate、原唯一Wait和持久见证，不新造lease/Wait。
func stockRealRolesDEVOnceLocked(ctx context.Context, v *normalSystemRoleV2Runtime, state *lockedState, r *record, role string, want *normalSystemRoleLeaseCandidate) (stockRealRolesDEVRoleReceipt, error) {
	out := stockRealRolesDEVRoleReceipt{Role: role}
	p, e := normalStartupPublisher(v.stock)
	if e != nil {
		return out, e
	}
	a := v.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	c := p.roles[role]
	if c == nil {
		return out, errNormalStartupUnpublished
	}
	run := v.broker.byRole[role]
	if c != want || c.publisher != p || run == nil || run.child != c.input.Child || run.pending.Nonce != c.input.Nonce || run.unknown {
		return out, ErrUnknown
	}
	if c.phase != normalSystemRoleLeaseExitedPhase || !run.terminal || c.waitResult == nil || !c.waitResult.Status.Exited() || c.waitResult.Status.ExitStatus() != 0 || c.waitResult.ExitCode != 0 {
		return out, ErrUnknown
	}
	if e = c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
		return out, e
	}
	name := normalSystemRoleStageName(run.pending.Nonce, "exited")
	expected, ok := run.seen[name]
	if !ok {
		return out, ErrUnknown
	}
	got, e := readNormalSystemRoleFileWitness(state, name)
	if e != nil || got != expected {
		return out, errors.Join(ErrUnknown, e)
	}
	out.Nonce = c.input.Nonce
	out.PID = c.input.Task.source.PID
	out.WaitZero = true
	out.ExitedWitnessSHA256 = got.SHA256
	return out, nil
}

// 当前配置来自原System清单的固定config声明。files只持ELF，不能假设其包含config。
// 此DEV谓词仍先由Fresh中的原R2检查限定；不注册Source或从路径存在签许可。
func stockRealRolesDEVEmptyConfig(v *normalSystemRoleV2Runtime) error {
	if v == nil || v.authority == nil || v.authority.source == nil {
		return ErrUnknown
	}
	const path = "/neonvm/config/compute-empty.json"
	source := v.authority.source
	source.mu.Lock()
	var entry NormalSystemSourceFile
	count := 0
	for _, item := range source.manifest.Files {
		if item.Path == path {
			entry = item
			count++
		}
	}
	valid := !source.closed && count == 1 && entry.Kind == "config" && entry.Bytes > 0 && entry.Bytes <= 8192 && entry.CanonicalPath != "" && source.logical[path] == entry.CanonicalPath
	source.mu.Unlock()
	if !valid {
		return ErrUnknown
	}
	canonical, e := nativeBirthCanonical(path)
	if e != nil || canonical != entry.CanonicalPath {
		return errors.Join(ErrUnknown, e)
	}
	f, e := nativeBirthOpenRO(canonical)
	if e != nil {
		return e
	}
	defer f.Close()
	raw, e := normalOrdinaryUdevV2ReadFD(f, 8192)
	if e != nil || uint64(len(raw)) != entry.Bytes || physicalDigest(raw) != entry.SHA256 {
		return errors.Join(ErrUnknown, e)
	}
	var d map[string]json.RawMessage
	if json.Unmarshal(raw, &d) != nil || !bytes.Equal(bytes.TrimSpace(d["spec"]), []byte("null")) {
		return ErrUnknown
	}
	source.mu.Lock()
	closed := source.closed
	source.mu.Unlock()
	if closed {
		return ErrUnknown
	}
	return nil
}
func stockRealRolesDEVFresh(ctx context.Context, v *normalSystemRoleV2Runtime, want map[string]*normalSystemRoleLeaseCandidate, previous *stockRealRolesDEVTerminal) (out *stockRealRolesDEVTerminal, err error) {
	if v == nil || v.broker == nil || v.stock == nil || v.authority == nil || v.broker.guard != v.stock.g {
		return nil, ErrUnknown
	}
	actual, e := originalNormalSystemRoleV2Runtime(v.broker)
	if e != nil || actual != v {
		return nil, errors.Join(ErrUnknown, e)
	}
	out = &stockRealRolesDEVTerminal{}
	defer func() {
		if err != nil && out != nil && previous == nil && out.listener != nil {
			out.listener.close()
		}
		if err != nil {
			out = nil
		}
	}()
	v.broker.mu.Lock()
	defer v.broker.mu.Unlock()
	err = v.broker.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		if v.broker.closed {
			return ErrUnknown
		}
		// 该原R2完整检查不等于Source/normal资格。init终态由门和下方samecandidate双重绑定。
		if e := v.stock.computeStartupLocked(ctx, state, r, v.broker); e != nil {
			return e
		}
		if e := stockRealRolesDEVEmptyConfig(v); e != nil {
			return e
		}
		a := v.authority
		a.producerMu.Lock()
		attach := a.attachment
		if attach == nil {
			a.producerMu.Unlock()
			return errNormalStartupUnpublished
		}
		if attach.ended || attach.peer == nil || previous != nil && attach != previous.attachment {
			a.producerMu.Unlock()
			return ErrUnknown
		}
		attach.peer.mu.Lock()
		e := attach.peer.verify()
		attach.peer.mu.Unlock()
		witness, readErr := readNormalSystemRoleFileWitness(state, "role-resource-attach-"+attach.nonce+"-attached.json")
		if e != nil || readErr != nil || witness != attach.attachedWitness {
			a.producerMu.Unlock()
			return errors.Join(ErrUnknown, e, readErr)
		}
		out.attachment = attach
		out.ResourceAttachmentNonce = attach.nonce
		out.ResourceAttachmentWitnessSHA256 = witness.SHA256
		a.producerMu.Unlock()
		for _, role := range stockRealRolesDEVOnceRoles {
			receipt, e := stockRealRolesDEVOnceLocked(ctx, v, state, r, role, want[role])
			if e != nil {
				return e
			}
			out.Roles = append(out.Roles, receipt)
		}
		for _, role := range stockRealRolesDEVActiveRoles {
			lease, e := normalStartupActiveLeaseLocked(ctx, v.stock, state, r, role)
			if e != nil {
				return e
			}
			e = func() error {
				defer lease.close()
				c := lease.candidate
				run := v.broker.byRole[role]
				if c != want[role] || run == nil || run.child != c.input.Child || run.pending.Nonce != c.input.Nonce || run.unknown || run.terminal {
					return ErrUnknown
				}
				lease.task.mu.Lock()
				e := lease.task.verify()
				lease.task.mu.Unlock()
				if e != nil {
					return e
				}
				out.Roles = append(out.Roles, stockRealRolesDEVRoleReceipt{Role: role, Nonce: c.input.Nonce, PID: lease.task.source.PID})
				if role == "compute" {
					if previous == nil {
						out.listener, e = stockRealRolesDEVFindComputeListener(lease.task)
					} else {
						out.listener = previous.listener
						e = out.listener.recheck(lease.task)
					}
					if e != nil {
						return e
					}
				}
				return nil
			}()
			if e != nil {
				return e
			}
		}
		if ctx.Err() != nil || !a.admission.businessAllowed() {
			return errors.Join(ErrUnknown, ctx.Err())
		}
		return nil
	})
	return out, err
}

type stockRealRolesDEVComputeListener struct {
	task             *normalSystemOriginalTask
	file             *os.File
	number           int
	network, address string
}

func (p *stockRealRolesDEVComputeListener) close() {
	if p == nil {
		return
	}
	if p.file != nil {
		p.file.Close()
	}
	if p.task != nil {
		p.task.close()
	}
}

// 只识别原Compute实际v4/v6监听族，拒SO_REUSEPORT，避免连接其它并列监听者。
func stockRealRolesDEVListenerAddress(fd int) (string, string, bool, error) {
	var st unix.Stat_t
	if e := unix.Fstat(fd, &st); e != nil {
		return "", "", false, e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return "", "", false, nil
	}
	domain, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
	if e != nil {
		return "", "", false, e
	}
	if domain != unix.AF_INET && domain != unix.AF_INET6 {
		return "", "", false, nil
	}
	typ, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if e != nil {
		return "", "", false, e
	}
	if typ != unix.SOCK_STREAM {
		return "", "", false, nil
	}
	address, e := unix.Getsockname(fd)
	if e != nil {
		return "", "", false, e
	}
	network, loop := "", ""
	port := 0
	allowed := false
	switch a := address.(type) {
	case *unix.SockaddrInet4:
		port = a.Port
		network = "tcp4"
		loop = "127.0.0.1:3080"
		allowed = a.Addr == [4]byte{} || a.Addr == [4]byte{127, 0, 0, 1}
	case *unix.SockaddrInet6:
		port = a.Port
		network = "tcp6"
		loop = "[::1]:3080"
		allowed = a.Addr == [16]byte{} || a.Addr == [16]byte{15: 1}
	default:
		return "", "", false, ErrUnknown
	}
	if port != 3080 {
		return "", "", false, nil
	}
	listening, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
	if e != nil {
		return "", "", false, e
	}
	if listening == 0 {
		return "", "", false, nil
	}
	if listening != 1 || !allowed {
		return "", "", false, ErrUnknown
	}
	reuse, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT)
	if e != nil || reuse != 0 {
		return "", "", false, errors.Join(ErrUnknown, e)
	}
	return network, loop, true, nil
}
func stockRealRolesDEVFindComputeListener(original *normalSystemOriginalTask) (out *stockRealRolesDEVComputeListener, err error) {
	task, e := duplicateNormalSystemOriginalTask(original)
	if e != nil {
		return nil, e
	}
	out = &stockRealRolesDEVComputeListener{task: task, number: -1}
	defer func() {
		if err != nil {
			out.close()
			out = nil
		}
	}()
	task.mu.Lock()
	defer task.mu.Unlock()
	if e = task.verify(); e != nil {
		return out, e
	}
	fd, e := unix.Openat(task.procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return out, e
	}
	directory := os.NewFile(uintptr(fd), "DEV original Compute fd directory")
	names, e := directory.Readdirnames(1025)
	directory.Close()
	if e != nil && !errors.Is(e, io.EOF) {
		return out, e
	}
	if len(names) > 1024 {
		return out, ErrUnknown
	}
	for _, name := range names {
		n, e := strconv.Atoi(name)
		if e != nil || n < 0 || strconv.Itoa(n) != name {
			return out, ErrUnknown
		}
		dup, e := unix.PidfdGetfd(task.pidfd, n, 0)
		if errors.Is(e, unix.EBADF) {
			if e = task.verify(); e != nil {
				return out, e
			}
			continue
		}
		if e != nil {
			return out, e
		}
		network, address, ready, e := stockRealRolesDEVListenerAddress(dup)
		if e != nil {
			unix.Close(dup)
			return out, e
		}
		if !ready {
			unix.Close(dup)
			continue
		}
		if out.file != nil {
			unix.Close(dup)
			return out, ErrUnknown
		}
		out.file = os.NewFile(uintptr(dup), "DEV original Compute listening OFD")
		out.number = n
		out.network = network
		out.address = address
	}
	if e = task.verify(); e != nil {
		return out, e
	}
	if out.file == nil {
		return out, errNormalStartupUnpublished
	}
	return out, nil
}
func (p *stockRealRolesDEVComputeListener) recheck(task *normalSystemOriginalTask) error {
	if p == nil || p.task == nil || p.file == nil || task == nil {
		return ErrUnknown
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if e := task.verify(); e != nil {
		return e
	}
	if e := arrayMatchPIDFD(task.pidfd, p.task.pidfd); e != nil {
		return e
	}
	fd, e := unix.PidfdGetfd(task.pidfd, p.number, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	if e = normalRoleStdioSameOFD(int(p.file.Fd()), fd); e != nil {
		return e
	}
	network, address, ready, e := stockRealRolesDEVListenerAddress(fd)
	if e != nil || !ready || network != p.network || address != p.address {
		return errors.Join(ErrUnknown, e)
	}
	return task.verify()
}
func stockRealRolesDEVMetricsEmpty(raw []byte) error {
	var parser expfmt.TextParser
	families, e := parser.TextToMetricFamilies(bytes.NewReader(raw))
	if e != nil {
		return e
	}
	family := families["compute_ctl_up"]
	if family == nil || family.GetType().String() != "GAUGE" {
		return ErrUnknown
	}
	empty := 0
	for _, metric := range family.GetMetric() {
		status := ""
		for _, label := range metric.GetLabel() {
			if label.GetName() == "status" {
				if status != "" {
					return ErrUnknown
				}
				status = label.GetValue()
			}
		}
		if status == "" || metric.Gauge == nil {
			return ErrUnknown
		}
		value := metric.GetGauge().GetValue()
		if status == "empty" {
			if value != 1 {
				return ErrUnknown
			}
			empty++
		} else if value != 0 {
			return ErrUnknown
		}
	}
	if empty != 1 {
		return ErrUnknown
	}
	return nil
}
func (p *stockRealRolesDEVComputeListener) emptyMetrics(ctx context.Context) (string, error) {
	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, e := (&net.Dialer{}).DialContext(call, p.network, p.address)
	if e != nil {
		return "", e
	}
	defer conn.Close()
	deadline, _ := call.Deadline()
	if e = conn.SetDeadline(deadline); e != nil {
		return "", e
	}
	request, e := http.NewRequestWithContext(call, http.MethodGet, "http://"+p.address+"/metrics", nil)
	if e != nil {
		return "", e
	}
	request.Close = true
	if e = request.Write(conn); e != nil {
		return "", e
	}
	const limit = 256 << 10
	wire := &io.LimitedReader{R: conn, N: limit + 8192}
	response, e := http.ReadResponse(bufio.NewReader(wire), request)
	if e != nil {
		return "", e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", ErrUnknown
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if e != nil || len(raw) > limit || wire.N <= 0 {
		return "", errors.Join(ErrUnknown, e)
	}
	if e = stockRealRolesDEVMetricsEmpty(raw); e != nil {
		return "", e
	}
	return physicalDigest(raw), nil
}
func (s *stockRealRolesDEVStartup) observe(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 100*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, candidates, e := s.snapshot()
		if e != nil {
			stockRealRolesDEVFail("startup-original-publication", e)
			return
		}
		// 阶段通知只是锁外调度；十个原候选均到齐后仍必须完整重验，绝不以map/计数通过。
		if v != nil && len(candidates) == len(stockRealRolesDEVOnceRoles)+len(stockRealRolesDEVActiveRoles) {
			first, e := stockRealRolesDEVFresh(ctx, v, candidates, nil)
			if e == nil {
				digest, e := first.listener.emptyMetrics(ctx)
				if e != nil {
					first.listener.close()
					stockRealRolesDEVFail("startup-original-Compute-Empty-metrics", e)
					return
				}
				final, e := stockRealRolesDEVFresh(ctx, v, candidates, first)
				first.listener.close()
				if e != nil {
					stockRealRolesDEVFail("startup-final-original-terminals", e)
					return
				}
				latestRuntime, latestCandidates, latestError := s.snapshot()
				if latestError != nil || latestRuntime != v || len(latestCandidates) != len(candidates) {
					stockRealRolesDEVFail("startup-final-observer-state", errors.Join(ErrUnknown, latestError))
					return
				}
				for role, candidate := range candidates {
					if latestCandidates[role] != candidate {
						stockRealRolesDEVFail("startup-final-original-generation", ErrUnknown)
						return
					}
				}
				if ctx.Err() != nil {
					stockRealRolesDEVFail("post-R1-role-observation-deadline", ctx.Err())
					return
				}
				final.MetricsSHA256 = digest
				final.ComputeEmpty = true
				final.PGVersionProbeOnly = true
				raw, e := json.Marshal(final)
				if e != nil {
					stockRealRolesDEVFail("startup-original-evidence-encoding", e)
					return
				}
				fmt.Printf("REAL_ROLES_DEV_STARTUP_EMPTY_TERMINAL_JSON %s\n", raw)
				fmt.Println("STOCK_PREPARATION_GUEST_EXIT=0")
				unix.Sync()
				if e = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF); e != nil {
					stockRealRolesDEVFail("startup-success-shutdown", e)
				}
				return
			}
			if e != errNormalStartupUnpublished {
				stockRealRolesDEVFail("startup-original-terminals", e)
				return
			}
		}
		select {
		case <-ctx.Done():
			stockRealRolesDEVFail("post-R1-role-observation-deadline", ctx.Err())
			return
		case <-s.changed:
		case <-ticker.C:
		}
	}
}

func TestStockRealRolesDEVActualEmptyGauge(t *testing.T) {
	for _, c := range []struct {
		name, body string
		ok         bool
	}{
		{"actual-empty", "# TYPE compute_ctl_up gauge\ncompute_ctl_up{build_tag=\"x\",status=\"empty\"} 1\n", true},
		{"running-not-empty", "# TYPE compute_ctl_up gauge\ncompute_ctl_up{status=\"running\"} 1\n", false},
		{"empty-and-running", "# TYPE compute_ctl_up gauge\ncompute_ctl_up{status=\"empty\"} 1\ncompute_ctl_up{status=\"running\"} 1\n", false},
		{"no-gauge", "# TYPE other gauge\nother 1\n", false},
		{"malformed", "compute_ctl_up{status=\"empty\" 1\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := stockRealRolesDEVMetricsEmpty([]byte(c.body))
			if (e == nil) != c.ok {
				t.Fatalf("actual Empty gauge ok=%t: %v", c.ok, e)
			}
		})
	}
}

// 由独立只读容器挂载固定真实Empty输入，实际到达完整config读取门。
// raw source对象仅测试DEV读谓词；不调用构造/发布Source，不等价真实R2正例。
func TestStockRealRolesDEVOriginalEmptyConfig(t *testing.T) {
	const path = "/neonvm/config/compute-empty.json"
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	real, e := nativeBirthCanonical(path)
	if e != nil {
		t.Fatal(e)
	}
	makeRuntime := func() *normalSystemRoleV2Runtime {
		source := &normalSystemLifecycleSource{files: map[string]*os.File{}, logical: map[string]string{path: real}, manifest: NormalSystemSourceManifest{Files: []NormalSystemSourceFile{{Path: path, CanonicalPath: real, Kind: "config", Bytes: uint64(len(raw)), SHA256: physicalDigest(raw)}}}}
		return &normalSystemRoleV2Runtime{authority: &normalSystemResourceAuthority{source: source}}
	}
	t.Run("realROConfigWithEmptyELFMap", func(t *testing.T) {
		if e := stockRealRolesDEVEmptyConfig(makeRuntime()); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("wrongDeclaredDigest", func(t *testing.T) {
		v := makeRuntime()
		v.authority.source.manifest.Files[0].SHA256 = physicalDigest(append(append([]byte(nil), raw...), 0))
		if e := stockRealRolesDEVEmptyConfig(v); e == nil {
			t.Fatal("different declared bytes accepted")
		}
	})
	t.Run("wrongOriginalCanonical", func(t *testing.T) {
		v := makeRuntime()
		v.authority.source.logical[path] = "/neonvm/config/foreign.json"
		if e := stockRealRolesDEVEmptyConfig(v); e == nil {
			t.Fatal("foreign logical binding accepted")
		}
	})
	t.Run("closedOriginal", func(t *testing.T) {
		v := makeRuntime()
		v.authority.source.closed = true
		if e := stockRealRolesDEVEmptyConfig(v); e == nil {
			t.Fatal("closed source accepted")
		}
	})
}
