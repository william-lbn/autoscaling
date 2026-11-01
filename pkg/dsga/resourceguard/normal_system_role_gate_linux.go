//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalSystemRoleGateAction = "role-exec-gate"
const normalSystemRoleGateWireBytes = 4096
const normalSystemRoleCreatorThreadFDFlag = unix.O_EXCL // 精确6.12.26 PIDFD_THREAD。
var normalSystemRoleGateMainPinned bool

// Go init固定在初始线程。只对这个固定私有action将main G持续钉到leader，
// 确保第二次exec仍由原TRACEME线程执行；其它Root/PG入口完全不锁线程。
// 此处不读取配置/执行操作；真实UID门禁仍先于Gate的任何源/FD处理。
func init() {
	if len(os.Args) == 2 && os.Args[1] == normalSystemRoleGateAction {
		runtime.LockOSThread()
		normalSystemRoleGateMainPinned = unix.Gettid() == os.Getpid()
	}
}

type normalSystemRoleGateReference struct{ Nonce, PendingSHA256 string }
type normalSystemRoleGateReferenceProvider interface {
	originalSystemRoleGateReference() (normalSystemRoleGateReference, error)
}
type normalSystemRoleGateCommand struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Role            string `json:"role"`
	GateNonce       string `json:"gate_nonce"`
	OriginalNonce   string `json:"original_nonce"`
	PendingSHA256   string `json:"pending_sha256"`
}

func normalSystemRoleGateReferenceOf(writer normalSystemRoleStoppedWriter) (normalSystemRoleGateReference, error) {
	if j, ok := writer.(*normalSystemRoleJournal); ok && j != nil && !j.stopped && j.pending.Stage == "pending" && normalSystemRoleGateDigest(j.pending.Nonce) && normalSystemRoleGateDigest(j.previous) {
		return normalSystemRoleGateReference{j.pending.Nonce, j.previous}, nil
	}
	if p, ok := writer.(normalSystemRoleGateReferenceProvider); ok {
		return p.originalSystemRoleGateReference()
	}
	return normalSystemRoleGateReference{}, errors.Join(ErrUnknown, fmt.Errorf("original role Gate requires durable pending reference"))
}
func normalSystemRoleGateDigest(s string) bool { _, e := nativeBirthDigest(s); return e == nil }
func normalSystemRoleGateArgv() []string {
	return []string{"/neonvm/bin/dsga-resource-guard", normalSystemRoleGateAction}
}
func normalSystemRoleGateEnvironment() []string { return []string{"PATH=/neonvm/bin"} }
func normalSystemRoleGateEncode(c normalSystemRoleGateCommand) ([]byte, error) {
	if c.ProtocolVersion != 1 || !normalSystemRoleGateDigest(c.GateNonce) || !normalSystemRoleGateDigest(c.OriginalNonce) || !normalSystemRoleGateDigest(c.PendingSHA256) {
		return nil, fmt.Errorf("original Gate command differs")
	}
	if c.Role != "compute" && c.Role != normalSystemRoleBrokerKind {
		if _, e := normalSystemFixedRoleArgv(c.Role); e != nil {
			return nil, e
		}
	}
	b, e := json.Marshal(c)
	if e != nil {
		return nil, e
	}
	if len(b) > normalSystemRoleGateWireBytes-12 {
		return nil, fmt.Errorf("original Gate command exceeds bound")
	}
	raw := make([]byte, normalSystemRoleGateWireBytes)
	copy(raw, "NSYSGO01")
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(b)))
	copy(raw[12:], b)
	return raw, nil
}
func normalSystemRoleGateDecode(raw []byte) (normalSystemRoleGateCommand, error) {
	var c normalSystemRoleGateCommand
	if len(raw) != normalSystemRoleGateWireBytes || string(raw[:8]) != "NSYSGO01" {
		return c, fmt.Errorf("fixed Gate frame differs")
	}
	n := int(binary.BigEndian.Uint32(raw[8:12]))
	if n <= 0 || n > len(raw)-12 {
		return c, fmt.Errorf("fixed Gate frame length differs")
	}
	for _, b := range raw[12+n:] {
		if b != 0 {
			return c, fmt.Errorf("fixed Gate frame padding differs")
		}
	}
	if e := control.UnmarshalObject(raw[12:12+n], &c); e != nil {
		return c, fmt.Errorf("fixed Gate frame malformed")
	}
	_, e := normalSystemRoleGateEncode(c)
	return c, e
}

func normalSystemRoleGateThreadAlive(fd int) error {
	flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if e != nil || flags&normalSystemRoleCreatorThreadFDFlag == 0 {
		return fmt.Errorf("original creator descriptor is not PIDFD_THREAD")
	}
	return arrayRegistryPIDFDAlive(fd)
}
func normalSystemRoleGateConnection(fd int) (*net.UnixConn, error) {
	typ, e := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if e != nil || typ != unix.SOCK_SEQPACKET {
		return nil, fmt.Errorf("fixed Gate inherited transport differs")
	}
	f := os.NewFile(uintptr(fd), "original-role-Gate-channel")
	if f == nil {
		return nil, fmt.Errorf("fixed Gate channel missing")
	}
	conn, e := net.FileConn(f)
	f.Close()
	if e != nil {
		return nil, e
	}
	u, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("fixed Gate inherited socket differs")
	}
	return u, nil
}

// 仅由Root主程序的固定root-only分支调用；没有path/env/argv参数。该可信
// prelude已在pending预算内，creator死亡或无原GO都不会exec任何目标/loader。
// Pdeathsig只是辅助：原线程可能在child设置prctl前已死，所以必须核线程pidfd。
func RunNormalSystemRoleExecGate(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("fixed role Gate context missing")
	}
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 || !normalSystemRoleGateMainPinned || unix.Gettid() != os.Getpid() {
		return fmt.Errorf("fixed role Gate requires original real root leader")
	}
	if e := normalSystemRoleGateThreadAlive(4); e != nil {
		return e
	}
	conn, e := normalSystemRoleGateConnection(3)
	if e != nil {
		return e
	}
	defer conn.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	raw, e := arrayServicePacket(conn, deadline, normalSystemRoleGateWireBytes, false)
	if e != nil {
		return e
	}
	if e = normalSystemRoleGateThreadAlive(4); e != nil {
		return e
	}
	command, e := normalSystemRoleGateDecode(raw)
	if e != nil {
		return e
	}
	s, e := inspectNormalSystemLifecycleOriginal(call)
	if e != nil {
		return e
	}
	defer s.close()
	source, e := sealedRuntimeSource()
	if e != nil {
		return e
	}
	f, e := rootFile(filepath.Join(source.Config.StateDir, normalSystemRoleStageName(command.OriginalNonce, "pending")), false)
	if e != nil {
		return e
	}
	pendingRaw, e := nativeBirthReadBounded(f, 64<<10)
	f.Close()
	if e != nil || physicalDigest(pendingRaw) != command.PendingSHA256 {
		return fmt.Errorf("original Gate pending bytes differ")
	}
	var pending normalSystemRoleJournalEntry
	if e = control.UnmarshalObject(pendingRaw, &pending); e != nil {
		return e
	}
	binding := s.binding
	binding.RootConsumer = pending.Binding.RootConsumer
	if pending.ProtocolVersion != 1 || pending.Scope != normalSystemRoleJournalScope || pending.Stage != "pending" || pending.Nonce != command.OriginalNonce || pending.Role != command.Role || pending.PreviousSHA256 != "" || pending.Task != nil || pending.OriginalTaskExitObserved || binding != pending.Binding {
		return fmt.Errorf("original Gate pending source differs")
	}
	peer, parentFD, e := arraySocketPeer(conn)
	if e != nil {
		return e
	}
	defer unix.Close(parentFD)
	if peer == nil || peer.Uid != 0 || peer.Gid != 0 || int(peer.Pid) != pending.Binding.RootConsumer.PID {
		return fmt.Errorf("original Gate parent differs")
	}
	if e = arrayRegistryPIDFDAlive(parentFD); e != nil {
		return e
	}
	// FD4对应原creator线程，TGID必须是同一原parent；只读钉住，不按请求PID重开。
	info, e := os.ReadFile("/proc/self/fdinfo/4")
	if e != nil || len(info) > 4096 {
		return fmt.Errorf("original creator identity unavailable")
	}
	tid, e := nativeTraceStatusValues(string(info), "Pid", 1)
	if e != nil || tid[0] <= 1 {
		return fmt.Errorf("original creator identity differs")
	}
	threadFD, e := unix.Open("/proc/"+strconv.Itoa(int(tid[0])), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	thread := os.NewFile(uintptr(threadFD), "original-creator-thread")
	defer thread.Close()
	status, e := nativeTraceProcText(threadFD, "status")
	if e != nil {
		return e
	}
	tgid, e := nativeTraceStatusValues(status, "Tgid", 1)
	if e != nil || tgid[0] != uint64(peer.Pid) {
		return fmt.Errorf("original creator TGID differs")
	}
	parentProc, e := unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	parent := os.NewFile(uintptr(parentProc), "original-Gate-parent")
	defer parent.Close()
	text, e := nativeTraceProcText(parentProc, "stat")
	if e != nil {
		return e
	}
	ticks, e := postmasterTicks(text, int(peer.Pid))
	if e != nil || ticks != uint64(pending.Binding.RootConsumer.StartTicks) {
		return fmt.Errorf("original Gate parent start differs")
	}
	for _, proc := range []int{threadFD, parentProc} {
		text, e := nativeTraceProcText(proc, "status")
		if e != nil {
			return e
		}
		for _, name := range []string{"Uid", "Gid"} {
			v, e := nativeTraceStatusValues(text, name, 4)
			if e != nil {
				return e
			}
			for _, value := range v {
				if value != 0 {
					return fmt.Errorf("original Gate parent is not real root")
				}
			}
		}
	}
	expected := s.files[s.logical["/neonvm/bin/dsga-resource-guard"]]
	if expected == nil {
		return fmt.Errorf("original Gate guard source absent")
	}
	exeFD, e := unix.Openat(parentProc, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	exe := os.NewFile(uintptr(exeFD), "original-Gate-parent-ELF")
	defer exe.Close()
	var got, want unix.Stat_t
	if unix.Fstat(exeFD, &got) != nil || unix.Fstat(int(expected.Fd()), &want) != nil || got.Dev != want.Dev || got.Ino != want.Ino {
		return fmt.Errorf("original Gate parent ELF identity differs")
	}
	gotSHA, e := nativeBirthFileHash(exe)
	if e != nil {
		return e
	}
	wantSHA, e := nativeBirthFileHash(expected)
	if e != nil || gotSHA != wantSHA {
		return fmt.Errorf("original Gate parent ELF bytes differ")
	}
	for _, proc := range []int{parentProc, threadFD} {
		group, e := nativeTraceProcText(proc, "cgroup")
		if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
			return fmt.Errorf("original Gate parent left System")
		}
	}
	parentArgv, e := normalSystemReadVector(parentProc, "cmdline")
	if e != nil {
		return e
	}
	if len(parentArgv) != 2 || parentArgv[0] != "/neonvm/bin/dsga-resource-guard" || (parentArgv[1] != "system-role-broker" && parentArgv[1] != "initial-boot" && parentArgv[1] != "recover-initial-boot") {
		return fmt.Errorf("original Gate parent action differs")
	}
	parentEnv, e := normalSystemReadVector(parentProc, "environ")
	if e != nil {
		return e
	}
	eh, e := normalSystemVectorSHA(parentEnv)
	if e != nil {
		return e
	}
	expectedEH, _ := normalSystemVectorSHA(normalSystemRoleGateEnvironment())
	if eh != expectedEH {
		return fmt.Errorf("original Gate parent environment differs")
	}
	plan, e := normalSystemRoleGateLaunchPlan(call, s, command.Role)
	if e != nil {
		return e
	}
	// 子进程的consumer nonce与原Root不同；仅在核完整不可变来源一致后，
	// 重建原Root那一份计划摘要，不能靠同role/同nonce忽略原pending向量绑定。
	originalPlan := plan
	originalPlan.binding = pending.Binding
	originalObservation, e := s.snapshot()
	if e != nil {
		return e
	}
	originalObservation.Binding = pending.Binding
	originalObservationRaw, e := json.Marshal(originalObservation)
	if e != nil {
		return e
	}
	originalPlan.sourceSHA = physicalDigest(originalObservationRaw)
	originalPlanSHA, e := originalPlan.referenceSHA256()
	if e != nil || originalPlanSHA != pending.PlanSHA256 {
		return fmt.Errorf("original Gate pending plan digest differs")
	}

	// Namespace必须仍是原pending中同一组真实对象；声明不代替实际FD读回。
	ns, e := captureNormalSystemRoleNamespaces()
	if e != nil {
		return e
	}
	defer ns.close()
	actual, e := ns.snapshot()
	if e != nil {
		return e
	}
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(pending.Namespace)
	if string(a) != string(b) {
		return fmt.Errorf("original Gate namespace differs")
	}
	if e = normalSystemRoleGateThreadAlive(4); e != nil {
		return e
	}
	if e = arrayRegistryPIDFDAlive(parentFD); e != nil {
		return e
	}
	if e = call.Err(); e != nil {
		return e
	}
	if plan.role == normalSystemRoleBrokerKind {
		// 声明已固定原initialRoot→PID1生命周期。清除发生在可信prelude且
		// TRACEEXEC|EXITKILL仍持有；第二stop持久之前父死仍必杀，不注入目标syscall。
		typ, e := unix.GetsockoptInt(5, unix.SOL_SOCKET, unix.SO_TYPE)
		if e != nil || typ != unix.SOCK_SEQPACKET {
			return fmt.Errorf("original broker inherited startup channel missing")
		}
		if e = unix.Prctl(unix.PR_SET_PDEATHSIG, 0, 0, 0, 0); e != nil {
			return e
		}
		if e = normalSystemRoleGateThreadAlive(4); e != nil {
			return e
		}
	}
	// 此后Parent已装TRACEEXEC|EXITKILL，credential变化即使清Pdeathsig也由原
	// tracer覆盖直到第二exec-stop/持久放行。只降非root；Compute保NNP0供既有SUID。
	_ = unix.Close(4)
	_ = conn.Close()
	if plan.uid != 0 {
		if e = unix.Prctl(unix.PR_SET_KEEPCAPS, 0, 0, 0, 0); e != nil {
			return e
		}
		if e = unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); e != nil {
			return e
		}
		if e = syscall.Setgroups([]int{int(plan.gid)}); e != nil {
			return e
		}
		if e = syscall.Setresgid(int(plan.gid), int(plan.gid), int(plan.gid)); e != nil {
			return e
		}
		if e = syscall.Setresuid(int(plan.uid), int(plan.uid), int(plan.uid)); e != nil {
			return e
		}
		if os.Getuid() != int(plan.uid) || os.Geteuid() != int(plan.uid) || os.Getgid() != int(plan.gid) || os.Getegid() != int(plan.gid) {
			return fmt.Errorf("fixed Compute role drop incomplete")
		}
	}
	if len(plan.argv) == 0 || strings.ContainsAny(plan.argv[0], "\x00\r\n") {
		return fmt.Errorf("fixed role target differs")
	}
	return syscall.Exec(plan.argv[0], plan.argv, plan.env)
}
