//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"sync"
	"time"
)

// 固定容量私有诊断，不携callback/interface/授权位；无ctx数据时纯no-op。
type normalSystemRoleDiagnosticKey struct{}
type normalSystemRoleDiagnosticStage uint16

const (
	normalRoleDiagFrameDecoded normalSystemRoleDiagnosticStage = iota + 1
	normalRoleDiagBeforeRecord
	normalRoleDiagRecordEntered
	normalRoleDiagFSComplete
	normalRoleDiagProofComplete
	normalRoleDiagLeaseComplete
	normalRoleDiagBeforeColdBegin
	normalRoleDiagAfterColdBegin
	normalRoleDiagRecordReturned
	normalRoleDiagBeforeColdWait
	normalRoleDiagAfterColdWait
	normalRoleDiagSpawnReturned
	normalRoleDiagKillAcceptedWrite
	normalRoleDiagKillClearDeadline
	normalRoleDiagKillServiceContext
	normalRoleDiagKillClientPIDFD
	normalRoleDiagKillConnectionGone
	normalRoleDiagKillCreatorDefer
	normalRoleDiagKillCreatorCleanup
	normalRoleDiagCreatorUnexpected
)
const normalSystemRoleDiagnosticCapacity = 64

type normalSystemRoleDiagnosticEvent struct {
	Stage            normalSystemRoleDiagnosticStage      `json:"stage"`
	Role             uint32                               `json:"role"`
	DecisionUnixNano int64                                `json:"decision_unix_nano"`
	RecordedUnixNano int64                                `json:"recorded_unix_nano"`
	Value            uint64                               `json:"value"`
	PeerPIDFD        *normalSystemRoleDiagnosticPeerPIDFD `json:"peer_pidfd,omitempty"`
}
type normalSystemRoleDiagnostic struct {
	mu      sync.Mutex
	events  [normalSystemRoleDiagnosticCapacity]normalSystemRoleDiagnosticEvent
	count   uint32
	dropped uint64
	changed chan struct{}
}
type normalSystemRoleDiagnosticSnapshot struct {
	Count   uint32                            `json:"count"`
	Dropped uint64                            `json:"dropped"`
	Events  []normalSystemRoleDiagnosticEvent `json:"events"`
}

func normalSystemRoleDiagnosticRecord(ctx context.Context, stage normalSystemRoleDiagnosticStage, role string, decision int64, value uint64) {
	if ctx == nil {
		return
	}
	d, _ := ctx.Value(normalSystemRoleDiagnosticKey{}).(*normalSystemRoleDiagnostic)
	if d == nil {
		return
	}
	normalSystemRoleDiagnosticAppend(d, stage, role, decision, value, nil)
}
func normalSystemRoleDiagnosticAppend(d *normalSystemRoleDiagnostic, stage normalSystemRoleDiagnosticStage, role string, decision int64, value uint64, peer *normalSystemRoleDiagnosticPeerPIDFD) {
	code, _ := normalSystemRoleServiceRoleCode(role)
	now := time.Now().UnixNano()
	if decision == 0 {
		decision = now
	}
	event := normalSystemRoleDiagnosticEvent{Stage: stage, Role: code, DecisionUnixNano: decision, RecordedUnixNano: now, Value: value, PeerPIDFD: peer}
	d.mu.Lock()
	if d.count < normalSystemRoleDiagnosticCapacity {
		d.events[d.count] = event
		d.count++
	} else {
		d.dropped++
	}
	d.mu.Unlock()
	select {
	case d.changed <- struct{}{}:
	default:
	}
}
func (d *normalSystemRoleDiagnostic) snapshot() normalSystemRoleDiagnosticSnapshot {
	if d == nil {
		return normalSystemRoleDiagnosticSnapshot{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	events := make([]normalSystemRoleDiagnosticEvent, d.count)
	copy(events, d.events[:d.count])
	for i := range events {
		if events[i].PeerPIDFD != nil {
			copy := *events[i].PeerPIDFD
			events[i].PeerPIDFD = &copy
		}
	}

	return normalSystemRoleDiagnosticSnapshot{d.count, d.dropped, events}
}

// 原分支已决定后的纯local时间采样；无诊断时连clock读取也省略，不取aux锁。
func normalSystemRoleDiagnosticDecision(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	d, _ := ctx.Value(normalSystemRoleDiagnosticKey{}).(*normalSystemRoleDiagnostic)
	if d == nil {
		return 0
	}
	return time.Now().UnixNano()
}

// 所有对象读取只用于观测。baseline在原ServicePeer/Task/descriptor验证后；
// after在原kill+Wait后。同一raw fd两次采样不构成Poll瞬间原子身份或存活证明。
type normalSystemRoleDiagnosticFDObject struct {
	StatOK           bool   `json:"stat_ok"`
	StatErrno        uint64 `json:"stat_errno"`
	StatErrnoKnown   bool   `json:"stat_errno_known"`
	Device           uint64 `json:"device"`
	Inode            uint64 `json:"inode"`
	Mode             uint32 `json:"mode"`
	StatFSOK         bool   `json:"statfs_ok"`
	StatFSErrno      uint64 `json:"statfs_errno"`
	StatFSErrnoKnown bool   `json:"statfs_errno_known"`
	FSType           int64  `json:"fs_type"`
}
type normalSystemRoleDiagnosticPeerPIDFD struct {
	BaselineCaptured bool                               `json:"baseline_captured"`
	OriginalPeerPID  int                                `json:"original_peer_pid"`
	OriginalFD       int                                `json:"original_fd"`
	BeforeMonitor    normalSystemRoleDiagnosticFDObject `json:"before_monitor"`
	OriginalPoll     arrayRegistryPIDFDObservation      `json:"original_poll"`
	AfterKillWait    normalSystemRoleDiagnosticFDObject `json:"after_kill_wait"`
}

func normalSystemRoleDiagnosticFDObjectRead(fd int) normalSystemRoleDiagnosticFDObject {
	var out normalSystemRoleDiagnosticFDObject
	var st unix.Stat_t
	var fs unix.Statfs_t
	e := unix.Fstat(fd, &st)
	out.StatOK = e == nil
	if errno, ok := e.(unix.Errno); ok {
		out.StatErrno, out.StatErrnoKnown = uint64(errno), true
	}
	if e == nil {
		out.Device, out.Inode, out.Mode = st.Dev, st.Ino, st.Mode
	}
	e = unix.Fstatfs(fd, &fs)
	out.StatFSOK = e == nil
	if errno, ok := e.(unix.Errno); ok {
		out.StatFSErrno, out.StatFSErrnoKnown = uint64(errno), true
	}
	if e == nil {
		out.FSType = int64(fs.Type)
	}
	return out
}
func normalSystemRoleDiagnosticPeerBaseline(ctx context.Context, peer *normalSystemOriginalTask) normalSystemRoleDiagnosticPeerPIDFD {
	var out normalSystemRoleDiagnosticPeerPIDFD
	if ctx == nil {
		return out
	}
	d, _ := ctx.Value(normalSystemRoleDiagnosticKey{}).(*normalSystemRoleDiagnostic)
	if d == nil || peer == nil {
		return out
	}
	out.BaselineCaptured, out.OriginalPeerPID, out.OriginalFD = true, peer.source.PID, peer.pidfd
	out.BeforeMonitor = normalSystemRoleDiagnosticFDObjectRead(peer.pidfd)
	return out
}
func normalSystemRoleDiagnosticRecordPeerPoll(ctx context.Context, role string, decision int64, baseline normalSystemRoleDiagnosticPeerPIDFD, poll arrayRegistryPIDFDObservation) {
	if ctx == nil {
		return
	}
	d, _ := ctx.Value(normalSystemRoleDiagnosticKey{}).(*normalSystemRoleDiagnostic)
	if d == nil {
		return
	}
	baseline.OriginalPoll = poll
	// 调用点已执行原cancelAndWait；不重Poll、不dup、不按PID重开，不取任何业务锁。
	baseline.AfterKillWait = normalSystemRoleDiagnosticFDObjectRead(baseline.OriginalFD)
	normalSystemRoleDiagnosticAppend(d, normalRoleDiagKillClientPIDFD, role, decision, 0, &baseline)
}
