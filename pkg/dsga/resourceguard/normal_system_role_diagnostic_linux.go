//go:build linux

package resourceguard

import (
	"context"
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
	Stage            normalSystemRoleDiagnosticStage `json:"stage"`
	Role             uint32                          `json:"role"`
	DecisionUnixNano int64                           `json:"decision_unix_nano"`
	RecordedUnixNano int64                           `json:"recorded_unix_nano"`
	Value            uint64                          `json:"value"`
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
	code, _ := normalSystemRoleServiceRoleCode(role)
	now := time.Now().UnixNano()
	if decision == 0 {
		decision = now
	}
	event := normalSystemRoleDiagnosticEvent{stage, code, decision, now, value}
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
