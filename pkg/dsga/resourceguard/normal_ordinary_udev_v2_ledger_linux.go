//go:build linux

package resourceguard

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"
)

const normalOrdinaryUdevV2LedgerSlots = 256
const normalOrdinaryUdevV2ColdBatch = 8
const normalOrdinaryUdevV2TicketLifetime = 60 * time.Second

type normalOrdinaryUdevV2Ticket struct {
	sequence   uint64
	generation uint64
	received   time.Time
	raw        []byte
	properties map[string]string
	inFlight   bool
	uuid       string
}
type normalOrdinaryUdevV2Ledger struct {
	generation uint64
	entries    map[uint64]*normalOrdinaryUdevV2Ticket
	failed     error
}

// 所有方法由producerMu串行；无runtime census或seq采样补票路径。
func (l *normalOrdinaryUdevV2Ledger) fail(e error) error {
	if l.failed == nil {
		l.failed = errors.Join(ErrUnknown, e)
	}
	return l.failed
}
func (l *normalOrdinaryUdevV2Ledger) add(raw []byte, properties map[string]string, seq uint64, now time.Time) error {
	if l.failed != nil {
		return l.failed
	}
	if seq == 0 || len(raw) == 0 || len(raw) > 8192 {
		return l.fail(fmt.Errorf("udev kernel ticket framing differs"))
	}
	if l.entries == nil {
		l.entries = map[uint64]*normalOrdinaryUdevV2Ticket{}
	}
	if len(l.entries) >= normalOrdinaryUdevV2LedgerSlots || l.generation == math.MaxUint64 {
		return l.fail(fmt.Errorf("udev bounded ledger exhausted"))
	}
	if _, dup := l.entries[seq]; dup {
		return l.fail(fmt.Errorf("udev original ticket sequence duplicated"))
	}
	l.generation++
	p := make(map[string]string, len(properties))
	for k, v := range properties {
		p[k] = v
	}
	l.entries[seq] = &normalOrdinaryUdevV2Ticket{sequence: seq, generation: l.generation, received: now, raw: bytes.Clone(raw), properties: p, uuid: p["SYNTH_UUID"]}
	return nil
}
func (l *normalOrdinaryUdevV2Ledger) take(seq uint64, now time.Time) (*normalOrdinaryUdevV2Ticket, error) {
	if l.failed != nil {
		return nil, l.failed
	}
	t := l.entries[seq]
	if t == nil || t.inFlight {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("udev original pending ticket absent or already used"))
	}
	if now.Before(t.received) || now.Sub(t.received) > normalOrdinaryUdevV2TicketLifetime {
		return nil, l.fail(fmt.Errorf("udev pending original ticket expired"))
	}
	t.inFlight = true
	return t, nil
}
func (l *normalOrdinaryUdevV2Ledger) commit(t *normalOrdinaryUdevV2Ticket) error {
	if l.failed != nil {
		return l.failed
	}
	if t == nil || !t.inFlight || l.entries[t.sequence] != t {
		return l.fail(fmt.Errorf("udev original ticket state differs"))
	}
	delete(l.entries, t.sequence)
	return nil
}
func (l *normalOrdinaryUdevV2Ledger) expire(now time.Time) error {
	if l.failed != nil {
		return l.failed
	}
	for _, t := range l.entries {
		if now.Before(t.received) || now.Sub(t.received) > normalOrdinaryUdevV2TicketLifetime {
			return l.fail(fmt.Errorf("udev pending event exceeded lifetime"))
		}
	}
	return nil
}
