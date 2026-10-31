//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 本文件全为明确模型U。它只证明状态/故障顺序，绝不充当真实pidfd、内核
// ptrace-stop、不可变ELF、线程退役或Root WAL来源；真实S在单独native tag。
type nativeTraceModel struct {
	frame                                []byte
	cfg                                  nativeBirthTraceConfig
	calls                                []string
	fail                                 string
	verifyCount, filterBase, filterFinal int
	cleanupConfirmed                     bool
	retire                               bool
	cleanupWait                          <-chan struct{}
}

var errNativeTraceModel = errors.New("explicit tracer model fault")

func (m *nativeTraceModel) hit(name string) error {
	m.calls = append(m.calls, name)
	if m.fail == name {
		return errNativeTraceModel
	}
	return nil
}
func (m *nativeTraceModel) verify(r *nativeBirthRequest, tid int) error {
	if r == nil {
		return m.hit("verify-original-before")
	}
	m.verifyCount++
	return m.hit(fmt.Sprintf("verify-stopped-%d", m.verifyCount))
}
func (m *nativeTraceModel) receive(context.Context) ([]byte, error) { return m.frame, m.hit("receive") }
func (m *nativeTraceModel) compile(_ context.Context, r nativeBirthRequest) (nativeFilterAuditBundle, error) {
	b := nativeFilterAuditBundle{Request: r}
	for i := range b.Programs {
		b.Programs[i] = []nativeFilterInstruction{{Code: 6, K: 0x7fff0000}}
	}
	return b, m.hit("trusted-compile")
}
func (m *nativeTraceModel) seize() error     { return m.hit("tentative-seize-zero") }
func (m *nativeTraceModel) interrupt() error { return m.hit("interrupt") }
func (m *nativeTraceModel) waitStop(_ context.Context, final bool) error {
	if final {
		return m.hit("wait-final-self-stop")
	}
	return m.hit("wait-base-interrupt-stop")
}
func (m *nativeTraceModel) exitKill() error { return m.hit("set-exitkill-after-confirm") }
func (m *nativeTraceModel) filters(_ nativeFilterAuditBundle, p nativeFilterPhase) error {
	if p == nativeFilterPMBase {
		m.filterBase++
		return m.hit(fmt.Sprintf("base-kernel-filter-%d", m.filterBase))
	}
	m.filterFinal++
	return m.hit(fmt.Sprintf("final-kernel-filter-%d", m.filterFinal))
}
func (m *nativeTraceModel) cont() error                                    { return m.hit("cont-zero") }
func (m *nativeTraceModel) ack(context.Context, nativeBirthExpected) error { return m.hit("ack") }
func (m *nativeTraceModel) closed198() error                               { return m.hit("fd198-closed") }
func (m *nativeTraceModel) detach() error                                  { return m.hit("detach-zero") }
func (m *nativeTraceModel) cleanup(ctx context.Context, confirmed bool) (bool, string, error) {
	m.cleanupConfirmed = confirmed
	_ = m.hit(fmt.Sprintf("cleanup-confirmed-%t", confirmed))
	if m.cleanupWait != nil {
		select {
		case <-m.cleanupWait:
		case <-ctx.Done():
			return true, "model-cleanup-unknown", ctx.Err()
		}
	}
	return m.retire, "explicit-model-only", nil
}
func nativeTraceModelFixture() (*nativeTraceModel, nativeBirthTraceConfig) {
	e, raw := nativeBirthWireFixture()
	cfg := nativeBirthTraceConfig{Original: ProcessIdentity{PostmasterPID: 42, PostmasterStartTicks: 1, AddressSpaceMaxBytes: control.Uint64(e.AddressSpaceBytes)}, PeerUID: 1000, PeerGID: 1000, Expected: e}
	m := &nativeTraceModel{frame: raw, cfg: cfg}
	cfg.Persist = func(_ context.Context, p nativeFilterPhase, _ nativeBirthTraceTranscript) error {
		return m.hit(fmt.Sprintf("persist-%d", p))
	}
	return m, cfg
}
func TestNativeBirthTracerModelSuccessOrdering(t *testing.T) {
	m, cfg := nativeTraceModelFixture()
	tr, err, retire := nativeBirthTraceFlow(context.Background(), cfg, m, 77)
	require.NoError(t, err)
	require.False(t, retire)
	require.True(t, tr.BaseVerified && tr.FinalVerified && tr.BasePersisted && tr.FinalPersisted && tr.DetachedNonAuthorizing)
	require.Equal(t, []string{"verify-original-before", "receive", "trusted-compile", "tentative-seize-zero", "interrupt", "wait-base-interrupt-stop", "verify-stopped-1", "set-exitkill-after-confirm", "base-kernel-filter-1", "persist-1", "verify-stopped-2", "base-kernel-filter-2", "cont-zero", "ack", "wait-final-self-stop", "verify-stopped-3", "fd198-closed", "final-kernel-filter-1", "persist-2", "verify-stopped-4", "fd198-closed", "final-kernel-filter-2", "detach-zero"}, m.calls)
	require.NotContains(t, tr.Cleanup, "NoEffect")
}
func TestNativeBirthTracerModelFaultsCannotReleaseOrKillReusedTarget(t *testing.T) {
	for _, tc := range []struct {
		fault              string
		cleanup, confirmed bool
	}{
		{"verify-original-before", false, false}, {"receive", false, false}, {"trusted-compile", false, false}, {"tentative-seize-zero", false, false},
		{"interrupt", true, false}, {"wait-base-interrupt-stop", true, false}, {"verify-stopped-1", true, false},
		{"set-exitkill-after-confirm", true, true}, {"base-kernel-filter-1", true, true}, {"persist-1", true, true},
		{"verify-stopped-2", true, true}, {"base-kernel-filter-2", true, true}, {"cont-zero", true, true}, {"ack", true, true},
		{"wait-final-self-stop", true, true}, {"verify-stopped-3", true, true}, {"fd198-closed", true, true},
		{"final-kernel-filter-1", true, true}, {"persist-2", true, true}, {"verify-stopped-4", true, true},
		{"final-kernel-filter-2", true, true}, {"detach-zero", true, true},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			m, cfg := nativeTraceModelFixture()
			m.fail = tc.fault
			tr, err, _ := nativeBirthTraceFlow(context.Background(), cfg, m, 77)
			require.ErrorIs(t, err, errNativeTraceModel)
			require.False(t, tr.DetachedNonAuthorizing)
			if tc.cleanup {
				require.ErrorIs(t, err, ErrUnknown)
				require.Equal(t, tc.confirmed, m.cleanupConfirmed)
				require.Equal(t, fmt.Sprintf("cleanup-confirmed-%t", tc.confirmed), m.calls[len(m.calls)-1])
			} else {
				require.NotContains(t, strings.Join(m.calls, ","), "cleanup-")
			}
			if !tc.confirmed {
				require.NotContains(t, m.calls, "set-exitkill-after-confirm")
				require.NotContains(t, m.calls, "ack")
			}
			if tc.fault != "detach-zero" {
				require.NotContains(t, m.calls, "detach-zero")
			}
		})
	}
}
func TestNativeBirthTracerModelTentativeDAndSETOPTIONSBoundary(t *testing.T) {
	for _, fault := range []string{"wait-base-interrupt-stop", "verify-stopped-1", "set-exitkill-after-confirm"} {
		t.Run(fault, func(t *testing.T) {
			m, cfg := nativeTraceModelFixture()
			m.fail = fault
			m.retire = true
			tr, err, retire := nativeBirthTraceFlow(context.Background(), cfg, m, 77)
			require.ErrorIs(t, err, ErrUnknown)
			require.True(t, retire)
			require.False(t, tr.DetachedNonAuthorizing)
			require.False(t, tr.TracerThreadRetired, "模型不能伪称真实线程已退出")
			require.Equal(t, fault == "set-exitkill-after-confirm", m.cleanupConfirmed)
			require.NotContains(t, m.calls, "ack")
		})
	}
}
func TestNativeBirthTracerModelCancellationWaitsForCleanup(t *testing.T) {
	for _, phase := range []nativeFilterPhase{nativeFilterPMBase, nativeFilterPMFinal} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			m, cfg := nativeTraceModelFixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			m.cleanupWait = release
			cfg.Persist = func(context.Context, nativeFilterPhase, nativeBirthTraceTranscript) error { return nil }
			cfg.Persist = func(_ context.Context, p nativeFilterPhase, _ nativeBirthTraceTranscript) error {
				_ = m.hit(fmt.Sprintf("persist-%d", p))
				if p == phase {
					cancel()
				}
				return nil
			}
			done := make(chan error, 1)
			go func() {
				tr, err, _ := nativeBirthTraceFlow(ctx, cfg, m, 77)
				if tr.DetachedNonAuthorizing {
					done <- fmt.Errorf("cancelled model detached")
					return
				}
				done <- err
			}()
			select {
			case <-done:
				t.Fatal("ctx cancellation returned before cleanup")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			require.ErrorIs(t, <-done, context.Canceled)
			require.True(t, m.cleanupConfirmed)
			require.NotContains(t, m.calls, "detach-zero")
			if phase == nativeFilterPMBase {
				require.NotContains(t, m.calls, "ack")
			}
		})
	}
}
func TestNativeBirthTracerModelStrictStatusAndReservationMaps(t *testing.T) {
	_, cfg := nativeTraceModelFixture()
	_, frame := nativeBirthWireFixture()
	request, err := decodeNativeBirthRequest(frame, cfg.Expected)
	require.NoError(t, err)
	var b strings.Builder
	for _, r := range request.Ranges {
		fmt.Fprintf(&b, "%x-%x ---p 00000000 00:00 0\n", r.Base, r.Base+r.ReservedBytes)
	}
	require.NoError(t, nativeTraceReservationMaps(b.String(), request))
	for _, bad := range []string{strings.Replace(b.String(), "---p", "rw-p", 1), strings.Replace(b.String(), "00:00 0", "00:00 9", 1), strings.Replace(b.String(), "00000000", "00001000", 1), strings.SplitN(b.String(), "\n", 2)[1]} {
		require.Error(t, nativeTraceReservationMaps(bad, request))
	}
	for _, bad := range []string{"Uid:\t1 1 1\n", "Uid:\t1 1 1 1\nUid:\t1 1 1 1\n", "Uid:\t01 1 1 1\n", "Uid:\t4294967296 1 1 1\n"} {
		_, err := nativeTraceStatusValues(bad, "Uid", 4)
		require.Error(t, err)
	}
	got, err := nativeTraceStatusValues("Uid:\t1000 1000 1000 1000\n", "Uid", 4)
	require.NoError(t, err)
	require.Equal(t, []uint64{1000, 1000, 1000, 1000}, got)
}

func TestNativeBirthTracerModelPidfdWaitDecodeAndReaperReuse(t *testing.T) {
	makeInfo := func(cause, status, pid, uid uint32) [128]byte {
		var raw [128]byte
		for offset, value := range map[int]uint32{0: uint32(unix.SIGCHLD), 8: cause, 16: pid, 20: uid, 24: status} {
			binary.LittleEndian.PutUint32(raw[offset:offset+4], value)
		}
		return raw
	}
	for _, tc := range []struct {
		name          string
		cause, status uint32
		check         func(*testing.T, unix.WaitStatus)
	}{
		{"interrupt-full-event", 4, uint32(unix.SIGTRAP) | (uint32(unix.PTRACE_EVENT_STOP) << 8), func(t *testing.T, s unix.WaitStatus) {
			require.True(t, s.Stopped())
			require.Equal(t, unix.SIGTRAP, s.StopSignal())
			require.Equal(t, unix.PTRACE_EVENT_STOP, s.TrapCause())
		}},
		{"self-sigstop-delivery", 4, uint32(unix.SIGSTOP), func(t *testing.T, s unix.WaitStatus) {
			require.True(t, s.Stopped())
			require.Equal(t, unix.SIGSTOP, s.StopSignal())
			require.Zero(t, uint32(s)>>16)
		}},
		{"exit-code", 1, 23, func(t *testing.T, s unix.WaitStatus) {
			require.True(t, s.Exited())
			require.Equal(t, 23, s.ExitStatus())
		}},
		{"killed", 2, uint32(unix.SIGKILL), func(t *testing.T, s unix.WaitStatus) {
			require.True(t, s.Signaled())
			require.Equal(t, unix.SIGKILL, s.Signal())
			require.False(t, s.CoreDump())
		}},
		{"dumped", 3, uint32(unix.SIGSEGV), func(t *testing.T, s unix.WaitStatus) {
			require.True(t, s.Signaled())
			require.Equal(t, unix.SIGSEGV, s.Signal())
			require.True(t, s.CoreDump())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pid, status, err := decodeNativeOriginalWait(makeInfo(tc.cause, tc.status, 42, 1000), 42, 1000)
			require.NoError(t, err)
			require.Equal(t, 42, pid)
			tc.check(t, status)
		})
	}
	pid, status, err := decodeNativeOriginalWait([128]byte{}, 42, 1000)
	require.NoError(t, err)
	require.Zero(t, pid)
	require.Zero(t, status)
	for _, bad := range [][128]byte{makeInfo(4, 0, 42, 1000), makeInfo(4, 0x10000, 42, 1000), makeInfo(1, 256, 42, 1000), makeInfo(2, 0, 42, 1000), makeInfo(5, 19, 42, 1000), makeInfo(6, 18, 42, 1000), makeInfo(4, 19, 43, 1000), makeInfo(4, 19, 42, 1001)} {
		_, _, err := decodeNativeOriginalWait(bad, 42, 1000)
		require.Error(t, err)
	}
	// 明确模型：其他同TGID reaper收走original，numeric slot42重新属于new-child；
	// 原pidfd91仍只指原对象。helper必须传P_PIDFD91；不得退回P_PID42去收new-child。
	originalReaped := true
	newChildReaped := false
	calls := 0
	reader := func(which, fd int, raw *[128]byte, options int) error {
		calls++
		require.Equal(t, 3, which)
		require.Equal(t, 91, fd)
		require.Equal(t, unix.WEXITED|unix.WSTOPPED|unix.WNOHANG|unix.WALL, options)
		if originalReaped {
			return unix.ECHILD
		}
		newChildReaped = true
		*raw = makeInfo(1, 0, 42, 1000)
		return nil
	}
	pid, _, err = nativePollOriginalWait(91, 42, 1000, reader)
	require.ErrorIs(t, err, unix.ECHILD)
	require.Zero(t, pid)
	require.Equal(t, 1, calls)
	require.False(t, newChildReaped)
	for _, errno := range []error{unix.EINVAL, unix.ENOSYS, unix.ECHILD} {
		calls = 0
		_, _, err = nativePollOriginalWait(91, 42, 1000, func(which, fd int, raw *[128]byte, options int) error {
			calls++
			require.Equal(t, 3, which)
			require.Equal(t, 91, fd)
			return errno
		})
		require.ErrorIs(t, err, errno)
		require.Equal(t, 1, calls, "unsupported/ECHILD must not numeric fallback")
	}
}
