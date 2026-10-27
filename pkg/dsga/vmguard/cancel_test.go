package vmguard

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

func cancelAuthority(a *authorization, origin journal.Owner) {
	a.Dispatch = false
	a.Abort = &cancellationAuthority{Origin: origin, Witness: Cancellation{ProtocolVersion: 1, Owner: a.Owner, VMSequence: 42, VMResourceVersion: "vm-rv-42"}}
}

// 真实Mac flock/fsync/rename与严格WAL解码；native/认证源为显式夹具。
// 没有以普通not_sent或当前容量等于before代替不可再发送的本地取消终态。
func TestRunnerCancelBeforeSendSealsOriginalAndRejectsLateReplay(t *testing.T) {
	for _, mode := range []string{"unseen", "prepared", "not-sent", "takeover", "expired", "noop"} {
		t.Run(mode, func(t *testing.T) {
			e, f, a := setupEngine(t)
			origin := a.Owner
			if mode == "noop" {
				a.Command.Request.TargetVMMemoryBytes = a.Command.BeforeBytes
			}
			if mode == "expired" {
				a.Command.Request.DeadlineUnixMillis = 1
			}
			if mode == "prepared" || mode == "not-sent" || mode == "takeover" {
				writeFixtureRecord(t, e, preparedFixture(t, e, a))
			}
			if mode == "not-sent" {
				require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
				require.Equal(t, notSent, readFixtureRecord(t, e).Last.Kind)
			}
			if mode == "takeover" {
				a.Owner = journal.Owner{HolderID: "owner-three", Epoch: 3}
			}
			cancelAuthority(a, origin)
			require.NoError(t, e.advance(context.Background()))
			r := readFixtureRecord(t, e)
			require.EqualValues(t, 2, r.ProtocolVersion)
			require.Equal(t, cancelledBeforeSend, r.Last.Kind)
			require.Nil(t, r.Pending)
			require.Nil(t, r.LastPublished, "immutable receipt publication is a distinct durable step")
			require.Equal(t, *a.Command, r.Last.Intent.Command)
			require.Equal(t, origin, r.Last.Intent.Owner)
			require.Equal(t, a.Owner, r.Last.Cancellation.Owner)
			require.Equal(t, a.Abort.Witness, *r.Last.Cancellation)
			require.Equal(t, r.Last.Intent.Before, r.Last.Actual)
			require.NoError(t, Receipt(*r.Last).Validate())
			require.True(t, Receipt(*r.Last).CancelledBeforeSend())
			require.False(t, Receipt(*r.Last).NotSent())
			require.False(t, Receipt(*r.Last).Complete())
			cloned := r.clone()
			cloned.Last.Cancellation.VMSequence++
			require.NotEqual(t, cloned.Last.Cancellation, r.Last.Cancellation)
			require.NoError(t, e.advance(context.Background()))
			require.Equal(t, r, readFixtureRecord(t, e), "same cancellation cannot replace original witness or grow sequence")
			// 模拟迟到的旧重放选择：即使上层错误恢复Dispatch，本地终态仍封住
			// 同Key发送，不接受普通not_sent重放规则，也不清高水位。
			a.Abort, a.Dispatch = nil, true
			a.ReplayNonce = "abcdef0123456789abcdef0123456789"
			a.ReplayReceipt = budget.ObjectRef{Name: "old-unsent-receipt", UID: "old-unsent-uid"}
			require.NoError(t, e.advance(context.Background()))
			require.Equal(t, r, readFixtureRecord(t, e))
			require.Zero(t, f.writes)
		})
	}
}

func TestRunnerCancellationCannotEraseMayHaveSentOrCompleted(t *testing.T) {
	e, f, a := setupEngine(t)
	f.apply, f.err = false, errors.New("fixture lost QMP reply")
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	original := readFixtureRecord(t, e)
	require.Equal(t, mayHaveSent, original.Pending.Stage)
	cancelAuthority(a, a.Owner)
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	unknown := readFixtureRecord(t, e)
	require.Equal(t, original, unknown, "current before cannot erase a possible late QMP command")
	// 原真实目标随后读回，只能落Completed，不能变成取消NoEffect。
	f.mu.Lock()
	f.actual = Observation{RequestedBytes: a.Command.Request.TargetVMMemoryBytes, ActualBytes: a.Command.Request.TargetVMMemoryBytes}
	f.mu.Unlock()
	require.NoError(t, e.advance(context.Background()))
	completedRecord := readFixtureRecord(t, e)
	require.Equal(t, completed, completedRecord.Last.Kind)
	require.Nil(t, completedRecord.Last.Cancellation)
	require.NoError(t, e.advance(context.Background()))
	require.Equal(t, completedRecord, readFixtureRecord(t, e))
	require.Equal(t, 1, f.writes)
}

func TestRunnerCancellationRechecksAuthorityBeforeDurableClosure(t *testing.T) {
	for _, mode := range []string{"removed", "new-owner", "old-sequence", "source-error", "actual-drift"} {
		t.Run(mode, func(t *testing.T) {
			e, f, a := setupEngine(t)
			cancelAuthority(a, a.Owner)
			if mode == "actual-drift" {
				f.actual.RequestedBytes = a.Command.Request.TargetVMMemoryBytes
			}
			reads := 0
			e.read = func(context.Context) (authorization, error) {
				reads++
				latest := *a
				if reads == 1 {
					return latest, nil
				}
				abort := *a.Abort
				latest.Abort = &abort
				switch mode {
				case "removed":
					latest.Abort = nil
				case "new-owner":
					latest.Owner = journal.Owner{HolderID: "owner-three", Epoch: 3}
				case "old-sequence":
					latest.Abort.Witness.VMSequence--
				case "source-error":
					return authorization{}, errors.New("fixture original Kube read failed")
				}
				return latest, nil
			}
			require.Error(t, e.advance(context.Background()))
			r := readFixtureRecord(t, e)
			require.Nil(t, r.HighWater)
			require.Nil(t, r.Last)
			require.Nil(t, r.Pending)
			require.Zero(t, f.writes)
		})
	}
}

func TestRunnerCancellationWaitsForOriginalSendingFileLock(t *testing.T) {
	e, f, a := setupEngine(t)
	var current atomic.Pointer[authorization]
	current.Store(a)
	e.read = func(context.Context) (authorization, error) { return *current.Load(), nil }
	f.entered, f.release = make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(f.release) })
	defer release()
	done := make(chan error, 1)
	go func() { done <- e.advance(context.Background()) }()
	select {
	case <-f.entered:
	case err := <-done:
		t.Fatalf("original sender exited before QMP barrier: %v", err)
	case <-time.After(time.Second):
		t.Fatal("original sender did not reach QMP barrier")
	}
	closing := *a
	cancelAuthority(&closing, a.Owner)
	current.Store(&closing)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	require.Error(t, e.advance(ctx), "cancel must not bypass in-flight original sender's flock")
	release()
	require.NoError(t, <-done)
	require.NoError(t, e.advance(context.Background()))
	r := readFixtureRecord(t, e)
	require.Equal(t, completed, r.Last.Kind)
	require.Nil(t, r.Last.Cancellation)
	require.Equal(t, 1, f.writes)
}

func TestRunnerCancellationV2RejectsMalformedWitnessAndPreservesVersion(t *testing.T) {
	e, f, a := setupEngine(t)
	cancelAuthority(a, a.Owner)
	require.NoError(t, e.advance(context.Background()))
	r := readFixtureRecord(t, e)
	for _, mode := range []string{"old-record-version", "missing-witness", "possible-send", "wrong-actual", "wrong-owner", "empty-sequence", "ordinary-kind"} {
		t.Run(mode, func(t *testing.T) {
			bad := r.clone()
			switch mode {
			case "old-record-version":
				bad.ProtocolVersion = 1
			case "missing-witness":
				bad.Last.Cancellation = nil
			case "possible-send":
				bad.Last.Intent.Stage = mayHaveSent
			case "wrong-actual":
				bad.Last.Actual.ActualBytes = a.Command.Request.TargetVMMemoryBytes
			case "wrong-owner":
				bad.Last.Cancellation.Owner.HolderID = "other-same-epoch"
			case "empty-sequence":
				bad.Last.Cancellation.VMSequence = 0
			case "ordinary-kind":
				bad.Last.Kind = notSent
			}
			require.Error(t, bad.validate())
		})
	}
	// 本地取消不阻塞下一条更高Key的已认证动作；v2不得因新普通结果降级。
	r.LastPublished = &budget.ObjectRef{Name: "cancelled-immutable", UID: "cancelled-uid"}
	writeFixtureRecord(t, e, r)
	next := *a.Command
	next.Request.Key.Revision++
	next.Request.Key.OperationID.Low++
	a.Command, a.Abort, a.Dispatch = &next, nil, true
	require.NoError(t, e.advance(context.Background()))
	r = readFixtureRecord(t, e)
	require.EqualValues(t, 2, r.ProtocolVersion)
	require.Equal(t, completed, r.Last.Kind)
	require.Nil(t, r.Last.Cancellation)
	require.Equal(t, 1, f.writes)
}
