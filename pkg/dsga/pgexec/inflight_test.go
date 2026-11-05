package pgexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// TLS/typed-Store均复用原夹具；只模拟Native回执，不声称真实PG/预算全链通过。
type flightFixture struct {
	executor   *Executor
	api        *vmAPI
	client     *compute.Client
	store      *journal.Store
	intent     control.Request
	entered    chan struct{}
	release    chan struct{}
	cancelled  chan struct{}
	once       sync.Once
	cancelOnce sync.Once
	released   sync.Once
	resizes    atomic.Int32
	queries    atomic.Int32
	cancels    atomic.Int32
	committed  atomic.Bool
	loseReply  bool
	badReply   bool
}

func newFlightFixture(t *testing.T, kind control.Kind, lifetime time.Duration, deadline ...control.Uint64) *flightFixture {
	t.Helper()
	intent := request(t, kind)
	intent.DeadlineUnixMillis = control.Uint64(time.Now().Add(lifetime).UnixMilli())
	require.LessOrEqual(t, len(deadline), 1)
	if len(deadline) == 1 {
		intent.DeadlineUnixMillis = deadline[0]
	}
	api, store := prepareStore(t, intent)
	f := &flightFixture{api: api, store: store, intent: intent, entered: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
	client, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/resize":
			// 读完原请求体后，server才能及时看到HTTP连接取消；不以测试等待替代客户端ctx。
			_, readErr := io.Copy(io.Discard, r.Body)
			require.NoError(t, readErr)
			f.resizes.Add(1)
			require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ResizePG).Phase)
			f.once.Do(func() { close(f.entered) })
			select {
			case <-f.release:
				if f.loseReply {
					http.Error(w, "lost after send", http.StatusServiceUnavailable)
					return
				}
				nativeIntent := intent
				if f.badReply {
					nativeIntent.BudgetCommitmentID += "-wrong-context"
				}
				send(w, response(t, nativeIntent))
			case <-r.Context().Done():
				f.cancelOnce.Do(func() { close(f.cancelled) })
			}
		case "/dsg/v1/resize/cancel":
			f.cancels.Add(1)
			fact := response(t, intent)
			fact.Receipt.Disposition = control.ReceiptTooLate
			send(w, fact)
		case "/dsg/v1/resize/status":
			f.queries.Add(1)
			fact := response(t, intent)
			fact.Receipt.Disposition = control.ReceiptReplayed
			if !f.committed.Load() {
				fact.Receipt.Disposition = control.ReceiptNotFound
				fact.Receipt.HasResult = false
				fact.Receipt.Result = nil
				fact.Observed.Operation = nil
				fact.Observed.CapacityGeneration = intent.ExpectedCapacityGeneration
				fact.Observed.LowBuffers = 256
				fact.Observed.HighBuffers = 256
			}
			send(w, fact)
		default:
			http.NotFound(w, r)
		}
	})
	f.client = client
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	f.executor, err = New(store, client, snapshot.State().Owner)
	require.NoError(t, err)
	t.Cleanup(f.unblock)
	return f
}
func (f *flightFixture) unblock() { f.released.Do(func() { close(f.release) }) }
func waitFlightEntered(t *testing.T, f *flightFixture) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("original Resize was not sent")
	}
}
func waitFlightCancelled(t *testing.T, f *flightFixture) {
	t.Helper()
	select {
	case <-f.cancelled:
	case <-time.After(6 * time.Second):
		t.Fatal("same-session cancellation did not cancel original HTTP")
	}
}
func enableFlight(t *testing.T, f *flightFixture, parent context.Context) context.CancelFunc {
	t.Helper()
	stop, err := f.executor.EnableInFlight(parent)
	require.NoError(t, err)
	t.Cleanup(stop)
	require.True(t, f.executor.InFlightEnabled())
	return stop
}
func startFlightAndRequireYield(t *testing.T, f *flightFixture, ctx context.Context) {
	t.Helper()
	type outcome struct {
		p Progress
		e error
	}
	done := make(chan outcome, 1)
	go func() { p, e := f.executor.Advance(ctx); done <- outcome{p, e} }()
	waitFlightEntered(t, f)
	select {
	case out := <-done:
		require.NoError(t, out.e)
		require.Equal(t, AwaitingFact, out.p)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("single writer remained blocked in original Resize HTTP instead of yielding")
	}
}
func flightSnapshot(t *testing.T, f *flightFixture) *journal.Snapshot {
	t.Helper()
	s, e := f.store.Load(context.Background())
	require.NoError(t, e)
	return s
}
func TestPGResizeInFlightYieldsToWriterAndObservesWithoutResend(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	enableFlight(t, f, parent)
	short, shortCancel := context.WithCancel(parent)
	startFlightAndRequireYield(t, f, short)
	shortCancel()
	require.True(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	other := f.intent
	other.BudgetCommitmentID += "-other"
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), other))
	progress, err := f.executor.ReplayOriginal(context.Background())
	require.NoError(t, err)
	require.Equal(t, AwaitingFact, progress)
	for i := 0; i < 3; i++ {
		progress, err = f.executor.Advance(context.Background())
		require.NoError(t, err)
		require.Equal(t, AwaitingFact, progress)
		require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	}
	require.GreaterOrEqual(t, f.queries.Load(), int32(3))
	require.Equal(t, int32(1), f.resizes.Load())
	f.committed.Store(true)
	progress, err = f.executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, Recorded, progress)
	require.Equal(t, journal.Settled, stepFor(t, f.store, journal.ResizePG).Phase)
	require.True(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	progress, err = f.executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, AwaitingFact, progress)
	require.Equal(t, int32(1), f.resizes.Load())
	require.NotNil(t, f.client.Pending())
	f.unblock()
	require.Eventually(t, func() bool { return f.client.Pending() == nil }, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { p, e := f.executor.Advance(context.Background()); return e == nil && p == Reconciled }, 3*time.Second, 5*time.Millisecond)
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightReplyIsRecordedOnlyByCurrentWriter(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	// 同一派发以外的合法RV更新不能让writer使用旧Start快照强写。
	f.api.mu.Lock()
	f.api.version++
	f.api.vm.ResourceVersion = "rv-external-update"
	f.api.mu.Unlock()
	f.unblock()
	require.Eventually(t, func() bool { return f.client.Pending() == nil }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase, "network goroutine must not write the journal")
	require.True(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent), "successful result awaiting writer remains same local task")
	require.Eventually(t, func() bool { p, e := f.executor.Advance(context.Background()); return e == nil && p == Recorded }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, journal.Settled, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightParentCancellationKeepsUnknown(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	parent, cancel := context.WithCancel(context.Background())
	enableFlight(t, f, parent)
	startFlightAndRequireYield(t, f, context.Background())
	cancel()
	waitFlightCancelled(t, f)
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	p, e := f.executor.Advance(context.Background())
	require.ErrorIs(t, e, context.Canceled)
	require.Equal(t, AwaitingFact, p)
	require.Eventually(t, func() bool { return f.client.Pending() != nil }, time.Second, 5*time.Millisecond)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightRequestDeadlineKeepsOriginalUnknown(t *testing.T) {
	f := newFlightFixture(t, control.Grow, 5*time.Second)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	waitFlightCancelled(t, f)
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	require.Eventually(t, func() bool { _, e := f.executor.Advance(context.Background()); return e != nil }, 3*time.Second, 5*time.Millisecond)
	require.NotNil(t, f.client.Pending())
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightLateReplyCannotCrossOwner(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	s := flightSnapshot(t, f)
	s, e := f.store.Takeover(context.Background(), s, s.State().Owner, "owner-b")
	require.NoError(t, e)
	require.False(t, f.executor.MatchInFlight(s, f.intent))
	f.unblock()
	require.Eventually(t, func() bool { return f.client.Pending() == nil }, 3*time.Second, 5*time.Millisecond)
	_, e = f.executor.Advance(context.Background())
	require.Error(t, e)
	current := flightSnapshot(t, f)
	require.Equal(t, "owner-b", current.State().Owner.HolderID)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightLostReplyQueriesOnlyOriginalKey(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	f.loseReply = true
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	f.unblock()
	require.Eventually(t, func() bool { _, e := f.executor.Advance(context.Background()); return e != nil }, 3*time.Second, 5*time.Millisecond)
	require.NotNil(t, f.client.Pending())
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	p, e := f.executor.Advance(context.Background())
	require.NoError(t, e)
	require.Equal(t, AwaitingFact, p)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	f.committed.Store(true)
	p, e = f.executor.Advance(context.Background())
	require.NoError(t, e)
	require.Equal(t, Recorded, p)
	require.Equal(t, journal.Settled, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightDisabledAndShrinkRemainSynchronous(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   control.Kind
		enable bool
	}{{"default", control.Grow, false}, {"shrink", control.Shrink, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFlightFixture(t, tc.kind, time.Minute)
			if tc.enable {
				enableFlight(t, f, context.Background())
			} else {
				require.False(t, f.executor.InFlightEnabled())
			}
			done := make(chan error, 1)
			go func() { _, e := f.executor.Advance(context.Background()); done <- e }()
			waitFlightEntered(t, f)
			select {
			case e := <-done:
				t.Fatalf("unapproved path became asynchronous: %v", e)
			case <-time.After(100 * time.Millisecond):
			}
			f.unblock()
			select {
			case e := <-done:
				require.NoError(t, e)
			case <-time.After(3 * time.Second):
				t.Fatal("synchronous path failed to finish")
			}
			require.Equal(t, int32(1), f.resizes.Load())
			require.Equal(t, journal.Settled, stepFor(t, f.store, journal.ResizePG).Phase)
		})
	}
}
func TestPGResizeInFlightContextBindingCannotBeReused(t *testing.T) {
	e := &Executor{}
	_, err := e.EnableInFlight(nil)
	require.Error(t, err)
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.EnableInFlight(dead)
	require.Error(t, err)
	stop, err := e.EnableInFlight(context.Background())
	require.NoError(t, err)
	stop()
	_, err = e.EnableInFlight(context.Background())
	require.Error(t, err)
}

func TestPGResizeInFlightAbortKeepsOriginalCancelPath(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	snapshot := flightSnapshot(t, f)
	_, err := f.store.RequestAbort(context.Background(), snapshot, snapshot.State().Owner, f.intent.Key)
	require.NoError(t, err)
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	progress, err := f.executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, Recorded, progress)
	require.Equal(t, int32(1), f.cancels.Load())
	require.Equal(t, int32(1), f.resizes.Load())
	require.True(t, flightSnapshot(t, f).State().Active.AbortRequested)
	require.Equal(t, journal.Settled, stepFor(t, f.store, journal.ResizePG).Phase)
}
func TestPGResizeInFlightWrongContextReplyKeepsUnknown(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	f.badReply = true
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	f.unblock()
	require.Eventually(t, func() bool { _, err := f.executor.Advance(context.Background()); return err != nil }, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	require.NotNil(t, f.client.Pending())
	require.Equal(t, int32(1), f.resizes.Load())
}

// uint64全范围是原schema合法输入；不得因Go int64回绕修改原请求或提前过期。
func TestPGResizeInFlightUnsignedDeadlineRetainsOriginalRequest(t *testing.T) {
	for _, deadline := range []control.Uint64{control.Uint64(math.MaxInt64) + 1, control.Uint64(math.MaxUint64)} {
		t.Run(fmt.Sprint(uint64(deadline)), func(t *testing.T) {
			f := newFlightFixture(t, control.Grow, time.Minute, deadline)
			parent, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			enableFlight(t, f, parent)
			startFlightAndRequireYield(t, f, context.Background())
			require.True(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
			require.Equal(t, deadline, flightSnapshot(t, f).State().Active.Request.DeadlineUnixMillis)
			f.unblock()
			require.Eventually(t, func() bool { p, e := f.executor.Advance(context.Background()); return e == nil && p == Recorded }, 3*time.Second, 5*time.Millisecond)
			require.Equal(t, int32(1), f.resizes.Load())
		})
	}
}

func waitOriginalFlightFinished(t *testing.T, f *flightFixture) {
	t.Helper()
	require.Eventually(t, func() bool {
		f.executor.flight.mu.Lock()
		defer f.executor.flight.mu.Unlock()
		return f.executor.flight.job != nil && f.executor.flight.job.finished
	}, 6*time.Second, 5*time.Millisecond)
}
func requireOnlyOriginalPGSettled(t *testing.T, f *flightFixture) {
	t.Helper()
	s := flightSnapshot(t, f)
	pgSeen := false
	for _, step := range s.State().Active.Steps {
		if step.Action == journal.ResizePG {
			pgSeen = true
			require.Equal(t, journal.Settled, step.Phase)
		} else if pgSeen {
			require.Equal(t, journal.Planned, step.Phase)
			require.Nil(t, step.Observation)
		}
	}
	require.True(t, pgSeen)
}
func TestPGResizeInFlightReadonlyLostReplyQueriesRetainedOriginal(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	f.loseReply = true
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	f.unblock()
	waitOriginalFlightFinished(t, f)
	p, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.Error(t, err)
	require.Equal(t, AwaitingFact, p)
	require.NotNil(t, f.client.Pending())
	p, err = f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, AwaitingFact, p)
	require.Equal(t, int32(1), f.queries.Load(), "原completion-only helper消费丢回复后没有原Key Status出口")
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	f.committed.Store(true)
	p, err = f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, Recorded, p)
	requireOnlyOriginalPGSettled(t, f)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightReadonlyLateReplyOnlyCurrentWriterRecords(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	f.unblock()
	waitOriginalFlightFinished(t, f)
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	p, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, Recorded, p)
	requireOnlyOriginalPGSettled(t, f)
	require.Zero(t, f.queries.Load())
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightReadonlyChildDeadlineStillQueriesOriginal(t *testing.T) {
	f := newFlightFixture(t, control.Grow, 5*time.Second)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	waitOriginalFlightFinished(t, f)
	require.False(t, f.executor.MatchInFlight(flightSnapshot(t, f), f.intent))
	_, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.Error(t, err)
	p, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, AwaitingFact, p)
	require.Equal(t, int32(1), f.queries.Load())
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	f.committed.Store(true)
	p, err = f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, Recorded, p)
	requireOnlyOriginalPGSettled(t, f)
	require.Equal(t, int32(1), f.resizes.Load())
}
func TestPGResizeInFlightReadonlyParentCancelAndOwnerChangeRefuseRecord(t *testing.T) {
	for _, mode := range []string{"parent-cancel", "owner-change"} {
		t.Run(mode, func(t *testing.T) {
			f := newFlightFixture(t, control.Grow, time.Minute)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			enableFlight(t, f, parent)
			startFlightAndRequireYield(t, f, context.Background())
			f.unblock()
			waitOriginalFlightFinished(t, f)
			s := flightSnapshot(t, f)
			if mode == "parent-cancel" {
				cancel()
			} else {
				var err error
				s, err = f.store.Takeover(context.Background(), s, s.State().Owner, "owner-b")
				require.NoError(t, err)
			}
			p, err := f.executor.ObserveDispatchedInFlight(context.Background(), s)
			require.Error(t, err)
			require.Equal(t, AwaitingFact, p)
			require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
			require.Zero(t, f.queries.Load())
			require.Equal(t, int32(1), f.resizes.Load())
		})
	}
}
func TestPGResizeInFlightReadonlyRequiresCapturedUnchangedSnapshot(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		f := newFlightFixture(t, control.Grow, time.Minute)
		_, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
		require.Error(t, err)
		require.Zero(t, f.queries.Load())
		require.Zero(t, f.resizes.Load())
	})
	t.Run("planned-no-captured-flight", func(t *testing.T) {
		f := newFlightFixture(t, control.Grow, time.Minute)
		enableFlight(t, f, context.Background())
		_, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
		require.Error(t, err)
		require.Equal(t, journal.Planned, stepFor(t, f.store, journal.ResizePG).Phase)
		require.Zero(t, f.queries.Load())
		require.Zero(t, f.resizes.Load())
	})
	t.Run("snapshot-RV", func(t *testing.T) {
		f := newFlightFixture(t, control.Grow, time.Minute)
		enableFlight(t, f, context.Background())
		startFlightAndRequireYield(t, f, context.Background())
		old := flightSnapshot(t, f)
		f.api.mu.Lock()
		f.api.version++
		f.api.vm.ResourceVersion = "changed-observer-rv"
		f.api.mu.Unlock()
		f.unblock()
		waitOriginalFlightFinished(t, f)
		_, err := f.executor.ObserveDispatchedInFlight(context.Background(), old)
		require.Error(t, err)
		require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
		require.Zero(t, f.queries.Load())
		p, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
		require.NoError(t, err)
		require.Equal(t, Recorded, p)
		requireOnlyOriginalPGSettled(t, f)
	})
}
func TestPGResizeInFlightReadonlyStatusFirstThenLateReplyNeverRewrites(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	enableFlight(t, f, context.Background())
	startFlightAndRequireYield(t, f, context.Background())
	f.committed.Store(true)
	p, err := f.executor.ObserveDispatchedInFlight(context.Background(), flightSnapshot(t, f))
	require.NoError(t, err)
	require.Equal(t, Recorded, p)
	before := flightSnapshot(t, f)
	f.unblock()
	waitOriginalFlightFinished(t, f)
	p, err = f.executor.ObserveDispatchedInFlight(context.Background(), before)
	require.NoError(t, err)
	require.Equal(t, Reconciled, p)
	after := flightSnapshot(t, f)
	require.Equal(t, before.State(), after.State())
	require.Equal(t, before.ResourceVersion(), after.ResourceVersion())
	requireOnlyOriginalPGSettled(t, f)
	require.Equal(t, int32(1), f.queries.Load())
	require.Equal(t, int32(1), f.resizes.Load())
}

// 已到原结果Record的真实typed HTTP边界再取消parent，不把调用前取消冒中途覆盖。
// server尚未接受CAS时拒写；这不声称能撤销apiserver已经接受的写入。
func TestPGResizeInFlightParentCancelDuringResultRecordCancelsJournalHTTP(t *testing.T) {
	f := newFlightFixture(t, control.Grow, time.Minute)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	enableFlight(t, f, parent)
	startFlightAndRequireYield(t, f, context.Background())
	f.unblock()
	waitOriginalFlightFinished(t, f)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			f.api.serve(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		r.Body = io.NopCloser(bytes.NewReader(body))
		close(entered)
		select {
		case <-r.Context().Done():
			close(cancelled)
			return
		case <-release:
			f.api.serve(w, r)
		}
	}))
	defer func() { unblock(); server.Close() }()
	binding := f.store.Binding()
	generated, err := vmclient.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	store, err := journal.NewStore(generated.NeonvmV1().VirtualMachines(binding.Namespace), binding)
	require.NoError(t, err)
	f.store = store
	f.executor.store = store
	type result struct {
		p   Progress
		err error
	}
	done := make(chan result, 1)
	go func() { p, err := f.executor.Advance(context.Background()); done <- result{p, err} }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("original reply did not enter typed Record HTTP")
	}
	cancelParent()
	select {
	case <-cancelled:
	case <-time.After(500 * time.Millisecond):
		unblock()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		t.Fatalf("parent cancellation did not reach pending original result journal HTTP; phase=%s", stepFor(t, f.store, journal.ResizePG).Phase)
	}
	select {
	case out := <-done:
		require.Error(t, out.err)
		require.Equal(t, AwaitingFact, out.p)
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled journal call remained blocked")
	}
	require.Equal(t, journal.Unconfirmed, stepFor(t, f.store, journal.ResizePG).Phase)
	require.Equal(t, int32(1), f.resizes.Load())
	require.Zero(t, f.queries.Load())
}
