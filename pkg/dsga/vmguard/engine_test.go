package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 协议夹具只替换QEMU和Kube认证来源。文件锁、目录权限、WAL严格解码、fsync/
// rename以及并发推进均是真实Mac执行，不声称已验证KVM/内存热拔或Host预留。
type memoryFixture struct {
	mu      sync.Mutex
	dir     string
	actual  Observation
	writes  int
	apply   bool
	err     error
	entered chan struct{}
	release chan struct{}
}

func (f *memoryFixture) Read(context.Context) (Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actual, nil
}

func (f *memoryFixture) Set(ctx context.Context, n control.Uint64) error {
	raw, err := os.ReadFile(filepath.Join(f.dir, "state.json"))
	if err != nil {
		return err
	}
	var r record
	if err := json.Unmarshal(raw, &r); err != nil || r.Pending == nil || r.Pending.Stage != mayHaveSent || r.Pending.Command.Request.TargetVMMemoryBytes != n || r.Pending.Owner != r.Floor {
		return fmt.Errorf("native side effect preceded complete durable may-have-sent intent")
	}
	if f.entered != nil {
		close(f.entered)
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var absent *notSentError
	if errors.As(f.err, &absent) {
		return f.err
	}
	f.writes++
	if f.apply {
		f.actual = Observation{RequestedBytes: n, ActualBytes: n}
	}
	return f.err
}

func fixtureIdentity() Identity {
	return Identity{VMUID: "vm-uid", Incarnation: "0123456789abcdef0123456789abcdef", RunnerPodUID: "pod-uid", RunnerBootID: "1234567890abcdef1234567890abcdef", NodeUID: "node-uid", MinimumBytes: 1 << 30, MaximumBytes: 4 << 30, BlockBytes: 8 << 20}
}

func fixtureCommand(id Identity) Command {
	return Command{
		ProtocolVersion: 1, Identity: id,
		BindingClaim: budget.ObjectRef{Name: "runner-birth", UID: "birth-uid"}, HostClaim: budget.ObjectRef{Name: "host-claim", UID: "host-uid"},
		Request:     control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: id.VMUID, ComputeInstanceID: "compute-instance", PGBootID: 1}, IssuerEpoch: 2, Revision: 1, OperationID: control.OperationID{High: 1, Low: 2}}, Kind: control.Shrink, ExpectedCapacityGeneration: 1, TargetBuffers: 128, TargetVMMemoryBytes: 2 << 30, BudgetCommitmentID: "original-budget", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Hour).UnixMilli())},
		BeforeBytes: 3 << 30, PreparedSHA256: strings.Repeat("a", 64), HostSHA256: strings.Repeat("b", 64), GuestSHA256: strings.Repeat("c", 64),
	}
}

func setupEngine(t *testing.T) (*engine, *memoryFixture, *authorization) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	id := fixtureIdentity()
	c := fixtureCommand(id)
	a := &authorization{Owner: journal.Owner{HolderID: "owner-two", Epoch: 2}, LeaseUID: "lease-uid", Command: &c, JournalSHA256: strings.Repeat("d", 64), Dispatch: true, ReplayNonce: "", Denied: nil}
	f := &memoryFixture{dir: dir, actual: Observation{RequestedBytes: c.BeforeBytes, ActualBytes: c.BeforeBytes}, apply: true, mu: sync.Mutex{}, writes: 0, err: nil, entered: nil, release: nil}
	e := &engine{dir: dir, id: id, binding: c.BindingClaim, native: f, read: func(context.Context) (authorization, error) { return *a, nil }, now: time.Now}
	s, err := lockState(context.Background(), dir)
	require.NoError(t, err)
	require.NoError(t, s.save(&record{ProtocolVersion: 1, Identity: id, BindingClaim: c.BindingClaim, Sequence: 1, Floor: journal.Owner{HolderID: "initial-owner", Epoch: 1}, LeaseUID: "", HighWater: nil, Pending: nil, Last: nil, LastPublished: nil}))
	s.close()
	return e, f, a
}

func readFixtureRecord(t *testing.T, e *engine) record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(e.dir, "state.json"))
	require.NoError(t, err)
	var r record
	require.NoError(t, json.Unmarshal(raw, &r))
	return r
}

func writeFixtureRecord(t *testing.T, e *engine, r record) {
	t.Helper()
	s, err := lockState(context.Background(), e.dir)
	require.NoError(t, err)
	defer s.close()
	require.NoError(t, s.save(&r))
}

func preparedFixture(t *testing.T, e *engine, a *authorization) record {
	t.Helper()
	r := readFixtureRecord(t, e)
	hash, err := a.Command.digest()
	require.NoError(t, err)
	key := a.Command.Request.Key
	r.Floor = a.Owner
	r.LeaseUID = a.LeaseUID
	r.HighWater = &key
	r.Pending = &intent{Command: *a.Command, CommandSHA256: hash, JournalSHA256: a.JournalSHA256, Owner: a.Owner, Stage: prepared, Before: Observation{RequestedBytes: a.Command.BeforeBytes, ActualBytes: a.Command.BeforeBytes}, ReplayNonce: ""}
	return r
}

func TestRunnerOriginalExecutionDurabilityAndNoop(t *testing.T) {
	for _, noop := range []bool{false, true} {
		t.Run(fmt.Sprintf("noop=%v", noop), func(t *testing.T) {
			e, f, a := setupEngine(t)
			if noop {
				a.Command.Request.TargetVMMemoryBytes = a.Command.BeforeBytes
			}
			require.NoError(t, e.advance(context.Background()))
			r := readFixtureRecord(t, e)
			require.Nil(t, r.Pending)
			require.Equal(t, completed, r.Last.Kind)
			require.Equal(t, *a.Command, r.Last.Intent.Command)
			require.Equal(t, a.Owner, r.Floor)
			require.Equal(t, a.Command.Request.TargetVMMemoryBytes, r.Last.Actual.ActualBytes)
			wantWrites := 1
			if noop {
				wantWrites = 0
			}
			require.Equal(t, wantWrites, f.writes)
			// 新engine模拟进程恢复，原完成幂等读取；不得再次派发。
			restarted := *e
			require.NoError(t, restarted.advance(context.Background()))
			require.Equal(t, wantWrites, f.writes)
		})
	}
}

func TestRunnerLostReplyRetainsOriginalAcrossTakeover(t *testing.T) {
	e, f, a := setupEngine(t)
	f.apply, f.err = false, errors.New("lost reply")
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	r := readFixtureRecord(t, e)
	require.Equal(t, mayHaveSent, r.Pending.Stage)
	original := *r.Pending
	for range 2 {
		require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	}
	a.ReplayNonce = strings.Repeat("1", 32)
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	require.Equal(t, 1, f.writes, "possible send must never be replayed")
	a.Owner = journal.Owner{HolderID: "owner-three", Epoch: 3}
	a.Command, a.ReplayNonce = nil, ""
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	r = readFixtureRecord(t, e)
	require.Equal(t, a.Owner, r.Floor)
	require.Equal(t, original, *r.Pending, "takeover cannot rewrite old intent")
	// 模拟唯一原qom-set迟到完成。只读原目标可证该唯一写已执行，无需重发。
	f.actual = Observation{RequestedBytes: original.Command.Request.TargetVMMemoryBytes, ActualBytes: original.Command.Request.TargetVMMemoryBytes}
	require.NoError(t, e.advance(context.Background()))
	r = readFixtureRecord(t, e)
	require.Nil(t, r.Pending)
	require.Equal(t, original, r.Last.Intent)
	require.Equal(t, 1, f.writes)
	// 旧owner即使保留整个原命令也不能再降低floor/写内存。
	a.Owner, a.Command = original.Owner, &original.Command
	require.ErrorContains(t, e.advance(context.Background()), "stale")
	require.Equal(t, 1, f.writes)
}

func TestRunnerPreparedCrashNeedsExplicitUnchangedReplay(t *testing.T) {
	t.Run("original-replay", func(t *testing.T) {
		e, f, a := setupEngine(t)
		writeFixtureRecord(t, e, preparedFixture(t, e, a))
		require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
		require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
		require.Zero(t, f.writes)
		a.ReplayNonce = strings.Repeat("2", 32)
		require.NoError(t, e.advance(context.Background()))
		r := readFixtureRecord(t, e)
		require.Equal(t, a.ReplayNonce, r.Last.Intent.ReplayNonce)
		require.Equal(t, completed, r.Last.Kind)
		require.Equal(t, 1, f.writes)
	})
	for _, changed := range []string{"target", "nonce", "host", "journal", "owner", "baseline", "expired", "same-replay"} {
		t.Run(changed, func(t *testing.T) {
			e, f, a := setupEngine(t)
			r := preparedFixture(t, e, a)
			r.Pending.ReplayNonce = strings.Repeat("3", 32)
			writeFixtureRecord(t, e, r)
			require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
			a.ReplayNonce = strings.Repeat("4", 32)
			switch changed {
			case "target":
				a.Command.Request.TargetVMMemoryBytes -= a.Command.Identity.BlockBytes
			case "nonce":
				a.Command.Request.Key.OperationID.Low++
			case "host":
				a.Command.HostClaim.UID = "replacement"
			case "journal":
				a.JournalSHA256 = strings.Repeat("f", 64)
			case "owner":
				a.Owner = journal.Owner{HolderID: "new-owner", Epoch: 3}
			case "baseline":
				f.actual.ActualBytes -= a.Command.Identity.BlockBytes
			case "expired":
				e.now = func() time.Time { return time.UnixMilli(int64(a.Command.Request.DeadlineUnixMillis) + 1) }
			case "same-replay":
				a.ReplayNonce = r.Pending.ReplayNonce
			}
			require.Error(t, e.advance(context.Background()))
			require.Zero(t, f.writes)
		})
	}
}

func TestRunnerSecondAuthorityCheckAndDefinitePrewriteFailure(t *testing.T) {
	t.Run("authority-changes-after-prepare", func(t *testing.T) {
		e, f, a := setupEngine(t)
		calls := 0
		e.read = func(context.Context) (authorization, error) {
			calls++
			observed := *a
			if calls == 2 {
				observed.Dispatch = false
			}
			return observed, nil
		}
		require.ErrorContains(t, e.advance(context.Background()), "authority changed")
		require.Zero(t, f.writes)
		require.Equal(t, prepared, readFixtureRecord(t, e).Pending.Stage)
		require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
		require.Zero(t, f.writes)
	})
	t.Run("native-proof-before-qom-set", func(t *testing.T) {
		e, f, _ := setupEngine(t)
		f.err = &notSentError{errors.New("connect failed before mutation")}
		require.ErrorIs(t, e.advance(context.Background()), ErrNotSent)
		r := readFixtureRecord(t, e)
		require.Nil(t, r.Pending)
		require.Equal(t, notSent, r.Last.Kind)
		require.Zero(t, f.writes)
	})
}

func TestRunnerExclusiveLockCoversNativeWriteAndFloor(t *testing.T) {
	e, f, a := setupEngine(t)
	f.entered, f.release = make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() { first <- e.advance(context.Background()) }()
	<-f.entered
	second := *e
	newOwner := journal.Owner{HolderID: "owner-three", Epoch: 3}
	second.read = func(context.Context) (authorization, error) {
		return authorization{Owner: newOwner, LeaseUID: "lease-uid", Command: nil, JournalSHA256: "", Dispatch: false, ReplayNonce: "", Denied: nil}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, second.advance(ctx), context.DeadlineExceeded)
	require.Equal(t, a.Owner, readFixtureRecord(t, e).Floor)
	close(f.release)
	require.NoError(t, <-first)
	require.NoError(t, second.advance(context.Background()))
	require.Equal(t, newOwner, readFixtureRecord(t, e).Floor)
	require.ErrorContains(t, e.advance(context.Background()), "stale")
	require.Equal(t, 1, f.writes)
}

func TestRunnerRejectsLostOrReplacedStateAndExhaustion(t *testing.T) {
	for _, what := range []string{"missing", "symlink-file", "symlink-dir", "permissions", "new-runner", "new-node", "sequence-exhausted"} {
		t.Run(what, func(t *testing.T) {
			e, f, _ := setupEngine(t)
			switch what {
			case "missing":
				require.NoError(t, os.Remove(filepath.Join(e.dir, "state.json")))
			case "symlink-file":
				path := filepath.Join(e.dir, "state.json")
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink(path+".original", path))
			case "symlink-dir":
				path := filepath.Join(t.TempDir(), "link")
				require.NoError(t, os.Symlink(e.dir, path))
				e.dir = path
			case "permissions":
				require.NoError(t, os.Chmod(e.dir, 0o755))
			case "new-runner":
				e.id.RunnerPodUID = "new-pod-uid"
			case "new-node":
				e.id.NodeUID = "new-node-uid"
			case "sequence-exhausted":
				r := readFixtureRecord(t, e)
				r.Sequence = control.Uint64(math.MaxUint64)
				writeFixtureRecord(t, e, r)
			}
			require.Error(t, e.advance(context.Background()))
			require.Zero(t, f.writes)
		})
	}
}

func TestRunnerStrictRecordRejectsChangedOriginalAndDuplicateFields(t *testing.T) {
	e, _, a := setupEngine(t)
	r := preparedFixture(t, e, a)
	for _, what := range []string{"command-hash", "journal-hash", "nonce", "before", "identity", "owner", "hwm", "stage", "unknown-field", "duplicate-field", "missing-field", "null-field"} {
		t.Run(what, func(t *testing.T) {
			next := r.clone()
			switch what {
			case "command-hash":
				next.Pending.CommandSHA256 = strings.Repeat("f", 64)
			case "journal-hash":
				next.Pending.JournalSHA256 = "not-a-hash"
			case "nonce":
				next.Pending.ReplayNonce = "invalid"
			case "before":
				next.Pending.Before.ActualBytes -= next.Identity.BlockBytes
			case "identity":
				next.Identity.RunnerBootID = strings.Repeat("9", 32)
			case "owner":
				next.Pending.Owner.HolderID = "other-holder"
			case "hwm":
				next.HighWater.OperationID.Low++
			case "stage":
				next.Pending.Stage = "executed"
			}
			raw, err := json.Marshal(next)
			require.NoError(t, err)
			switch what {
			case "unknown-field":
				raw = append([]byte(`{"unknown":true,`), raw[1:]...)
			case "duplicate-field":
				raw = append([]byte(`{"protocol_version":1,`), raw[1:]...)
			case "missing-field":
				raw = []byte(strings.Replace(string(raw), `"pending":`+mustJSON(t, next.Pending)+`,`, "", 1))
			case "null-field":
				raw = []byte(strings.Replace(string(raw), `"sequence":"1"`, `"sequence":null`, 1))
			}
			var out record
			require.Error(t, json.Unmarshal(raw, &out))
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}
