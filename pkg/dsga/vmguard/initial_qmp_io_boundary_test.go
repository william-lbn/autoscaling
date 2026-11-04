package vmguard

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// initialQMPBoundaryConn observes one command without contacting a live QEMU.
// Short reads deliberately split events and replies across many Read calls.
type initialQMPBoundaryConn struct {
	reads, writes int
	deadlines     []time.Time
	deadlineErr   error
	onDeadline    func()
	writeErr      error
	partial       bool
	missingReply  bool
	events        bool
	oneByteReads  bool
	response      *bytes.Reader
}

func (c *initialQMPBoundaryConn) Read(p []byte) (int, error) {
	c.reads++
	if c.response == nil {
		return 0, io.EOF
	}
	if c.oneByteReads && len(p) > 1 {
		p = p[:1]
	}
	return c.response.Read(p)
}
func (c *initialQMPBoundaryConn) Write(p []byte) (int, error) {
	c.writes++
	if c.partial {
		return len(p) / 2, io.ErrShortWrite
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if !c.missingReply {
		var command struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(p, &command); err != nil {
			return 0, err
		}
		prefix := []byte(nil)
		if c.events {
			prefix = []byte("{\"event\":\"RESUME\"}\n{\"event\":\"STOP\"}\n")
		}
		reply, err := json.Marshal(struct {
			Return map[string]any  `json:"return"`
			ID     json.RawMessage `json:"id"`
		}{Return: map[string]any{}, ID: command.ID})
		if err != nil {
			return 0, err
		}
		c.response = bytes.NewReader(append(append(prefix, reply...), '\n'))
	}
	return len(p), nil
}
func (c *initialQMPBoundaryConn) Close() error { return nil }
func (c *initialQMPBoundaryConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "initial-qmp-boundary-local", Net: "unix"}
}
func (c *initialQMPBoundaryConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "initial-qmp-boundary-peer", Net: "unix"}
}
func (c *initialQMPBoundaryConn) SetDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if c.onDeadline != nil {
		c.onDeadline()
	}
	return c.deadlineErr
}
func (c *initialQMPBoundaryConn) SetReadDeadline(time.Time) error {
	return errors.New("unexpected independent read deadline")
}
func (c *initialQMPBoundaryConn) SetWriteDeadline(time.Time) error {
	return errors.New("unexpected independent write deadline")
}

func initialQMPBoundaryConnection(c *initialQMPBoundaryConn) *qmpConnection {
	return &qmpConnection{conn: c, read: bufio.NewReaderSize(c, qmpFrameLimit+1)}
}
func initialQMPBoundaryRequireUnknown(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("uncertain command unexpectedly succeeded")
	}
	var unsent *notSentError
	if errors.As(err, &unsent) {
		t.Fatal("I/O failure was incorrectly classified not-sent")
	}
}

func TestInitialQMPIOBoundaryPreservesParentAndNoReplay(t *testing.T) {
	t.Run("expired_parent_before_any_IO", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		c := &initialQMPBoundaryConn{}
		_, err := initialQMPCommand(withInitialQMPIO(ctx, q), q, initialQMPBoundaryConnection(c), "qom-set", map[string]any{"value": 64})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired parent was not retained: %v", err)
		}
		if len(c.deadlines) != 0 || c.writes != 0 {
			t.Fatal("expired context reached setter or write")
		}
	})
	t.Run("cancelled_parent_before_any_IO", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		c := &initialQMPBoundaryConn{}
		_, err := initialQMPCommand(withInitialQMPIO(ctx, q), q, initialQMPBoundaryConnection(c), "qom-set", nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation was not retained: %v", err)
		}
		if len(c.deadlines) != 0 || c.writes != 0 {
			t.Fatal("cancelled context reached setter or write")
		}
	})
	t.Run("setter_failure_is_unknown_without_write", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		failed := errors.New("fixed deadline setter failure")
		c := &initialQMPBoundaryConn{deadlineErr: failed}
		_, err := initialQMPCommand(withInitialQMPIO(context.Background(), q), q, initialQMPBoundaryConnection(c), "qom-set", nil)
		initialQMPBoundaryRequireUnknown(t, err)
		if !errors.Is(err, failed) || len(c.deadlines) != 1 || c.writes != 0 {
			t.Fatal("setter failure lost its cause or attempted a write")
		}
	})
	t.Run("parent_cancelled_inside_setter_does_not_write", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := &initialQMPBoundaryConn{onDeadline: cancel}
		_, err := initialQMPCommand(withInitialQMPIO(ctx, q), q, initialQMPBoundaryConnection(c), "qom-set", nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("setter-time cancellation lost: %v", err)
		}
		initialQMPBoundaryRequireUnknown(t, err)
		if len(c.deadlines) != 1 || c.writes != 0 {
			t.Fatal("cancelled parent reached command write after setter")
		}
	})
	t.Run("partial_write_is_not_repeated", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		c := &initialQMPBoundaryConn{partial: true}
		_, err := initialQMPCommand(withInitialQMPIO(context.Background(), q), q, initialQMPBoundaryConnection(c), "qom-set", nil)
		initialQMPBoundaryRequireUnknown(t, err)
		if c.writes != 1 || len(c.deadlines) != 1 {
			t.Fatal("partial command write or deadline was repeated")
		}
	})
	t.Run("missing_reply_is_not_repeated", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		c := &initialQMPBoundaryConn{missingReply: true}
		_, err := initialQMPCommand(withInitialQMPIO(context.Background(), q), q, initialQMPBoundaryConnection(c), "qom-set", nil)
		initialQMPBoundaryRequireUnknown(t, err)
		if c.writes != 1 || len(c.deadlines) != 1 {
			t.Fatal("command or deadline was repeated after reply loss")
		}
	})
	t.Run("events_and_partial_reads_share_one_parent_bounded_window", func(t *testing.T) {
		q := &nativeQMP{timeout: time.Second}
		deadline := time.Now().Add(250 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		marked := withInitialQMPIO(ctx, q)
		got, ok := marked.Deadline()
		if !ok || !got.Equal(deadline) {
			t.Fatal("marker changed the parent deadline")
		}
		c := &initialQMPBoundaryConn{events: true, oneByteReads: true}
		_, err := initialQMPCommand(marked, q, initialQMPBoundaryConnection(c), "qom-set", nil)
		if err != nil {
			t.Fatal(err)
		}
		if c.writes != 1 || c.reads <= 2 || len(c.deadlines) != 1 {
			t.Fatal("event or partial-read frames renewed the operation")
		}
		if !c.deadlines[0].Equal(deadline) {
			t.Fatal("operation extended its earlier parent deadline")
		}
	})
	for _, otherPointer := range []bool{false, true} {
		name := "unmarked_ordinary_uses_original_execute"
		if otherPointer {
			name = "marker_for_other_qmp_is_not_authority"
		}
		t.Run(name, func(t *testing.T) {
			q := &nativeQMP{timeout: time.Second}
			ctx := context.Background()
			if otherPointer {
				ctx = withInitialQMPIO(ctx, &nativeQMP{timeout: time.Second})
			}
			c := &initialQMPBoundaryConn{}
			_, err := initialQMPCommand(ctx, q, initialQMPBoundaryConnection(c), "qom-set", nil)
			if err != nil {
				t.Fatal(err)
			}
			if c.writes != 1 || len(c.deadlines) != 0 {
				t.Fatal("ordinary or foreign-marked QMP changed deadline semantics")
			}
		})
	}
}
