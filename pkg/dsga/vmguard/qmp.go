package vmguard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const qmpFrameLimit = 64 * 1024

func errOrShortWrite(err error) error {
	if err != nil {
		return err
	}
	return io.ErrShortWrite
}

// QEMUUUID从原Runner随机boot派生，跨连接保持不变。不能用可重用Pod名，也
// 不能在读到另一个QEMU UUID后修改本地绑定。生产启动参数必须设置同一-uuid。
func (id Identity) QEMUUUID() (string, error) {
	if err := id.Validate(); err != nil {
		return "", err
	}
	s := id.RunnerBootID
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], nil
}

// notSentError只用于“qom-set编码/写入尚未开始”的本地失败；一旦开始Write，
// 包括短写、没有回包、拒绝回复、ctx到期都按可能已发送处理，不降级此类型。
type notSentError struct{ err error }

func (e *notSentError) Error() string {
	return "private QMP failed before mutation write: " + e.err.Error()
}
func (e *notSentError) Unwrap() error { return e.err }

type nativeQMP struct {
	id      Identity
	path    string
	timeout time.Duration
}

type qmpConnection struct {
	conn net.Conn
	read *bufio.Reader
	seq  uint64
	stop func() bool
}

func (c *qmpConnection) close() {
	if c.stop != nil {
		c.stop()
	}
	c.conn.Close()
}

func (c *qmpConnection) frame() (map[string]json.RawMessage, error) {
	// 每帧和事件数分别有界，不能让无期限事件流或缺换行耗尽内存。deadline
	// 与ctx关闭作用于同一conn；没有后台Run goroutine在释放本VM锁后继续写。
	raw, err := c.read.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(raw) > qmpFrameLimit || control.ValidateJSON(raw, qmpFrameLimit, 4096) != nil {
		return nil, fmt.Errorf("invalid or oversized private QMP frame")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("private QMP frame must be an object")
	}
	return fields, nil
}

func (c *qmpConnection) execute(name string, args any) (json.RawMessage, error) {
	c.seq++
	id := fmt.Sprintf("dsga-%d", c.seq)
	command := struct {
		Execute   string `json:"execute"`
		Arguments any    `json:"arguments,omitempty"`
		ID        string `json:"id"`
	}{Execute: name, Arguments: args, ID: id}
	raw, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	if n, err := c.conn.Write(raw); err != nil || n != len(raw) {
		return nil, fmt.Errorf("private QMP write unconfirmed: %w", errOrShortWrite(err))
	}
	for eventCount := 0; eventCount <= 64; eventCount++ {
		f, err := c.frame()
		if err != nil {
			return nil, err
		}
		if _, event := f["event"]; event {
			if f["id"] != nil || f["return"] != nil || f["error"] != nil {
				return nil, fmt.Errorf("private QMP event also contains a response")
			}
			continue
		}
		var got string
		if json.Unmarshal(f["id"], &got) != nil || got != id || (f["return"] == nil) == (f["error"] == nil) {
			return nil, fmt.Errorf("private QMP response ID or result is ambiguous")
		}
		if f["error"] != nil {
			// 错误文本可能含QEMU路径，不回显任意响应；普通QMP错误亦不证明
			// 旧动作NoEffect，外层保留may_have_sent再走原目标读回。
			return nil, fmt.Errorf("private QMP command %s returned error", name)
		}
		return f["return"], nil
	}
	return nil, fmt.Errorf("private QMP event bound exceeded")
}

func (q *nativeQMP) connect(ctx context.Context) (*qmpConnection, error) {
	if q.id.Validate() != nil || !filepath.IsAbs(q.path) || filepath.Clean(q.path) != q.path || q.timeout < 100*time.Millisecond || q.timeout > 10*time.Second {
		return nil, fmt.Errorf("invalid fixed private QMP binding")
	}
	info, err := os.Lstat(q.path)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return nil, fmt.Errorf("original private QMP socket unavailable")
	}
	deadline := time.Now().Add(q.timeout)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dialer := net.Dialer{Deadline: deadline}
	conn, err := dialer.DialContext(ctx, "unix", q.path)
	if err != nil {
		return nil, err
	}
	c := &qmpConnection{conn: conn, read: bufio.NewReaderSize(conn, qmpFrameLimit+1)}
	if err := conn.SetDeadline(deadline); err != nil {
		c.close()
		return nil, err
	}
	c.stop = context.AfterFunc(ctx, func() { conn.Close() })
	ok := false
	defer func() {
		if !ok {
			c.close()
		}
	}()
	greeting, err := c.frame()
	if err != nil || greeting["QMP"] == nil || greeting["return"] != nil || greeting["error"] != nil {
		return nil, fmt.Errorf("private QMP greeting missing or invalid")
	}
	if _, err := c.execute("qmp_capabilities", nil); err != nil {
		return nil, err
	}
	raw, err := c.execute("query-uuid", nil)
	if err != nil {
		return nil, err
	}
	var identity struct {
		UUID string `json:"UUID"`
	}
	expected, _ := q.id.QEMUUUID()
	if json.Unmarshal(raw, &identity) != nil || identity.UUID != expected {
		return nil, fmt.Errorf("private QMP belongs to another original QEMU boot")
	}
	ok = true
	return c, nil
}

func (q *nativeQMP) read(c *qmpConnection) (Observation, error) {
	raw, err := c.execute("query-memory-devices", nil)
	if err != nil {
		return Observation{}, err
	}
	var devices []struct {
		Type string `json:"type"`
		Data struct {
			ID        string  `json:"id"`
			Memdev    string  `json:"memdev"`
			Requested *uint64 `json:"requested-size"`
			Size      *uint64 `json:"size"`
			Maximum   *uint64 `json:"max-size"`
			Block     *uint64 `json:"block-size"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &devices) != nil || string(raw) == "null" {
		return Observation{}, fmt.Errorf("private QMP memory devices malformed")
	}
	o := Observation{RequestedBytes: q.id.MinimumBytes, ActualBytes: q.id.MinimumBytes}
	if q.id.MinimumBytes == q.id.MaximumBytes {
		if len(devices) != 0 {
			return Observation{}, fmt.Errorf("fixed-size QEMU unexpectedly has memory devices")
		}
	} else {
		if len(devices) != 1 {
			return Observation{}, fmt.Errorf("original single virtio-mem device missing or replaced")
		}
		d := devices[0]
		if d.Type != "virtio-mem" || d.Data.ID != "vm0" || d.Data.Memdev != "/objects/vmem0" || d.Data.Requested == nil || d.Data.Size == nil || d.Data.Maximum == nil || d.Data.Block == nil || *d.Data.Maximum != uint64(q.id.MaximumBytes-q.id.MinimumBytes) || *d.Data.Block != uint64(q.id.BlockBytes) || *d.Data.Requested > *d.Data.Maximum || *d.Data.Size > *d.Data.Maximum {
			return Observation{}, fmt.Errorf("original virtio-mem geometry differs")
		}
		o.RequestedBytes += control.Uint64(*d.Data.Requested)
		o.ActualBytes += control.Uint64(*d.Data.Size)
	}
	raw, err = c.execute("query-memory-size-summary", nil)
	if err != nil {
		return Observation{}, err
	}
	var summary struct {
		Base    *uint64 `json:"base-memory"`
		Plugged *uint64 `json:"plugged-memory"`
	}
	if json.Unmarshal(raw, &summary) != nil || summary.Base == nil || summary.Plugged == nil || *summary.Base != uint64(q.id.MinimumBytes) || *summary.Plugged != uint64(o.ActualBytes-q.id.MinimumBytes) || o.validate(q.id) != nil {
		return Observation{}, fmt.Errorf("private QMP device and actual memory summary differ")
	}
	return o, nil
}

func (q *nativeQMP) Read(ctx context.Context) (Observation, error) {
	c, err := q.connect(ctx)
	if err != nil {
		return Observation{}, err
	}
	defer c.close()
	return q.read(c)
}

func (q *nativeQMP) Set(ctx context.Context, target control.Uint64) error {
	if !q.id.memoryValid(target) || q.id.MinimumBytes == q.id.MaximumBytes {
		return &notSentError{fmt.Errorf("target outside original virtio-mem geometry")}
	}
	c, err := q.connect(ctx)
	if err != nil {
		return &notSentError{err}
	}
	defer c.close()
	if _, err := q.read(c); err != nil {
		return &notSentError{err}
	}
	_, err = c.execute("qom-set", struct {
		Path     string `json:"path"`
		Property string `json:"property"`
		Value    uint64 `json:"value"`
	}{Path: "/machine/peripheral/vm0", Property: "requested-size", Value: uint64(target - q.id.MinimumBytes)})
	return err
}
