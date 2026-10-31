package bootprotocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

const frameHeaderBytes = 12
const challengeFrameKind = 1
const receiptFrameKind = 2

// 固定魔数/版本/消息类型+长度。未知类型在读取payload前拒绝；不接受可配置路径或
// 认证开关。调用者必须给真实固定端点设置期限，部分写/丢回复不能自动重发原操作。
func writeFrame(w io.Writer, kind uint16, value any) error {
	if w == nil {
		return fmt.Errorf("original boot writer required")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > MaxFrameBytes {
		return fmt.Errorf("boot frame exceeds bound")
	}
	var h [frameHeaderBytes]byte
	copy(h[:4], "DSGB")
	binary.BigEndian.PutUint16(h[4:6], 1)
	binary.BigEndian.PutUint16(h[6:8], kind)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(raw)))
	// 有意不重试短写，避免把同一语义操作隐式重放成新请求。
	if n, e := w.Write(h[:]); e != nil {
		return e
	} else if n != len(h) {
		return io.ErrShortWrite
	}
	if n, e := w.Write(raw); e != nil {
		return e
	} else if n != len(raw) {
		return io.ErrShortWrite
	}
	return nil
}

func readFrame(r io.Reader, kind uint16, into any) error {
	if r == nil {
		return fmt.Errorf("original boot reader required")
	}
	var h [frameHeaderBytes]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(h[8:12])
	if string(h[:4]) != "DSGB" || binary.BigEndian.Uint16(h[4:6]) != 1 || binary.BigEndian.Uint16(h[6:8]) != kind || size == 0 || size > MaxFrameBytes {
		return fmt.Errorf("unsupported original boot frame header")
	}
	raw := make([]byte, int(size))
	if _, err := io.ReadFull(r, raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}

func WriteInitialPrebootChallenge(w io.Writer, c InitialPrebootChallenge) error {
	if err := c.Validate(); err != nil {
		return err
	}
	return writeFrame(w, challengeFrameKind, c)
}
func ReadInitialPrebootChallenge(r io.Reader) (InitialPrebootChallenge, error) {
	var c InitialPrebootChallenge
	if err := readFrame(r, challengeFrameKind, &c); err != nil {
		return InitialPrebootChallenge{}, err
	}
	if err := c.Validate(); err != nil {
		return InitialPrebootChallenge{}, err
	}
	return c, nil
}
func WriteInitialPrebootReceipt(w io.Writer, r InitialPrebootReceipt) error {
	if err := r.Validate(); err != nil {
		return err
	}
	return writeFrame(w, receiptFrameKind, r)
}
func ReadInitialPrebootReceipt(r io.Reader) (InitialPrebootReceipt, error) {
	var c InitialPrebootReceipt
	if err := readFrame(r, receiptFrameKind, &c); err != nil {
		return InitialPrebootReceipt{}, err
	}
	if err := c.Validate(); err != nil {
		return InitialPrebootReceipt{}, err
	}
	return c, nil
}
