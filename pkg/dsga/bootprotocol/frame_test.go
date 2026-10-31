package bootprotocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

type bootFragmentReader struct{ r io.Reader }

func (r bootFragmentReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.r.Read(p)
}

type bootCountWriter struct {
	calls   int
	shortAt int
	errAt   int
	buf     bytes.Buffer
}

func (w *bootCountWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.errAt {
		return 0, io.ErrClosedPipe
	}
	if w.calls == w.shortAt {
		return 0, nil
	}
	return w.buf.Write(p)
}

type bootPayloadTrap struct {
	header     *bytes.Reader
	extraReads int
}

func (r *bootPayloadTrap) Read(p []byte) (int, error) {
	if r.header.Len() == 0 {
		r.extraReads++
		return 0, io.ErrClosedPipe
	}
	return r.header.Read(p)
}

func TestInitialPrebootFramesRoundTrip(t *testing.T) {
	c, r := prebootFixture()
	var b bytes.Buffer
	if err := WriteInitialPrebootChallenge(&b, c); err != nil {
		t.Fatal(err)
	}
	if err := WriteInitialPrebootReceipt(&b, r); err != nil {
		t.Fatal(err)
	}
	got, e := ReadInitialPrebootChallenge(bootFragmentReader{&b})
	if e != nil || got != c {
		t.Fatal("fragment challenge", e)
	}
	receipt, e := ReadInitialPrebootReceipt(bootFragmentReader{&b})
	if e != nil || receipt != r || receipt.MatchChallenge(c) != nil {
		t.Fatal("fragment receipt", e)
	}
	if b.Len() != 0 {
		t.Fatal("read beyond or left original frame")
	}
}

func TestInitialPrebootFramesRejectHeaderBeforePayload(t *testing.T) {
	c, _ := prebootFixture()
	var b bytes.Buffer
	if err := WriteInitialPrebootChallenge(&b, c); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte){
		"magic":        func(h []byte) { h[0] = 'X' },
		"version":      func(h []byte) { binary.BigEndian.PutUint16(h[4:6], 2) },
		"foreign-kind": func(h []byte) { binary.BigEndian.PutUint16(h[6:8], receiptFrameKind) },
		"unknown-kind": func(h []byte) { binary.BigEndian.PutUint16(h[6:8], 65535) },
		"zero":         func(h []byte) { binary.BigEndian.PutUint32(h[8:12], 0) },
		"oversize":     func(h []byte) { binary.BigEndian.PutUint32(h[8:12], MaxFrameBytes+1) },
		"overflow":     func(h []byte) { binary.BigEndian.PutUint32(h[8:12], ^uint32(0)) },
	} {
		t.Run(name, func(t *testing.T) {
			h := bytes.Clone(b.Bytes()[:frameHeaderBytes])
			mutate(h)
			r := &bootPayloadTrap{header: bytes.NewReader(h)}
			if _, e := ReadInitialPrebootChallenge(r); e == nil || r.extraReads != 0 {
				t.Fatal("bad header reached payload", e, r.extraReads)
			}
		})
	}
}

func TestInitialPrebootFramesPartialNeverRetry(t *testing.T) {
	c, _ := prebootFixture()
	for _, at := range []int{1, 2} {
		t.Run(string(rune('0'+at)), func(t *testing.T) {
			w := &bootCountWriter{shortAt: at}
			if e := WriteInitialPrebootChallenge(w, c); !errors.Is(e, io.ErrShortWrite) || w.calls != at {
				t.Fatal("short write retried", e, w.calls)
			}
			w = &bootCountWriter{errAt: at}
			if e := WriteInitialPrebootChallenge(w, c); !errors.Is(e, io.ErrClosedPipe) || w.calls != at {
				t.Fatal("failed write retried", e, w.calls)
			}
		})
	}
	bad := c
	bad.Nonce = ""
	w := &bootCountWriter{}
	if WriteInitialPrebootChallenge(w, bad) == nil || w.calls != 0 {
		t.Fatal("invalid input wrote")
	}
	var b bytes.Buffer
	if err := WriteInitialPrebootChallenge(&b, c); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, frameHeaderBytes - 1, frameHeaderBytes, frameHeaderBytes + 1, b.Len() - 1} {
		if _, e := ReadInitialPrebootChallenge(bytes.NewReader(b.Bytes()[:size])); e == nil {
			t.Fatal("truncated frame accepted", size)
		}
	}
}

func TestInitialPrebootFramesStrictBodyAndKinds(t *testing.T) {
	c, r := prebootFixture()
	var b bytes.Buffer
	if e := writeFrame(&b, challengeFrameKind, map[string]any{"verified": true}); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialPrebootChallenge(&b); e == nil {
		t.Fatal("unknown wire body accepted")
	}
	b.Reset()
	bad := c
	bad.SourceSHA256 = ""
	if e := writeFrame(&b, challengeFrameKind, bad); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialPrebootChallenge(&b); e == nil {
		t.Fatal("invalid body accepted")
	}
	b.Reset()
	if e := WriteInitialPrebootReceipt(&b, r); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialPrebootChallenge(&b); e == nil {
		t.Fatal("receipt borrowed as challenge")
	}
	b.Reset()
	if e := WriteInitialPrebootChallenge(&b, c); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialPrebootReceipt(&b); e == nil {
		t.Fatal("challenge borrowed as receipt")
	}
	w := &bootCountWriter{}
	if e := writeFrame(w, challengeFrameKind, strings.Repeat("x", MaxFrameBytes)); e == nil || w.calls != 0 {
		t.Fatal("oversized write reached endpoint")
	}
	if WriteInitialPrebootChallenge(nil, c) == nil {
		t.Fatal("nil writer")
	}
	if _, e := ReadInitialPrebootReceipt(nil); e == nil {
		t.Fatal("nil reader")
	}
}
