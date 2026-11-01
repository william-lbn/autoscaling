package bootprotocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

func memoryOriginFixture(t *testing.T) NormalMemoryOriginHostReference {
	t.Helper()
	c, m, end := movableFixture()
	c.Preboot.Challenge.Identity.BlockBytes = 8 << 20
	m.Challenge = c
	m.PreparedReceiptSHA256, _ = c.Preboot.SHA256()
	end.Challenge = c
	end.MovableReceiptSHA256, _ = m.SHA256()
	base, max := uint64(c.Preboot.Challenge.OriginalBase), uint64(c.Preboot.Challenge.Maximum)
	offset := uint64(c.TargetBytes) - base
	j := func(v any) []byte {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	q := NormalMemoryQMPInputs{
		Version:     []byte(" { \"qemu\":{\"major\":8,\"minor\":1,\"micro\":5}} \n"),
		Machine:     []byte(fmt.Sprintf("{\"size\":%d,\"max-size\":%d,\"slots\":1}", base, max)),
		Devices:     []byte(fmt.Sprintf("[{\"type\":\"virtio-mem\",\"data\":{\"id\":\"vm0\",\"memdev\":\"/objects/vmem0\",\"requested-size\":%d,\"size\":%d,\"max-size\":%d,\"block-size\":8388608,\"node\":0,\"memaddr\":4294967296}}]", offset, offset, max-base)),
		Summary:     []byte(fmt.Sprintf("{\"base-memory\":%d,\"plugged-memory\":%d}", base, offset)),
		Backends:    []byte(fmt.Sprintf("[{\"id\":\"pc.ram\",\"size\":%d,\"host-nodes\":[],\"policy\":\"default\"},{\"id\":\"vmem0\",\"size\":%d,\"host-nodes\":[],\"policy\":\"default\"}]", base, max-base)),
		BackendSize: j(max - base),
	}
	h := strings.Repeat("ab", 32)
	out := NormalMemoryOriginHostReference{ProtocolVersion: 1, Scope: NormalMemoryOriginScope, ProfileID: NormalMemoryOriginProfileID, Prepared: c.Preboot, Movable: m, Completion: end, Process: NormalMemoryQEMUReference{PID: 1234, StartTicks: 99, ExecutableDevice: 1, ExecutableInode: 2, ExecutableSHA256: h, CommandSHA256: h, LaunchNonce: strings.Repeat("12", 16)}, MaximumVCPUs: 2, HardwareSHA256: h, QMP: q}
	if e := out.Validate(); e != nil {
		t.Fatal(e)
	}
	return out
}
func TestNormalMemoryOriginWirePreservesActualFramesAndBindings(t *testing.T) {
	r := memoryOriginFixture(t)
	var b bytes.Buffer
	if e := WriteNormalMemoryOriginHostReference(&b, r); e != nil {
		t.Fatal(e)
	}
	if b.Len() > MaxFrameBytes+frameHeaderBytes {
		t.Fatal("actual encoded frame exceeds original bound")
	}
	got, e := ReadNormalMemoryOriginHostReference(&b)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(got.QMP.Version, r.QMP.Version) {
		t.Fatal("raw QMP whitespace was recoded")
	}
	a, _ := r.SHA256()
	z, _ := got.SHA256()
	if a != z {
		t.Fatal("reference changed")
	}
	ack := NormalMemoryOriginGuestAck{1, NormalMemoryOriginAckScope, a, strings.Repeat("cd", 32), r.Prepared.GuestBootID, r.Movable.RootInstanceNonce}
	if e = WriteNormalMemoryOriginGuestAck(&b, ack); e != nil {
		t.Fatal(e)
	}
	back, e := ReadNormalMemoryOriginGuestAck(&b)
	if e != nil || back.MatchReference(r) != nil {
		t.Fatalf("Guest binding: %v", e)
	}
	ack.GuestBootID = "12345678-abcd-1234-abcd-123456789abd"
	if ack.MatchReference(r) == nil {
		t.Fatal("another Guest adopted original source")
	}
	t.Logf("model_reference_payload_bytes=%d", func() int { x, _ := json.Marshal(r); return len(x) }())
}
func TestNormalMemoryOriginGeometryRejectsScalarAndBackendSubstitutes(t *testing.T) {
	r := memoryOriginFixture(t)
	id := r.Prepared.Challenge.Identity
	base := r.Prepared.Challenge.OriginalBase
	g, e := DecodeNormalMemoryGeometry(id, base, r.QMP)
	if e != nil {
		t.Fatal(e)
	}
	if g.MaximumBytes != id.MaximumBytes || g.BaseBytes+g.BackendBytes != g.MaximumBytes || g.MaximumBytes == g.ActualBytes {
		t.Fatal("maximum used current online/requested")
	}
	cases := map[string]func(*NormalMemoryQMPInputs){
		"current-as-machine-maximum": func(q *NormalMemoryQMPInputs) {
			q.Machine = []byte(fmt.Sprintf("{\"size\":%d,\"max-size\":%d,\"slots\":1}", base, g.ActualBytes))
		},
		"missing-pcram": func(q *NormalMemoryQMPInputs) { q.Backends = []byte("[]") },
		"unknown-backend": func(q *NormalMemoryQMPInputs) {
			q.Backends = bytes.Replace(q.Backends, []byte("pc.ram"), []byte("foreign"), 1)
		},
		"duplicate-backend": func(q *NormalMemoryQMPInputs) {
			q.Backends = bytes.Replace(q.Backends, []byte("pc.ram"), []byte("vmem0"), 1)
		},
		"backend-size-drift": func(q *NormalMemoryQMPInputs) { q.BackendSize = []byte("1") },
		"null-machine":       func(q *NormalMemoryQMPInputs) { q.Machine = []byte("null") },
		"duplicate-qmp-key": func(q *NormalMemoryQMPInputs) {
			q.Summary = []byte("{\"base-memory\":1,\"base-memory\":2,\"plugged-memory\":3}")
		},
		"second-memory-device": func(q *NormalMemoryQMPInputs) { q.Devices = []byte("[{},{}]") },
		"wrong-qemu":           func(q *NormalMemoryQMPInputs) { q.Version = []byte("{\"qemu\":{\"major\":8,\"minor\":2,\"micro\":0}}") },
		"oversize-raw": func(q *NormalMemoryQMPInputs) {
			q.Version = append(bytes.Repeat([]byte(" "), MaxFrameBytes), q.Version...)
		},
	}
	for n, f := range cases {
		t.Run(n, func(t *testing.T) {
			q := r.QMP
			f(&q)
			if _, e := DecodeNormalMemoryGeometry(id, base, q); e == nil {
				t.Fatal("malformed maximum adopted")
			}
		})
	}
}
func TestNormalMemoryOriginStrictScopeAndJSON(t *testing.T) {
	original := memoryOriginFixture(t)
	cases := map[string]func(*NormalMemoryOriginHostReference){
		"old-profile":               func(r *NormalMemoryOriginHostReference) { r.ProfileID = "neon-linux-6.12.26-dsga-normal-v1" },
		"old-version":               func(r *NormalMemoryOriginHostReference) { r.ProtocolVersion = 0 },
		"future-version":            func(r *NormalMemoryOriginHostReference) { r.ProtocolVersion = 2 },
		"missing-process":           func(r *NormalMemoryOriginHostReference) { r.Process.StartTicks = 0 },
		"wrong-completion":          func(r *NormalMemoryOriginHostReference) { r.Completion.MovableReceiptSHA256 = strings.Repeat("00", 32) },
		"wrong-historical-prepared": func(r *NormalMemoryOriginHostReference) { r.Prepared.RootInstanceNonce = strings.Repeat("cd", 32) },
		"cpu-unbounded":             func(r *NormalMemoryOriginHostReference) { r.MaximumVCPUs = 65 },
	}
	for n, f := range cases {
		t.Run(n, func(t *testing.T) {
			r := original
			f(&r)
			if r.Validate() == nil {
				t.Fatal("foreign source accepted")
			}
		})
	}
	raw, _ := json.Marshal(original)
	for _, bad := range [][]byte{
		append([]byte("{\"protocol_version\":1,"), raw[1:]...),
		append(append([]byte(nil), raw[:len(raw)-1]...), []byte(",\"trusted\":true}")...),
		bytes.Replace(raw, []byte("\"process\":{"), []byte("\"process\":{\"unknown\":1,"), 1),
		bytes.Replace(raw, []byte("\"pid\":"), []byte("\"PID\":"), 1),
		bytes.Replace(raw, []byte("\"version\":"), []byte("\"Version\":"), 1),
	} {
		var v NormalMemoryOriginHostReference
		if json.Unmarshal(bad, &v) == nil {
			t.Fatal("ambiguous/unknown wire accepted")
		}
	}
}
func TestNormalMemoryOriginFramesFailBeforePayloadAndNeverRetry(t *testing.T) {
	r := memoryOriginFixture(t)
	var good bytes.Buffer
	if e := WriteNormalMemoryOriginHostReference(&good, r); e != nil {
		t.Fatal(e)
	}
	raw := good.Bytes()
	for _, off := range []int{0, 4, 6, 8} {
		b := bytes.Clone(raw[:frameHeaderBytes])
		b[off] ^= 0xff
		if off == 8 {
			binary.BigEndian.PutUint32(b[8:], MaxFrameBytes+1)
		}
		trap := &bootPayloadTrap{header: bytes.NewReader(b)}
		if _, e := ReadNormalMemoryOriginHostReference(trap); e == nil {
			t.Fatal("bad header accepted")
		}
		if trap.extraReads != 0 {
			t.Fatal("payload consumed before kind/version/length rejection")
		}
	}
	w := &bootCountWriter{shortAt: 1}
	if e := WriteNormalMemoryOriginHostReference(w, r); e != io.ErrShortWrite {
		t.Fatalf("short header: %v", e)
	}
	if w.calls != 1 {
		t.Fatal("short write was retried")
	}
	for _, n := range []int{0, frameHeaderBytes - 1, len(raw) - 1} {
		if _, e := ReadNormalMemoryOriginHostReference(bytes.NewReader(raw[:n])); e == nil {
			t.Fatal("truncation accepted")
		}
	}
}

// 数字数组可被Go默认[]byte解码，但原线协议声明base64字符串，不能悄悄兼容另一形状。
func TestNormalMemoryOriginRejectsAlternateByteArrayEncoding(t *testing.T) {
	r := memoryOriginFixture(t)
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	canonical, e := json.Marshal(r.QMP.Version)
	if e != nil {
		t.Fatal(e)
	}
	ints := make([]int, len(r.QMP.Version))
	for i, v := range r.QMP.Version {
		ints[i] = int(v)
	}
	array, e := json.Marshal(ints)
	if e != nil {
		t.Fatal(e)
	}
	bad := bytes.Replace(raw, canonical, array, 1)
	if bytes.Equal(bad, raw) {
		t.Fatal("test failed to replace actual version representation")
	}
	var decoded NormalMemoryOriginHostReference
	if json.Unmarshal(bad, &decoded) == nil {
		t.Fatal("numeric array silently replaced original base64 bytes")
	}
}
