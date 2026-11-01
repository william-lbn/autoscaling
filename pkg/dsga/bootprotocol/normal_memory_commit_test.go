package bootprotocol

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func memoryCommitFixture(t *testing.T) (NormalMemoryOriginHostReference, NormalMemoryOriginGuestAck, NormalMemoryOriginHostCommit) {
	t.Helper()
	h := memoryOriginFixture(t)
	s, e := h.SHA256()
	if e != nil {
		t.Fatal(e)
	}
	a := NormalMemoryOriginGuestAck{1, NormalMemoryOriginAckScope, s, strings.Repeat("cd", 32), h.Prepared.GuestBootID, h.Movable.RootInstanceNonce}
	c, e := NewNormalMemoryOriginHostCommit(h, a)
	if e != nil {
		t.Fatal(e)
	}
	return h, a, c
}
func TestNormalMemoryCommit2KeepsOriginalDataABIAndExactRoundtrip(t *testing.T) {
	h, a, c := memoryCommitFixture(t)
	beforeH, _ := json.Marshal(h)
	beforeA, _ := json.Marshal(a)
	var b bytes.Buffer
	if e := WriteNormalMemoryOriginHostCommit(&b, c); e != nil {
		t.Fatal(e)
	}
	got, e := ReadNormalMemoryOriginHostCommit(&b)
	if e != nil || got != c || got.MatchReferenceAndAck(h, a) != nil {
		t.Fatal("commit binding", e)
	}
	if h.ProtocolVersion != 1 || a.ProtocolVersion != 1 {
		t.Fatal("old data ABI changed")
	}
	afterH, _ := json.Marshal(h)
	afterA, _ := json.Marshal(a)
	if !bytes.Equal(beforeH, afterH) || !bytes.Equal(beforeA, afterA) {
		t.Fatal("commit recoded original6/7")
	}
}
func TestNormalMemoryCommit2RejectsDifferentOriginalHistory(t *testing.T) {
	h, a, c := memoryCommitFixture(t)
	for name, mutate := range map[string]func(*NormalMemoryOriginHostCommit){"nonce": func(x *NormalMemoryOriginHostCommit) { x.OriginalNonce = strings.Repeat("12", 32) }, "stage": func(x *NormalMemoryOriginHostCommit) { x.HostStageSHA256 = strings.Repeat("12", 32) }, "reference": func(x *NormalMemoryOriginHostCommit) { x.HostReferenceSHA256 = strings.Repeat("12", 32) }, "ack": func(x *NormalMemoryOriginHostCommit) { x.GuestAckSHA256 = strings.Repeat("12", 32) }, "root": func(x *NormalMemoryOriginHostCommit) { x.RootInstanceNonce = strings.Repeat("12", 32) }, "boot": func(x *NormalMemoryOriginHostCommit) { x.GuestBootID = "12345678-abcd-1234-abcd-123456789abd" }, "old-version": func(x *NormalMemoryOriginHostCommit) { x.ProtocolVersion = 1 }, "unknown-version": func(x *NormalMemoryOriginHostCommit) { x.ProtocolVersion = 3 }} {
		t.Run(name, func(t *testing.T) {
			v := c
			mutate(&v)
			if v.MatchReferenceAndAck(h, a) == nil {
				t.Fatal("foreign commit accepted")
			}
		})
	}
	changed := a
	changed.GuestSourceSHA256 = strings.Repeat("34", 32)
	if c.MatchReferenceAndAck(h, changed) == nil {
		t.Fatal("new Guest source substituted originalAck")
	}
}
func TestNormalMemoryCommit2StrictJSONAndNoPayloadBeforeHeader(t *testing.T) {
	_, _, c := memoryCommitFixture(t)
	raw, _ := json.Marshal(c)
	for name, bad := range map[string][]byte{"case": bytes.Replace(raw, []byte("original_nonce"), []byte("Original_Nonce"), 1), "duplicate": bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"protocol_version":2`), 1), "unknown": bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":2,"ready":true`), 1), "null": bytes.Replace(raw, []byte(`"protocol_version":2`), []byte(`"protocol_version":null`), 1), "trailing": append(bytes.Clone(raw), []byte("{}")...)} {
		t.Run(name, func(t *testing.T) {
			var v NormalMemoryOriginHostCommit
			if e := json.Unmarshal(bad, &v); e == nil {
				t.Fatal("ambiguous commit accepted")
			}
		})
	}
	var b bytes.Buffer
	if e := WriteNormalMemoryOriginHostCommit(&b, c); e != nil {
		t.Fatal(e)
	}
	frame := b.Bytes()
	bad := bytes.Clone(frame[:frameHeaderBytes])
	bad[7] = 7
	trap := &bootPayloadTrap{header: bytes.NewReader(bad)}
	if _, e := ReadNormalMemoryOriginHostCommit(trap); e == nil || trap.extraReads != 0 {
		t.Fatal("wrongkind consumed payload")
	}
}
func TestNormalMemoryCommit2PartialFrameNeverRetries(t *testing.T) {
	_, _, c := memoryCommitFixture(t)
	var b bytes.Buffer
	if e := WriteNormalMemoryOriginHostCommit(&b, c); e != nil {
		t.Fatal(e)
	}
	for _, n := range []int{0, frameHeaderBytes - 1, frameHeaderBytes, len(b.Bytes()) - 1} {
		if _, e := ReadNormalMemoryOriginHostCommit(bytes.NewReader(b.Bytes()[:n])); e == nil {
			t.Fatal("partial accepted", n)
		}
	}
	w := &bootCountWriter{shortAt: 1}
	if e := WriteNormalMemoryOriginHostCommit(w, c); e != io.ErrShortWrite || w.calls != 1 {
		t.Fatal("partial header retried", e, w.calls)
	}
}
