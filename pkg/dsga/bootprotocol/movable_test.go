package bootprotocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func movableFixture() (InitialMovableChallenge, InitialMovableReceipt, InitialCompletion) {
	_, p := prebootFixture()
	c := InitialMovableChallenge{ProtocolVersion: 1, Scope: InitialMovableScope, Preboot: p, HostStageSHA256: strings.Repeat("cd", 32), TargetBytes: p.Challenge.BusinessMinimum}
	h, _ := p.SHA256()
	delta := c.TargetBytes - p.Challenge.OriginalBase
	r := InitialMovableReceipt{Challenge: c, GuestBootID: p.GuestBootID, RootInstanceNonce: strings.Repeat("ef", 32), PreparedReceiptSHA256: h, PhysicalOriginSHA256: p.PhysicalOriginSHA256, PreSystemSHA256: p.PreSystemSHA256, CurrentPhysicalSHA256: strings.Repeat("12", 32), ExpectedIncrementBytes: delta, MovablePresentPages: delta / 4096}
	h, _ = r.SHA256()
	end := InitialCompletion{ProtocolVersion: 1, Scope: InitialCompletionScope, Challenge: c, MovableReceiptSHA256: h, HostStageSHA256: strings.Repeat("34", 32)}
	return c, r, end
}

func TestInitialMovableWireHistoricalAndCurrentRoot(t *testing.T) {
	c, r, end := movableFixture()
	if r.RootInstanceNonce == c.Preboot.RootInstanceNonce {
		t.Fatal("fixture must represent recovered new Root")
	}
	if e := r.MatchChallenge(c); e != nil {
		t.Fatal(e)
	}
	if e := end.MatchReceipt(r); e != nil {
		t.Fatal(e)
	}
	var b bytes.Buffer
	if e := WriteInitialMovableChallenge(&b, c); e != nil {
		t.Fatal(e)
	}
	if e := WriteInitialMovableReceipt(&b, r); e != nil {
		t.Fatal(e)
	}
	if e := WriteInitialCompletion(&b, end); e != nil {
		t.Fatal(e)
	}
	got, e := ReadInitialMovableChallenge(bootFragmentReader{&b})
	if e != nil || got != c {
		t.Fatal(e)
	}
	seen, e := ReadInitialMovableReceipt(bootFragmentReader{&b})
	if e != nil || seen != r {
		t.Fatal(e)
	}
	done, e := ReadInitialCompletion(bootFragmentReader{&b})
	if e != nil || done != end || done.MatchReceipt(seen) != nil || b.Len() != 0 {
		t.Fatal("original sequence", e)
	}
	// 新Root的资格必须由真实Recover产生，本包只允许历史/当前身份分离的线表达。
}

func TestInitialMovableWireRejectsDrift(t *testing.T) {
	for name, change := range map[string]func(*InitialMovableReceipt){
		"original-preboot": func(r *InitialMovableReceipt) { r.Challenge.Preboot.RootInstanceNonce = strings.Repeat("56", 32) },
		"host-stage":       func(r *InitialMovableReceipt) { r.Challenge.HostStageSHA256 = "" },
		"target": func(r *InitialMovableReceipt) {
			r.Challenge.TargetBytes += r.Challenge.Preboot.Challenge.Identity.BlockBytes
		},
		"boot":           func(r *InitialMovableReceipt) { r.GuestBootID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" },
		"current-root":   func(r *InitialMovableReceipt) { r.RootInstanceNonce = "" },
		"prepared":       func(r *InitialMovableReceipt) { r.PreparedReceiptSHA256 = strings.Repeat("56", 32) },
		"origin":         func(r *InitialMovableReceipt) { r.PhysicalOriginSHA256 = strings.Repeat("56", 32) },
		"presys":         func(r *InitialMovableReceipt) { r.PreSystemSHA256 = strings.Repeat("56", 32) },
		"physical":       func(r *InitialMovableReceipt) { r.CurrentPhysicalSHA256 = "" },
		"increment":      func(r *InitialMovableReceipt) { r.ExpectedIncrementBytes++ },
		"present-pages":  func(r *InitialMovableReceipt) { r.MovablePresentPages++ },
		"pages-overflow": func(r *InitialMovableReceipt) { r.MovablePresentPages = ^r.MovablePresentPages },
	} {
		t.Run(name, func(t *testing.T) {
			c, r, end := movableFixture()
			change(&r)
			if r.Validate() == nil || r.MatchChallenge(c) == nil || end.MatchReceipt(r) == nil {
				t.Fatal("drift accepted")
			}
		})
	}
	c, r, end := movableFixture()
	other := c
	other.HostStageSHA256 = strings.Repeat("78", 32)
	if other.Validate() != nil || r.MatchChallenge(other) == nil {
		t.Fatal("foreign valid stage")
	}
	end.MovableReceiptSHA256 = strings.Repeat("78", 32)
	if end.MatchReceipt(r) == nil {
		t.Fatal("foreign completion")
	}
}

func TestInitialMovableWireRejectsAmbiguityAndStage(t *testing.T) {
	_, r, end := movableFixture()
	raw, _ := json.Marshal(r)
	for name, body := range map[string]string{
		"duplicate": strings.Replace(string(raw), `"movable_present_pages":`, `"movable_present_pages":"1","movable_present_pages":`, 1),
		"unknown":   strings.Replace(string(raw), `"movable_present_pages":`, `"normal_permit":true,"movable_present_pages":`, 1),
		"number":    strings.Replace(string(raw), `"expected_increment_bytes":"805306368"`, `"expected_increment_bytes":805306368`, 1),
		"missing":   strings.Replace(string(raw), `"protocol_version":1,`, "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			var v InitialMovableReceipt
			if json.Unmarshal([]byte(body), &v) == nil {
				t.Fatal("ambiguous movable wire")
			}
		})
	}
	var b bytes.Buffer
	if e := WriteInitialMovableReceipt(&b, r); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialCompletion(&b); e == nil {
		t.Fatal("receipt borrowed as completion")
	}
	b.Reset()
	if e := WriteInitialCompletion(&b, end); e != nil {
		t.Fatal(e)
	}
	if _, e := ReadInitialMovableReceipt(&b); e == nil {
		t.Fatal("completion borrowed as receipt")
	}
}
