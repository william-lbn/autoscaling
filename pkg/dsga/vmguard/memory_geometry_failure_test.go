package vmguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

// 直接使用原合法QMP fixture；原summary不一致仍拒绝，错误仅保留同次响应。
func TestOriginalGeometryFailureSameFrames(t *testing.T) {
	in := geometryInputFixture()
	in.Summary = json.RawMessage(`{"base-memory":268435456,"plugged-memory":8388608}`)
	_, err := decodeOriginalMemoryGeometry(geometryIdentityFixture(), 256<<20, in)
	var failure *originalMemoryGeometryFailure
	if !errors.As(err, &failure) || failure.Stage != "summary" || failure.FrameIndex != -1 {
		t.Fatalf("typed failure absent: %T", err)
	}
	if err.Error() != "original QEMU maximum memory geometry differs" {
		t.Fatal("original error text changed")
	}
	if !bytes.Equal(failure.Inputs.Summary, in.Summary) || !bytes.Equal(failure.Inputs.Devices, in.Devices) {
		t.Fatal("same-call frames not retained")
	}
	before := append([]byte(nil), failure.Inputs.Summary...)
	in.Summary[0] = '['
	if !bytes.Equal(failure.Inputs.Summary, before) {
		t.Fatal("caller mutation changed failure evidence")
	}
	if _, e := json.Marshal(failure); e != nil {
		t.Fatal(e)
	}
}

func TestOriginalGeometryFailureBoundsInvalidFrames(t *testing.T) {
	in := geometryInputFixture()
	in.Version = json.RawMessage(bytes.Repeat([]byte("x"), qmpFrameLimit+1))
	_, err := decodeOriginalMemoryGeometry(geometryIdentityFixture(), 256<<20, in)
	var failure *originalMemoryGeometryFailure
	if !errors.As(err, &failure) || failure.Stage != "json-frame" || failure.FrameIndex != 0 {
		t.Fatalf("wrong bound failure %T", err)
	}
	if failure.Inputs.Version != nil || failure.FrameLengths[0] != len(in.Version) {
		t.Fatal("oversized frame was retained or length lost")
	}
	if _, e := json.Marshal(failure); e != nil {
		t.Fatal(e)
	}
}
