package resourceguard

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 原来源引用经过JSON持久化时必须保留超过float64精度和uint64边界的准确值。
// 此处只验跨平台数据合同，不打开FD、不构造opaque来源，也不补Linux能力。
func TestArrayPopulateFilePortableReferenceCodec(t *testing.T) {
	const golden = `{"device":"18446744073709551615","inode":"9007199254740993","file_bytes":"18446744073709551614","offset":"9007199254740995","length":"4096"}`
	want := arrayPopulateFile{Device: control.Uint64(^uint64(0)), Inode: 9007199254740993, FileBytes: control.Uint64(^uint64(0) - 1), Offset: 9007199254740995, Length: 4096}
	t.Run("reference_preserves_exact_uint64_strings", func(t *testing.T) {
		ref := normalPGMemoryControlReference{Files: [4]arrayPopulateFile{want}}
		raw, err := json.Marshal(ref)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Files []json.RawMessage `json:"files"`
		}
		if err = json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		if len(envelope.Files) != 4 || string(envelope.Files[0]) != golden {
			t.Fatalf("original four-file canonical JSON changed: %s", raw)
		}
		var restored arrayPopulateFile
		if err = json.Unmarshal(envelope.Files[0], &restored); err != nil {
			t.Fatal(err)
		}
		if restored != want {
			t.Fatalf("portable reference lost exact uint64 value: %#v", restored)
		}
	})
	for _, tc := range []struct{ name, raw string }{
		{"reject_number", `{"device":9007199254740993}`},
		{"reject_exponent_string", `{"inode":"9.007199254740993e15"}`},
		{"reject_overflow", `{"file_bytes":"18446744073709551616"}`},
		{"reject_leading_zero", `{"offset":"04096"}`},
		{"reject_negative", `{"length":"-1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f arrayPopulateFile
			if err := json.Unmarshal([]byte(tc.raw), &f); err == nil {
				t.Fatal("noncanonical uint64 silently accepted")
			}
		})
	}
}
