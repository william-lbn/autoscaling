package dsga

import (
	"strings"
	"testing"
)

type cachedObjectFixture struct {
	Count Uint64  `json:"count"`
	Child *string `json:"child"`
	Note  string  `json:"note,omitempty"`
}

func TestObjectSchemaCacheKeepsPerDocumentChecks(t *testing.T) {
	valid := []byte(`{"count":"27","child":null}`)
	var dst cachedObjectFixture
	if err := UnmarshalObject(valid, &dst); err != nil || dst.Count != 27 || dst.Child != nil {
		t.Fatalf("could not warm schema: %+v %v", dst, err)
	}
	for _, bad := range []string{
		`{"count":"27","child":null,"Count":"28"}`, `{"count":null,"child":null}`,
		`{"count":"27"}`, `{"count":"27","child":null,"note":null}`,
		`{"count":"27","count":"28","child":null}`, `{"count":27,"child":null}`,
	} {
		if err := UnmarshalObject([]byte(bad), &dst); err == nil {
			t.Errorf("warm schema accepted %s", bad)
		}
	}
}

func BenchmarkUnmarshalObjectSchema(b *testing.B) {
	raw := []byte(`{"count":"27","child":null}`)
	b.ReportAllocs()
	for range b.N {
		var dst cachedObjectFixture
		if err := UnmarshalObject(raw, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func TestObservationJSONKeepsWriteLimitAndPreservesLargeRead(t *testing.T) {
	raw := []byte(`{"shards":[{"counter":18446744073709551615,"metadata":"` + strings.Repeat("x", 1<<20) + `"}]}`)
	if ValidateJSON(raw, 1<<20, 65536) == nil || ValidateJSON([]byte(`{}`), 8<<20, 262144) == nil {
		t.Fatal("ordinary control protocol limit widened")
	}
	if err := ValidateObservationJSON(raw, 8<<20, 262144); err != nil {
		t.Fatal("bounded aggregate read rejected", err)
	}
	if ValidateObservationJSON(raw, len(raw)-1, 262144) == nil || ValidateObservationJSON([]byte(`{}`), 9<<20, 262144) == nil || ValidateObservationJSON([]byte(`{}`), 8<<20, 262145) == nil {
		t.Fatal("observation limit bypassed")
	}
}

func TestObservationJSONRetainsStructuralGuards(t *testing.T) {
	for name, raw := range map[string][]byte{
		"duplicate_nested": []byte(`{"shards":[{"pg_version":16,"pg_version":17}]}`),
		"trailing":         []byte(`{} {}`),
		"utf8":             append([]byte(`{"x":"`), 0xff, '"', '}'),
		"depth":            []byte(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)),
	} {
		t.Run(name, func(t *testing.T) {
			if ValidateObservationJSON(raw, 8<<20, 262144) == nil {
				t.Fatal("invalid aggregate JSON accepted")
			}
		})
	}
	if ValidateObservationJSON([]byte(`[1,2,3]`), 8<<20, 3) == nil {
		t.Fatal("value count not enforced")
	}
}
