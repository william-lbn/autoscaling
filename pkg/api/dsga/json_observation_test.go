package dsga

import (
	"strings"
	"testing"
)

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
