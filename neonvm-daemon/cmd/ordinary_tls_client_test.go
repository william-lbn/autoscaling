package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestOrdinaryTLSReplacementIsBoundedFixedAndUnavailable(t *testing.T) {
	data := base64.StdEncoding.EncodeToString([]byte("PUBLIC_TEST_DATA_NOT_A_KEY"))
	raw := []byte(fmt.Sprintf(`{"tls.crt":{"Data":%q}}`, data))
	if _, e := decodeOrdinaryTLSReplacement(raw); e == nil {
		t.Fatal("alias accepted")
	}
	raw = []byte(fmt.Sprintf(`{"tls.crt":{"data":%q}}`, data))
	p, e := decodeOrdinaryTLSReplacement(raw)
	if e != nil || p.ProtocolVersion != 2 || len(p.Files) != 1 {
		t.Fatal(e)
	}
	if d, e := ordinaryTLSDelegateUnsupported(); d != nil || !errors.Is(e, errOrdinaryTLSRootDelegateUnavailable) {
		t.Fatal("missing original role endpoint authorized")
	}
}
func TestOrdinaryTLSReplacementRejectsPathsDuplicatesAndOversize(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"../tls.key":{"data":""}}`, `{"tls.key":null}`, `{"tls.key":{"data":"","data":""}}`, `{"tls.key":{"data":"","verified":true}}`, `{"tls.key":{"data":"!"}}`, `{"tls.key":{"data":"` + strings.Repeat("A", 64<<10) + `"}}`} {
		if _, e := decodeOrdinaryTLSReplacement([]byte(raw)); e == nil {
			t.Fatalf("bad TLS envelope accepted length=%d", len(raw))
		}
	}
}
