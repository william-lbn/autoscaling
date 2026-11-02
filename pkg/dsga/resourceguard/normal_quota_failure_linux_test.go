//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"syscall"
	"testing"
)

func TestNormalQuotaFailureKeepsOriginalAndOnlyScalarFields(t *testing.T) {
	marker := "must-not-log-original-error"
	for _, tc := range []struct {
		name  string
		err   error
		errno uint64
	}{
		{"plain", errors.New(marker), 0},
		{"errno", fmt.Errorf(marker+": %w", syscall.EPERM), uint64(syscall.EPERM)},
		{"unknown-errno", errors.Join(ErrUnknown, fmt.Errorf(marker+": %w", syscall.EBADF)), uint64(syscall.EBADF)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := normalQuotaFailureLine(3210, tc.err)
			if !regexp.MustCompile(`^DSGA_NORMAL_RPC_FAILURE stage=[0-9]+ errno=[0-9]+$`).MatchString(line) || strings.Contains(line, marker) || line != fmt.Sprintf("DSGA_NORMAL_RPC_FAILURE stage=3210 errno=%d", tc.errno) {
				t.Fatal("diagnostic is not fixed scalar-only")
			}
			if got := normalQuotaFailure(3210, tc.err); got != tc.err {
				t.Fatal("original failure identity changed")
			}
		})
	}
	if normalQuotaFailure(3210, nil) != nil {
		t.Fatal("nil original result changed")
	}
}
