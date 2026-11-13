package runnerdiagnostic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestDiagnosticPreservesCauseAndNeverSerializesErrorText(t *testing.T) {
	for _, tc := range []struct {
		cause error
		code  Code
	}{
		{context.Canceled, CodeContextCanceled}, {context.DeadlineExceeded, CodeDeadlineExceeded},
		{apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "private-name", errors.New("private-token")), CodeAPIForbidden},
		{apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "private-name"), CodeAPINotFound},
		{errors.New("private-token"), CodeFailed},
	} {
		err := At(StageQEMURun, fmt.Errorf("outer private-token: %w", At(StageInitialFreeze, tc.cause)))
		require.ErrorIs(t, err, tc.cause)
		report := FromError(err)
		require.Equal(t, StageInitialFreeze, report.Stage)
		require.Equal(t, tc.code, report.Code)
		raw, e := Encode(report)
		require.NoError(t, e)
		require.Less(t, len(raw), MaxBytes)
		require.NotContains(t, string(raw), "private")
		parsed, e := Parse(raw)
		require.NoError(t, e)
		require.Equal(t, report, parsed)
	}
}

func TestDiagnosticRejectsNoncanonicalOrUnboundedMessage(t *testing.T) {
	for _, raw := range []string{
		`{"version":1,"stage":"initialFreeze","code":"failed","secret":"private-token"}`,
		`{"version":1,"stage":"initialFreeze","stage":"commandStart","code":"failed"}`,
		`{"version":1,"stage":"private-token","code":"failed"}`,
		`{"version":2,"stage":"initialFreeze","code":"failed"}`,
		`{"version":1,"stage":"initialFreeze","code":"private-token"}`,
		`{"version":1,"stage":"initialFreeze","code":"failed"} {}`,
		strings.Repeat("x", MaxBytes+1), "private-token", "",
	} {
		_, err := Parse([]byte(raw))
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-token")
	}
}

func TestDiagnosticClassifiesOriginalStageBeforeCleanupJoin(t *testing.T) {
	for _, tc := range []struct {
		primary, cleanup error
		code             Code
	}{
		{apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "private-name", errors.New("private-token")), context.DeadlineExceeded, CodeAPIForbidden},
		{apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "private-name", errors.New("private-token")), context.Canceled, CodeAPIForbidden},
		{errors.New("private-token"), apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "private-name", errors.New("cleanup-private-token")), CodeFailed},
	} {
		err := errors.Join(At(StageInitialFreeze, tc.primary), tc.cleanup)
		report := FromError(err)
		require.Equal(t, StageInitialFreeze, report.Stage)
		require.Equal(t, tc.code, report.Code)
		require.ErrorIs(t, err, tc.primary)
		require.ErrorIs(t, err, tc.cleanup)
	}
}
