package owner

import (
	"context"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
	"time"
)

type privateReopenError struct{}

func (privateReopenError) Error() string { panic("private Error must not be called") }

type cyclicReopenError struct{}

func (*cyclicReopenError) Error() string   { panic("opaque cyclic Error must not be called") }
func (e *cyclicReopenError) Unwrap() error { return e }

func TestServiceReopenFixedStageAndLeafKeepOriginalError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		leaf string
	}{
		{"native", errors.Join(ErrServiceSafety, &compute.CallError{Code: "invalid_admission_lease_fact"}), "native_lease_fact_invalid"},
		{"conflict", errors.Join(ErrServiceSafety, apierrors.NewConflict(schema.GroupResource{Resource: "private"}, "private", errors.New("private"))), "k8s_conflict"},
		{"deadline", fmt.Errorf("private wrapper: %w", context.DeadlineExceeded), "context_deadline"},
		{"stale", errors.Join(ErrServiceSafety, errors.New("journal snapshot is stale")), "journal_snapshot_stale"},
		{"bound", errors.New("service reopen cannot retain complete bounded lifecycle evidence"), "lifecycle_capacity_bound"},
		{"opaque", &privateReopenError{}, "validation_unclassified"},
		{"cycle", &cyclicReopenError{}, "validation_unclassified"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.leaf, serviceReopenErrorLeaf(c.err))
			out := recordServiceReopenCheck("store_start", time.Now(), c.err)
			require.Same(t, c.err, out)
		})
	}
	require.Equal(t, "none", serviceReopenErrorLeaf(nil))
	require.Equal(t, "unclassified", serviceReopenDiagnosticStage("private-url-token"))
}
