package admissionexec

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
)

type opaqueActualReadCause struct{}

func (*opaqueActualReadCause) Error() string          { panic("opaque Error must not be called") }
func (*opaqueActualReadCause) Format(fmt.State, rune) { panic("opaque Format must not be called") }

func TestServiceActualSourceReadFailuresKeepOriginalCause(t *testing.T) {
	markers := []error{errServiceActualPGSourceRead, errServiceActualRootSourceRead, errServiceActualVMSourceRead}
	for i, marker := range markers {
		t.Run([]string{"pg", "root", "vm"}[i], func(t *testing.T) {
			opaque := &opaqueActualReadCause{}
			out := serviceActualReadFailure(marker, opaque)
			if out == nil || !errors.Is(out, marker) || !errors.Is(out, opaque) {
				t.Fatal("opaque original source cause not retained")
			}
			var recovered *opaqueActualReadCause
			if !errors.As(out, &recovered) || recovered != opaque {
				t.Fatal("original opaque cause type lost")
			}
			cancelled := serviceActualReadFailure(marker, context.Canceled)
			if !errors.Is(cancelled, context.Canceled) || !errors.Is(cancelled, marker) {
				t.Fatal("original cancellation lost")
			}
			native := &compute.CallError{Code: "transport_unconfirmed", MayHaveExecuted: true}
			out = serviceActualReadFailure(marker, native)
			var call *compute.CallError
			if !errors.As(out, &call) || call != native || !call.MayHaveExecuted {
				t.Fatal("native original error or MAY changed")
			}
		})
	}
}
