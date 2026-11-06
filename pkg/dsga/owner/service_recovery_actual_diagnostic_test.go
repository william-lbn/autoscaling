package owner

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
)

type opaqueActualDiagnosticCause struct{}

func (*opaqueActualDiagnosticCause) Error() string { panic("opaque Error must not be called") }
func (*opaqueActualDiagnosticCause) Format(fmt.State, rune) {
	panic("opaque Format must not be called")
}

func TestServiceRecoveryActualDiagnosticSubstagesKeepOriginalError(t *testing.T) {
	for _, stage := range []string{"actual_pg_reconcile", "actual_budget", "actual_qualification"} {
		t.Run(stage, func(t *testing.T) {
			if serviceReopenDiagnosticStage(stage) != stage {
				t.Fatal("fixed actual stage was dropped")
			}
			cause := &opaqueActualDiagnosticCause{}
			if out := recordServiceReopenCheck(stage, time.Now(), cause); out != cause {
				t.Fatal("original error identity changed")
			}
			if out := recordServiceReopenCheck(stage, time.Now(), nil); out != nil {
				t.Fatal("nil success changed")
			}
		})
	}
	if serviceReopenDiagnosticStage("private-body-token") != "unclassified" {
		t.Fatal("arbitrary stage was exposed")
	}
}

func TestServiceRecoveryActualFixedLeavesDoNotFormatCause(t *testing.T) {
	cases := map[string]string{
		"service actual check lacks independent retained budget":                       "actual_budget_missing",
		"service actual recovery requires complete original closed production domains": "actual_domains_not_ready",
		"service actual lacks retained proof for original PG pending":                  "actual_pg_pending_proof",
		"service actual PG serialized fence remains pending":                           "actual_pg_fence_pending",
		"service actual PG changed stable original closed boundary":                    "actual_pg_gate_changed",
		"service actual Guest remains independently unknown":                           "actual_guest_unknown",
		"service actual Root changed independent original Guest source":                "actual_guest_source_changed",
		"service actual Root changed original completed Guest source":                  "actual_guest_completed_changed",
		"service actual original Guest effect is not authenticated":                    "actual_guest_not_authenticated",
		"service actual Root is not held by original permanent budget":                 "actual_root_not_held",
		"service actual VM remains independently unknown":                              "actual_vm_unknown",
		"service actual current Root or original native VM source changed":             "actual_root_vm_source_changed",
		"service actual current PG or VM differs from retained budget":                 "actual_pg_vm_budget_changed",
		"service actual original Runner and Host domains differ":                       "actual_runner_host_changed",
		"service actual PG changed during source verification":                         "actual_pg_tail_changed",
		"service actual Root changed during source verification":                       "actual_root_tail_changed",
		"service actual native capacity changed during source verification":            "actual_vm_tail_changed",
		"service actual PG source read failed during verification":                     "actual_pg_tail_read_failed",
		"service actual Root source read failed during verification":                   "actual_root_tail_read_failed",
		"service actual native capacity source read failed during verification":        "actual_vm_tail_read_failed",
		"PG service recovery requires exact committed resource closure":                "actual_pg_closure_not_ready",
		"actual service PG source differs from original dispatch or SDK slot":          "actual_pg_slot_changed",
		"actual service PG original VM UID/RV changed before reconciliation":           "actual_pg_snapshot_changed",
		"cleanup qualification changed original Root or Runner":                        "actual_qualification_changed",
		"cleanup qualification lost original settled Runner":                           "actual_qualification_unsettled",
	}
	for message, want := range cases {
		t.Run(want, func(t *testing.T) {
			cause := &opaqueActualDiagnosticCause{}
			marker := errors.New(message)
			joined := errors.Join(marker, cause)
			if got := serviceReopenErrorLeaf(joined); got != want {
				t.Fatalf("fixed leaf mismatch: %s", got)
			}
			if !errors.Is(joined, marker) || !errors.Is(joined, cause) {
				t.Fatal("joined cause identity lost")
			}
			if out := recordServiceReopenCheck("actual_budget", time.Now(), joined); out != joined {
				t.Fatal("original joined error changed")
			}
		})
	}
	native := errors.Join(errors.New("service actual PG source read failed during verification"), &compute.CallError{Code: "transport_unconfirmed", MayHaveExecuted: true})
	if serviceReopenErrorLeaf(native) != "actual_pg_tail_read_failed" {
		t.Fatal("fixed original read stage classification lost")
	}
	var retained *compute.CallError
	if !errors.As(native, &retained) || !retained.MayHaveExecuted {
		t.Fatal("native original cause or MAY lost")
	}
}
