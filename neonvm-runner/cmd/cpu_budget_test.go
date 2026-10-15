package main

import (
	"errors"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"sync/atomic"
	"testing"
)

func TestCPUBudgetCommitsOnlyAfterKernelQuotaSuccess(t *testing.T) {
	var current atomic.Uint32
	current.Store(250)
	failed := errors.New("synthetic kernel quota rejection")
	calls := []vmv1.MilliCPU{}
	apply := func(value vmv1.MilliCPU) error {
		calls = append(calls, value)
		if value == 500 && len(calls) == 1 {
			return failed
		}
		return nil
	}
	if err := commitCPUBudget(500, &current, apply); !errors.Is(err, failed) || current.Load() != 250 {
		t.Fatal("failed kernel update was acknowledged")
	}
	if err := commitCPUBudget(500, &current, apply); err != nil || current.Load() != 500 {
		t.Fatal("retry did not apply quota before reporting")
	}
	if err := commitCPUBudget(500, &current, apply); err != nil || current.Load() != 500 {
		t.Fatal("same-budget retry was not idempotent")
	}
	if err := commitCPUBudget(250, &current, apply); err != nil || current.Load() != 250 || len(calls) != 4 || calls[3] != 250 {
		t.Fatal("downscale was not applied to kernel")
	}
}
