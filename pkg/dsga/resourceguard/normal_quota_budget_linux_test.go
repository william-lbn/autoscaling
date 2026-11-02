//go:build linux && amd64

package resourceguard

import (
	"context"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
	"testing"
)

func TestNormalQuotaBudgetFixedCPUKeepsLegacyAndTaskMargins(t *testing.T) {
	f := normalInflightMetadataTestFacts(t)
	old, e := normalInflightCalculateFixed(f, f.Build)
	if e != nil {
		t.Fatal(e)
	}
	same, e := normalInflightCalculateCPUFixed(f, f.Build, 64)
	if e != nil || !reflect.DeepEqual(old, same) {
		t.Fatal("legacy64 changed", e)
	}
	small, e := normalInflightCalculateCPUFixed(f, f.Build, uint64(f.Physical.PossibleCPUs))
	if e != nil {
		t.Fatal(e)
	}
	if small.MigrationDestinationsBytes != old.MigrationDestinationsBytes || small.TaskRetireBytes != old.TaskRetireBytes || small.CompactionIsolatedFreeReserveBytes != old.CompactionIsolatedFreeReserveBytes || small.LateExitSingleRetireBytes != old.LateExitSingleRetireBytes {
		t.Fatal("CPU tightening reduced independent task/retire bounds")
	}
	c := uint64(f.Physical.PossibleCPUs)
	if small.IRQConditionalOccupiedBytes*64 != old.IRQConditionalOccupiedBytes*c || small.CounterHierarchyGapBytes*64 != old.CounterHierarchyGapBytes*c || small.CPUStockPaidBytes*64 != old.CPUStockPaidBytes*c {
		t.Fatal("CPU-proportional margins differ")
	}
	if small.AllSourcesClosed || small.NormalDecreaseAuthorized || len(small.MissingSources) != 8 {
		t.Fatal("arithmetic granted permission")
	}
	for _, c := range []uint64{0, 1, 65, ^uint64(0)} {
		if _, e = normalInflightCalculateCPUFixed(f, f.Build, c); e == nil {
			t.Fatalf("invalid original CPU bound accepted: %d", c)
		}
	}
}

func TestNormalQuotaBudgetUnboundCannotAuthorizeOrWrite(t *testing.T) {
	for _, g := range []*Guard{nil, {}, {config: Config{ExecutionModel: TrustedControllerExecutionModel}}} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if e := g.prepareNormalQuotaBudgetSource(ctx); !errors.Is(e, ErrUnknown) {
			t.Fatal("cancelled preparation gained source", e)
		}
		for _, c := range []context.Context{nil, ctx, context.Background()} {
			v, e := g.normalQuotaBudgetLocked(c, nil, nil, NormalSplitTopologySnapshot{}, control.Uint64(64<<10))
			if !errors.Is(e, ErrUnknown) || v != (normalQuotaBudget{}) {
				t.Fatal("unbound request produced a budget", e)
			}
		}
	}
	// 一个JSON/同配置结构没有Source registry见证或原memory历史，不能变成预算。
	var decoded normalQuotaBudgetSource
	_, _, _, e := decoded.freshLocked(context.Background(), nil, nil)
	if !errors.Is(e, ErrUnknown) {
		t.Fatal("empty source qualified", e)
	}
}
