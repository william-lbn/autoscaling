package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
)

// 真Runner本地WAL、Kube发布和Reader联合执行。QMP及平台授权仍为显式领域
// 夹具；此处只证明来源语义及读回，不能据此宣称Linux实际内存已经归还。
func TestCapacityReaderKeepsCompletedAndUnsentCancelledDistinct(t *testing.T) {
	for _, mode := range []string{"grow-completed", "shrink-cancelled", "may-have-sent"} {
		t.Run(mode, func(t *testing.T) {
			f, ctx := newRunnerFixture(t), context.Background()
			kind := control.Grow
			if mode == "shrink-cancelled" {
				kind = control.Shrink
			}
			command, outer := f.beginKind(t, kind)
			memory := &memoryFixture{mu: sync.Mutex{}, dir: f.config.StateDir, actual: Observation{RequestedBytes: command.BeforeBytes, ActualBytes: command.BeforeBytes}, writes: 0, apply: true, err: nil, entered: nil, release: nil}
			f.runner.engine.native = memory
			if mode == "shrink-cancelled" {
				s, err := f.store.Load(ctx)
				require.NoError(t, err)
				_, err = f.store.RequestAbort(ctx, s, f.actor, command.Request.Key)
				require.NoError(t, err)
			}
			if mode == "may-have-sent" {
				memory.apply, memory.err = false, errors.New("explicit QMP fixture lost reply")
				require.ErrorIs(t, f.runner.Tick(ctx), ErrUnknown)
			} else {
				require.NoError(t, f.runner.Tick(ctx))
			}
			reader, err := NewReader(f.core.CoreV1(), f.store.Binding(), "lease-uid")
			require.NoError(t, err)
			source, confirmed, err := reader.CapacityForOriginal(ctx, command, outer, f.actor)
			if mode == "may-have-sent" {
				require.Error(t, err, "当前before不等于原未发送终态")
				return
			}
			require.NoError(t, err)
			require.Equal(t, mode == "shrink-cancelled", source.Cancelled)
			checked, err := reader.CheckCapacity(ctx, source, f.actor)
			require.NoError(t, err)
			require.Equal(t, confirmed.Sources, checked.Sources)
			if mode == "grow-completed" {
				verifyReconciledClaimReader(t, f, source)
			}
			wrong := source
			wrong.Cancelled = !wrong.Cancelled
			_, err = reader.CheckCapacity(ctx, wrong, f.actor)
			require.Error(t, err, "同一原回执不得改变Completed/Cancelled语义")
			wrong = source
			wrong.BeforeBytes += 256 << 20
			_, err = reader.CheckCapacity(ctx, wrong, f.actor)
			require.Error(t, err)
			wrong = source
			wrong.Sources.ReceiptClaim.UID += "-replacement"
			_, err = reader.CheckCapacity(ctx, wrong, f.actor)
			require.Error(t, err)
			memory.mu.Lock()
			memory.actual.ActualBytes += 256 << 20
			memory.mu.Unlock()
			require.NoError(t, f.runner.Tick(ctx))
			_, err = reader.CheckCapacity(ctx, source, f.actor)
			require.Error(t, err, "不可变来源不能代替当前实际容量")
		})
	}
}

// 复用同一Runner组合核真实来源读取与Node已发布持有。PG/Root预算值是明确
// 合同夹具，仅本函数的CM/Pod/Node真实性由实际Reader检查，不称完整Agent恢复。
func verifyReconciledClaimReader(t *testing.T, f *runnerFixture, source budget.NativeCapacity) {
	t.Helper()
	ctx := context.Background()
	s, err := f.store.Load(ctx)
	require.NoError(t, err)
	s, err = f.store.RequestAbort(ctx, s, f.actor, source.Request.Key)
	require.NoError(t, err)
	var p budget.Prepared
	require.NoError(t, json.Unmarshal(s.State().Active.Steps[0].Observation.Data, &p))
	cm, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, source.HostClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var host budget.HostReservation
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &host))
	cm, err = f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(ctx, p.OutsideClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var out budget.OutsideReservation
	require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &out))
	upper, err := p.Limits.TotalUpperBytes()
	require.NoError(t, err)
	c := budget.ReconciledCapacity{
		ProtocolVersion: 1, VMShrinkPermit: false, Request: p.Request, IssuerEpoch: f.actor.Epoch, RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount,
		VMBeforeMemoryBytes: p.VMBeforeMemoryBytes, VMMemoryBytes: source.MemoryBytes(), GuestUsableMemoryBytes: p.GuestUsableMemoryBytes,
		CapacityGeneration: p.Shared.Observed.CapacityGeneration, Buffers: p.Shared.Observed.HighBuffers, MaximumBuffers: p.Shared.Observed.MaximumBuffers,
		StartupMainUpperBytes: p.Shared.StartupMainUpperBytes, StartupArraysUpperBytes: p.Shared.StartupArraysUpperBytes, RetainedSharedUpperBytes: p.Shared.CurrentMainAndArraysUpperBytes, PGOperation: p.Shared.Observed.Operation,
		PGAggregate: budget.Bound{Kind: budget.HardLimit, UpperBytes: control.Uint64(upper) - p.Limits.OutsideUpperBytes, SourceID: "explicit-root-contract-fixture"}, RootSHA256: strings.Repeat("f", 64),
		OutsideClaim: *p.OutsideClaim, OutsideRequest: out.Request, ResidualProfileID: out.ResidualProfileID, SystemOutsidePG: out.SystemOutsidePG, Transition: out.Transition, Safety: out.Safety,
		HostClaim: source.HostClaim, HostReservationID: host.SourceID, VM: source,
	}
	require.NoError(t, c.Validate())
	r, err := budgetclaims.New(f.core.CoreV1(), f.store.Binding())
	require.NoError(t, err)
	observed, err := r.ReadReconciled(ctx, s, c)
	require.NoError(t, err)
	require.Equal(t, host, observed.Host)
	require.Equal(t, out, observed.Outside)
	require.NoError(t, r.RecheckReconciled(ctx, s, c, observed))
	wrong := c
	wrong.OutsideClaim.UID += "-replacement"
	_, err = r.ReadReconciled(ctx, s, wrong)
	require.Error(t, err)
	wrong = c
	wrong.HostClaim.UID += "-replacement"
	wrong.VM.HostClaim = wrong.HostClaim
	_, err = r.ReadReconciled(ctx, s, wrong)
	require.Error(t, err)
	wrong = c
	wrong.VMBeforeMemoryBytes += 256 << 20
	_, err = r.ReadReconciled(ctx, s, wrong)
	require.Error(t, err, "实际Node计划before不同不能借相同target接受")
	wrong = c
	wrong.RunnerRestartCount++
	_, err = r.ReadReconciled(ctx, s, wrong)
	require.Error(t, err)
}
