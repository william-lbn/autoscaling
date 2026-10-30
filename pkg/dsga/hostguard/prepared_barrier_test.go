package hostguard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 本文件是PG17-U-P1节点单元/HTTP CAS验证。VM闭门/Prepare由显式领域夹具
// 输入；真实Publisher/Journal/Runner完整恢复的单服务/E2E测试必须另行验证，
// 本夹具不宣称已完成跨域业务、apiserver RBAC或物理内存归还验收。
func preparedBarrierFixture(t *testing.T) (*nodeAPI, *Store, *Snapshot, Plan) {
	t.Helper()
	f, store := newNodeAPI(t)
	p := plan("vm-one")
	row := Placement{Namespace: p.Namespace, PodName: "runner", PodUID: p.RunnerPodUID,
		VMName: p.VMName, VMUID: p.Request.Key.Instance.VMUID, Incarnation: p.Incarnation,
		Managed: true, MemoryBytes: p.BeforeBytes, HostOverheadBytes: p.HostOverheadBytes}
	n, err := store.InitializeAllocations(context.Background(), "node-rv-1", 1<<30, allocationProfile(), []Placement{row})
	require.NoError(t, err)
	protectBarrierPod(f, row)
	return f, store, n, p
}

func protectBarrierPod(f *nodeAPI, p Placement) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods[p.Namespace+"/"+p.PodName] = core.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: p.Namespace, Name: p.PodName, UID: types.UID(p.PodUID), ResourceVersion: "pod-rv-original",
		Finalizers: []string{PodProtectionFinalizer}}, Spec: core.PodSpec{NodeName: nodeBinding().NodeName}}
}

func preparedClosure(p Plan, sequence control.Uint64) PreparedClosure {
	return PreparedClosure{Plan: p, PreparedSHA256: strings.Repeat("a", 64), OperationSHA256: strings.Repeat("b", 64),
		VMOwner:    journal.Owner{HolderID: "original-vm-owner", Epoch: p.Request.Key.IssuerEpoch},
		VMSequence: sequence, VMResourceVersion: "vm-original-rv", ClosedSHA256: strings.Repeat("c", 64), ClosedSequence: sequence - 1}
}

func barrierNode(f *nodeAPI) *core.Node {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.node.DeepCopy()
}

func TestPG17UP1SealBeforeReserveAndPlacementBarrier(t *testing.T) {
	t.Log("PG17-U-P1-001: Seal先赢真实Node CAS，旧/fresh执行与placement旁路全部拒绝")
	f, store, n, p := preparedBarrierFixture(t)
	ctx := context.Background()
	before := n.State()
	reached, release := make(chan struct{}), make(chan struct{})
	f.mu.Lock()
	f.beforePatch = func() { close(reached); <-release }
	f.mu.Unlock()
	result := make(chan error, 1)
	go func() { _, _, err := store.Reserve(ctx, n, p); result <- err }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("旧Reserve未到达锁外CAS阻塞点")
	}
	sealed, b, err := store.SealPrepared(ctx, n, preparedClosure(p, 10))
	close(release)
	require.NoError(t, err)
	require.Error(t, <-result, "旧RV不能写过已经落账的封存")
	require.Nil(t, b.Held)
	require.Equal(t, n.ResourceVersion(), b.NodeResourceVersion, "保存写前原RV而非伪造写后RV")
	require.Equal(t, before.Sequence+1, b.NodeSequence)
	require.EqualValues(t, 9, sealed.State().ProtocolVersion)
	require.Equal(t, before.Holds, sealed.State().Holds)
	row, err := sealed.State().Placement(p.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, p.BeforeBytes, row.MemoryBytes)
	_, _, err = store.Reserve(ctx, sealed, p)
	require.ErrorIs(t, err, ErrSealed)
	alias := p
	alias.Request.Key.OperationID.Low++
	_, _, err = store.Reserve(ctx, sealed, alias)
	require.ErrorIs(t, err, ErrSealed)

	input := row
	input.Barrier = nil
	writer := f.store(t, "another-scheduler-replica")
	same, held, err := writer.HoldPlacement(ctx, sealed, input)
	require.NoError(t, err, "无Key可保留原容量且不要求Node owner同副本")
	require.Equal(t, sealed.ResourceVersion(), same.ResourceVersion())
	require.Equal(t, row.Barrier, held.Barrier)
	input.MemoryBytes++
	_, _, err = writer.HoldPlacement(ctx, sealed, input)
	require.Error(t, err, "无Key不能提高封存行额度")
	input.MemoryBytes = row.MemoryBytes
	_, _, err = writer.HoldPlacementFor(ctx, sealed, input, p.Request.Key)
	require.ErrorIs(t, err, ErrSealed, "旧Key即使目标相同也不能重获资格")
	_, _, err = writer.HoldPlacement(ctx, sealed, row)
	require.Error(t, err, "调用方不得自报或替换Barrier")
	future := p.Request.Key
	future.Revision++
	future.OperationID.Low++
	input.MemoryBytes = 3 << 30
	newer, held, err := writer.HoldPlacementFor(ctx, sealed, input, future)
	require.NoError(t, err, "更高原Key仍可由非Node owner的Scheduler容量副本持有")
	require.Equal(t, input.MemoryBytes, held.MemoryBytes)
	require.Equal(t, row.Barrier, held.Barrier)
	require.EqualValues(t, 9, newer.State().ProtocolVersion)
	wrongBoot := future
	wrongBoot.Instance.PGBootID++
	_, _, err = writer.HoldPlacementFor(ctx, newer, input, wrongBoot)
	require.Error(t, err)
}

func TestPG17UP1ReservedSealUnknownAndLateBind(t *testing.T) {
	t.Log("PG17-U-P1-002: Reserve先赢、封存回复丢失，只读恢复后迟到Bind仍拒绝")
	f, store, n, p := preparedBarrierFixture(t)
	ctx := context.Background()
	reserved, h, err := store.Reserve(ctx, n, p)
	require.NoError(t, err)
	before := reserved.State()
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	unknownSnapshot, unknownBarrier, err := store.SealPrepared(ctx, reserved, preparedClosure(p, 10))
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	require.Nil(t, unknownSnapshot)
	require.Empty(t, unknownBarrier.NodeOrigin, "未知写不返回屏障票据")
	restarted := f.store(t, "host-controller-one")
	sealed, err := restarted.Load(ctx)
	require.NoError(t, err)
	b, err := sealed.PreparedBarrier(p)
	require.NoError(t, err)
	require.NotNil(t, b.Held)
	require.Equal(t, h.SourceID, b.Held.SourceID)
	require.Nil(t, b.Held.Claim, "已Reserve不能投影成Held=nil，也不能补成已发布Claim")
	require.Equal(t, before.Holds, sealed.State().Holds)
	ref := budget.ObjectRef{Name: "late-original-host", UID: "late-original-host-uid"}
	_, err = restarted.BindClaim(ctx, reserved, p, ref)
	require.Error(t, err, "封存前的旧Node RV无效")
	_, err = restarted.BindClaim(ctx, sealed, p, ref)
	require.ErrorIs(t, err, ErrSealed, "迟到CREATE即使留下原protected CM，最终Bind仍拒绝")
	version := sealed.ResourceVersion()
	same, sameBarrier, err := restarted.SealPrepared(ctx, sealed, preparedClosure(p, 10))
	require.NoError(t, err)
	require.Equal(t, version, same.ResourceVersion())
	require.Equal(t, b, sameBarrier)
	changed := preparedClosure(p, 10)
	changed.PreparedSHA256 = strings.Repeat("d", 64)
	_, _, err = restarted.SealPrepared(ctx, sealed, changed)
	require.Error(t, err, "同Key不能更换原Prepare或闭门选择")
	b.Held.SourceID = strings.Repeat("e", 64)
	restored, err := sealed.PreparedBarrier(p)
	require.NoError(t, err)
	require.Equal(t, h.SourceID, restored.Held.SourceID, "返回屏障必须深拷贝")
}

func TestPG17UP1BoundSealPreservesRetainedBudgetAndSettlement(t *testing.T) {
	t.Log("PG17-U-P1-003: 原Claim已绑定时封存禁执行、旧预算只读保持、明确Through才结清")
	f, store, n, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	cancelled := current
	cancelled.Request.Key.Revision++
	cancelled.Request.Key.OperationID.Low++
	cancelled.Request.Kind = control.Grow
	cancelled.BeforeBytes, cancelled.Request.TargetVMMemoryBytes = 1<<30, 3<<30
	n, _, err := store.Reserve(ctx, n, cancelled)
	require.NoError(t, err)
	ref := budget.ObjectRef{Name: "cancelled-bound-host", UID: "cancelled-bound-uid"}
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	_, err = store.BindClaim(ctx, n, cancelled, ref)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	n, err = store.Load(ctx)
	require.NoError(t, err)
	h, err := n.Hold(cancelled)
	require.NoError(t, err)
	require.Equal(t, &ref, h.Claim, "Bind丢回复后从原Node恢复准确原UID")
	before := n.State()
	sealed, b, err := store.SealPrepared(ctx, n, preparedClosure(cancelled, 50))
	require.NoError(t, err)
	require.Equal(t, &ref, b.Held.Claim)
	require.Equal(t, before.Holds, sealed.State().Holds)
	require.Equal(t, (*before.Placements)[0].MemoryBytes, (*sealed.State().Placements)[0].MemoryBytes)
	node := barrierNode(f)
	require.ErrorIs(t, InspectClaim(node, cancelled, h.SourceID, ref), ErrSealed)
	require.ErrorIs(t, InspectClaim(node, current, SourceID(sealed.State().Origin, current), closed.HostClaim), ErrSealed)
	require.NoError(t, InspectRetainedClaim(node, current, SourceID(sealed.State().Origin, current), closed.HostClaim, closed))
	_, err = store.BindClaim(ctx, sealed, cancelled, ref)
	require.ErrorIs(t, err, ErrSealed, "准确已绑定Claim的幂等分支也不得绕过门禁")
	replacement := ref
	replacement.UID = "same-name-new-uid"
	require.Error(t, InspectRetainedClaim(node, cancelled, h.SourceID, replacement, closed))
	copyModel := sealed.State()
	(*copyModel.Placements)[0].Barrier.Held.Claim.UID = "caller-mutated"
	row, err := sealed.State().Placement(cancelled.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, ref.UID, row.Barrier.Held.Claim.UID)
	row.Barrier.Held.Claim.Name = "caller-mutated-name"
	original, err := sealed.PreparedBarrier(cancelled)
	require.NoError(t, err)
	require.Equal(t, ref.Name, original.Held.Claim.Name)

	extended := closed
	extended.ThroughKey, extended.ThroughSequence = cancelled.Request.Key, closed.BudgetSequence+10
	settled, retired, err := store.Settle(ctx, sealed, current.RunnerPodUID, extended)
	require.NoError(t, err)
	require.Len(t, retired, 2, "仅完整旧预算Through才可删除旧峰值")
	require.EqualValues(t, 9, settled.State().ProtocolVersion)
	require.Len(t, settled.State().Holds, 1)
	after, err := settled.PreparedBarrier(cancelled)
	require.NoError(t, err, "原Hold退休后Barrier仍保存完整原封存观察")
	require.Equal(t, b, after)
	require.NoError(t, InspectRetainedClaim(barrierNode(f), current, SourceID(settled.State().Origin, current), closed.HostClaim, extended))
	require.Error(t, InspectRetainedClaim(barrierNode(f), current, SourceID(settled.State().Origin, current), closed.HostClaim, closed))
	_, _, err = store.RetireSealedHolds(ctx, settled, b)
	require.Error(t, err, "普通永久Host预算不能借Initial退休入口删除")
	future := cancelled
	future.Request.Key.Revision++
	future.Request.Key.OperationID.Low++
	future.Request.TargetBuffers = 2048
	future.Request.TargetVMMemoryBytes = 4 << 30
	newer, _, err := store.Reserve(ctx, settled, future)
	require.NoError(t, err, "严格后继真实新目标可申请")
	newer, err = store.BindClaim(ctx, newer, future, budget.ObjectRef{Name: "future-host", UID: "future-host-uid"})
	require.NoError(t, err)
	require.EqualValues(t, 9, newer.State().ProtocolVersion, "发布更新不能降级v9")
}

func TestPG17UP1LatestBarrierRetiresTrustedInitialPrefix(t *testing.T) {
	t.Log("PG17-U-P1-004: 显式可信Initial完成后按最新屏障退休整个前缀，丢回复不漏旧Hold")
	f, store, n, first := preparedBarrierFixture(t)
	ctx := context.Background()
	other := plan("vm-two")
	otherRow := Placement{Namespace: other.Namespace, PodName: "other-runner", PodUID: other.RunnerPodUID,
		VMName: other.VMName, VMUID: other.Request.Key.Instance.VMUID, Incarnation: other.Incarnation,
		Managed: true, MemoryBytes: other.BeforeBytes, HostOverheadBytes: other.HostOverheadBytes}
	n, _, err := store.HoldPlacement(ctx, n, otherRow)
	require.NoError(t, err)
	n, _, err = store.Reserve(ctx, n, other)
	require.NoError(t, err)
	n, _, err = store.Reserve(ctx, n, first)
	require.NoError(t, err)
	n, prior, err := store.SealPrepared(ctx, n, preparedClosure(first, 10))
	require.NoError(t, err)
	second := first
	second.Request.Key.Revision++
	second.Request.Key.OperationID.Low++
	second.Request.TargetVMMemoryBytes = 3 << 30
	n, _, err = store.Reserve(ctx, n, second)
	require.NoError(t, err)
	ref := budget.ObjectRef{Name: "second-original-host", UID: "second-original-host-uid"}
	n, err = store.BindClaim(ctx, n, second, ref)
	require.NoError(t, err)
	n, latest, err := store.SealPrepared(ctx, n, preparedClosure(second, 20))
	require.NoError(t, err)
	future := second
	future.Request.Key.Revision++
	future.Request.Key.OperationID.Low++
	future.Request.TargetVMMemoryBytes = 4 << 30
	n, _, err = store.Reserve(ctx, n, future)
	require.NoError(t, err)
	beforeRow, err := n.State().Placement(first.RunnerPodUID)
	require.NoError(t, err)
	_, _, err = store.RetireSealedHolds(ctx, n, prior)
	require.Error(t, err, "旧屏障不能代替最新完整水位")
	changed := latest.Clone()
	changed.Held.Claim.UID = "replacement-uid"
	_, _, err = store.RetireSealedHolds(ctx, n, changed)
	require.Error(t, err)
	// 完整VM/RetainedCoverage/Runner认证由生产适配负责；本测试显式模拟该
	// 可信调用点，只核Node CAS不删除较新/无关来源及不降低原placement。
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	unknownSnapshot, removed, err := store.RetireSealedHolds(ctx, n, latest)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	require.Nil(t, unknownSnapshot)
	require.Nil(t, removed, "未知退休不返回可据此删CM的来源列表")
	rebuilt, err := store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, rebuilt.State().Holds, 2)
	_, err = rebuilt.Hold(first)
	require.Error(t, err)
	_, err = rebuilt.Hold(second)
	require.Error(t, err)
	_, err = rebuilt.Hold(future)
	require.NoError(t, err)
	_, err = rebuilt.Hold(other)
	require.NoError(t, err)
	afterRow, err := rebuilt.State().Placement(first.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, beforeRow, afterRow, "Initial前缀退休保留原placement和完整Barrier")
	version := rebuilt.ResourceVersion()
	rebuilt, removed, err = store.RetireSealedHolds(ctx, rebuilt, latest)
	require.NoError(t, err)
	require.Empty(t, removed)
	require.Equal(t, version, rebuilt.ResourceVersion())
	_, _, err = store.Reserve(ctx, rebuilt, first)
	require.ErrorIs(t, err, ErrSealed)
	alias := second
	alias.Request.Key.OperationID.Low++
	_, _, err = store.Reserve(ctx, rebuilt, alias)
	require.ErrorIs(t, err, ErrSealed)
}

func TestPG17UP1BarrierCodecCloneAndSourceDrift(t *testing.T) {
	t.Log("PG17-U-P1-005: v9严格格式/旧格式拒绝/独立深拷贝/来源漂移")
	_, store, n, p := preparedBarrierFixture(t)
	ctx := context.Background()
	oldRaw, err := json.Marshal(n.State())
	require.NoError(t, err)
	require.NotContains(t, string(oldRaw), "barrier", "无新字段保持原旧格式形状")
	n, _, err = store.Reserve(ctx, n, p)
	require.NoError(t, err)
	n, err = store.BindClaim(ctx, n, p, budget.ObjectRef{Name: "original-host", UID: "original-host-uid"})
	require.NoError(t, err)
	// 原Claim已经绑定后，幂等Reserve的返回值也必须是独立副本；不能
	// 只保护State/Hold/Barrier而留下最终封存来源的指针写入旁路。
	_, returned, err := store.Reserve(ctx, n, p)
	require.NoError(t, err)
	require.NotNil(t, returned.Claim)
	returned.Claim.UID = "caller-mutated-idempotent-reserve"
	held, err := n.Hold(p)
	require.NoError(t, err)
	require.Equal(t, "original-host-uid", held.Claim.UID)
	n, b, err := store.SealPrepared(ctx, n, preparedClosure(p, 10))
	require.NoError(t, err)
	require.Equal(t, "original-host-uid", b.Held.Claim.UID)
	raw, err := json.Marshal(n.State())
	require.NoError(t, err)
	var decoded Model
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, n.State(), decoded)
	for _, version := range []uint32{1, 2, 3, 4, 5, 6, 7, 8, 10} {
		changed := n.State()
		changed.ProtocolVersion = version
		changedRaw, err := json.Marshal(changed)
		require.NoError(t, err)
		require.Error(t, json.Unmarshal(changedRaw, &decoded), "Node版本%d不能吞掉新屏障", version)
	}
	require.NoError(t, json.Unmarshal(oldRaw, &decoded), "原v3仍可读取")
	barrierRaw, err := json.Marshal(b)
	require.NoError(t, err)
	var restored budget.HostReservationBarrier
	require.NoError(t, json.Unmarshal(barrierRaw, &restored))
	require.Equal(t, b, restored)
	unknownField := strings.Replace(string(barrierRaw), "{", "{\"injected\":1,", 1)
	require.Error(t, json.Unmarshal([]byte(unknownField), &restored))
	duplicateField := strings.Replace(string(barrierRaw), "{", "{\"protocol_version\":1,", 1)
	require.Error(t, json.Unmarshal([]byte(duplicateField), &restored))
	for _, mode := range []string{"source", "node-uid", "lease-uid", "sequence", "host-owner", "vm-owner", "boot", "claim", "namespace", "version"} {
		t.Run(mode, func(t *testing.T) {
			m := n.State()
			proof := (*m.Placements)[0].Barrier
			switch mode {
			case "source":
				proof.Held.SourceID = strings.Repeat("d", 64)
			case "node-uid":
				proof.NodeUID = "recreated-node"
			case "lease-uid":
				proof.LeaseUID = "recreated-lease"
			case "sequence":
				proof.NodeSequence = m.Sequence + 1
			case "host-owner":
				proof.HostOwner.Epoch = m.LeaseFloor.Epoch + 1
			case "vm-owner":
				proof.VMOwner.Epoch = p.Request.Key.IssuerEpoch - 1
			case "boot":
				proof.Request.Key.Instance.PGBootID++
			case "claim":
				m.Holds[0].Claim.UID = "same-name-recreated-cm"
			case "namespace":
				proof.Namespace = "foreign"
			case "version":
				proof.ProtocolVersion++
			}
			require.Error(t, m.Validate())
		})
	}
	changedPlan := p
	changedPlan.Request.TargetBuffers++
	_, err = n.PreparedBarrier(changedPlan)
	require.Error(t, err)
	copyBarrier := b.Clone()
	copyBarrier.Held.Claim.Name = "caller-mutation"
	require.NotEqual(t, b, copyBarrier)
	require.NoError(t, n.State().Validate())
}

func TestPG17UP1SealRejectsChangedNodeLeaseAndOwner(t *testing.T) {
	t.Log("PG17-U-P1-006: Node/Lease UID、旧RV、失租及CAS中途变化均拒绝且保留原持有")
	for _, mode := range []string{"node-rv", "node-uid", "lease-uid", "expired", "owner", "cas-rv"} {
		t.Run(mode, func(t *testing.T) {
			f, store, n, p := preparedBarrierFixture(t)
			ctx := context.Background()
			n, _, err := store.Reserve(ctx, n, p)
			require.NoError(t, err)
			before := n.State()
			f.mu.Lock()
			switch mode {
			case "node-rv":
				f.node.ResourceVersion = "new-node-rv"
			case "node-uid":
				f.node.UID = "same-name-new-node"
			case "lease-uid":
				f.lease.UID = "same-name-new-lease"
			case "expired":
				stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
				f.lease.Spec.RenewTime = &stamp
			case "owner":
				owner := "replacement-owner"
				transitions := int32(1)
				f.lease.Spec.HolderIdentity, f.lease.Spec.LeaseTransitions = &owner, &transitions
			case "cas-rv":
				f.beforePatch = func() { f.mu.Lock(); f.node.ResourceVersion = "changed-after-authorization"; f.mu.Unlock() }
			}
			f.mu.Unlock()
			result, b, err := store.SealPrepared(ctx, n, preparedClosure(p, 10))
			require.Error(t, err)
			require.Nil(t, result)
			require.Empty(t, b.NodeOrigin)
			var after Model
			require.NoError(t, json.Unmarshal([]byte(barrierNode(f).Annotations[LedgerAnnotation]), &after))
			require.Equal(t, before, after, "拒绝来源不得清Hold/重置账本")
		})
	}
	t.Run("takeover", func(t *testing.T) {
		f, oldStore, n, p := preparedBarrierFixture(t)
		ctx := context.Background()
		n, first, err := oldStore.SealPrepared(ctx, n, preparedClosure(p, 10))
		require.NoError(t, err)
		f.mu.Lock()
		holder, transitions := "replacement-owner", int32(1)
		f.lease.Spec.HolderIdentity, f.lease.Spec.LeaseTransitions = &holder, &transitions
		f.mu.Unlock()
		next := f.store(t, holder)
		n, err = next.Load(ctx)
		require.NoError(t, err)
		n, err = next.Fence(ctx, n)
		require.NoError(t, err)
		read, err := n.PreparedBarrier(p)
		require.NoError(t, err)
		require.Equal(t, first, read, "接管只抬floor，不伪造新封存来源")
		_, _, err = oldStore.Reserve(ctx, n, p)
		require.Error(t, err)
		future := p
		future.Request.Key.Revision++
		future.Request.Key.OperationID.Low++
		n, second, err := next.SealPrepared(ctx, n, preparedClosure(future, 20))
		require.NoError(t, err)
		require.EqualValues(t, 2, second.HostOwner.Epoch)
		require.EqualValues(t, 9, n.State().ProtocolVersion)
		_, _, err = next.SealPrepared(ctx, n, preparedClosure(p, 10))
		require.Error(t, err)
	})
}

func TestPG17UP1KeyedPlacementHonorsClosedThroughWithoutSeal(t *testing.T) {
	t.Log("PG17-U-P1-007: 无Barrier时已有ClosedThrough也必须封住无Key增量和旧Key旁路")
	f, store, n, _, current, closed := settlementFixture(t)
	ctx := context.Background()
	extended := closed
	extended.ThroughKey = current.Request.Key
	extended.ThroughKey.Revision++
	extended.ThroughKey.OperationID.Low++
	extended.ThroughSequence = closed.BudgetSequence + 10
	n, _, err := store.Settle(ctx, n, current.RunnerPodUID, extended)
	require.NoError(t, err)
	row, err := n.State().Placement(current.RunnerPodUID)
	require.NoError(t, err)
	protectBarrierPod(f, row)
	input := row
	input.Settled = nil
	writer := f.store(t, "another-scheduler-replica")
	same, held, err := writer.HoldPlacement(ctx, n, input)
	require.NoError(t, err)
	require.Equal(t, n.ResourceVersion(), same.ResourceVersion())
	require.Equal(t, row.Settled, held.Settled)
	input.MemoryBytes++
	_, _, err = writer.HoldPlacement(ctx, n, input)
	require.Error(t, err)
	input.MemoryBytes = row.MemoryBytes
	_, _, err = writer.HoldPlacementFor(ctx, n, input, current.Request.Key)
	require.ErrorIs(t, err, ErrSettled)
	alias := extended.ThroughKey
	alias.OperationID.Low++
	_, _, err = writer.HoldPlacementFor(ctx, n, input, alias)
	require.ErrorIs(t, err, ErrSettled)
	future := extended.ThroughKey
	future.Revision++
	future.OperationID.Low++
	input.MemoryBytes = 2 << 30
	newer, held, err := writer.HoldPlacementFor(ctx, n, input, future)
	require.NoError(t, err)
	require.Equal(t, row.Settled, held.Settled)
	require.Equal(t, input.MemoryBytes, held.MemoryBytes)
	require.Equal(t, n.State().ProtocolVersion, newer.State().ProtocolVersion, "未接Barrier无需污染旧版本形状")
}
