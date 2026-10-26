package hostguard

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func TestNodeLedgerPublishedClaimKeepsOriginalUIDAndIndependentCopies(t *testing.T) {
	f, store := newNodeAPI(t)
	ctx := context.Background()
	n, err := store.Initialize(ctx, "node-rv-1", 1<<30)
	require.NoError(t, err)
	p := plan("vm-one")
	n, held, err := store.Reserve(ctx, n, p)
	require.NoError(t, err)
	ref := budget.ObjectRef{Name: "original-host", UID: "original-host-uid"}
	require.ErrorIs(t, InspectClaim(n.node, p, held.SourceID, ref), ErrUnpublished, "只有容量，没有原CM UID时等待发布，不得消费")
	n, err = store.BindClaim(ctx, n, p, ref)
	require.NoError(t, err)
	require.NoError(t, InspectClaim(n.node, p, held.SourceID, ref))
	require.EqualValues(t, 2, n.State().ProtocolVersion)
	copyModel := n.State()
	copyModel.Holds[0].Claim.UID = "changed-by-caller"
	copyHold, err := n.Hold(p)
	require.NoError(t, err)
	copyHold.Claim.UID = "also-changed-by-caller"
	require.NoError(t, InspectClaim(n.node, p, held.SourceID, ref), "返回模型/持有不能共享可改引用")
	patches := f.patches
	retry, err := store.BindClaim(ctx, n, p, ref)
	require.NoError(t, err)
	require.Equal(t, n.ResourceVersion(), retry.ResourceVersion())
	require.Equal(t, patches, f.patches, "相同原发布只读确认")
	_, err = store.BindClaim(ctx, n, p, budget.ObjectRef{Name: ref.Name, UID: "replacement-uid"})
	require.Error(t, err)
	legacy := n.State()
	legacy.ProtocolVersion = 1
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	var decoded Model
	require.Error(t, json.Unmarshal(raw, &decoded), "不能将v2发布投影为v1容量声明")
}

func TestNodeLedgerPublicationLostReplyAndSourceFailuresNeverRebind(t *testing.T) {
	f, store := newNodeAPI(t)
	ctx := context.Background()
	n, err := store.Initialize(ctx, "node-rv-1", 1<<30)
	require.NoError(t, err)
	p := plan("vm-one")
	n, held, err := store.Reserve(ctx, n, p)
	require.NoError(t, err)
	ref := budget.ObjectRef{Name: "original-host", UID: "original-host-uid"}
	f.loseReply = true
	unconfirmed, err := store.BindClaim(ctx, n, p, ref)
	require.Error(t, err)
	require.Nil(t, unconfirmed, "CAS回复未知不得返回发布票据")
	n, err = store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, InspectClaim(n.node, p, held.SourceID, ref), "实际已持久UID可由新读取恢复")
	for _, mode := range []string{"different-claim-uid", "different-source", "different-before", "node-recreated", "ledger-missing", "origin-missing", "protection-missing", "capacity-reduced"} {
		t.Run(mode, func(t *testing.T) {
			node, request, source, claim := n.node.DeepCopy(), p, held.SourceID, ref
			switch mode {
			case "different-claim-uid":
				claim.UID = "new-claim-uid"
			case "different-source":
				source = "new-source"
			case "different-before":
				request.BeforeBytes++
			case "node-recreated":
				node.UID = "new-node-uid"
			case "ledger-missing":
				delete(node.Annotations, LedgerAnnotation)
			case "origin-missing":
				delete(node.Annotations, OriginAnnotation)
			case "protection-missing":
				node.Finalizers = nil
			case "capacity-reduced":
				node.Status.Allocatable = nil
			}
			require.Error(t, InspectClaim(node, request, source, claim))
			require.NoError(t, InspectClaim(n.node, p, held.SourceID, ref), "反向读取不得改变/清空原持有")
		})
	}
}
