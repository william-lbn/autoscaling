package journal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 验证真正HTTP/clientset/RFC6902 UID-RV条件写；Runner/预算真实性不由此
// 低层测试证明。选择只改同VM元数据，原完整命令/步骤/序列均不变。
func TestVMReplaySelectionUsesOriginalCASWithoutRewritingJournal(t *testing.T) {
	for _, mode := range []string{"selected", "lost-reply", "stale-version", "wrong-owner", "wrong-key", "wrong-digest", "cancelled", "already-settled"} {
		t.Run(mode, func(t *testing.T) {
			f, store := newStoreAPI(t)
			s := initializeStore(t, f, store)
			ctx := context.Background()
			actor := s.State().Owner
			req := testRequest(s.State(), 1, control.Grow)
			var err error
			s, _, err = store.Begin(ctx, s, actor, req, 0)
			require.NoError(t, err)
			for _, action := range []Action{PrepareBudget, ReserveHost} {
				var dispatch *Dispatch
				s, dispatch, err = store.Start(ctx, s, actor, req.Key, action, "prior-domain", testCommand)
				require.NoError(t, err)
				s, err = store.Record(ctx, s, actor, req.Key, observation(s.State(), dispatch.Step, Completed))
				require.NoError(t, err)
			}
			var dispatch *Dispatch
			s, dispatch, err = store.Start(ctx, s, actor, req.Key, ResizeVM, "runner", testCommand)
			require.NoError(t, err)
			choice := VMReplaySelection{ProtocolVersion: 1, Key: req.Key, Owner: actor, Sources: runnerSources(), Nonce: strings.Repeat("7", 32)}
			choice.Sources.JournalSHA256 = dispatch.Step.CommandSHA256
			switch mode {
			case "lost-reply":
				f.mu.Lock()
				f.failAfterCommit = true
				f.mu.Unlock()
			case "stale-version":
				f.change(func(vm *vmv1.VirtualMachine) { vm.Labels = map[string]string{"concurrent": "change"} })
			case "wrong-owner":
				choice.Owner.HolderID = "not-current-owner"
			case "wrong-key":
				choice.Key.OperationID.Low++
			case "wrong-digest":
				choice.Sources.JournalSHA256 = strings.Repeat("f", 64)
			case "cancelled":
				s, err = store.RequestAbort(ctx, s, actor, req.Key)
				require.NoError(t, err)
			case "already-settled":
				s, err = store.Record(ctx, s, actor, req.Key, observation(s.State(), dispatch.Step, Completed))
				require.NoError(t, err)
			}
			original := s.VirtualMachine().Annotations[control.JournalAnnotation]
			result, err := store.SelectVMReplay(ctx, s, actor, choice)
			if mode == "selected" {
				require.NoError(t, err)
				require.Equal(t, s.State(), result.State())
				again, err := store.SelectVMReplay(ctx, result, actor, choice)
				require.NoError(t, err)
				require.Equal(t, result.ResourceVersion(), again.ResourceVersion(), "重复原选择只读确认")
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			current, err := store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, original, current.VirtualMachine().Annotations[control.JournalAnnotation])
			if mode == "selected" || mode == "lost-reply" {
				var got VMReplaySelection
				require.NoError(t, json.Unmarshal([]byte(current.VirtualMachine().Annotations[VMReplayAnnotation]), &got))
				require.Equal(t, choice, got)
			} else {
				require.Empty(t, current.VirtualMachine().Annotations[VMReplayAnnotation])
			}
		})
	}
}

func TestVMReplaySelectionStrictWireRejectsAmbiguousChoice(t *testing.T) {
	v := VMReplaySelection{ProtocolVersion: 1, Key: testRequest(testModel(t), 1, control.Grow).Key, Owner: testModel(t).Owner, Sources: runnerSources(), Nonce: strings.Repeat("8", 32)}
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	for _, bad := range []string{string(raw[:len(raw)-1]) + `,"nonce":"11111111111111111111111111111111"}`, string(raw[:len(raw)-1]) + `,"implicit_retry":true}`, strings.Replace(string(raw), v.Nonce, strings.Repeat("0", 32), 1), strings.Repeat(" ", 2049) + string(raw)} {
		_, err := ParseVMReplaySelection(bad)
		require.Error(t, err)
	}
}

// 关门是独立业务栅栏；即使原owner、原Key及完整重放摘要仍相同，也不能
// 利用旧not_sent提示重新取得派发资格。这里只检查Store条件写，不证明Runner。
func TestVMReplaySelectionRejectsClosedOriginalOperation(t *testing.T) {
	api, store := newStoreAPI(t)
	_ = initializeStore(t, api, store)
	m, closed, fact := activeClosureModel(t, control.Grow, ResizeVM, true)
	var err error
	m, err = m.StartActiveServiceClosure(m.Owner, closed)
	require.NoError(t, err)
	m, err = m.RecordServiceClosure(m.Owner, fact, false)
	require.NoError(t, err)
	_, raw, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = raw })
	s, err := store.Load(context.Background())
	require.NoError(t, err)
	choice := VMReplaySelection{ProtocolVersion: 1, Key: m.Active.Request.Key, Owner: m.Owner, Sources: runnerSources(), Nonce: strings.Repeat("9", 32)}
	for _, step := range m.Active.Steps {
		if step.Action == ResizeVM {
			choice.Sources.JournalSHA256 = step.CommandSHA256
		}
	}
	result, err := store.SelectVMReplay(context.Background(), s, m.Owner, choice)
	require.Error(t, err)
	require.Nil(t, result)
	current, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, s.State(), current.State())
	require.Equal(t, s.ResourceVersion(), current.ResourceVersion())
	require.Empty(t, current.VirtualMachine().Annotations[VMReplayAnnotation])
}
