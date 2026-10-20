package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestLegacyControllerRejectsManagedBeforeCPUOrQMP(t *testing.T) {
	for _, annotations := range []map[string]string{{control.ModeAnnotation: "bad"}, {control.JournalAnnotation: ""}} {
		vm := defaultVm()
		vm.Annotations = annotations
		vm.Status.Phase = vmv1.VmScaling
		// 不提供client/QMP，若先触碰旧副作用路径会失败或panic，不能伪造成功。
		var reconciler VMReconciler
		require.ErrorContains(t, reconciler.doReconcile(context.Background(), vm), "fenced scaling controller")
		_, err := QmpSetVirtioMem(vm, 0)
		require.ErrorContains(t, err, "fenced Runner")
	}
}

func TestVMWebhookDebugExceptionCannotEraseManagedMarker(t *testing.T) {
	old := defaultVm()
	old.Annotations = map[string]string{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: "opaque"}
	next := old.DeepCopy()
	next.Annotations = nil
	// DSGA标记保护必须先于cfg/旧调试例外。空Config若被先访问将panic，直接证明入口顺序。
	var webhook VMWebhook
	_, err := webhook.ValidateUpdate(context.Background(), old, next)
	require.ErrorContains(t, err, "mode/journal")
}
