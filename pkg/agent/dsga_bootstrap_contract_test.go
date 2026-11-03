package agent

import (
	"context"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// 原创建合同：managed nil bootstrap始终拒绝；不能因首启Owner接线给它新权限。
// 普通未受管nil仍返回原legacy选择。这里只验证模式门，不运行legacy执行器。
func TestDSGABootstrapNilPreservesOriginalCreationContract(t *testing.T) {
	for _, managed := range []bool{true, false} {
		name := "unmanaged-legacy"
		var annotations map[string]string
		if managed {
			name = "managed-missing-bootstrap"
			annotations = map[string]string{control.ModeAnnotation: "1:1234567890abcdef1234567890abcdef", control.JournalAnnotation: "not-decoded-before-bootstrap-gate"}
		}
		t.Run(name, func(t *testing.T) {
			runner, calls := modeRunner(t, annotations, false, false)
			core := kubefake.NewSimpleClientset()
			runner.global.kubeClient = core
			_, changed := util.NewCondChannelPair()
			selected, err := runner.runDSGA(context.Background(), zap.NewNop(), changed)
			require.Equal(t, managed, selected)
			if managed {
				require.ErrorContains(t, err, "VM bootstrap is held or invalid; legacy fallback forbidden")
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, calls.Load())
			leases, err := core.CoordinationV1().Leases("testing").List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			require.Empty(t, leases.Items)
			for _, action := range core.Actions() {
				require.False(t, action.GetVerb() == "create" && action.GetResource().Resource == "leases")
			}
			require.Nil(t, runner.executorStateDump)
		})
	}
}
