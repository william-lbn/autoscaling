package state

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestHostCPUViewRetainsMaximumWhileGuestTargetChanges(t *testing.T) {
	node := NodeStateFromParams("node", 4000, 8<<30, 0.8, map[string]string{})
	pod := Pod{UID: "original", NamespacedName: util.NamespacedName{Namespace: "ns", Name: "pod"},
		HostCPUAccountingVersion: 1, HostCommittedCPU: 2000,
		CPU: PodResources[vmv1.MilliCPU]{Reserved: 500, Requested: 500, Factor: 250, Overcommit: resource.NewQuantity(4, resource.DecimalSI)},
		Mem: PodResources[api.Bytes]{Reserved: 1 << 30, Requested: 1 << 30, Overcommit: resource.NewQuantity(1, resource.DecimalSI)}}
	node.AddPod(pod)
	require.EqualValues(t, 2000, node.CPU.Reserved)
	require.EqualValues(t, 500, pod.CPU.Reserved, "不得把Host固定承诺反投影成Guest目标")
	pod.CPU.Requested = 1500
	require.True(t, node.ReconcilePodReserved(&pod))
	require.EqualValues(t, 2000, node.CPU.Reserved)
	require.EqualValues(t, 1500, pod.CPU.Reserved)
	pod.CPU.Requested = 500
	require.True(t, node.ReconcilePodReserved(&pod))
	require.EqualValues(t, 2000, node.CPU.Reserved, "Guest shrink不归还Node固定包络")
	require.True(t, node.RemovePod(pod.UID))
	require.Zero(t, node.CPU.Reserved, "本地删除不是物理释放；实际退还仍需原Host退出协议")
}
