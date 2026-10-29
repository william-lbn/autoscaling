package hostscheduler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 真实Start/Wait夹具在此只提供完整原Birth；移除API中的Wait来源是明确
// 故障注入，验证消费者面对缺失Wait的行为。实际Runner父进程被杀、没有
// 本地Wait事实的窗口由vmguard独立子进程测试覆盖，不把两者混称完整集群。
func TestSchedulerMissingWaitUsesOnlyOriginalCompletePodTermination(t *testing.T) {
	for _, mode := range []string{"complete", "late-wait", "source-replaced", "source-missing", "corrupt-wait"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, claims, pod := retiredRunnerFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			cms, err := claims.CoreV1().ConfigMaps(pod.Namespace).List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			var wait *core.ConfigMap
			for i := range cms.Items {
				if cms.Items[i].Labels[vmguard.ClaimLabel] == "process-exit-v1" {
					wait = cms.Items[i].DeepCopy()
				}
			}
			require.NotNil(t, wait)
			if mode == "corrupt-wait" {
				wait.Data[vmguard.ClaimKey] = "{broken"
				require.NoError(t, claims.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), wait, wait.Namespace))
				before, err := d.store.Load(ctx)
				require.NoError(t, err)
				require.Error(t, s.retirePod(ctx, d, pod), "坏Wait来源不能降级")
				after, err := d.store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State())
				return
			}
			require.NoError(t, claims.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), wait.Namespace, wait.Name))
			if mode != "complete" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.LoseNodeReply = true })
				require.Error(t, s.retirePod(ctx, d, pod), "先保留Node CAS丢回复")
			}
			var selected hostguard.Retirement
			if mode != "complete" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					require.NoError(t, json.Unmarshal([]byte(f.Pods["testing/runner"].Annotations[hostguard.RetirementAnnotation]), &selected))
				})
				cm, err := claims.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, selected.RunnerExit.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, vmguard.StoppedPodKind, cm.Labels[vmguard.ClaimLabel])
				switch mode {
				case "source-replaced":
					cm.UID = types.UID("replacement-termination")
					require.NoError(t, claims.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				case "source-missing":
					require.NoError(t, claims.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name))
				}
				// 已选择整Pod来源后，即使原Wait迟到也不切换关闭来源来清未知。
				require.NoError(t, claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), wait, wait.Namespace))
			}
			err = s.retirePod(ctx, d, pod)
			if mode == "source-replaced" || mode == "source-missing" {
				require.Error(t, err)
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
					require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
				})
				return
			}
			require.NoError(t, err)
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Empty(t, *after.State().Placements)
			require.Empty(t, after.State().Holds)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				require.NotContains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
				require.NoError(t, hostguard.InspectRetiredPod(&f.Node, p))
				require.Equal(t, p.Annotations[hostguard.RetirementAnnotation], f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
			})
			require.NoError(t, s.retirePod(ctx, d, pod))
		})
	}
}
