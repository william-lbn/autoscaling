package qualificationissuer

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// 复用原Root UDS/Runner/Lease/完整Capture协议peer，不冒充真实Guest资格。
func issuerPodFloorFixture(t *testing.T) (*Issuer, *kubefake.Clientset, Config, Bundle, *atomic.Int32) {
	t.Helper()
	issuer, client, cfg, bundle, requests := issuerCaptureFixture(t, false)
	var report Report
	require.NoError(t, json.Unmarshal(bundle.Report, &report))
	name := report.Qualification.RunnerPodName
	pod, err := client.CoreV1().Pods(cfg.Binding.Namespace).Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(t, err)
	pod.Spec.Containers = []core.Container{{Name: "runner", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("11Gi")}}}}
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	node, err := client.CoreV1().Nodes().Get(context.Background(), cfg.NodeName, metav1.GetOptions{})
	require.NoError(t, err)
	var model hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &model))
	model.ProtocolVersion = 10
	require.Len(t, *model.Placements, 1)
	(*model.Placements)[0].PodMemoryLimitBytes = 11 << 30
	require.NoError(t, model.Validate())
	raw, err := json.Marshal(model)
	require.NoError(t, err)
	node.Annotations[hostguard.LedgerAnnotation] = string(raw)
	node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("24Gi")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
	// 原fixture的list闭包冻结了旧Pod；此独立测试只让typed list读同tracker原对象。
	client.PrependReactor("list", "pods", func(kt.Action) (bool, runtime.Object, error) {
		obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("pods"), cfg.Binding.Namespace, name)
		if err != nil {
			return true, nil, err
		}
		return true, &core.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "floor-pod-inventory-1"}, Items: []core.Pod{*obj.(*core.Pod).DeepCopy()}}, nil
	})
	return issuer, client, cfg, bundle, requests
}

func TestIssuerPodFloorCaptureUsesOriginalElevenGiPod(t *testing.T) {
	issuer, client, cfg, bundle, requests := issuerPodFloorFixture(t)
	result, err := issuer.Publish(context.Background(), bundle)
	require.NoError(t, err)
	require.EqualValues(t, 3, requests.Load(), "仍须两次写前与一次写后真实Root读回")
	reader, err := budgetclaims.New(client.CoreV1(), cfg.Binding)
	require.NoError(t, err)
	_, err = reader.Qualification(context.Background(), result.Source)
	require.NoError(t, err)
	for _, action := range client.Actions() {
		require.NotContains(t, []string{"delete", "patch", "update"}, action.GetVerb(), "采集不能修Node/Pod来迎合报告")
	}
}

func TestIssuerPodFloorCaptureRejectsMissingDriftAndRecreatedPod(t *testing.T) {
	for _, mode := range []string{"missing", "drift", "uid"} {
		t.Run(mode, func(t *testing.T) {
			issuer, client, cfg, bundle, _ := issuerPodFloorFixture(t)
			var report Report
			require.NoError(t, json.Unmarshal(bundle.Report, &report))
			if mode == "missing" {
				node, err := client.CoreV1().Nodes().Get(context.Background(), cfg.NodeName, metav1.GetOptions{})
				require.NoError(t, err)
				var model hostguard.Model
				require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &model))
				(*model.Placements)[0].PodMemoryLimitBytes = 0
				raw, err := json.Marshal(model)
				require.NoError(t, err)
				node.Annotations[hostguard.LedgerAnnotation] = string(raw)
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
			} else {
				pod, err := client.CoreV1().Pods(cfg.Binding.Namespace).Get(context.Background(), report.Qualification.RunnerPodName, metav1.GetOptions{})
				require.NoError(t, err)
				if mode == "drift" {
					pod.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
				} else {
					pod.UID = "replacement-floor-pod"
				}
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
			}
			_, err := issuer.Publish(context.Background(), bundle)
			require.Error(t, err)
			for _, action := range client.Actions() {
				require.NotEqual(t, "create", action.GetVerb(), "缺floor或原Pod漂移须在资格CREATE前拒绝")
			}
		})
	}
}
