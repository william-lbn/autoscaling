package hostscheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestPodProtectionReportsPermissionFailureWhenOriginalRemainsUnprotected(t *testing.T) {
	for _, observed := range []bool{false, true} {
		name := "permission_denied"
		if observed {
			name = "concurrent_protection_observed"
		}
		t.Run(name, func(t *testing.T) {
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "cache-fixture", Namespace: "test", UID: "original-pod", ResourceVersion: "1",
			}}
			client := kubefake.NewSimpleClientset(pod)
			client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if observed {
					// 原拒绝期间已有另一操作者提交相同保护；以同 UID GET 的已见事实为准。
					current := pod.DeepCopy()
					current.ResourceVersion = "2"
					current.Finalizers = []string{hostguard.PodProtectionFinalizer}
					require.NoError(t, client.Tracker().Update(action.GetResource(), current, pod.Namespace))
				}
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"},
					pod.Name, errors.New("private Kubernetes authorization detail"))
			})
			service := &Service{core: client.CoreV1()}
			actual, err := service.protectPod(context.Background(), pod)
			if observed {
				require.NoError(t, err)
				require.Equal(t, pod.UID, actual.UID)
				require.Contains(t, actual.Finalizers, hostguard.PodProtectionFinalizer)
				return
			}
			require.Nil(t, actual)
			require.ErrorContains(t, err, "scheduler lacks permission to persist Pod protection")
			require.NotContains(t, err.Error(), "private Kubernetes authorization detail")
		})
	}
}
