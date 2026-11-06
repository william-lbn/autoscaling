package budgetclaims

import (
	"context"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"math"
	"strings"
	"testing"
)

func TestHostNotPublishedMarksOnlyInitialGetAbsence(t *testing.T) {
	for _, mode := range []string{"missing", "disappeared-after-first-get", "other-unknown", "forbidden", "bad-source"} {
		t.Run(mode, func(t *testing.T) {
			binding := journal.Binding{Namespace: "test-ns", Name: "test-vm", VMUID: types.UID("test-vm-uid"), Incarnation: strings.Repeat("a", 32)}
			request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: string(binding.VMUID), ComputeInstanceID: "test-compute", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 2}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 2 << 30, DeadlineUnixMillis: control.Uint64(math.MaxUint64), BudgetCommitmentID: "test-budget"}
			require.NoError(t, request.Validate())
			name, err := HostClaimName(binding, request)
			require.NoError(t, err)
			core := kubefake.NewSimpleClientset()
			reader, err := New(core.CoreV1(), binding)
			require.NoError(t, err)
			count := 0
			if mode != "missing" {
				core.PrependReactor("get", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
					count++
					switch mode {
					case "disappeared-after-first-get":
						if count == 1 {
							return true, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: binding.Namespace, UID: types.UID("original-host-uid")}}, nil
						}
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
					case "other-unknown":
						return true, nil, resourceguard.ErrUnknown
					case "forbidden":
						return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, name, errors.New("fixed denied"))
					case "bad-source":
						return true, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: binding.Namespace, UID: types.UID("original-host-uid")}}, nil
					}
					return false, nil, nil
				})
			}
			_, err = reader.HostReference(context.Background(), request)
			require.Error(t, err)
			if mode == "missing" {
				require.ErrorIs(t, err, ErrHostNotPublished)
				require.ErrorIs(t, err, resourceguard.ErrUnknown)
			} else {
				require.False(t, errors.Is(err, ErrHostNotPublished), "other missing/source failures must not become normal wait")
			}
			if mode == "disappeared-after-first-get" {
				require.Equal(t, 2, count)
				require.True(t, apierrors.IsNotFound(err))
			}
			if mode == "other-unknown" {
				require.ErrorIs(t, err, resourceguard.ErrUnknown)
			}
			if mode == "forbidden" {
				require.True(t, apierrors.IsForbidden(err))
			}
		})
	}
}
