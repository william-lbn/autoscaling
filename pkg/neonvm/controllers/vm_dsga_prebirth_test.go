package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Pod已经GC的恢复场景仍须直接读原VM和来源；旧缓存中的合法锚点不能授权
// 清理迟到来源或改变过的日志。两轮失败都保留原受保护来源，不重建任何对象。
func TestManagedVMPreBirthClosureRereadsFrozenSourceAfterPodGC(t *testing.T) {
	for _, mode := range []string{"complete", "journal-changed", "birth-late", "state-late", "source-unknown", "anchor-changed", "vm-replaced"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			p, vm, node, sources := controllerRetirementFixture(t)
			var closed hostguard.Retirement
			require.NoError(t, json.Unmarshal([]byte(vm.Annotations[hostguard.VMRetirementAnnotation]), &closed))
			closed.ProtocolVersion = 3
			closed.RunnerBirth = nil
			closed.RunnerExit = nil
			closed.RunnerExitSHA = ""
			closed.PreBirth = &hostguard.PreBirthStop{VMJournalSHA256: vmguard.SourceDigest([]byte(vm.Annotations[control.JournalAnnotation]))}
			raw, err := json.Marshal(closed)
			require.NoError(t, err)
			vm.Annotations[hostguard.VMRetirementAnnotation] = string(raw)
			actual := vm.DeepCopy()
			switch mode {
			case "journal-changed":
				actual.Annotations[control.JournalAnnotation] = "changed-after-original-anchor"
			case "anchor-changed":
				actual.Annotations[hostguard.VMRetirementAnnotation] = "changed-after-original-cache-read"
			case "vm-replaced":
				actual.UID = "replacement"
			}
			// 保留已有outside/qualification来源作为实际GC观察对象；没有虚造birth。
			retained := sources[2]
			objects := []client.Object{actual, node, retained}
			binding := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation}
			names := vmguard.BirthSourceNames(binding)
			if mode == "birth-late" || mode == "state-late" {
				i := 0
				if mode == "state-late" {
					i = 1
				}
				objects = append(objects, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: vm.Namespace, Name: names[i], UID: "late-original-source"}})
			}
			c := fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if mode == "source-unknown" && (key.Name == names[0] || key.Name == names[1]) {
					return errors.New("original source read unavailable")
				}
				return c.Get(ctx, key, obj, opts...)
			}}).Build()
			p.r.Client, p.r.APIReader = c, c
			for range 2 {
				err := p.r.finalizeManagedMemory(ctx, vm)
				if mode == "complete" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					current := new(core.ConfigMap)
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(retained), current))
					require.Equal(t, retained.UID, current.UID)
					require.Equal(t, retained.Finalizers, current.Finalizers)
				}
			}
		})
	}
}
