package journal

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// HTTP GET仍每次执行；即使原文命中缓存，也必须采用本次RV、删除状态、
// bootstrap和身份。完整再关闭/退休模型用于覆盖深层未知证据，不能用
// 小空模型通过来推定实际生命周期的隔离性。
func TestStoreValidatedDecodeCacheNeverCachesCurrentVMOrErrors(t *testing.T) {
	api, store := newStoreAPI(t)
	_ = initializeStore(t, api, store)
	m, before := retryReopenModel(t, "unknown-after")
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	m, err := m.RetryServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	_, wire, err := encode(m, MaxEncodedBytes)
	require.NoError(t, err)
	api.change(func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire })
	first, err := store.Load(context.Background())
	require.NoError(t, err)
	api.change(func(vm *vmv1.VirtualMachine) { vm.Status.PodName = "current-runner" })
	current, err := store.Load(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, first.ResourceVersion(), current.ResourceVersion())
	require.Equal(t, "current-runner", current.VirtualMachine().Status.PodName)
	require.Equal(t, first.State(), current.State())
	good := current.VirtualMachine()
	for name, change := range map[string]func(*vmv1.VirtualMachine){
		"uid":             func(vm *vmv1.VirtualMachine) { vm.UID = "recreated" },
		"incarnation":     func(vm *vmv1.VirtualMachine) { vm.Annotations[control.ModeAnnotation] = "changed" },
		"bootstrap":       func(vm *vmv1.VirtualMachine) { vm.Spec.DSGABootstrap.Incarnation = "changed" },
		"compute":         func(vm *vmv1.VirtualMachine) { vm.Spec.DSGABootstrap.ComputeInstanceID = "changed" },
		"missing-journal": func(vm *vmv1.VirtualMachine) { delete(vm.Annotations, control.JournalAnnotation) },
		"changed-bytes":   func(vm *vmv1.VirtualMachine) { vm.Annotations[control.JournalAnnotation] = wire + "!" },
	} {
		t.Run(name, func(t *testing.T) {
			// 故意保留同RV，验证不能以RV相同替代原文字节/当前元数据检查。
			bad := good.DeepCopy()
			change(bad)
			api.mu.Lock()
			api.vm = bad
			api.mu.Unlock()
			_, err := store.Load(context.Background())
			require.Error(t, err)
			_, err = Inspect(bad, store.Binding())
			require.Error(t, err, "无缓存Inspect必须同样拒绝")
			api.mu.Lock()
			api.vm = good.DeepCopy()
			api.mu.Unlock()
			restored, err := store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, m, restored.State(), "失败不能污染或投影成空闲模型")
		})
	}
	newer, err := m.Takeover(m.Owner, "new-cache-owner")
	require.NoError(t, err)
	_, changed, err := encode(newer, MaxEncodedBytes)
	require.NoError(t, err)
	api.mu.Lock()
	api.vm.Annotations[control.JournalAnnotation] = changed
	api.mu.Unlock()
	updated, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, newer, updated.State(), "同RV的不同有效原文也必须完整重解码")
}

// 缓存不共享可变模型：不同合法原文并发命中/替换时，各返回值仍独占
// 完整Context、PriorClose、原预算/未知和数组。race检测同时核缓存锁与
// Snapshot.Clone的深层隔离，不引入跨Store全局缓存或无界键集合。
func TestStoreValidatedDecodeCacheConcurrentRetirementIsolation(t *testing.T) {
	_, store := newStoreAPI(t)
	m, before := retryReopenModel(t, "unknown-after")
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	m, err := m.RetryServiceReopen(m.Owner, selector, before)
	require.NoError(t, err)
	next, err := m.Takeover(m.Owner, "concurrent-cache-owner")
	require.NoError(t, err)
	models := []Model{m, next}
	wires := make([]string, len(models))
	for i := range models {
		_, wires[i], err = encode(models[i], MaxEncodedBytes)
		require.NoError(t, err)
	}
	var group sync.WaitGroup
	errors := make(chan error, 16)
	for i := range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			for turn := range 4 {
				index := (i + turn) % len(models)
				loaded, err := store.decodeModel(wires[index])
				if err != nil {
					errors <- err
					return
				}
				if !reflect.DeepEqual(models[index], loaded) {
					errors <- fmt.Errorf("parallel decode changed original model")
					return
				}
				loaded.ServiceClosure.Reopen.Retired.Context[0] ^= 1
				loaded.ServiceClosure.Reopen.PriorClose.Revision++
				loaded.InitialHeld.Admission.Command[0] ^= 1
				if _, err := store.decodeModel(""); err == nil {
					errors <- fmt.Errorf("missing history reused cached model")
					return
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	for i := range models {
		loaded, err := store.decodeModel(wires[i])
		require.NoError(t, err)
		require.Equal(t, models[i], loaded)
	}
}
