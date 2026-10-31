package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// initializeManagedMemory在原QEMU启动前完成Runner唯一出生绑定。Kube端点/CA/
// token只取固定InClusterConfig，不接收HTTP调用者选择cluster或提供Bearer。
// 该入口没有普通QMP重试/旧spec缩容回退；已有出生或丢WAL必须受控恢复。
func initializeManagedMemory(spec *vmv1.VirtualMachineSpec, initial *vmguard.InitialBootstrapConfig) (*vmguard.Runner, error) {
	if spec.DSGABootstrap == nil {
		return nil, nil
	}
	if spec.CpuScalingMode == nil || *spec.CpuScalingMode != vmv1.CpuScalingModeSysfs {
		return nil, fmt.Errorf("DSGA Runner requires original sysfs CPU mode")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	config.Timeout = 5 * time.Second
	config.UserAgent = "neonvm-runner-dsga-memory"
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	vms, err := vmclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	b := spec.DSGABootstrap
	binding := journal.Binding{Namespace: os.Getenv("K8S_NAMESPACE"), Name: os.Getenv("DSGA_VM_NAME"), VMUID: types.UID(b.VMUID), Incarnation: b.Incarnation}
	store, err := journal.NewStore(vms.NeonvmV1().VirtualMachines(binding.Namespace), binding)
	if err != nil {
		return nil, err
	}
	// active是明确的新Runner安装目录，父目录由Controller原Pod emptyDir提供。
	// 只允许mkdir这个固定子目录；存在时保留原内容，Initialize会拒绝重新出生。
	if err := os.Mkdir(vmguard.FixedStateDir, 0o700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	slot, exact := spec.Guest.MemorySlotSize.AsInt64()
	if !exact || slot <= 0 {
		return nil, fmt.Errorf("DSGA Runner memory slot must be exact positive bytes")
	}
	minimum := spec.Guest.MemorySlots.Min
	maximum := spec.Guest.MemorySlots.Max
	if minimum < 1 || maximum < minimum || uint64(maximum) > uint64(^uint64(0)>>1)/uint64(slot) {
		return nil, fmt.Errorf("DSGA Runner memory geometry overflows")
	}
	return vmguard.Initialize(context.Background(), store, core.CoreV1(), core.CoordinationV1(), vmguard.Config{StateDir: vmguard.FixedStateDir, InitialBootstrap: initial, PodName: os.Getenv("K8S_POD_NAME"), PodUID: os.Getenv("K8S_POD_UID"), NodeName: os.Getenv("K8S_NODE_NAME"), MinimumBytes: control.Uint64(uint64(minimum) * uint64(slot)), MaximumBytes: control.Uint64(uint64(maximum) * uint64(slot))})
}
