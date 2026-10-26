package controllers

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

type bootstrapCountingClient struct {
	client.Client
	creates atomic.Int64
}

func (c *bootstrapCountingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.creates.Add(1)
	return c.Client.Create(ctx, obj, opts...)
}

func controllerBootstrapInputs(t *testing.T) (*vmv1.VirtualMachine, *vmv1.VirtualMachine, *corev1.Secret) {
	t.Helper()
	vm := defaultVm()
	vm.UID, vm.ResourceVersion = "vm-uid", "1"
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	clients := vmfake.NewSimpleClientset(vm)
	store, err := journal.NewStore(clients.NeonvmV1().VirtualMachines(vm.Namespace), journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: vm.Spec.DSGABootstrap.Incarnation})
	require.NoError(t, err)
	initial, err := store.Initialize(context.Background(), "1", "bootstrap-owner")
	require.NoError(t, err)
	ready, err := store.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	controller := true
	secret := new(corev1.Secret)
	immutable := true
	secret.Immutable = &immutable
	secret.Name, secret.Namespace, secret.UID, secret.ResourceVersion = vm.Name+"-config", vm.Namespace, "config-uid", "1"
	secret.OwnerReferences = []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller, BlockOwnerDeletion: nil}}
	// 只核验身份启动屏障，非预算或实际PG启动夹具。
	secret.Data = map[string][]byte{"config.json": []byte(`{"spec":{"dynamic_shared_buffers":{"protocol_version":2,"vm_uid":"vm-uid","compute_instance_id":"compute-1"}}}`)}
	return initial.VirtualMachine(), ready.VirtualMachine(), secret
}

func TestHeldBootstrapNeverAllocatesRunnerIPAMOrSecrets(t *testing.T) {
	p := newTestParams(t)
	vm := defaultVm()
	vm.UID = "vm-uid"
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	p.initVM(vm)
	count := new(bootstrapCountingClient)
	count.Client = p.client
	p.r.Client = count
	for range 3 {
		_, err := p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
		require.NoError(t, err)
	}
	current := p.getVM()
	require.True(t, current.DSGAUndispatched())
	require.Zero(t, count.creates.Load())
	require.Nil(t, current.Spec.CpuScalingMode, "held path must return before ordinary default/status/resource work")
	require.Empty(t, current.Status.Conditions)
	// doReconcile本身也须保护，防止后续重构/直接调用绕过上层分流。
	require.NoError(t, p.r.doReconcile(p.ctx, current))
	require.Zero(t, count.creates.Load())
}

func TestInitialReconcileRefusesCacheFromPreviousVMUID(t *testing.T) {
	p := newTestParams(t)
	vm := defaultVm()
	vm.UID = "original-uid"
	p.initVM(vm)
	fresh := vm.DeepCopy()
	fresh.UID = "recreated-uid"
	fresh.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	p.r.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(fresh).Build()
	_, err := p.r.Reconcile(p.ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(vm)})
	require.ErrorContains(t, err, "identity changed")
	require.Empty(t, p.getVM().Finalizers, "even metadata update must not adopt another UID")
}

func TestReadyBootstrapStillCannotEnterLegacyScalingSideEffects(t *testing.T) {
	p := newTestParams(t)
	_, vm, secret := controllerBootstrapInputs(t)
	vm.Status.Phase = vmv1.VmScaling
	p.r.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(secret).Build()
	require.ErrorContains(t, p.r.doReconcile(p.ctx, vm), "sysfs CPU mode")
}

func TestBootstrapReleaseChecksFullJournalAndSecretOutsideDebugSkip(t *testing.T) {
	p := newTestParams(t)
	old, next, secret := controllerBootstrapInputs(t)
	w := new(VMWebhook)
	w.Config = new(ReconcilerConfig)
	w.Config.SkipUpdateValidationFor = map[client.ObjectKey]struct{}{client.ObjectKeyFromObject(next): {}}
	w.Recorder = record.NewFakeRecorder(10)
	w.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(secret).Build()
	_, err := w.ValidateUpdate(context.Background(), old, next)
	require.NoError(t, err)
	for _, scenario := range []string{"missing_reader", "recreated_secret", "corrupt_journal", "wrong_native_identity"} {
		t.Run(scenario, func(t *testing.T) {
			vm, s := next.DeepCopy(), secret.DeepCopy()
			switch scenario {
			case "recreated_secret":
				s.UID = "replacement"
			case "corrupt_journal":
				vm.Annotations[control.JournalAnnotation] = "malformed"
			case "wrong_native_identity":
				s.Data["config.json"] = []byte(`{"spec":{"dynamic_shared_buffers":{"protocol_version":2,"vm_uid":"different","compute_instance_id":"compute-1"}}}`)
			}
			w.APIReader = fake.NewClientBuilder().WithScheme(p.r.Scheme).WithObjects(s).Build()
			if scenario == "missing_reader" {
				w.APIReader = nil
			}
			_, err := w.ValidateUpdate(context.Background(), old, vm)
			require.Error(t, err)
		})
	}
}
