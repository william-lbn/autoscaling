package cpureader

import (
	"context"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmtyped "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// source只提供原对象GET。生产构造没有任意URL/HTTP transport/授权来源参数。
type source interface {
	vm(context.Context, string, string) (*vmv1.VirtualMachine, error)
	pod(context.Context, string, string) (*core.Pod, error)
	node(context.Context, string) (*core.Node, error)
	configMap(context.Context, string, string) (*core.ConfigMap, error)
}
type typedSource struct {
	core coreclient.CoreV1Interface
	vms  vmtyped.NeonvmV1Interface
}

func (s typedSource) vm(ctx context.Context, ns, name string) (*vmv1.VirtualMachine, error) {
	return s.vms.VirtualMachines(ns).Get(ctx, name, metav1.GetOptions{})
}
func (s typedSource) pod(ctx context.Context, ns, name string) (*core.Pod, error) {
	return s.core.Pods(ns).Get(ctx, name, metav1.GetOptions{})
}
func (s typedSource) node(ctx context.Context, name string) (*core.Node, error) {
	return s.core.Nodes().Get(ctx, name, metav1.GetOptions{})
}
func (s typedSource) configMap(ctx context.Context, ns, name string) (*core.ConfigMap, error) {
	return s.core.ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
}

type directSource struct{ reader client.Reader }

func (s directSource) vm(ctx context.Context, ns, name string) (*vmv1.VirtualMachine, error) {
	v := &vmv1.VirtualMachine{}
	err := s.reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, v)
	return v, err
}
func (s directSource) pod(ctx context.Context, ns, name string) (*core.Pod, error) {
	v := &core.Pod{}
	err := s.reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, v)
	return v, err
}
func (s directSource) node(ctx context.Context, name string) (*core.Node, error) {
	v := &core.Node{}
	err := s.reader.Get(ctx, client.ObjectKey{Name: name}, v)
	return v, err
}
func (s directSource) configMap(ctx context.Context, ns, name string) (*core.ConfigMap, error) {
	v := &core.ConfigMap{}
	err := s.reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, v)
	return v, err
}

// New用于CP。客户端应使用原配置直接API；本包不接收调用者任意URL。
func New(coreClient coreclient.CoreV1Interface, vms vmtyped.NeonvmV1Interface) (*Reader, error) {
	if coreClient == nil || vms == nil {
		return nil, fmt.Errorf("original CPU API clients required")
	}
	return newReader(typedSource{core: coreClient, vms: vms}), nil
}

// NewForAPIReader用于Controller非缓存APIReader；不能传Informer缓存冒充即时来源。
func NewForAPIReader(reader client.Reader) (*Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("original direct API reader required")
	}
	return newReader(directSource{reader: reader}), nil
}
