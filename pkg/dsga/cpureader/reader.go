// Package cpureader只读复核本次原Runner Kernel+Guest+Node持有。nonce不是预算、
// Owner或TTL；HTTP成功、旧Condition、Pod.Spec都不能替代原来源与内核读回。
package cpureader

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"io"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

var ErrPending = errors.New("original CPU target or pending action is not yet confirmed")

type Reader struct {
	source source
	http   *http.Client
}

func cpuHTTPClient() *http.Client {
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: 32, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 4 * time.Second}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

var sharedCPUHTTP = cpuHTTPClient()

func newReader(source source) *Reader { return &Reader{source: source, http: sharedCPUHTTP} }

type frame struct {
	vm                 *vmv1.VirtualMachine
	pod                *core.Pod
	birth              vmguard.Birth
	birthRef           budget.ObjectRef
	nodeCPU            runnercpu.NodeCommitment
	identity           runnercpu.Identity
	containerID        string
	maximum, committed uint32
}

func (r *Reader) frame(ctx context.Context, binding journal.Binding, compute string) (frame, error) {
	var f frame
	vm, err := r.source.vm(ctx, binding.Namespace, binding.Name)
	if err != nil || vm == nil {
		return f, errors.Join(err, fmt.Errorf("original CPU VM unavailable"))
	}
	held, heldErr := vm.DSGABootstrapHeld()
	b := vm.Spec.DSGABootstrap
	if heldErr != nil || held || vm.UID != binding.VMUID || vm.DeletionTimestamp != nil || vm.ResourceVersion == "" ||
		vm.Generation <= 0 || b == nil || !b.Ready || b.RuntimeContractVersion != vmv1.RuntimeContractFixedCPU ||
		b.VMUID != string(binding.VMUID) || b.ComputeInstanceID != compute || b.Incarnation != binding.Incarnation ||
		vm.Status.RestartCount != 0 || vm.Status.PodName == "" || vm.Spec.CpuScalingMode == nil || *vm.Spec.CpuScalingMode != vmv1.CpuScalingModeSysfs ||
		(vm.Status.Phase != vmv1.VmRunning && vm.Status.Phase != vmv1.VmScaling && vm.Status.Phase != vmv1.VmPending) {
		return f, errors.Join(heldErr, fmt.Errorf("current original CPU VM is absent, historical or changing identity"))
	}
	if _, err = journal.Inspect(vm, binding); err != nil {
		return f, err
	}
	maximum, committed, err := vm.Spec.DSGARuntimeCPUBudget()
	if err != nil {
		return f, err
	}
	cm, err := r.source.configMap(ctx, binding.Namespace, vmguard.BindingName(binding))
	if err != nil {
		return f, err
	}
	birth, ref, err := vmguard.InspectBirth(cm, binding, nil)
	if err != nil {
		return f, err
	}
	// Birth在QEMU启动前已固定；不读PG初始化后才有的runtime selection或publication。
	pod, err := r.source.pod(ctx, binding.Namespace, vm.Status.PodName)
	if err != nil || pod == nil {
		return f, errors.Join(err, fmt.Errorf("original CPU Pod unavailable"))
	}
	owner := metav1.GetControllerOf(pod)
	if pod.UID == "" || string(pod.UID) != birth.Identity.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil ||
		len(pod.OwnerReferences) != 1 || owner == nil || owner.UID != binding.VMUID || owner.Name != binding.Name ||
		owner.Kind != "VirtualMachine" || owner.APIVersion != vmv1.SchemeGroupVersion.String() || pod.Spec.NodeName == "" ||
		pod.Status.Phase != core.PodRunning || net.ParseIP(pod.Status.PodIP) == nil || vm.Status.PodIP != pod.Status.PodIP ||
		vm.Spec.RunnerPort <= 0 || vm.Spec.RunnerPort > 65535 || pod.Spec.HostNetwork {
		return f, fmt.Errorf("current CPU endpoint is not the original same-Pod Runner")
	}
	runnerCount := 0
	for _, container := range pod.Spec.Containers {
		if container.Name != "neonvm-runner" {
			continue
		}
		runnerCount++
		req, e1 := vmv1.ExactRuntimeMillicores(*container.Resources.Requests.Cpu())
		limit, e2 := vmv1.ExactRuntimeMillicores(*container.Resources.Limits.Cpu())
		if e1 != nil || e2 != nil || req != committed || limit != committed {
			return f, fmt.Errorf("original fixed Runner Pod CPU budget differs")
		}
	}
	stateCount := 0
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name != "neonvm-runner" {
			continue
		}
		stateCount++
		if container.RestartCount != 0 || container.State.Running == nil || container.ContainerID == "" {
			return f, fmt.Errorf("original Runner container is not the same living incarnation")
		}
		f.containerID = container.ContainerID
	}
	if runnerCount != 1 || stateCount != 1 {
		return f, fmt.Errorf("original Runner CPU task source is ambiguous")
	}
	node, err := r.source.node(ctx, pod.Spec.NodeName)
	if err != nil || node == nil {
		return f, errors.Join(err, fmt.Errorf("original CPU Node unavailable"))
	}
	if string(node.UID) != birth.Identity.NodeUID || node.DeletionTimestamp != nil {
		return f, fmt.Errorf("original CPU Node changed")
	}
	fact, err := hostguard.InspectCPUCommitment(node, binding, pod.Name, string(pod.UID), maximum, committed)
	if err != nil {
		return f, err
	}
	whole, err := hostguard.PodCPURequest(pod)
	if err != nil || uint64(whole) > fact.CommittedMillicores {
		return f, fmt.Errorf("original Node lacks the complete current Pod CPU request")
	}
	// 原出生已关闭、来源读取未知或格式异常都不能发新资格；不推断退出后再收养。
	for _, name := range []string{vmguard.ExitClaimName(binding, birth), vmguard.StoppedPodClaimName(binding, birth)} {
		_, err := r.source.configMap(ctx, binding.Namespace, name)
		if !apierrors.IsNotFound(err) {
			return f, errors.Join(err, fmt.Errorf("original CPU birth closure is present or unknown"))
		}
	}
	f.vm, f.pod, f.birth, f.birthRef, f.nodeCPU, f.maximum, f.committed = vm, pod, birth, ref, fact, maximum, committed
	f.identity = runnercpu.Identity{VMUID: string(binding.VMUID), ComputeInstanceID: compute, Incarnation: binding.Incarnation,
		RunnerPodUID: string(pod.UID), RunnerBootID: birth.Identity.RunnerBootID, NodeUID: string(node.UID)}
	if err = f.identity.Validate(); err != nil {
		return frame{}, err
	}
	return f, nil
}

// Observe进行一次有界新challenge和前后原来源校验，不重试、不写CPU、不增加floor。
// 有效部分读回返回observation+ErrPending；未知身份/来源返回nil+error。CP必须等待。
func (r *Reader) Observe(ctx context.Context, binding journal.Binding, computeInstanceID string) (*runnercpu.Observation, error) {
	if r == nil || r.source == nil || binding.Namespace == "" || binding.Name == "" || binding.VMUID == "" || computeInstanceID == "" {
		return nil, fmt.Errorf("complete original CPU binding required")
	}
	bounded, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	before, err := r.frame(bounded, binding, computeInstanceID)
	if err != nil {
		return nil, err
	}
	nonceRaw := make([]byte, 16)
	if _, err = rand.Read(nonceRaw); err != nil {
		return nil, err
	}
	nonce := hex.EncodeToString(nonceRaw)
	if err = runnercpu.ValidateObservationNonce(nonce); err != nil {
		return nil, err
	}
	endpoint := url.URL{Scheme: "http", Host: net.JoinHostPort(before.pod.Status.PodIP, strconv.Itoa(int(before.vm.Spec.RunnerPort))), Path: "/cpu_current"}
	query := url.Values{}
	query.Set("observation_nonce", nonce)
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(bounded, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("original CPU readback status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(raw) > 4096 {
		return nil, errors.Join(err, fmt.Errorf("original CPU readback exceeds bound"))
	}
	var answer api.VCPUCgroup
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&answer); err != nil {
		return nil, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("original CPU readback has trailing content")
	}
	observation := answer.Managed
	if observation == nil || answer.HistoricalGuest != nil || observation.Validate() != nil || observation.ObservationNonce != nonce ||
		observation.ObservedBootTimeNS == 0 || observation.Identity != before.identity || observation.Generation > before.vm.Generation ||
		observation.MaximumMillicores != before.maximum || observation.MaximumVCPUs != before.vm.Spec.Guest.CPUs.Max.RoundedUp() ||
		observation.CommittedMillicores != before.committed || observation.TargetMillicores < uint32(before.vm.Spec.Guest.CPUs.Min) {
		return nil, fmt.Errorf("current CPU challenge is missing, foreign, stale or outside the original envelope")
	}
	current, err := observation.Millicores()
	if err != nil || uint32(answer.VCPUs) != current {
		return nil, fmt.Errorf("CPU response differs from actual kernel quota")
	}
	after, err := r.frame(bounded, binding, computeInstanceID)
	if err != nil {
		return nil, err
	}
	if before.vm.Generation != after.vm.Generation || !reflect.DeepEqual(before.vm.Spec, after.vm.Spec) ||
		before.birthRef != after.birthRef || !reflect.DeepEqual(before.birth, after.birth) || before.identity != after.identity ||
		before.pod.Name != after.pod.Name || before.pod.Status.PodIP != after.pod.Status.PodIP || before.containerID != after.containerID ||
		!reflect.DeepEqual(before.pod.Spec, after.pod.Spec) || before.nodeCPU.LedgerOrigin != after.nodeCPU.LedgerOrigin ||
		before.nodeCPU.CommittedMillicores != after.nodeCPU.CommittedMillicores ||
		observation.NodeCommitment.LedgerOrigin != before.nodeCPU.LedgerOrigin ||
		observation.NodeCommitment.CommittedMillicores != before.nodeCPU.CommittedMillicores ||
		observation.NodeCommitment.LedgerSequence < before.nodeCPU.LedgerSequence ||
		observation.NodeCommitment.LedgerSequence > after.nodeCPU.LedgerSequence {
		return nil, fmt.Errorf("original CPU sources changed during this challenge")
	}
	// 其他Pod使Node RV/sequence前进可接受；原CPU行持有未增授、未退还。
	if observation.Pending || observation.Generation != after.vm.Generation ||
		current != uint32(after.vm.Spec.Guest.CPUs.Use) || observation.OnlineVCPUs != after.vm.Spec.Guest.CPUs.Use.RoundedUp() {
		return observation, ErrPending
	}
	return observation, nil
}

// Digest仅供VM/CP审计持久原读回；不能用摘要或其出现时间取代新的Observe。
func Digest(observation *runnercpu.Observation) (string, error) {
	if observation == nil || observation.Validate() != nil {
		return "", fmt.Errorf("valid CPU observation required")
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
