package qualificationissuer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberesource "k8s.io/kubernetes/pkg/api/v1/resource"
)

func (i *Issuer) capture(ctx context.Context, r Report) (string, error) {
	q, b := r.Qualification, i.config.Binding
	vm, err := i.vm.NeonvmV1().VirtualMachines(b.Namespace).Get(ctx, b.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if vm == nil || vm.UID != b.VMUID || vm.ResourceVersion == "" || vm.DeletionTimestamp != nil || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != b.Incarnation {
		return "", fmt.Errorf("original VM identity changed")
	}
	snapshot, err := journal.Inspect(vm, b)
	if err != nil {
		return "", err
	}
	m := snapshot.State()
	if !snapshot.Dispatchable() || m.Active != nil || m.ServiceClosure != nil || m.ResourceIdentity == nil || *m.ResourceIdentity != q.Identity {
		return "", fmt.Errorf("qualification requires stable original idle resource binding")
	}
	lease, err := i.coord.Leases(b.Namespace).Get(ctx, journal.LeaseName(b), metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	// VM Owner.Epoch只由原journal CAS Takeover产生；首Lease CREATE的Transitions=0
	// 可以对应Owner.Epoch=2。Transitions由ValidateLease独立核形状，绝不据它造epoch。
	// 下方仍交叉核Root owner/floor、原LeaseUID及Runner publication，Host协议不变。
	if journal.ValidateLease(lease, b, journal.LeaseName(b), "") != nil || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != m.Owner.HolderID || !freshLease(lease.Spec.RenewTime, lease.Spec.LeaseDurationSeconds) {
		return "", fmt.Errorf("original VM owner Lease unconfirmed")
	}
	discovered, view, err := i.root.Read(ctx, b)
	if err != nil {
		return "", err
	}
	if discovered.Instance.ComputeInstanceID != q.Identity.ComputeInstanceID || discovered.OwnerEpoch != m.Owner.Epoch || view.Identity != q.Identity || view.IssuerFloorEpoch != m.Owner.Epoch || view.NormalQuotaProtocolVersion != 1 || view.ArrayAuthorityProtocolVersion != 1 || view.Birth == nil {
		return "", fmt.Errorf("Root identity, original birth or current owner changed")
	}
	// collector产生的报告在每次写前/写后capture都重新核完整投影；旧SHA不含子cap，不能单用。
	if r.GuestArtifactSHA256 != "" {
		artifact := view.GuestQualification
		if artifact == nil || view.GuestQualificationUnavailable != nil || artifact.MatchView(view) != nil || artifact.ConfigSecretUID != r.ConfigSecretUID || artifact.ResourceSourceSHA256 != r.ResourceSourceSHA256 {
			return "", fmt.Errorf("original complete Guest artifact unavailable during qualification capture")
		}
		sha, e := canonicalGuestArtifactSHA(*artifact)
		if e != nil || sha != r.GuestArtifactSHA256 {
			return "", fmt.Errorf("original complete Guest artifact changed during qualification capture")
		}
	}
	if m.PGInstance != nil && (m.PGInstance.ComputeInstanceID != discovered.Instance.ComputeInstanceID || discovered.Instance.PGBootID != 0 && discovered.Instance.PGBootID != m.PGInstance.PGBootID) {
		return "", fmt.Errorf("Agent PG discovery differs from original journal")
	}
	limits, err := view.ConfirmedLimits()
	if err != nil {
		return "", err
	}
	rootRaw, err := json.Marshal(view.Bootstrap)
	if err != nil || SHA256(rootRaw) != q.RootBootstrapSHA256 {
		return "", fmt.Errorf("original Root bootstrap digest changed")
	}
	secret, err := i.core.Secrets(b.Namespace).Get(ctx, b.Name+"-config", metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if bootstrap.CheckReady(vm, secret, false) != nil || string(secret.UID) != r.ConfigSecretUID || SHA256(secret.Data[resourceguard.ResourceSecretKey]) != r.ResourceSourceSHA256 {
		return "", fmt.Errorf("original immutable Runtime Secret source changed")
	}
	if len(secret.Data[resourceguard.ResourceSecretKey]) == 0 || len(secret.Data[resourceguard.ResourceSecretKey]) > 16*1024 {
		return "", fmt.Errorf("original Runtime resource source exceeds existing contract")
	}
	var source resourceguard.SourceConfig
	if json.Unmarshal(secret.Data[resourceguard.ResourceSecretKey], &source) != nil || source.Validate() != nil || source.Config.ExecutionModel != resourceguard.TrustedControllerExecutionModel || source.Config.VMUID != q.Identity.VMUID || source.Config.ComputeInstanceID != q.Identity.ComputeInstanceID || view.Bootstrap.BudgetCommitmentID != source.BudgetCommitmentID || view.Bootstrap.IssuerEpoch != source.IssuerEpoch || !reflect.DeepEqual(view.Bootstrap.Limits, source.Limits) {
		return "", fmt.Errorf("Root bootstrap does not match original enforced Runtime source")
	}
	reader, err := vmguard.NewReader(i.core, b, string(lease.UID))
	if err != nil {
		return "", err
	}
	birth, ref, pub, err := reader.Inspect(ctx, &q.RunnerBirth, m.Owner)
	if err != nil {
		return "", err
	}
	identityRaw, err := json.Marshal(birth.Identity)
	if err != nil || SHA256(identityRaw) != q.RunnerIdentitySHA256 || ref != q.RunnerBirth || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || pub.State == nil || pub.State.Pending != nil || pub.Actual == nil || pub.Actual.ActualBytes == 0 || pub.Actual.RequestedBytes != pub.Actual.ActualBytes {
		return "", fmt.Errorf("original settled Runner publication changed")
	}
	if vm.Status.MemorySize == nil {
		return "", fmt.Errorf("original VM actual memory missing")
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actual <= 0 || control.Uint64(actual) != pub.Actual.ActualBytes || vm.Status.PodName != q.RunnerPodName || vm.Status.Node != q.NodeName || vm.Status.RestartCount != q.RunnerRestartCount {
		return "", fmt.Errorf("VM status differs from original Runner actual capacity")
	}
	for _, target := range q.Targets {
		if target.VMMemoryBytes < birth.Identity.MinimumBytes || target.VMMemoryBytes > birth.Identity.MaximumBytes || birth.Identity.BlockBytes == 0 || (target.VMMemoryBytes-birth.Identity.MinimumBytes)%birth.Identity.BlockBytes != 0 || target.Limits.PidsMaxTasks != source.Limits.PidsMaxTasks || target.Limits.AddressSpaceMaxBytes != source.Limits.AddressSpaceMaxBytes || target.Limits.SystemMemoryMaxBytes != source.Limits.SystemMemoryMaxBytes || target.Limits.OutsideUpperBytes != source.Limits.OutsideUpperBytes || len(target.Limits.HugePools) != len(source.Limits.HugePools) {
			return "", fmt.Errorf("qualified target changed immutable enforced profile or original geometry")
		}
		for index, pool := range source.Limits.HugePools {
			if target.Limits.HugePools[index].PageSizeKB != pool.PageSizeKB {
				return "", fmt.Errorf("qualified target changed original HugeTLB hstate")
			}
		}
	}
	pod, err := i.core.Pods(b.Namespace).Get(ctx, q.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if pod == nil {
		return "", fmt.Errorf("original Runner Pod missing")
	}
	owner := metav1.GetControllerOf(pod)
	if string(pod.UID) != q.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Status.PodIP != vm.Status.PodIP || pod.Spec.NodeName != q.NodeName || owner == nil || owner.APIVersion != vmv1.SchemeGroupVersion.String() || owner.Kind != "VirtualMachine" || owner.Name != b.Name || owner.UID != b.VMUID {
		return "", fmt.Errorf("original live Runner Pod changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount != q.RunnerRestartCount {
			return "", fmt.Errorf("original Runner container restarted")
		}
	}
	host, err := hostguard.NewStore(i.core, i.coord, r.HostBinding, "qualification-issuer-read-only")
	if err != nil {
		return "", err
	}
	held, err := host.Load(ctx)
	if err != nil {
		return "", err
	}
	node, ledger := held.Node(), held.State()
	if string(node.UID) != i.config.NodeUID || node.DeletionTimestamp != nil || ledger.Profile == nil || ledger.OutsideBytes != r.HostSystemBytes {
		return "", fmt.Errorf("original Host profile or System budget changed")
	}
	if _, err := hostguard.PodMemoryFloor(node, pod); err != nil {
		return "", err
	}
	profileRaw, err := json.Marshal(ledger.Profile)
	if err != nil || SHA256(profileRaw) != r.HostProfileSHA256 || hostguard.InspectInitialVM(node, b, q.RunnerPodName, q.RunnerPodUID, pub.Actual.ActualBytes) != nil {
		return "", fmt.Errorf("original Host placement or profile unconfirmed")
	}
	if r.HostKubernetesBudget != nil {
		if err := r.HostKubernetesBudget.Inspect(node, *ledger.Profile, ledger.OutsideBytes); err != nil {
			return "", err
		}
	}
	capacity, exact := node.Status.Allocatable.Memory().AsInt64()
	used, err := ledger.UsedBytes()
	if err != nil || !exact || capacity <= 0 || used > control.Uint64(capacity) {
		return "", fmt.Errorf("complete Host commitments exceed exact Node allocatable")
	}
	hostLease, err := i.coord.Leases(r.HostBinding.Namespace).Get(ctx, r.HostBinding.LeaseName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if hostLease == nil || hostLease.UID != r.HostBinding.LeaseUID || hostLease.ResourceVersion == "" || hostLease.DeletionTimestamp != nil || hostLease.Annotations[hostguard.LeaseAnnotation] != hostguard.LeaseMarker(r.HostBinding) || hostLease.Spec.HolderIdentity == nil || *hostLease.Spec.HolderIdentity != ledger.LeaseFloor.HolderID || len(hostLease.OwnerReferences) != 1 || hostLease.Spec.Strategy != nil || hostLease.Spec.PreferredHolder != nil || hostLease.Spec.LeaseTransitions == nil || *hostLease.Spec.LeaseTransitions < 0 || control.Uint64(uint64(*hostLease.Spec.LeaseTransitions)+1) != ledger.LeaseFloor.Epoch || !freshLease(hostLease.Spec.RenewTime, hostLease.Spec.LeaseDurationSeconds) {
		return "", fmt.Errorf("original Host Lease unconfirmed")
	}
	hostOwner := metav1.GetControllerOf(hostLease)
	if hostOwner == nil || hostOwner.APIVersion != "v1" || hostOwner.Kind != "Node" || hostOwner.Name != node.Name || hostOwner.UID != node.UID {
		return "", fmt.Errorf("Host Lease belongs to another Node")
	}
	// 当前合同对 kube-system 同样要求真实硬限额；SystemBytes 没有按 namespace
	// 豁免普通 Pod 的旁路。缺 limit/placement 时保持未签发，不据 requests/RSS 补值。
	pods, err := i.core.Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node.Name, Limit: hostguard.MaxPlacements + 1})
	if err != nil {
		return "", err
	}
	if pods.ResourceVersion == "" || pods.Continue != "" || len(pods.Items) > hostguard.MaxPlacements {
		return "", fmt.Errorf("complete bounded Host Pod inventory unavailable")
	}
	var inventory []string
	hasRunner := false
	for index := range pods.Items {
		current := &pods.Items[index]
		if current.UID == "" || current.ResourceVersion == "" {
			return "", fmt.Errorf("original Host Pod inventory identity missing")
		}
		if string(current.UID) == q.RunnerPodUID {
			hasRunner = true
		}
		if current.Spec.NodeName != node.Name {
			return "", fmt.Errorf("Host Pod inventory returned another Node")
		}
		if current.Status.Phase == core.PodSucceeded || current.Status.Phase == core.PodFailed {
			continue
		}
		placement, err := ledger.Placement(string(current.UID))
		if err != nil || placement.Namespace != current.Namespace || placement.PodName != current.Name {
			return "", fmt.Errorf("live Host Pod lacks original full placement")
		}
		if _, isVM := vmv1.VirtualMachineOwnerForPod(current); !isVM {
			if placement.Managed || len(current.Spec.Containers) == 0 || len(current.Spec.EphemeralContainers) != 0 {
				return "", fmt.Errorf("ordinary Host Pod has unsupported unbounded containers")
			}
			for _, containers := range [][]core.Container{current.Spec.Containers, current.Spec.InitContainers} {
				for _, container := range containers {
					memory, exact := container.Resources.Limits.Memory().AsInt64()
					if !exact || memory <= 0 {
						return "", fmt.Errorf("ordinary Host Pod requires actual positive hard memory limits")
					}
				}
			}
			aggregate := kuberesource.PodLimits(current, kuberesource.PodResourcesOptions{})
			memory, exact := aggregate.Memory().AsInt64()
			if !exact || memory <= 0 || placement.MemoryBytes < control.Uint64(memory) || placement.HostOverheadBytes != ledger.Profile.PodOverheadBytes {
				return "", fmt.Errorf("ordinary Host Pod hard limit exceeds original placement")
			}
		}
		inventory = append(inventory, string(current.UID)+":"+current.ResourceVersion)
	}
	if !hasRunner {
		return "", fmt.Errorf("complete Host inventory omitted original Runner Pod")
	}
	sort.Strings(inventory)
	// 不比较 RSS/瞬时 current。这里冻结原身份、完整命令/硬限和原来源的稳定部分，
	// Healthy Lease 续租不改变持有者身份；取证前后任一来源漂移均不返回资格票据。
	stable := []any{vm.ResourceVersion, m.Owner, lease.UID, view.Identity, view.IssuerFloorEpoch, view.Bootstrap, view.Original, view.Held, view.Applied, view.Cancelled, view.Birth, limits, view.Actual.GuestMemTotalBytes, birth, ref, pub, node.Annotations[hostguard.LedgerAnnotation], hostLease.UID, ledger.LeaseFloor, secret.UID, inventory}
	raw, err := json.Marshal(stable)
	if err != nil {
		return "", err
	}
	return SHA256(raw), nil
}

func freshLease(renew *metav1.MicroTime, seconds *int32) bool {
	if renew == nil || seconds == nil || *seconds < 1 || *seconds > 86400 {
		return false
	}
	now := time.Now()
	return !renew.After(now.Add(5*time.Second)) && now.Before(renew.Add(time.Duration(*seconds)*time.Second))
}
