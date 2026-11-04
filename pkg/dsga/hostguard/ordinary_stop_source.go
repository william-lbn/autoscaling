package hostguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"path"
	"reflect"
	"slices"
	"strings"
)

const OrdinaryStopNamespace = "kube-system"
const OrdinaryStopScope = "ordinary-pod-node-stop-v1"
const OrdinaryStopSourceKey = "ordinary-stop.json"
const OrdinaryStopSourceAnnotation = "internal.autoscaling.neon.tech/dsga-ordinary-stop-source-v1"
const OrdinaryStopSourceFinalizer = "autoscaling.neon.tech/dsga-ordinary-stop-source-protection"
const OrdinaryStopSourceMarker = "internal.autoscaling.neon.tech/dsga-ordinary-stop-source-kind"

// 来源认证是固定kube-system的受信operator写入边界；Node ownerRef仅绑定身份，
// 不能单独认证写者。原始观测必须完整，缺字段/截断/错误从不按空集处理。
type OrdinaryStopSource struct {
	ProtocolVersion uint32               `json:"protocol_version"`
	Scope           string               `json:"scope"`
	Binding         Binding              `json:"binding"`
	Origin          string               `json:"origin"`
	Placement       Placement            `json:"placement"`
	Before          OrdinaryStopPodFacts `json:"before"`
	After           OrdinaryStopPodFacts `json:"after"`
	ContainerNames  []string             `json:"container_names"`
	Probe           OrdinaryStopProbe    `json:"probe"`
}

type OrdinaryStopPodFacts struct {
	Namespace    string        `json:"namespace"`
	Name         string        `json:"name"`
	UID          string        `json:"uid"`
	NodeName     string        `json:"node_name"`
	Phase        core.PodPhase `json:"phase"`
	SpecSHA256   string        `json:"spec_sha256"`
	StatusSHA256 string        `json:"status_sha256"`
}

type OrdinaryStopProbe struct {
	NodeName       string                `json:"node_name"`
	NodeUID        string                `json:"node_uid"`
	BootBefore     string                `json:"boot_before"`
	BootAfter      string                `json:"boot_after"`
	APINodeBefore  OrdinaryStopNodeFacts `json:"api_node_before"`
	APINodeAfter   OrdinaryStopNodeFacts `json:"api_node_after"`
	Kubelet        OrdinaryStopKubelet   `json:"kubelet"`
	SelfPID        uint32                `json:"self_pid"`
	SelfNS         OrdinaryStopNS        `json:"self_ns"`
	InitNS         OrdinaryStopNS        `json:"init_ns"`
	KubeletNS      OrdinaryStopNS        `json:"kubelet_ns"`
	Mount          OrdinaryStopMount     `json:"mount"`
	Runtime        OrdinaryStopRuntime   `json:"runtime"`
	CgroupDirs     []string              `json:"cgroup_dirs"`
	WalkErrors     []string              `json:"walk_errors"`
	WalkTruncated  bool                  `json:"walk_truncated"`
	ProcEnumerated []uint32              `json:"proc_enumerated"`
	Processes      []OrdinaryStopProcess `json:"processes"`
	ProcErrors     []string              `json:"proc_errors"`
	Issues         []string              `json:"issues"`
	Live           OrdinaryStopLive      `json:"live"`
}

type OrdinaryStopNodeFacts struct {
	Name   string `json:"name"`
	UID    string `json:"uid"`
	BootID string `json:"boot_id"`
}
type OrdinaryStopKubelet struct {
	PID            uint32         `json:"pid"`
	BirthBefore    control.Uint64 `json:"birth_before"`
	BirthAfter     control.Uint64 `json:"birth_after"`
	Exe            string         `json:"exe"`
	ExeSHA256      string         `json:"exe_sha256"`
	Version        string         `json:"version"`
	Endpoint       string         `json:"endpoint"`
	ResolvedSocket string         `json:"resolved_socket"`
}

type OrdinaryStopNS struct {
	PID    control.Uint64 `json:"pid"`
	Mount  control.Uint64 `json:"mount"`
	Cgroup control.Uint64 `json:"cgroup"`
}

type OrdinaryStopMount struct {
	Root   string `json:"root"`
	Point  string `json:"point"`
	FSType string `json:"fs_type"`
}

type OrdinaryStopRuntime struct {
	CRIContainerExit *int32                  `json:"cri_container_exit"`
	CRISandboxExit   *int32                  `json:"cri_sandbox_exit"`
	CTRExit          *int32                  `json:"ctr_exit"`
	CTRNamespace     string                  `json:"ctr_namespace"`
	Containers       []OrdinaryStopContainer `json:"containers"`
	Sandboxes        []OrdinaryStopSandbox   `json:"sandboxes"`
	CTRContainerIDs  []string                `json:"ctr_container_ids"`
}

type OrdinaryStopContainer struct {
	ID        string `json:"id"`
	PodUID    string `json:"pod_uid"`
	SandboxID string `json:"sandbox_id"`
	State     *int32 `json:"state"`
}

type OrdinaryStopSandbox struct {
	ID     string `json:"id"`
	PodUID string `json:"pod_uid"`
	State  *int32 `json:"state"`
}

type OrdinaryStopProcess struct {
	PID         uint32   `json:"pid"`
	CgroupPaths []string `json:"cgroup_paths"`
}

type OrdinaryStopLive struct {
	UID         string `json:"uid"`
	ContainerID string `json:"container_id"`
	SandboxID   string `json:"sandbox_id"`
}

type OrdinaryStopReference struct {
	Claim      budget.ObjectRef `json:"claim"`
	SHA256     string           `json:"sha256"`
	SpecSHA256 string           `json:"spec_sha256"`
}

func OrdinaryStopClaimName(podUID string) string {
	digest := sha256.Sum256([]byte(podUID))
	return "dsga-os-" + hex.EncodeToString(digest[:])[:55]
}

// Canonical hashes use sorted object keys, UTF-8 and no insignificant whitespace.
// They bind API observations; SHA alone never authenticates their producer.
func ordinaryCanonicalSHA(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return "", err
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return "", err
	}
	return digestBytes(bytes.TrimSuffix(out.Bytes(), []byte("\n"))), nil
}
func OrdinaryPodSpecSHA(pod *core.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("ordinary Pod is absent")
	}
	return ordinaryCanonicalSHA(pod.Spec)
}
func OrdinaryPodStatusSHA(pod *core.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("ordinary Pod is absent")
	}
	return ordinaryCanonicalSHA(pod.Status)
}
func ordinaryPlacement(p Placement) bool {
	_, err := p.Bytes()
	return err == nil && !p.Managed && p.VMUID == "" && p.VMName == "" && p.Incarnation == "" && p.PodMemoryLimitBytes == 0 && p.Settled == nil && p.Barrier == nil
}
func ordinaryPod(pod *core.Pod) bool {
	if pod == nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || len(pod.Spec.Containers) == 0 || len(pod.Spec.EphemeralContainers) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 || (pod.Status.Phase != core.PodFailed && pod.Status.Phase != core.PodSucceeded) {
		return false
	}
	if _, ok := vmv1.VirtualMachineOwnerForPod(pod); ok {
		return false
	}
	_, _, migration := vmv1.MigrationOwnerForPod(pod)
	return !migration
}
func (r OrdinaryStopReference) Validate() error {
	if r.Claim.Validate() != nil || len(validation.IsDNS1123Subdomain(r.Claim.Name)) != 0 || !retirementDigest(r.SHA256) || !retirementDigest(r.SpecSHA256) {
		return fmt.Errorf("ordinary stop source reference is malformed")
	}
	return nil
}
func OrdinaryStopReferenceForPod(pod *core.Pod) (OrdinaryStopReference, error) {
	var ref OrdinaryStopReference
	if pod == nil || json.Unmarshal([]byte(pod.Annotations[OrdinaryStopSourceAnnotation]), &ref) != nil || ref.Validate() != nil || ref.Claim.Name != OrdinaryStopClaimName(string(pod.UID)) {
		return ref, fmt.Errorf("ordinary stop source reference is absent or changed")
	}
	return ref, nil
}
func ordinaryUIDPath(p, uid string) bool {
	if p == "" || !strings.HasPrefix(p, "/") || path.Clean(p) != p || uid == "" {
		return false
	}
	for _, component := range strings.Split(p, "/") {
		if component == "pod"+uid {
			return true
		}
		normalized := strings.ReplaceAll(uid, "-", "_")
		if strings.HasPrefix(component, "kubepods") && strings.HasSuffix(component, "-pod"+normalized+".slice") {
			return true
		}
	}
	return false
}
func ordinaryID(id string) bool { return retirementDigest(id) }
func (v OrdinaryStopSource) Validate() error {
	if v.ProtocolVersion != 1 || v.Scope != OrdinaryStopScope || v.Binding.Validate() != nil || control.ValidateIncarnation(v.Origin) != nil || !ordinaryPlacement(v.Placement) {
		return fmt.Errorf("ordinary stop source changed original allocation domain")
	}
	b, a, p := v.Before, v.After, v.Probe
	if b != a || b.Namespace != v.Placement.Namespace || b.Name != v.Placement.PodName || b.UID != v.Placement.PodUID || b.NodeName != v.Binding.NodeName || (b.Phase != core.PodFailed && b.Phase != core.PodSucceeded) || !retirementDigest(b.SpecSHA256) || !retirementDigest(b.StatusSHA256) || len(v.ContainerNames) == 0 {
		return fmt.Errorf("ordinary stop API observations are incomplete or changed")
	}
	names := map[string]bool{}
	for _, name := range v.ContainerNames {
		if name == "" || names[name] {
			return fmt.Errorf("ordinary stop container names are incomplete")
		}
		names[name] = true
	}
	if p.NodeName != v.Binding.NodeName || p.NodeUID != string(v.Binding.NodeUID) || p.BootBefore == "" || p.BootBefore != p.BootAfter || p.APINodeBefore != p.APINodeAfter || p.APINodeBefore.Name != p.NodeName || p.APINodeBefore.UID != p.NodeUID || p.APINodeBefore.BootID != p.BootBefore {
		return fmt.Errorf("ordinary stop original Node capture changed")
	}
	k := p.Kubelet
	if k.PID == 0 || k.BirthBefore == 0 || k.BirthBefore != k.BirthAfter || k.Exe != "/usr/local/bin/kubelet" || !retirementDigest(k.ExeSHA256) || k.Version != "Kubernetes v1.31.2" || (k.Endpoint != "unix:///run/containerd/containerd.sock" && k.Endpoint != "unix:///var/run/containerd/containerd.sock") || k.ResolvedSocket != "/run/containerd/containerd.sock" {
		return fmt.Errorf("ordinary stop trusted kubelet/runtime capture is incomplete")
	}
	if p.SelfPID == 0 || p.SelfNS.PID == 0 || p.SelfNS.Mount == 0 || p.SelfNS.Cgroup == 0 || p.SelfNS != p.InitNS || p.SelfNS != p.KubeletNS || p.Mount.Root != "/" || p.Mount.Point != "/sys/fs/cgroup" || p.Mount.FSType != "cgroup2" {
		return fmt.Errorf("ordinary stop probe lacks the complete host view")
	}
	rt := p.Runtime
	if rt.CRIContainerExit == nil || *rt.CRIContainerExit != 0 || rt.CRISandboxExit == nil || *rt.CRISandboxExit != 0 || rt.CTRExit == nil || *rt.CTRExit != 0 || rt.CTRNamespace != "k8s.io" || rt.Containers == nil || rt.Sandboxes == nil || rt.CTRContainerIDs == nil || p.CgroupDirs == nil || len(p.CgroupDirs) == 0 || p.WalkErrors == nil || len(p.WalkErrors) != 0 || p.WalkTruncated || p.ProcEnumerated == nil || len(p.ProcEnumerated) == 0 || p.Processes == nil || p.ProcErrors == nil || len(p.ProcErrors) != 0 || p.Issues == nil || len(p.Issues) != 0 {
		return fmt.Errorf("ordinary stop complete runtime/kernel coverage is unknown")
	}
	ctr := map[string]bool{}
	for _, id := range rt.CTRContainerIDs {
		if !ordinaryID(id) || ctr[id] {
			return fmt.Errorf("ordinary stop containerd inventory is malformed")
		}
		ctr[id] = true
	}
	sandboxes := map[string]OrdinaryStopSandbox{}
	for _, sb := range rt.Sandboxes {
		if !ordinaryID(sb.ID) || sb.PodUID == "" || sb.State == nil || (*sb.State != 0 && *sb.State != 1) {
			return fmt.Errorf("ordinary stop sandbox inventory is malformed")
		}
		if _, ok := sandboxes[sb.ID]; ok {
			return fmt.Errorf("ordinary stop sandbox inventory is duplicated")
		}
		sandboxes[sb.ID] = sb
		if sb.PodUID == b.UID && *sb.State != 1 {
			return fmt.Errorf("ordinary Pod sandbox is still ready")
		}
	}
	containers := map[string]OrdinaryStopContainer{}
	for _, c := range rt.Containers {
		if !ordinaryID(c.ID) || c.PodUID == "" || c.State == nil || *c.State < 0 || *c.State > 3 || !ordinaryID(c.SandboxID) {
			return fmt.Errorf("ordinary stop container inventory is malformed")
		}
		if _, ok := containers[c.ID]; ok {
			return fmt.Errorf("ordinary stop container inventory is duplicated")
		}
		containers[c.ID] = c
		if c.PodUID == b.UID && *c.State != 2 {
			return fmt.Errorf("ordinary Pod container is not stopped")
		}
	}
	live := p.Live
	lc, ok := containers[live.ContainerID]
	ls, sok := sandboxes[live.SandboxID]
	if live.UID == "" || live.UID == b.UID || !ok || !sok || lc.PodUID != live.UID || lc.SandboxID != live.SandboxID || ls.PodUID != live.UID || *lc.State != 1 || *ls.State != 0 || !ctr[live.ContainerID] {
		return fmt.Errorf("ordinary stop live CRI/containerd positive control does not join")
	}
	liveGroup := false
	seenDirs := map[string]bool{}
	for _, dir := range p.CgroupDirs {
		if !strings.HasPrefix(dir, "/") || path.Clean(dir) != dir || seenDirs[dir] {
			return fmt.Errorf("ordinary stop cgroup walk is malformed")
		}
		seenDirs[dir] = true
		if ordinaryUIDPath(dir, b.UID) {
			return fmt.Errorf("ordinary Pod cgroup still exists")
		}
		liveGroup = liveGroup || ordinaryUIDPath(dir, live.UID)
	}
	enumerated := map[uint32]bool{}
	for _, pid := range p.ProcEnumerated {
		if pid == 0 || enumerated[pid] {
			return fmt.Errorf("ordinary stop proc enumeration is malformed")
		}
		enumerated[pid] = true
	}
	read := map[uint32]bool{}
	liveProcess := false
	for _, proc := range p.Processes {
		if !enumerated[proc.PID] || read[proc.PID] || proc.CgroupPaths == nil || len(proc.CgroupPaths) == 0 {
			return fmt.Errorf("ordinary stop proc coverage is incomplete")
		}
		read[proc.PID] = true
		for _, group := range proc.CgroupPaths {
			if !strings.HasPrefix(group, "/") || path.Clean(group) != group {
				return fmt.Errorf("ordinary stop proc cgroup is malformed")
			}
			if ordinaryUIDPath(group, b.UID) {
				return fmt.Errorf("ordinary Pod process still exists")
			}
			liveProcess = liveProcess || ordinaryUIDPath(group, live.UID)
		}
	}
	if len(read) != len(enumerated) || !enumerated[p.SelfPID] || !enumerated[k.PID] || !liveGroup || !liveProcess {
		return fmt.Errorf("ordinary stop kernel positive control or complete proc coverage is absent")
	}
	return nil
}
func ordinaryFactsMatch(v OrdinaryStopSource, pod *core.Pod) error {
	if !ordinaryPod(pod) || v.After.Namespace != pod.Namespace || v.After.Name != pod.Name || v.After.UID != string(pod.UID) || v.After.NodeName != pod.Spec.NodeName || v.After.Phase != pod.Status.Phase {
		return fmt.Errorf("ordinary stop original terminal Pod differs")
	}
	spec, err := OrdinaryPodSpecSHA(pod)
	if err != nil || spec != v.After.SpecSHA256 {
		return fmt.Errorf("ordinary stop original Pod spec changed")
	}
	// 证明捕获前后status相同；之后CID可被GC，不要求历史status永远逐字不变。
	names := []string{}
	for _, c := range pod.Spec.InitContainers {
		names = append(names, "init/"+c.Name)
	}
	for _, c := range pod.Spec.Containers {
		names = append(names, "app/"+c.Name)
	}
	if !slices.Equal(names, v.ContainerNames) {
		return fmt.Errorf("ordinary stop original complete container set changed")
	}
	actual, err := PodMemoryLimit(pod)
	if err != nil || actual > v.Placement.MemoryBytes {
		return fmt.Errorf("ordinary stop original complete hard limit changed")
	}
	return nil
}

// ReadOrdinaryStop只从固定受信namespace重新GET不可变原UID。无TTL：来源证明
// 同UID不可再次启动的过去终态，节点后来重启不撤销它；当前NodeUID仍必须相同。
func (s *Store) ReadOrdinaryStop(ctx context.Context, snapshot *Snapshot, pod *core.Pod, placement Placement, expected *OrdinaryStopReference) (OrdinaryStopReference, error) {
	var none OrdinaryStopReference
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.Profile == nil || !ordinaryPlacement(placement) || placement.HostOverheadBytes != snapshot.model.Profile.PodOverheadBytes {
		return none, fmt.Errorf("ordinary stop requires original configured ordinary allocation")
	}
	ref, err := OrdinaryStopReferenceForPod(pod)
	if err != nil {
		return none, err
	}
	if expected != nil && ref != *expected {
		return none, fmt.Errorf("ordinary stop original source reference changed")
	}
	cm, err := s.core.ConfigMaps(OrdinaryStopNamespace).Get(ctx, ref.Claim.Name, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	owner := metav1.GetControllerOf(cm)
	if cm.Namespace != OrdinaryStopNamespace || cm.Name != ref.Claim.Name || string(cm.UID) != ref.Claim.UID || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Annotations[OrdinaryStopSourceMarker] != OrdinaryStopScope || !slices.Contains(cm.Finalizers, OrdinaryStopSourceFinalizer) || len(cm.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "v1" || owner.Kind != "Node" || owner.Name != s.binding.NodeName || owner.UID != s.binding.NodeUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return none, fmt.Errorf("ordinary stop immutable trusted namespace source protection/binding changed")
	}
	raw := []byte(cm.Data[OrdinaryStopSourceKey])
	if digestBytes(raw) != ref.SHA256 {
		return none, fmt.Errorf("ordinary stop original source body changed")
	}
	var source OrdinaryStopSource
	if err := json.Unmarshal(raw, &source); err != nil {
		return none, err
	}
	if source.Binding != s.binding || source.Origin != snapshot.model.Origin || !source.Placement.sameOriginal(placement) || source.After.SpecSHA256 != ref.SpecSHA256 {
		return none, fmt.Errorf("ordinary stop source changed original Host allocation")
	}
	if err := ordinaryFactsMatch(source, pod); err != nil {
		return none, err
	}
	return ref, nil
}

// InspectRetiredPodWithSource在Node CAS后重读原来源，不把缺行或旧boot当退出证明。
func (s *Store) InspectRetiredPodWithSource(ctx context.Context, snapshot *Snapshot, pod *core.Pod) error {
	r, err := InspectRetirement(pod)
	if err != nil {
		return err
	}
	if r.ProtocolVersion == 1 {
		return InspectRetiredPod(snapshot.node, pod)
	}
	if _, err := s.ReadOrdinaryStop(ctx, snapshot, pod, r.Placement, r.OrdinaryStopSource); err != nil {
		return err
	}
	return inspectRetirementCompletion(snapshot.node, r)
}

// ObserveRetiredOrdinaryWithSource保持原Lease→Node→Lease及fresh Pod双读，
// 只识别同一已闭合普通事件；不补行、不释放、不返回伪零allocation。
func (s *Store) ObserveRetiredOrdinaryWithSource(ctx context.Context, snapshot *Snapshot, expected *core.Pod) error {
	if snapshot == nil || !ordinaryPod(expected) || slices.Contains(expected.Finalizers, PodProtectionFinalizer) {
		return fmt.Errorf("ordinary source retired event is not complete/unprotected")
	}
	if err := s.InspectRetiredPodWithSource(ctx, snapshot, expected); err != nil {
		return err
	}
	if _, _, err := s.placementNode(ctx, snapshot); err != nil {
		return err
	}
	pod, err := s.core.Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pod.UID != expected.UID || pod.ResourceVersion != expected.ResourceVersion || !reflect.DeepEqual(pod.Spec, expected.Spec) || !reflect.DeepEqual(pod.OwnerReferences, expected.OwnerReferences) || !reflect.DeepEqual(pod.Finalizers, expected.Finalizers) || pod.Annotations[RetirementAnnotation] != expected.Annotations[RetirementAnnotation] || pod.Annotations[OrdinaryStopSourceAnnotation] != expected.Annotations[OrdinaryStopSourceAnnotation] {
		return fmt.Errorf("ordinary source retired event changed across original Host observation")
	}
	return s.InspectRetiredPodWithSource(ctx, snapshot, pod)
}

func (v *OrdinaryStopPodFacts) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopPodFacts
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopPodFacts(p)
	return nil
}

func (v *OrdinaryStopProbe) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopProbe
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopProbe(p)
	return nil
}

func (v *OrdinaryStopNodeFacts) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopNodeFacts
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopNodeFacts(p)
	return nil
}

func (v *OrdinaryStopKubelet) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopKubelet
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopKubelet(p)
	return nil
}

func (v *OrdinaryStopNS) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopNS
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopNS(p)
	return nil
}

func (v *OrdinaryStopMount) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopMount
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopMount(p)
	return nil
}

func (v *OrdinaryStopRuntime) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopRuntime
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopRuntime(p)
	return nil
}

func (v *OrdinaryStopContainer) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopContainer
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopContainer(p)
	return nil
}

func (v *OrdinaryStopSandbox) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopSandbox
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopSandbox(p)
	return nil
}

func (v *OrdinaryStopProcess) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopProcess
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopProcess(p)
	return nil
}

func (v *OrdinaryStopLive) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopLive
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopLive(p)
	return nil
}

func (v *OrdinaryStopReference) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopReference
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopReference(p)
	return nil
}

func (v *OrdinaryStopSource) UnmarshalJSON(raw []byte) error {
	type plain OrdinaryStopSource
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, 1<<20, 65536); err != nil {
		return err
	}
	*v = OrdinaryStopSource(p)
	return v.Validate()
}
