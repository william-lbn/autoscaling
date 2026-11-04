package hostguard

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"strings"
	"testing"
)

func ordinarySourceValidationFixture() OrdinaryStopSource {
	zero, running, ready := int32(0), int32(1), int32(0)
	b := Binding{NodeName: "node", NodeUID: "node-uid", Namespace: "kube-system", LeaseName: "host-lease", LeaseUID: "lease-uid"}
	p := Placement{Namespace: "testing", PodName: "stopped", PodUID: "12345678-1234-4234-8234-123456789abc", MemoryBytes: 1 << 30, HostOverheadBytes: 16 << 20}
	facts := OrdinaryStopPodFacts{Namespace: p.Namespace, Name: p.PodName, UID: p.PodUID, NodeName: b.NodeName, Phase: core.PodFailed, SpecSHA256: strings.Repeat("1", 64), StatusSHA256: strings.Repeat("2", 64)}
	live := "22345678-1234-4234-8234-123456789abc"
	cid, sid := strings.Repeat("a", 64), strings.Repeat("b", 64)
	ns := OrdinaryStopNS{PID: 1, Mount: 2, Cgroup: 3}
	nf := OrdinaryStopNodeFacts{Name: b.NodeName, UID: string(b.NodeUID), BootID: "captured-boot"}
	return OrdinaryStopSource{ProtocolVersion: 1, Scope: OrdinaryStopScope, Binding: b, Origin: strings.Repeat("a", 32), Placement: p, Before: facts, After: facts, ContainerNames: []string{"app/app"}, Probe: OrdinaryStopProbe{NodeName: b.NodeName, NodeUID: string(b.NodeUID), BootBefore: nf.BootID, BootAfter: nf.BootID, APINodeBefore: nf, APINodeAfter: nf, Kubelet: OrdinaryStopKubelet{PID: 2, BirthBefore: 11, BirthAfter: 11, Exe: "/usr/local/bin/kubelet", ExeSHA256: strings.Repeat("c", 64), Version: "Kubernetes v1.31.2", Endpoint: "unix:///var/run/containerd/containerd.sock", ResolvedSocket: "/run/containerd/containerd.sock"}, SelfPID: 1, SelfNS: ns, InitNS: ns, KubeletNS: ns, Mount: OrdinaryStopMount{Root: "/", Point: "/sys/fs/cgroup", FSType: "cgroup2"}, Runtime: OrdinaryStopRuntime{CRIContainerExit: &zero, CRISandboxExit: &zero, CTRExit: &zero, CTRNamespace: "k8s.io", Containers: []OrdinaryStopContainer{{ID: cid, PodUID: live, SandboxID: sid, State: &running}}, Sandboxes: []OrdinaryStopSandbox{{ID: sid, PodUID: live, State: &ready}}, CTRContainerIDs: []string{cid}}, CgroupDirs: []string{"/", "/kubepods/pod" + live}, WalkErrors: []string{}, ProcEnumerated: []uint32{1, 2, 3}, Processes: []OrdinaryStopProcess{{PID: 1, CgroupPaths: []string{"/"}}, {PID: 2, CgroupPaths: []string{"/"}}, {PID: 3, CgroupPaths: []string{"/kubepods/pod" + live}}}, ProcErrors: []string{}, Issues: []string{}, Live: OrdinaryStopLive{UID: live, ContainerID: cid, SandboxID: sid}}}
}
func TestOrdinaryStopSourceRequiresCompleteIdentityBoundCoverage(t *testing.T) {
	for _, which := range []string{"valid_var_run_alias", "valid_run_alias", "managed", "legacy_VM", "wrong_NodeUID", "boot_changed", "API_boot_changed", "API_status_changed", "wrong_socket", "wrong_kubelet", "different_namespace", "subtree_mount", "missing_CRI_exit", "CRI_failed", "missing_CRI_array", "missing_sandbox_array", "missing_ctr_array", "wrong_ctr_ns", "missing_live_ctr", "live_wrong_sandbox", "live_wrong_UID", "live_stopped", "target_running", "target_ready_sandbox", "target_cgroup", "target_process", "walk_error", "walk_missing", "walk_truncated", "proc_error", "proc_missing", "proc_omission", "proc_duplicate", "live_kernel_absent", "missing_names"} {
		t.Run(which, func(t *testing.T) {
			v := ordinarySourceValidationFixture()
			p := &v.Probe
			switch which {
			case "valid_run_alias":
				p.Kubelet.Endpoint = "unix:///run/containerd/containerd.sock"
			case "managed":
				v.Placement.Managed = true
			case "legacy_VM":
				v.Placement.VMName = "vm"
				v.Placement.VMUID = "vm-uid"
			case "wrong_NodeUID":
				p.NodeUID = "replacement"
			case "boot_changed":
				p.BootAfter = "replacement"
			case "API_boot_changed":
				p.APINodeAfter.BootID = "replacement"
			case "API_status_changed":
				v.After.StatusSHA256 = strings.Repeat("3", 64)
			case "wrong_socket":
				p.Kubelet.ResolvedSocket = "/run/other.sock"
			case "wrong_kubelet":
				p.Kubelet.Version = "Kubernetes v1.30.0"
			case "different_namespace":
				p.KubeletNS.PID = 99
			case "subtree_mount":
				p.Mount.Root = "/private"
			case "missing_CRI_exit":
				p.Runtime.CRIContainerExit = nil
			case "CRI_failed":
				bad := int32(1)
				p.Runtime.CRISandboxExit = &bad
			case "missing_CRI_array":
				p.Runtime.Containers = nil
			case "missing_sandbox_array":
				p.Runtime.Sandboxes = nil
			case "missing_ctr_array":
				p.Runtime.CTRContainerIDs = nil
			case "wrong_ctr_ns":
				p.Runtime.CTRNamespace = "default"
			case "missing_live_ctr":
				p.Runtime.CTRContainerIDs = []string{}
			case "live_wrong_sandbox":
				p.Runtime.Containers[0].SandboxID = strings.Repeat("d", 64)
			case "live_wrong_UID":
				p.Runtime.Sandboxes[0].PodUID = "other-uid"
			case "live_stopped":
				stopped := int32(2)
				p.Runtime.Containers[0].State = &stopped
			case "target_running":
				running := int32(1)
				p.Runtime.Containers = append(p.Runtime.Containers, OrdinaryStopContainer{ID: strings.Repeat("d", 64), PodUID: v.Placement.PodUID, SandboxID: strings.Repeat("e", 64), State: &running})
			case "target_ready_sandbox":
				ready := int32(0)
				p.Runtime.Sandboxes = append(p.Runtime.Sandboxes, OrdinaryStopSandbox{ID: strings.Repeat("e", 64), PodUID: v.Placement.PodUID, State: &ready})
			case "target_cgroup":
				p.CgroupDirs = append(p.CgroupDirs, "/kubepods.slice/kubepods-burstable-pod"+strings.ReplaceAll(v.Placement.PodUID, "-", "_")+".slice")
			case "target_process":
				p.Processes[0].CgroupPaths = []string{"/kubepods/pod" + v.Placement.PodUID}
			case "walk_error":
				p.WalkErrors = append(p.WalkErrors, "permission denied")
			case "walk_missing":
				p.WalkErrors = nil
			case "walk_truncated":
				p.WalkTruncated = true
			case "proc_error":
				p.ProcErrors = append(p.ProcErrors, "PID race")
			case "proc_missing":
				p.ProcErrors = nil
			case "proc_omission":
				p.Processes = p.Processes[:2]
			case "proc_duplicate":
				p.ProcEnumerated = append(p.ProcEnumerated, 2)
			case "live_kernel_absent":
				p.CgroupDirs = []string{"/"}
			case "missing_names":
				v.ContainerNames = nil
			}
			raw, err := json.Marshal(v)
			require.NoError(t, err)
			var decoded OrdinaryStopSource
			err = json.Unmarshal(raw, &decoded)
			if strings.HasPrefix(which, "valid_") {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
func TestOrdinaryStopSourceStrictJSONAndExactPodPath(t *testing.T) {
	v := ordinarySourceValidationFixture()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	for _, which := range []string{"unknown", "duplicate", "missing_false_field", "null_array", "null_exit", "unknown_nested"} {
		t.Run(which, func(t *testing.T) {
			var value map[string]any
			require.NoError(t, json.Unmarshal(raw, &value))
			probe := value["probe"].(map[string]any)
			switch which {
			case "unknown":
				value["unexpected"] = true
			case "missing_false_field":
				delete(probe, "walk_truncated")
			case "null_array":
				probe["walk_errors"] = nil
			case "null_exit":
				probe["runtime"].(map[string]any)["cri_container_exit"] = nil
			case "unknown_nested":
				probe["runtime"].(map[string]any)["unexpected"] = 0
			}
			body, e := json.Marshal(value)
			require.NoError(t, e)
			if which == "duplicate" {
				body = append([]byte(`{"protocol_version":1,`), raw[1:]...)
			}
			var got OrdinaryStopSource
			require.Error(t, json.Unmarshal(body, &got))
		})
	}
	uid := v.Placement.PodUID
	require.True(t, ordinaryUIDPath("/kubepods/pod"+uid, uid))
	require.False(t, ordinaryUIDPath("/kubepods/pod"+uid+"-other", uid))
	require.False(t, ordinaryUIDPath("/else/prefixpod"+uid, uid))
	name := OrdinaryStopClaimName(uid)
	require.Len(t, name, 63)
	require.Empty(t, validation.IsDNS1123Subdomain(name))
}
