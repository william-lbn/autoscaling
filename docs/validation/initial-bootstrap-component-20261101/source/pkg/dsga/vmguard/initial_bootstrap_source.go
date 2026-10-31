//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Source只由原Runner固定客户端/原flock读取，非HTTP schema/自报预算。
// VM/Node RV只用于本次读取一致性；持久身份不随无关心跳RV重造。
type initialBootSource struct {
	ProtocolVersion   uint32            `json:"protocol_version"`
	Identity          Identity          `json:"identity"`
	BirthClaim        budget.ObjectRef  `json:"birth_claim"`
	StateClaim        budget.ObjectRef  `json:"state_claim"`
	Owner             journal.Owner     `json:"owner"`
	LeaseUID          string            `json:"lease_uid"`
	OriginalBase      control.Uint64    `json:"original_base_bytes"`
	RuntimeSHA256     string            `json:"runtime_document_sha256"`
	ResourceSHA256    string            `json:"resource_source_sha256"`
	ConfigSecretUID   string            `json:"config_secret_uid"`
	TemplatesSHA256   string            `json:"templates_sha256"`
	KernelBuildSHA256 string            `json:"kernel_build_sha256"`
	HostBinding       hostguard.Binding `json:"host_binding"`
	HostOrigin        string            `json:"host_origin"`
	HostProfile       string            `json:"host_profile"`
	PlacementSHA256   string            `json:"placement_sha256"`
	ReservedBytes     control.Uint64    `json:"reserved_bytes"`
	HostOverheadBytes control.Uint64    `json:"host_overhead_bytes"`
	CommandSHA256     string            `json:"command_sha256"`
	LaunchNonce       string            `json:"launch_nonce"`
	ContainerID       string            `json:"container_id"`
	QEMUPath          string            `json:"qemu_path"`
	QEMUDevice        uint64            `json:"qemu_device"`
	QEMUInode         uint64            `json:"qemu_inode"`
	QEMUSHA256        string            `json:"qemu_sha256"`
}

func (s initialBootSource) validate() error {
	if s.ProtocolVersion != 2 || s.Identity.Validate() != nil || s.Identity.BlockBytes != VirtioBlockBytes || s.BirthClaim.Validate() != nil || s.StateClaim.Validate() != nil || s.OriginalBase == 0 || s.OriginalBase > s.Identity.MinimumBytes || s.OriginalBase%s.Identity.BlockBytes != 0 || (s.Identity.MinimumBytes-s.OriginalBase)%s.Identity.BlockBytes != 0 || s.Identity.MaximumBytes == s.OriginalBase || s.HostBinding.Validate() != nil || string(s.HostBinding.NodeUID) != s.Identity.NodeUID || control.ValidateIncarnation(s.HostOrigin) != nil || control.ValidateIncarnation(s.LaunchNonce) != nil || s.QEMUDevice == 0 || s.QEMUInode == 0 || !filepath.IsAbs(s.QEMUPath) || filepath.Clean(s.QEMUPath) != s.QEMUPath {
		return fmt.Errorf("incomplete original initial bootstrap source")
	}
	for _, v := range []string{s.Owner.HolderID, s.LeaseUID, s.ConfigSecretUID, s.HostProfile, s.ContainerID} {
		if control.ValidateID(v, "initial source") != nil {
			return fmt.Errorf("invalid initial source identity")
		}
	}
	for _, v := range []string{s.RuntimeSHA256, s.ResourceSHA256, s.TemplatesSHA256, s.KernelBuildSHA256, s.PlacementSHA256, s.CommandSHA256, s.QEMUSHA256} {
		if !digestValid(v) {
			return fmt.Errorf("invalid original initial source digest")
		}
	}
	if s.Owner.Epoch == 0 || s.ReservedBytes < s.Identity.MinimumBytes || s.ReservedBytes > math.MaxInt64 || s.HostOverheadBytes == 0 || s.HostOverheadBytes > control.Uint64(math.MaxInt64)-s.ReservedBytes {
		return fmt.Errorf("initial source lacks complete finite reservation")
	}
	return nil
}

type initialBootReservation struct {
	Owner     journal.Owner
	LeaseUID  string
	Host      hostguard.Model
	Placement hostguard.Placement
}

// 调用者已持原Runner文件锁；沿原API读者认证首次持有，不绕到需要PG的budget.Initial。
func (r *Runner) readInitialBootReservation(ctx context.Context) (initialBootReservation, error) {
	var out initialBootReservation
	if r == nil || r.engine == nil {
		return out, fmt.Errorf("original Runner missing")
	}
	a, e := r.authorize(ctx)
	if e != nil {
		return out, e
	}
	snap, e := r.store.Load(ctx)
	if e != nil {
		return out, e
	}
	m, vm := snap.State(), snap.VirtualMachine()
	if m.Owner != a.Owner || m.Active != nil || m.HighWater != nil || len(m.History) != 0 || m.ExpiredResults != 0 || m.ResourceIdentity != nil || m.PGInstance != nil || m.BudgetHeld != nil || m.CapacityHeld != nil || m.ServiceClosure != nil || vm.DeletionTimestamp != nil || vm.Spec.DSGABootstrap == nil || !vm.Spec.DSGABootstrap.Ready {
		return out, fmt.Errorf("initial source is no longer pre-PG pristine")
	}
	l, e := r.readLease(ctx)
	if e != nil {
		return out, e
	}
	if string(l.UID) != a.LeaseUID || *l.Spec.HolderIdentity != m.Owner.HolderID || control.Uint64(uint64(*l.Spec.LeaseTransitions)+1) != m.Owner.Epoch {
		return out, fmt.Errorf("initial source original owner epoch changed")
	}
	slot, exact := vm.Spec.Guest.MemorySlotSize.AsInt64()
	slots := vm.Spec.Guest.MemorySlots
	if !exact || slot <= 0 || slots.Min == 0 || slots.Use < slots.Min || slots.Max < slots.Use || uint64(slots.Max) > uint64(math.MaxInt64)/uint64(slot) || control.Uint64(uint64(slot)*uint64(slots.Min)) != r.birth.Identity.MinimumBytes || control.Uint64(uint64(slot)*uint64(slots.Max)) != r.birth.Identity.MaximumBytes {
		return out, fmt.Errorf("original initial VM geometry changed")
	}
	node, e := r.core.Nodes().Get(ctx, r.node, metav1.GetOptions{})
	if e != nil {
		return out, e
	}
	if string(node.UID) != r.birth.Identity.NodeUID {
		return out, fmt.Errorf("original initial Host Node replaced")
	}
	if e = hostguard.InspectInitialVM(node, r.store.Binding(), r.pod, r.birth.Identity.RunnerPodUID, control.Uint64(uint64(slot)*uint64(slots.Use))); e != nil {
		return out, e
	}
	var host hostguard.Model
	if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &host) != nil || host.Profile == nil {
		return out, fmt.Errorf("original Host profile absent")
	}
	placement, e := host.Placement(r.birth.Identity.RunnerPodUID)
	if e != nil || placement.Settled != nil || placement.Barrier != nil || placement.HostOverheadBytes != host.Profile.VMOverheadBytes {
		return out, fmt.Errorf("initial placement has old service/barrier or changed overhead")
	}
	hl, e := r.coord.Leases(host.Binding.Namespace).Get(ctx, host.Binding.LeaseName, metav1.GetOptions{})
	if e != nil {
		return out, e
	}
	if hl == nil || hl.UID != host.Binding.LeaseUID || hl.ResourceVersion == "" || hl.DeletionTimestamp != nil || hl.Annotations[hostguard.LeaseAnnotation] != hostguard.LeaseMarker(host.Binding) || len(hl.OwnerReferences) != 1 {
		return out, fmt.Errorf("original Host Lease replaced")
	}
	owner := hl.OwnerReferences[0]
	if owner.APIVersion != "v1" || owner.Kind != "Node" || owner.Name != host.Binding.NodeName || owner.UID != host.Binding.NodeUID || owner.Controller == nil || !*owner.Controller {
		return out, fmt.Errorf("original Host Lease owner changed")
	}
	lastLease, e := r.readLease(ctx)
	if e != nil {
		return out, e
	}
	if lastLease.UID != l.UID || *lastLease.Spec.HolderIdentity != m.Owner.HolderID || control.Uint64(uint64(*lastLease.Spec.LeaseTransitions)+1) != m.Owner.Epoch {
		return out, fmt.Errorf("original VM owner changed across Host source reads")
	}
	// 预留不会因Host租约过期被释放；这里只固定原UID来源，不把新holder当本VM执行许可。
	return initialBootReservation{a.Owner, a.LeaseUID, host, placement}, nil
}

// 仅供未来RunQEMU在写process-launch之后、Start之前同flock调用；当前无共享入口。
// Runtime必须是Controller已冻结原字节，实际Secret在此独立GET，不重新生成Doc。
func (r *Runner) freezeInitialBootstrap(ctx context.Context, runtimeRaw []byte, cmd *exec.Cmd, base control.Uint64) (initialBootRef, error) {
	var ref initialBootRef
	if r == nil || r.engine == nil {
		return ref, fmt.Errorf("original Runner missing")
	}
	if len(runtimeRaw) == 0 || len(runtimeRaw) > 64*1024 {
		return ref, fmt.Errorf("original runtime exceeds bound")
	}
	err := r.engine.withRecord(ctx, func(rec *record, s *lockedState) error {
		if rec.Pending != nil || rec.Last != nil || rec.HighWater != nil || rec.LastPublished != nil {
			return fmt.Errorf("initial local journal already has memory history")
		}
		reservation, e := r.readInitialBootReservation(ctx)
		if e != nil {
			return e
		}
		snap, e := r.store.Load(ctx)
		if e != nil {
			return e
		}
		vm := snap.VirtualMachine()
		secret, e := r.core.Secrets(vm.Namespace).Get(ctx, vm.Name+"-config", metav1.GetOptions{})
		if e != nil {
			return e
		}
		if e = bootstrap.CheckReady(vm, secret, false); e != nil {
			return e
		}
		var doc resourceguard.RuntimeDocument
		if e = json.Unmarshal(runtimeRaw, &doc); e != nil {
			return e
		}
		if doc.ProtocolVersion != 2 || doc.ConfigSecretUID != string(secret.UID) || doc.Incarnation != r.store.Binding().Incarnation || !bytes.Equal(doc.SourceJSON, secret.Data[resourceguard.ResourceSecretKey]) || !bytes.Equal(doc.PolicyTemplatesJSON, secret.Data[resourceguard.PolicyTemplatesSecretKey]) {
			return fmt.Errorf("runtime no longer matches original immutable Secret")
		}
		logical, e := resourceguard.RenderRuntimePolicyAssets(runtimeRaw)
		if e != nil {
			return e
		}
		logical[resourceguard.RuntimeResourceAssetName] = append([]byte(nil), runtimeRaw...)
		plan, e := resourceguard.DeriveRuntimeBootRequirements(runtimeRaw, logical)
		if e != nil {
			return e
		}
		if !plan.Enabled() {
			return fmt.Errorf("initial base mode requires complete supported original policy assets")
		}
		originalResource, e := doc.Source()
		if e != nil {
			return e
		}
		if originalResource.Config.VMUID != r.birth.Identity.VMUID || originalResource.Config.ComputeInstanceID != vm.Spec.DSGABootstrap.ComputeInstanceID {
			return fmt.Errorf("resource identity differs from original VM")
		}
		digest, e := r.validateProcessCommand(cmd)
		if e != nil {
			return e
		}
		qemu, e := initialBootArgv(cmd, r.birth.Identity, base)
		if e != nil {
			return e
		}
		if uint64(base) > uint64(plan.BootMemoryUpperBoundBytes()) {
			return fmt.Errorf("selected original base exceeds qualified boot upper bound")
		}
		launchRaw, e := s.readProcessFile("process-launch.json")
		if e != nil {
			return e
		}
		var launch processLaunch
		if launchRaw == nil || control.UnmarshalObject(launchRaw, &launch) != nil || !bytes.Equal(launchRaw, initialBootCanonical(launch)) || launch.Identity != r.birth.Identity || launch.BirthClaim != r.engine.binding || launch.CommandSHA256 != digest || control.ValidateIncarnation(launch.Nonce) != nil || control.ValidateID(launch.ContainerID, "original container") != nil {
			return fmt.Errorf("original pre-Start process-launch binding missing")
		}
		device, inode, qemuSHA, e := initialBootExecutable(qemu)
		if e != nil {
			return e
		}
		last, e := r.readInitialBootReservation(ctx)
		if e != nil {
			return e
		}
		if reservation.Owner != last.Owner || reservation.LeaseUID != last.LeaseUID || reservation.Host.Binding != last.Host.Binding || reservation.Host.Origin != last.Host.Origin || *reservation.Host.Profile != *last.Host.Profile || !bytes.Equal(initialBootCanonical(reservation.Placement), initialBootCanonical(last.Placement)) {
			return fmt.Errorf("original initial reservation changed during source read")
		}
		source := initialBootSource{ProtocolVersion: 2, Identity: r.birth.Identity, BirthClaim: r.engine.binding, StateClaim: r.birth.StateClaim, Owner: last.Owner, LeaseUID: last.LeaseUID, OriginalBase: base, RuntimeSHA256: initialBootSHA(runtimeRaw), ResourceSHA256: doc.SourceSHA256, ConfigSecretUID: doc.ConfigSecretUID, TemplatesSHA256: doc.PolicyTemplatesSHA256, KernelBuildSHA256: initialBootSHA(initialBootCanonical(plan.KernelBuildReference())), HostBinding: last.Host.Binding, HostOrigin: last.Host.Origin, HostProfile: last.Host.Profile.ID, PlacementSHA256: initialBootSHA(initialBootCanonical(last.Placement)), ReservedBytes: last.Placement.MemoryBytes, HostOverheadBytes: last.Placement.HostOverheadBytes, CommandSHA256: digest, LaunchNonce: launch.Nonce, ContainerID: launch.ContainerID, QEMUPath: qemu, QEMUDevice: device, QEMUInode: inode, QEMUSHA256: qemuSHA}
		var e2 error
		ref, e2 = initialBootFreeze(s, source)
		return e2
	})
	return ref, err
}

// parse仅支持本显式模式的原生8MiB vm0；拒替换backend、重复字段和隐含默认。
func initialBootMemoryBytes(v string) (control.Uint64, error) {
	if v == "" {
		return 0, fmt.Errorf("missing QEMU bytes")
	}
	shift := uint(0)
	if c := v[len(v)-1]; c == 'K' || c == 'M' || c == 'G' {
		shift = map[byte]uint{'K': 10, 'M': 20, 'G': 30}[c]
		v = v[:len(v)-1]
	}
	n, e := strconv.ParseUint(v, 10, 64)
	if e != nil || n == 0 || strconv.FormatUint(n, 10) != v || n > uint64(math.MaxInt64)>>shift {
		return 0, fmt.Errorf("invalid finite QEMU bytes")
	}
	return control.Uint64(n << shift), nil
}
func initialBootFields(v string) (map[string]string, error) {
	out := map[string]string{}
	for _, f := range strings.Split(v, ",") {
		k, x, ok := strings.Cut(f, "=")
		if !ok || k == "" || x == "" {
			return nil, fmt.Errorf("QEMU field requires explicit value")
		}
		if _, ok := out[k]; ok {
			return nil, fmt.Errorf("duplicate QEMU field")
		}
		out[k] = x
	}
	return out, nil
}
func initialBootArgv(cmd *exec.Cmd, id Identity, base control.Uint64) (string, error) {
	if cmd == nil || len(cmd.Args) < 2 || cmd.Process != nil || id.Validate() != nil || base == 0 || base > id.MinimumBytes || base%id.BlockBytes != 0 || id.MaximumBytes == base {
		return "", fmt.Errorf("unstarted original low-base QEMU required")
	}
	args := cmd.Args[1:]
	qemu := cmd.Path
	if filepath.Base(qemu) == "cgexec" {
		if len(args) < 4 {
			return "", fmt.Errorf("bad original wrapper")
		}
		qemu, args = args[2], args[3:]
	}
	if !filepath.IsAbs(qemu) || filepath.Clean(qemu) != qemu {
		return "", fmt.Errorf("absolute original QEMU required")
	}
	seenM, seenB, seenD := false, false, false
	for i := 0; i < len(args); i++ {
		flag := args[i]
		if flag != "-m" && flag != "-object" && flag != "-device" {
			continue
		}
		if i+1 >= len(args) {
			return "", fmt.Errorf("incomplete QEMU geometry")
		}
		i++
		v := args[i]
		switch flag {
		case "-m":
			if seenM {
				return "", fmt.Errorf("duplicate machine memory")
			}
			seenM = true
			f, e := initialBootFields(v)
			if e != nil || len(f) != 3 || f["slots"] != "1" {
				return "", fmt.Errorf("explicit single-slot machine required")
			}
			b, e := initialBootMemoryBytes(f["size"])
			if e != nil || b != base {
				return "", fmt.Errorf("original base differs")
			}
			m, e := initialBootMemoryBytes(f["maxmem"])
			if e != nil || m != id.MaximumBytes {
				return "", fmt.Errorf("original max differs")
			}
		case "-object":
			if !strings.HasPrefix(v, "memory-backend-") {
				continue
			}
			if seenB {
				return "", fmt.Errorf("multiple memory backends")
			}
			seenB = true
			kind, rest, ok := strings.Cut(v, ",")
			f, e := initialBootFields(rest)
			if !ok || kind != "memory-backend-ram" || e != nil || f["id"] != "vmem0" || (len(f) != 2 && !(len(f) == 3 && f["prealloc"] == "off")) {
				return "", fmt.Errorf("unsupported original memory backend")
			}
			n, e := initialBootMemoryBytes(f["size"])
			if e != nil || n != id.MaximumBytes-base {
				return "", fmt.Errorf("original backend differs")
			}
		case "-device":
			if !strings.HasPrefix(v, "virtio-mem") {
				continue
			}
			if seenD {
				return "", fmt.Errorf("multiple virtio memory devices")
			}
			seenD = true
			kind, rest, ok := strings.Cut(v, ",")
			f, e := initialBootFields(rest)
			if !ok || kind != "virtio-mem-pci" || e != nil || len(f) != 5 || f["id"] != "vm0" || f["memdev"] != "vmem0" || f["node"] != "0" || f["requested-size"] != "0" {
				return "", fmt.Errorf("explicit empty original vm0 required")
			}
			n, e := initialBootMemoryBytes(f["block-size"])
			if e != nil || n != id.BlockBytes {
				return "", fmt.Errorf("original block differs")
			}
		}
	}
	if !seenM || !seenB || !seenD {
		return "", fmt.Errorf("missing explicit low-base geometry")
	}
	return qemu, nil
}
func initialBootExecutable(path string) (uint64, uint64, string, error) {
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if e != nil {
		return 0, 0, "", e
	}
	f := os.NewFile(uintptr(fd), "original-qemu")
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0022 != 0 || st.Mode&0111 == 0 || st.Uid != uint32(os.Geteuid()) || st.Size <= 0 || st.Size > 256<<20 {
		return 0, 0, "", fmt.Errorf("original QEMU image is not bounded protected executable")
	}
	b, e := io.ReadAll(io.LimitReader(f, 256<<20+1))
	if e != nil || int64(len(b)) != st.Size {
		return 0, 0, "", fmt.Errorf("original QEMU bytes changed")
	}
	return uint64(st.Dev), st.Ino, initialBootSHA(b), nil
}
