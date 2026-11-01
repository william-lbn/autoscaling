//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"sync"
)

// 仅不可变历史值；比较/JSON不能产生同Guard或原FD能力。
type normalKernelPrepV2HandoffReference struct {
	ProtocolVersion       uint32                               `json:"protocol_version"`
	Family                string                               `json:"family"`
	PreparationSHA256     string                               `json:"preparation_sha256"`
	RuntimeDocumentSHA256 string                               `json:"runtime_document_sha256"`
	PhysicalPolicySHA256  string                               `json:"physical_policy_sha256"`
	KernelPolicySHA256    string                               `json:"kernel_policy_sha256"`
	ProducerELFSHA256     string                               `json:"producer_elf_sha256"`
	OriginalRoot          nativeBirthConsumer                  `json:"original_root"`
	Identity              Identity                             `json:"identity"`
	Build                 PhysicalKernelBuildReference         `json:"build"`
	StateDevice           uint64                               `json:"state_device"`
	StateInode            uint64                               `json:"state_inode"`
	OriginalLock          normalKernelPrepV2Object             `json:"original_lock"`
	OriginalControl       normalKernelPrepV2Object             `json:"original_control"`
	OriginalNamespaces    [8]normalSystemRoleNamespaceIdentity `json:"original_namespaces"`
	OriginalWAL           [2]normalSystemRoleFileWitness       `json:"original_wal"`
}

func (r normalKernelPrepV2HandoffReference) validate() error {
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || r.ProtocolVersion != 1 || r.Family != normalInitialWALFamilyKernelPrepV2 || r.Build != ref || !ref.ControlMetadataGuard() || !ref.StrictPageCounterIRQBoundary() || r.Identity.Validate() != nil || r.OriginalRoot.PID <= 1 || r.OriginalRoot.StartTicks == 0 || !physicalDigestValid(r.OriginalRoot.InstanceNonce) || r.StateDevice == 0 || r.StateInode == 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("kernel2 original handoff identity/version differs"))
	}
	for _, h := range []string{r.PreparationSHA256, r.RuntimeDocumentSHA256, r.PhysicalPolicySHA256, r.KernelPolicySHA256, r.ProducerELFSHA256} {
		if !physicalDigestValid(h) {
			return ErrUnknown
		}
	}
	for i, o := range []normalKernelPrepV2Object{r.OriginalControl, r.OriginalLock} {
		mode := uint32(0644)
		if i == 1 {
			mode = 0600
		}
		if o.Device == 0 || o.Inode == 0 || o.MountID == 0 || o.UID != 0 || o.GID != 0 || o.Mode&unix.S_IFMT != unix.S_IFREG || o.Mode&0777 != mode {
			return ErrUnknown
		}
	}
	seen := map[string]bool{}
	for _, n := range r.OriginalNamespaces {
		switch n.Name {
		case "mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time":
		default:
			return ErrUnknown
		}
		if seen[n.Name] || n.Device == 0 || n.Inode == 0 || n.Name == "user" && n.Inode != 0xEFFFFFFD {
			return ErrUnknown
		}
		seen[n.Name] = true
	}
	for _, w := range r.OriginalWAL {
		if w.Device == 0 || w.Inode == 0 || w.Size <= 0 || w.Size > 64<<10 || !physicalDigestValid(w.SHA256) {
			return ErrUnknown
		}
	}
	return nil
}
func (r normalKernelPrepV2HandoffReference) referenceSHA256() (string, error) {
	if e := r.validate(); e != nil {
		return "", e
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return "", e
	}
	return physicalDigest(raw), nil
}

// P2的后继opaque proof实现此私有能力。没有decl3/transfer2时type assertion拒绝，绝不退回open路径。
type normalKernelPrepV2TransferReader interface {
	originalKernelPrepV2TransferLocked(context.Context, *lockedState, *normalSystemLifecycleSource) (normalKernelPrepV2HandoffReference, *os.File, error)
}

type normalKernelPrepV2HandoffPin struct {
	mu        sync.Mutex
	closed    bool
	g         *Guard
	source    *normalKernelPrepV2OriginalGuardSource
	reference normalKernelPrepV2HandoffReference
	control   *os.File
}

func (g *Guard) pinOriginalKernelPrepV2ForBrokerLocked(ctx context.Context, state *lockedState, filesystem *normalFilesystemOriginSource) (*normalKernelPrepV2HandoffPin, error) {
	if g == nil || state == nil || filesystem == nil || filesystem.g != g {
		return nil, ErrUnknown
	}
	if _, e := filesystem.referenceLocked(ctx, state); e != nil {
		return nil, e
	}
	original, e := g.normalKernelPrepV2OriginalLocked(ctx, state)
	if e != nil {
		return nil, e
	}
	src := original.source
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.closed || len(src.witnesses) != 2 {
		return nil, ErrUnknown
	}
	b, ok := src.backend.(*normalKernelPrepV2LinuxBackend)
	if !ok {
		return nil, ErrUnknown
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if e = b.verify(); e != nil {
		return nil, e
	}
	after := src.evidence.After
	build, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil {
		return nil, e
	}
	ref := normalKernelPrepV2HandoffReference{ProtocolVersion: 1, Family: normalInitialWALFamilyKernelPrepV2, PreparationSHA256: original.preparationSHA, RuntimeDocumentSHA256: src.evidence.RuntimeDocumentSHA256, PhysicalPolicySHA256: src.evidence.PhysicalOriginPolicySHA256, KernelPolicySHA256: src.evidence.PolicySHA256, ProducerELFSHA256: after.ProducerELFSHA256, OriginalRoot: after.OriginalRoot, Identity: after.Kernel.Identity, Build: build, StateDevice: uint64(after.Kernel.StateDevice), StateInode: uint64(after.Kernel.StateInode), OriginalLock: after.Lock, OriginalControl: after.Control}
	if len(after.Namespaces) != len(ref.OriginalNamespaces) {
		return nil, ErrUnknown
	}
	copy(ref.OriginalNamespaces[:], after.Namespaces)
	ref.OriginalWAL = [2]normalSystemRoleFileWitness{src.witnesses[normalKernelPrepV2IntentName], src.witnesses[normalKernelPrepV2CompleteName]}
	if e = normalKernelPrepV2MatchFilesystemReference(state, filesystem, ref); e != nil {
		return nil, e
	}
	fd, e := unix.FcntlInt(b.control.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	held := os.NewFile(uintptr(fd), "original kernel2 handoff read-only control")
	if e = normalKernelPrepV2TransferredControl(held, ref.OriginalControl); e != nil {
		held.Close()
		return nil, e
	}
	return &normalKernelPrepV2HandoffPin{g: g, source: original, reference: ref, control: held}, nil
}
func (p *normalKernelPrepV2HandoffPin) originalTransferDescriptorLocked(ctx context.Context, state *lockedState) (normalKernelPrepV2HandoffReference, *os.File, error) {
	var zero normalKernelPrepV2HandoffReference
	if p == nil {
		return zero, nil, ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.control == nil || p.source == nil {
		return zero, nil, ErrUnknown
	}
	h, e := p.source.preparationReferenceLocked(ctx, state)
	if e != nil || h != p.reference.PreparationSHA256 {
		return zero, nil, errors.Join(ErrUnknown, e)
	}
	if e = normalKernelPrepV2TransferredControl(p.control, p.reference.OriginalControl); e != nil {
		return zero, nil, e
	}
	fd, e := unix.FcntlInt(p.control.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return zero, nil, e
	}
	return p.reference, os.NewFile(uintptr(fd), "original kernel2 SCM read-only control"), nil
}
func (p *normalKernelPrepV2HandoffPin) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		if p.control != nil {
			p.control.Close()
		}
	}
}
