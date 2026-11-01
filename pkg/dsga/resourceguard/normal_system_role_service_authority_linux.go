//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"

	"golang.org/x/sys/unix"
)

const normalSystemAuthorityScope = "original-broker-owned-resource-authority-v1"

type normalSystemAuthoritySocket struct {
	Path   string `json:"path"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}
type normalSystemAuthorityReference struct {
	ProtocolVersion   uint32                         `json:"protocol_version"`
	Mode              string                         `json:"mode"`
	InstanceNonce     string                         `json:"instance_nonce"`
	Broker            nativeBirthConsumer            `json:"broker"`
	DeclarationSHA256 string                         `json:"declaration_sha256"`
	PreparedSHA256    string                         `json:"prepared_sha256"`
	Sockets           [3]normalSystemAuthoritySocket `json:"sockets"`
}

func (r normalSystemAuthorityReference) validate() error {
	if r.ProtocolVersion != 1 || r.Mode != normalSystemRoleBrokerOwnedAuthorityMode || r.Broker.PID <= 1 || r.Broker.StartTicks == 0 || !normalSystemRoleDeclarationDigest(r.Broker.InstanceNonce) {
		return ErrUnknown
	}
	for _, h := range []string{r.InstanceNonce, r.DeclarationSHA256, r.PreparedSHA256} {
		if !normalSystemRoleDeclarationDigest(h) {
			return ErrUnknown
		}
	}
	for i, path := range []string{FixedSocketPath, fixedArrayBirthSocketPath, fixedArrayPopulateSocketPath} {
		if r.Sockets[i].Path != path || r.Sockets[i].Device == 0 || r.Sockets[i].Inode == 0 {
			return ErrUnknown
		}
	}
	return nil
}

type normalSystemAuthorityStage struct {
	ProtocolVersion uint32                         `json:"protocol_version"`
	Scope           string                         `json:"scope"`
	Phase           string                         `json:"phase"`
	Origin          normalSystemRoleServiceOrigin  `json:"origin"`
	Reference       normalSystemAuthorityReference `json:"reference"`
	PreviousSHA256  string                         `json:"previous_sha256,omitempty"`
}
type normalSystemResourceAuthority struct {
	guard       *Guard
	source      *normalSystemLifecycleSource
	declaration *normalSystemRoleDeclarationSource
	origin      normalSystemRoleServiceOrigin
	admission   *normalAuthorityAdmission
	instance    *normalGuestResourceInstance
	reference   normalSystemAuthorityReference
	prepared    normalSystemRoleFileWitness
	producerMu  sync.Mutex
	compute     *normalSystemOriginalComputeLease
	attachment  *normalSystemResourceAttachment
	attachments map[string]*normalSystemResourceAttachment
	attachCount uint64
}

func normalSystemAuthorityStageName(nonce, phase string) string {
	return "role-resource-authority-" + nonce + "-" + phase + ".json"
}

// 本索引只保存本进程原g的opaque对象，不能由Config/路径/nonce重建；关闭后留
// tombstone防普通重启同g补签。生产单VM一个；过多构造拒绝，不开无限缓存。
var normalSystemAuthorityIndex = struct {
	sync.Mutex
	byGuard map[*Guard]*normalSystemResourceAuthority
}{byGuard: map[*Guard]*normalSystemResourceAuthority{}}

func registerNormalSystemAuthority(a *normalSystemResourceAuthority) error {
	normalSystemAuthorityIndex.Lock()
	defer normalSystemAuthorityIndex.Unlock()
	if a == nil || a.guard == nil || normalSystemAuthorityIndex.byGuard[a.guard] != nil || len(normalSystemAuthorityIndex.byGuard) >= 64 {
		return ErrUnknown
	}
	normalSystemAuthorityIndex.byGuard[a.guard] = a
	return nil
}
func lookupNormalSystemAuthority(g *Guard) *normalSystemResourceAuthority {
	normalSystemAuthorityIndex.Lock()
	defer normalSystemAuthorityIndex.Unlock()
	return normalSystemAuthorityIndex.byGuard[g]
}
func prepareNormalSystemResourceAuthority(lifetime, ctx context.Context, g *Guard, s *normalSystemLifecycleSource, d *normalSystemRoleDeclarationSource, proof *normalSystemRoleBrokerExecProof) (out *normalSystemResourceAuthority, err error) {
	if lifetime == nil || lifetime.Err() != nil || ctx == nil || g == nil || s == nil || d == nil || proof == nil || !d.declaration.brokerOwnsAuthority() {
		return nil, ErrUnknown
	}
	admission, e := newNormalAuthorityAdmission(g, proof.task)
	if e != nil {
		return nil, e
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return nil, e
	}
	a := &normalSystemResourceAuthority{guard: g, source: s, declaration: d, origin: proof.transfer.Origin, admission: admission, reference: normalSystemAuthorityReference{ProtocolVersion: 1, Mode: normalSystemRoleBrokerOwnedAuthorityMode, InstanceNonce: nonce, Broker: s.binding.RootConsumer, DeclarationSHA256: d.rawSHA}}
	if e = registerNormalSystemAuthority(a); e != nil {
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			a.close()
		}
	}()
	var intentSHA string
	e = g.withRecord(ctx, func(r *record, state *lockedState) error {
		if e := a.matchOriginalRecord(r, state); e != nil {
			return e
		}
		raw, e := json.Marshal(normalSystemAuthorityStage{ProtocolVersion: 1, Scope: normalSystemAuthorityScope, Phase: "intent", Origin: a.origin, Reference: a.reference})
		if e != nil {
			return e
		}
		if e = state.createPhysicalFile(normalSystemAuthorityStageName(nonce, "intent"), raw); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		intentSHA = physicalDigest(raw)
		return nil
	})
	if e != nil {
		return nil, e
	}
	sealed, e := sealedRuntimeSource()
	if e != nil {
		return nil, e
	}
	a.instance, e = prepareNormalGuestResourceInstance(lifetime, g, sealed, admission)
	if e != nil {
		return nil, e
	}
	if e = a.instance.probe(ctx); e != nil {
		return nil, e
	}
	if e = normalAuthorityPrepareCheckpoint(ctx, a); e != nil {
		return nil, e
	}
	for i, path := range []string{FixedSocketPath, fixedArrayBirthSocketPath, fixedArrayPopulateSocketPath} {
		var st unix.Stat_t
		if unix.Lstat(path, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != 0 {
			return nil, ErrUnknown
		}
		a.reference.Sockets[i] = normalSystemAuthoritySocket{Path: path, Device: st.Dev, Inode: st.Ino}
	}
	e = g.withRecord(ctx, func(r *record, state *lockedState) error {
		if e := a.matchOriginalRecord(r, state); e != nil {
			return e
		}
		raw, e := json.Marshal(normalSystemAuthorityStage{ProtocolVersion: 1, Scope: normalSystemAuthorityScope, Phase: "prepared", Origin: a.origin, Reference: a.reference, PreviousSHA256: intentSHA})
		if e != nil {
			return e
		}
		name := normalSystemAuthorityStageName(nonce, "prepared")
		if e = state.createPhysicalFile(name, raw); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		a.prepared, e = readNormalSystemRoleFileWitness(state, name)
		if e != nil || a.prepared.SHA256 != physicalDigest(raw) {
			return ErrUnknown
		}
		a.reference.PreparedSHA256 = a.prepared.SHA256
		return a.reference.validate()
	})
	if e != nil {
		return nil, e
	}
	success = true
	return a, nil
}
func (a *normalSystemResourceAuthority) matchOriginalRecord(r *record, state *lockedState) error {
	if a == nil || r == nil || state == nil || r.Identity != a.origin.ResourceIdentity || r.Floor != a.origin.ResourceFloor || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || r.Original != nil || r.Birth != nil {
		return ErrUnknown
	}
	raw, e := json.Marshal(r)
	if e != nil || physicalDigest(raw) != a.origin.ResourceStageSHA256 {
		return ErrUnknown
	}
	var st unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &st) != nil || st.Dev != a.origin.StateDevice || st.Ino != a.origin.StateInode {
		return ErrUnknown
	}
	return nil
}

// COMMIT处R1仍持resource锁；只核原prepared的私有fsync见证/实际字节并启门，
// 不反取锁、不写新的persistent阶段，避免R1等ACK而R2等同一flock。
func (a *normalSystemResourceAuthority) activate(ref *normalSystemAuthorityReference) error {
	if a == nil || ref == nil || !reflect.DeepEqual(*ref, a.reference) || a.reference.validate() != nil {
		return ErrUnknown
	}
	dir, e := os.Open(a.guard.config.StateDir)
	if e != nil {
		return e
	}
	defer dir.Close()
	var st unix.Stat_t
	if unix.Fstat(int(dir.Fd()), &st) != nil || st.Dev != a.origin.StateDevice || st.Ino != a.origin.StateInode {
		return ErrUnknown
	}
	got, e := readNormalSystemRoleFileWitness(&lockedState{dir: dir}, normalSystemAuthorityStageName(a.reference.InstanceNonce, "prepared"))
	if e != nil || got != a.prepared {
		return errors.Join(ErrUnknown, e)
	}
	if a.instance == nil || a.instance.guard != a.guard || a.instance.handler.guard != a.guard || a.instance.native == nil || a.instance.native.guard != a.guard {
		return ErrUnknown
	}
	return a.admission.activate()
}
func (a *normalSystemResourceAuthority) close() {
	if a == nil {
		return
	}
	a.admission.fail()
	if a.instance != nil {
		a.instance.close()
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if a.compute != nil {
		a.compute.close()
	}
	for _, attachment := range a.attachments {
		attachment.close()
	}
}
func (d *normalSystemRoleDeclarationSource) checkAuthorityMetadata(m normalSystemRoleServiceMetadata) error {
	if d == nil {
		return ErrUnknown
	}
	if !d.declaration.brokerOwnsAuthority() {
		if m.ResourceAuthority != nil {
			return ErrUnknown
		}
		return nil
	}
	if m.ResourceAuthority == nil || m.ResourceAuthority.validate() != nil || m.ResourceAuthority.DeclarationSHA256 != d.rawSHA || m.ResourceAuthority.Broker != m.Broker.RootConsumer {
		return ErrUnknown
	}
	return nil
}

func rejectNormalStandaloneBrokerOwnedAuthority() error {
	if _, e := os.Lstat(NormalSystemSourceManifestPath); os.IsNotExist(e) {
		return nil
	} else if e != nil {
		return e
	}
	f, e := rootFile(NormalSystemSourceManifestPath, false)
	if e != nil {
		return e
	}
	raw, e := nativeBirthReadBounded(f, 512<<10)
	f.Close()
	if e != nil {
		return e
	}
	m, e := DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		return e
	}
	if m.ProtocolVersion != 3 {
		return nil
	}
	f, e = rootFile(normalSystemRoleBrokerDeclarationPath, false)
	if e != nil {
		return e
	}
	raw, e = nativeBirthReadBounded(f, 16<<10)
	f.Close()
	if e != nil {
		return e
	}
	d, e := decodeNormalSystemRoleBrokerDeclaration(raw)
	if e != nil {
		return e
	}
	if d.brokerOwnsAuthority() {
		return errors.Join(ErrUnknown, fmt.Errorf("resource authority belongs to original role broker"))
	}
	return nil
}

// 仅构造器私有DEV checkpoint，可继续或报错；不能开启gate/选择来源。生产
// CLI的Background无此键，HTTP/request字段无法向这条启动ctx注入回调。
type normalAuthorityPrepareCheckpointKey struct{}

func normalAuthorityPrepareCheckpoint(ctx context.Context, a *normalSystemResourceAuthority) error {
	if checkpoint, ok := ctx.Value(normalAuthorityPrepareCheckpointKey{}).(func(*normalSystemResourceAuthority) error); ok {
		return checkpoint(a)
	}
	return nil
}
