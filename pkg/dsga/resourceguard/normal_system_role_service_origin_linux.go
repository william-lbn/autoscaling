//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const normalSystemRoleServiceStartScope = "original-system-role-broker-start-v1"
const normalSystemRoleServiceStartupBytes = 32768
const normalSystemRoleServiceMetadataPath = "/run/neon-dsga/system-role-consumer.json"

type normalSystemRoleServiceOrigin struct {
	ProtocolVersion     uint32                              `json:"protocol_version"`
	Scope               string                              `json:"scope"`
	Nonce               string                              `json:"nonce"`
	Parent              normalSystemLifecycleBinding        `json:"parent"`
	Filesystem          normalFilesystemOriginReference     `json:"filesystem"`
	DeclarationSHA256   string                              `json:"declaration_sha256"`
	Namespace           []normalSystemRoleNamespaceIdentity `json:"namespace"`
	ResourceIdentity    Identity                            `json:"resource_identity"`
	ResourceFloor       control.Uint64                      `json:"resource_floor"`
	StateDevice         uint64                              `json:"state_device"`
	StateInode          uint64                              `json:"state_inode"`
	ResourceStageSHA256 string                              `json:"resource_stage_sha256"`
}
type normalSystemRoleServiceStage struct {
	ResourceAuthority *normalSystemAuthorityReference `json:"resource_authority,omitempty"`
	ProtocolVersion   uint32                          `json:"protocol_version"`
	Origin            normalSystemRoleServiceOrigin   `json:"origin"`
	Stage             string                          `json:"stage"`
	PreviousSHA256    string                          `json:"previous_sha256,omitempty"`
	BrokerTask        *normalSystemRoleObservation    `json:"broker_task,omitempty"`
	Broker            *normalSystemLifecycleBinding   `json:"broker,omitempty"`
	SocketDevice      uint64                          `json:"socket_device,omitempty"`
	SocketInode       uint64                          `json:"socket_inode,omitempty"`
	ProbeNonceSHA256  string                          `json:"probe_nonce_sha256,omitempty"`
}
type normalSystemRoleServiceStartup struct {
	ProtocolVersion uint32                       `json:"protocol_version"`
	Released        normalSystemRoleServiceStage `json:"released"`
	ReleasedSHA256  string                       `json:"released_sha256"`
}
type normalSystemRoleServiceMetadata struct {
	ResourceAuthority *normalSystemAuthorityReference `json:"resource_authority,omitempty"`
	ProtocolVersion   uint32                          `json:"protocol_version"`
	Origin            normalSystemRoleServiceOrigin   `json:"origin"`
	Broker            normalSystemLifecycleBinding    `json:"broker"`
	ReleasedSHA256    string                          `json:"released_sha256"`
	ReadySHA256       string                          `json:"ready_sha256,omitempty"`
	SocketDevice      uint64                          `json:"socket_device"`
	SocketInode       uint64                          `json:"socket_inode"`
}

func normalSystemRoleServiceStageName(nonce, stage string) string {
	return "role-broker-start-" + nonce + "-" + stage + ".json"
}
func (o normalSystemRoleServiceOrigin) validate() error {
	if o.ProtocolVersion != 1 || o.Scope != normalSystemRoleServiceStartScope || !normalSystemRoleDeclarationDigest(o.Nonce) || !normalSystemRoleDeclarationDigest(o.DeclarationSHA256) || !normalSystemRoleDeclarationDigest(o.ResourceStageSHA256) || o.ResourceFloor == 0 || o.StateDevice == 0 || o.StateInode == 0 || o.Parent.RootConsumer.PID <= 1 || o.Parent.RootConsumer.StartTicks == 0 || !normalSystemRoleDeclarationDigest(o.Parent.RootConsumer.InstanceNonce) || !validGuestBoot(o.Parent.GuestBootID) {
		return fmt.Errorf("original broker origin malformed")
	}
	f := o.Filesystem
	if f.LayoutVersion != 2 || normalFilesystemValidateReferenceVersion(f) != nil || f.OriginalRoot != o.Parent.RootConsumer || f.GuestBootID != o.Parent.GuestBootID || f.RuntimeDocumentSHA256 != o.Parent.RuntimeDocumentSHA256 || f.NativeBuildSHA256 != o.Parent.NativeBuildSHA256 || f.SystemManifestSHA256 != o.Parent.SystemManifestSHA256 || !normalSystemRoleDeclarationDigest(f.IntentSHA256) || !normalSystemRoleDeclarationDigest(f.CompletionSHA256) || !normalSystemRoleDeclarationDigest(f.VminitSHA256) || !f.Root.Readonly || f.Root.MountID == 0 || f.Namespace.Inode == 0 {
		return fmt.Errorf("original broker filesystem producer differs")
	}
	if len(o.Namespace) != 8 {
		return fmt.Errorf("original broker namespace inventory differs")
	}
	seen := map[string]bool{}
	for _, n := range o.Namespace {
		if n.Device == 0 || n.Inode == 0 || seen[n.Name] {
			return fmt.Errorf("original broker namespace identity differs")
		}
		seen[n.Name] = true
		if n.Name == "mnt" && (n.Device != f.Namespace.Device || n.Inode != f.Namespace.Inode) {
			return fmt.Errorf("original broker filesystem namespace differs")
		}
	}
	for _, n := range []string{"mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time"} {
		if !seen[n] {
			return fmt.Errorf("original broker namespace missing")
		}
	}
	if o.ResourceIdentity.GuestBootID != o.Parent.GuestBootID || o.ResourceIdentity.VMUID != o.Parent.VMUID || o.ResourceIdentity.ComputeInstanceID != o.Parent.ComputeInstanceID {
		return fmt.Errorf("original broker VM identity differs")
	}
	return nil
}
func (m normalSystemRoleServiceMetadata) validatePair() error {
	if m.ResourceAuthority != nil && (m.ResourceAuthority.validate() != nil || m.ResourceAuthority.Broker != m.Broker.RootConsumer || m.ResourceAuthority.DeclarationSHA256 != m.Origin.DeclarationSHA256) {
		return ErrUnknown
	}
	if m.ProtocolVersion != 1 || m.Origin.validate() != nil || !normalSystemRoleImmutableBindingsEqual(m.Origin.Parent, m.Broker) || m.Broker.RootConsumer.PID <= 1 || m.Broker.RootConsumer.StartTicks == 0 || !normalSystemRoleDeclarationDigest(m.Broker.RootConsumer.InstanceNonce) || m.Broker.RootConsumer == m.Origin.Parent.RootConsumer || !normalSystemRoleDeclarationDigest(m.ReleasedSHA256) {
		return fmt.Errorf("original initialRoot/broker pair differs")
	}
	if m.ReadySHA256 != "" && !normalSystemRoleDeclarationDigest(m.ReadySHA256) {
		return fmt.Errorf("original broker ready reference differs")
	}
	return nil
}
func (m normalSystemRoleServiceMetadata) validate() error {
	if e := m.validatePair(); e != nil {
		return e
	}
	if m.SocketDevice == 0 || m.SocketInode == 0 {
		return fmt.Errorf("original broker socket identity missing")
	}
	return nil
}
func normalSystemRoleServiceWriteObject(conn *net.UnixConn, magic string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if len(magic) != 8 || len(b) > normalSystemRoleServiceStartupBytes-12 {
		return fmt.Errorf("fixed broker startup exceeds bound")
	}
	raw := make([]byte, normalSystemRoleServiceStartupBytes)
	copy(raw, magic)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(b)))
	copy(raw[12:], b)
	n, e := conn.Write(raw)
	if e != nil || n != len(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("original broker startup write unconfirmed"))
	}
	return nil
}
func normalSystemRoleServiceReadObject(conn *net.UnixConn, deadlineContext context.Context, magic string, out any) error {
	deadline, ok := deadlineContext.Deadline()
	if !ok {
		return fmt.Errorf("bounded broker startup required")
	}
	raw, e := arrayServicePacket(conn, deadline, normalSystemRoleServiceStartupBytes, false)
	if e != nil {
		return e
	}
	if string(raw[:8]) != magic {
		return fmt.Errorf("original broker startup action differs")
	}
	n := int(binary.BigEndian.Uint32(raw[8:12]))
	if n <= 0 || n > len(raw)-12 {
		return fmt.Errorf("original broker startup length differs")
	}
	for _, b := range raw[12+n:] {
		if b != 0 {
			return fmt.Errorf("original broker startup padding differs")
		}
	}
	return control.UnmarshalObject(raw[12:12+n], out)
}
func normalSystemRoleServiceReadStage(state *lockedState, origin normalSystemRoleServiceOrigin, stage, sha string) (normalSystemRoleServiceStage, error) {
	var out normalSystemRoleServiceStage
	raw, found, e := state.readPhysicalFile(normalSystemRoleServiceStageName(origin.Nonce, stage))
	if e != nil || !found || physicalDigest(raw) != sha {
		return out, errors.Join(ErrUnknown, e, fmt.Errorf("original broker durable stage missing or changed"))
	}
	return decodeNormalSystemRoleServiceStage(raw, origin, stage)
}
func normalSystemRoleServiceAppendStage(state *lockedState, entry normalSystemRoleServiceStage) (string, error) {
	raw, e := json.Marshal(entry)
	if e != nil {
		return "", e
	}
	if e = state.createPhysicalFile(normalSystemRoleServiceStageName(entry.Origin.Nonce, entry.Stage), raw); e != nil {
		return "", errors.Join(ErrUnknown, e)
	}
	return physicalDigest(raw), nil
}
func normalSystemRoleServiceRejectHistory(state *lockedState) error {
	fd, e := unix.Openat(int(state.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "original-role-broker-history")
	defer f.Close()
	names, e := f.Readdirnames(4097)
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if len(names) > 4096 {
		return errors.Join(ErrUnknown, fmt.Errorf("broker history inventory exceeds bound"))
	}
	for _, name := range names {
		if strings.HasPrefix(name, "role-broker-start-") {
			return errors.Join(ErrUnknown, fmt.Errorf("original broker history requires explicit original recovery"))
		}
	}
	return nil
}

func decodeNormalSystemRoleServiceStage(raw []byte, origin normalSystemRoleServiceOrigin, stage string) (normalSystemRoleServiceStage, error) {
	var out normalSystemRoleServiceStage
	if e := control.UnmarshalObject(raw, &out); e != nil {
		return out, e
	}
	a, _ := json.Marshal(out.Origin)
	b, _ := json.Marshal(origin)
	if out.ProtocolVersion != 1 || out.Stage != stage || string(a) != string(b) {
		return out, fmt.Errorf("original broker stage binding differs")
	}
	return out, nil
}
