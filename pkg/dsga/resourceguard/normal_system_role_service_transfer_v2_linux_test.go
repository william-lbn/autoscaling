//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 仅语法/传输模型，绝不由这些值构造Kernel/FS/Memory生产来源。
func normalRoleTransferV2Model(t *testing.T) normalSystemRoleServiceTransfer {
	t.Helper()
	v := normalRoleServiceTransferFixture()
	h := strings.Repeat("ab", 32)
	v.Origin.ResourceIdentity.CgroupDevice = 31
	v.Origin.ResourceIdentity.CgroupInode = 32
	v.Origin.ResourceIdentity.SystemCgroupDevice = 50
	v.Origin.ResourceIdentity.SystemCgroupInode = 51
	v.ProtocolVersion = 2
	v.Origin.ProtocolVersion = 2
	v.Origin.Scope = normalSystemRoleServiceStartV2Scope
	v.Origin.Filesystem.ProtocolVersion = 2
	v.Origin.Filesystem.MemorySourceSHA256 = h
	for i := range v.Origin.Namespace {
		if v.Origin.Namespace[i].Name == "user" {
			v.Origin.Namespace[i].Inode = 0xEFFFFFFD
		}
	}
	build, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	k := normalKernelPrepV2HandoffReference{ProtocolVersion: 1, Family: normalInitialWALFamilyKernelPrepV2, PreparationSHA256: h, RuntimeDocumentSHA256: v.Origin.Parent.RuntimeDocumentSHA256, PhysicalPolicySHA256: h, KernelPolicySHA256: h, ProducerELFSHA256: h, OriginalRoot: v.Origin.Parent.RootConsumer, Identity: v.Origin.ResourceIdentity, Build: build, StateDevice: v.Origin.StateDevice, StateInode: v.Origin.StateInode, OriginalControl: normalKernelPrepV2Object{Device: 1, Inode: 2, MountID: 3, Mode: unix.S_IFREG | 0644}, OriginalLock: normalKernelPrepV2Object{Device: 1, Inode: 4, MountID: 3, Mode: unix.S_IFREG | 0600}, OriginalWAL: [2]normalSystemRoleFileWitness{{Device: 1, Inode: 10, Size: 20, SHA256: h}, {Device: 1, Inode: 11, Size: 21, SHA256: h}}}
	copy(k.OriginalNamespaces[:], v.Origin.Namespace)
	v.KernelPreparation = &k
	v.Origin.KernelPreparationSHA256, e = k.referenceSHA256()
	require.NoError(t, e)
	require.NoError(t, v.validate())
	return v
}
func cloneRoleTransferV2(v normalSystemRoleServiceTransfer) normalSystemRoleServiceTransfer {
	v.Origin.Namespace = append([]normalSystemRoleNamespaceIdentity{}, v.Origin.Namespace...)
	if v.KernelPreparation != nil {
		k := *v.KernelPreparation
		v.KernelPreparation = &k
	}
	return v
}
func TestNormalSystemRoleTransferV2OriginalKernelBinding(t *testing.T) {
	valid := normalRoleTransferV2Model(t)
	b, e := json.Marshal(valid)
	require.NoError(t, e)
	var decoded normalSystemRoleServiceTransfer
	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Equal(t, valid, decoded)
	for name, edit := range map[string]func(*normalSystemRoleServiceTransfer){
		"no ref":          func(v *normalSystemRoleServiceTransfer) { v.KernelPreparation = nil },
		"wrong version":   func(v *normalSystemRoleServiceTransfer) { v.ProtocolVersion = 3 },
		"old origin":      func(v *normalSystemRoleServiceTransfer) { v.Origin.ProtocolVersion = 1 },
		"old FS":          func(v *normalSystemRoleServiceTransfer) { v.Origin.Filesystem.ProtocolVersion = 1 },
		"no memory SHA":   func(v *normalSystemRoleServiceTransfer) { v.Origin.Filesystem.MemorySourceSHA256 = "" },
		"wrong bound SHA": func(v *normalSystemRoleServiceTransfer) { v.Origin.KernelPreparationSHA256 = strings.Repeat("cd", 32) },
		"wrong boot": func(v *normalSystemRoleServiceTransfer) {
			v.KernelPreparation.Identity.GuestBootID = "22222222-2222-4333-8444-555555555555"
		},
		"wrong original nonce": func(v *normalSystemRoleServiceTransfer) {
			v.KernelPreparation.OriginalRoot.InstanceNonce = strings.Repeat("01", 32)
		},
		"wrong namespace": func(v *normalSystemRoleServiceTransfer) { v.KernelPreparation.OriginalNamespaces[0].Inode++ },
		"wrong state":     func(v *normalSystemRoleServiceTransfer) { v.KernelPreparation.StateInode++ },
		"wrong runtime": func(v *normalSystemRoleServiceTransfer) {
			v.KernelPreparation.RuntimeDocumentSHA256 = strings.Repeat("cd", 32)
		},
		"old build": func(v *normalSystemRoleServiceTransfer) {
			v.KernelPreparation.Build, _ = LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
		},
	} {
		t.Run(name, func(t *testing.T) { v := cloneRoleTransferV2(valid); edit(&v); require.Error(t, v.validate()) })
	}
	dRaw, e := NormalSystemRoleBrokerDeclarationV3JSON(valid.KernelPreparation.ProducerELFSHA256, strings.Repeat("cd", 32))
	require.NoError(t, e)
	d, e := decodeNormalSystemRoleBrokerDeclaration(dRaw)
	require.NoError(t, e)
	require.NoError(t, valid.matchesDeclaration(d))
	d.ExecutableSHA256 = strings.Repeat("ef", 32)
	require.Error(t, valid.matchesDeclaration(d))
	for _, version := range []int{1, 2} {
		var raw []byte
		if version == 1 {
			raw, e = NormalSystemRoleBrokerDeclarationJSON(strings.Repeat("ab", 32), strings.Repeat("cd", 32))
		} else {
			raw, e = NormalSystemRoleBrokerDeclarationV2JSON(strings.Repeat("ab", 32), strings.Repeat("cd", 32))
		}
		require.NoError(t, e)
		old, e := decodeNormalSystemRoleBrokerDeclaration(raw)
		require.NoError(t, e)
		require.Error(t, valid.matchesDeclaration(old))
		require.NoError(t, normalRoleServiceTransferFixture().matchesDeclaration(old))
	}
	require.Error(t, normalRoleServiceTransferFixture().matchesDeclaration(d))
}
func TestNormalSystemRoleTransferV2StrictNestedJSON(t *testing.T) {
	valid := normalRoleTransferV2Model(t)
	raw, e := json.Marshal(valid)
	require.NoError(t, e)
	for name, edit := range map[string]func(map[string]json.RawMessage){
		"missing ref": func(m map[string]json.RawMessage) { delete(m, "kernel_preparation") },
		"null ref":    func(m map[string]json.RawMessage) { m["kernel_preparation"] = json.RawMessage(`null`) },
		"nested alias": func(m map[string]json.RawMessage) {
			m["kernel_preparation"] = []byte(strings.Replace(string(m["kernel_preparation"]), `"family":`, `"Family":`, 1))
		},
		"deep alias": func(m map[string]json.RawMessage) {
			m["kernel_preparation"] = []byte(strings.Replace(string(m["kernel_preparation"]), `"start_ticks":`, `"Start_Ticks":`, 1))
		},
		"deep missing": func(m map[string]json.RawMessage) {
			m["kernel_preparation"] = []byte(strings.Replace(string(m["kernel_preparation"]), `"uid":0,`, "", 1))
		},
		"wrong WAL array": func(m map[string]json.RawMessage) {
			var k map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(m["kernel_preparation"], &k))
			k["original_wal"] = json.RawMessage(`[]`)
			m["kernel_preparation"], _ = json.Marshal(k)
		},
		"origin nested case alias": func(m map[string]json.RawMessage) {
			m["origin"] = []byte(strings.Replace(string(m["origin"]), `"vm_uid":`, `"VM_UID":`, 1))
		},
		"origin namespace nested case alias": func(m map[string]json.RawMessage) {
			m["origin"] = []byte(strings.Replace(string(m["origin"]), `"name":`, `"Name":`, 1))
		},
		"missing origin SHA": func(m map[string]json.RawMessage) {
			var o map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(m["origin"], &o))
			delete(o, "kernel_preparation_sha256")
			m["origin"], _ = json.Marshal(o)
		},
		"null origin SHA": func(m map[string]json.RawMessage) {
			var o map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(m["origin"], &o))
			o["kernel_preparation_sha256"] = json.RawMessage(`null`)
			m["origin"], _ = json.Marshal(o)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var m map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &m))
			edit(m)
			b, e := json.Marshal(m)
			require.NoError(t, e)
			var v normalSystemRoleServiceTransfer
			require.Error(t, json.Unmarshal(b, &v))
		})
	}
	for _, value := range []string{`null`, `{}`} {
		t.Run("legacy refuses ref "+value, func(t *testing.T) {
			b, e := json.Marshal(normalRoleServiceTransferFixture())
			require.NoError(t, e)
			var m map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(b, &m))
			m["kernel_preparation"] = json.RawMessage(value)
			b, e = json.Marshal(m)
			require.NoError(t, e)
			var v normalSystemRoleServiceTransfer
			require.Error(t, json.Unmarshal(b, &v))
		})
	}
	old := normalRoleServiceTransferFixture()
	b, e := json.Marshal(old.Origin)
	require.NoError(t, e)
	for _, value := range []string{`null`, `""`} {
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(b, &m))
		m["kernel_preparation_sha256"] = json.RawMessage(value)
		x, e := json.Marshal(m)
		require.NoError(t, e)
		var o normalSystemRoleServiceOrigin
		require.Error(t, json.Unmarshal(x, &o))
	}
}
func TestNormalSystemRoleServiceFixedFiveDescriptorTransfer(t *testing.T) {
	for _, count := range []int{4, 5, 6} {
		t.Run(fmt.Sprintf("rights %d", count), func(t *testing.T) {
			sender, receiver := roleServiceTestPair(t)
			files := make([]*os.File, count)
			fds := make([]int, count)
			for i := range files {
				f, e := os.CreateTemp(t.TempDir(), "original")
				require.NoError(t, e)
				files[i] = f
				fds[i] = int(f.Fd())
				defer f.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			v := normalRoleTransferV2Model(t)
			if count == 5 {
				require.NoError(t, normalSystemRoleServiceWriteTransfer(sender, v, files, ctx))
			} else {
				payload, e := json.Marshal(v)
				require.NoError(t, e)
				raw := make([]byte, normalSystemRoleServiceStartupBytes)
				copy(raw, v.transferMagic())
				binary.BigEndian.PutUint32(raw[8:12], uint32(len(payload)))
				copy(raw[12:], payload)
				_, _, e = sender.WriteMsgUnix(raw, unix.UnixRights(fds...), nil)
				require.NoError(t, e)
			}
			actual, received, e := normalSystemRoleServiceReadTransfer(receiver, ctx)
			if count != 5 {
				require.ErrorIs(t, e, ErrUnknown)
				require.Nil(t, received)
				return
			}
			require.NoError(t, e)
			require.Equal(t, v, actual)
			require.Len(t, received, 5)
			for i, f := range received {
				defer f.Close()
				flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
				require.NoError(t, e)
				require.NotZero(t, flags&unix.FD_CLOEXEC)
				// SCM继承同一OFD：在原fd上Seek后，收到的对应fd观察同一offset。不是仅比inode。
				_, e = files[i].Seek(int64(i+3), 0)
				require.NoError(t, e)
				offset, e := f.Seek(0, 1)
				require.NoError(t, e)
				require.Equal(t, int64(i+3), offset)
			}
		})
	}
	for _, magic := range []string{"NSYSBF01", "NSYSBF03"} {
		t.Run("header "+magic, func(t *testing.T) {
			sender, receiver := roleServiceTestPair(t)
			v := normalRoleTransferV2Model(t)
			b, e := json.Marshal(v)
			require.NoError(t, e)
			raw := make([]byte, normalSystemRoleServiceStartupBytes)
			copy(raw, magic)
			binary.BigEndian.PutUint32(raw[8:12], uint32(len(b)))
			copy(raw[12:], b)
			fds := []int{}
			for i := 0; i < 5; i++ {
				f, e := os.CreateTemp(t.TempDir(), "orig")
				require.NoError(t, e)
				defer f.Close()
				fds = append(fds, int(f.Fd()))
			}
			_, _, e = sender.WriteMsgUnix(raw, unix.UnixRights(fds...), nil)
			require.NoError(t, e)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, received, e := normalSystemRoleServiceReadTransfer(receiver, ctx)
			require.ErrorIs(t, e, ErrUnknown)
			require.Nil(t, received)
		})
	}
}
func TestNormalSystemRoleKernelTransferRequiresOriginalProof(t *testing.T) {
	for _, p := range []*normalSystemRoleBrokerExecProof{nil, {}} {
		_, fd, e := p.originalKernelPrepV2TransferLocked(context.Background(), nil, nil)
		require.ErrorIs(t, e, ErrUnknown)
		require.Nil(t, fd)
	}
}
