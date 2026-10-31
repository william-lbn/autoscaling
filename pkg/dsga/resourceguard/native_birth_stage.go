//go:build linux

package resourceguard

import (
	"encoding/binary"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

type nativeBirthConsumer struct {
	InstanceNonce string         `json:"instance_nonce"`
	PID           int            `json:"pid"`
	StartTicks    control.Uint64 `json:"start_ticks"`
}
type nativeBirthPending struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	Scope           string                   `json:"scope"`
	Nonce           string                   `json:"nonce"`
	Source          nativeBirthSourceBinding `json:"source"`
	Original        ProcessIdentity          `json:"original"`
	Consumer        nativeBirthConsumer      `json:"consumer"`
	PeerUID         uint32                   `json:"peer_uid"`
	PeerGID         uint32                   `json:"peer_gid"`
	PrivateDevice   control.Uint64           `json:"private_device"`
	PrivateInode    control.Uint64           `json:"private_inode"`
	StateDevice     control.Uint64           `json:"state_device"`
	StateInode      control.Uint64           `json:"state_inode"`
	ResourceFloor   control.Uint64           `json:"resource_floor"`
}

func (p nativeBirthPending) validate() error {
	if _, err := nativeBirthDigest(p.Consumer.InstanceNonce); err != nil {
		return err
	}
	if p.ProtocolVersion != 1 || p.Scope != nativeBirthStageScope || p.Original.Validate() != nil ||
		p.Consumer.PID <= 1 || p.Consumer.PID == p.Original.PostmasterPID || p.Consumer.StartTicks == 0 || p.PeerUID == 0 || p.PeerGID == 0 ||
		p.PrivateDevice != p.Original.Identity.CgroupDevice || p.PrivateInode == 0 ||
		p.PrivateInode == p.Original.Identity.CgroupInode || p.StateDevice == 0 || p.StateInode == 0 || p.ResourceFloor == 0 {
		return fmt.Errorf("incomplete original native birth pending")
	}
	_, err := p.Source.expected(p.Nonce, p.Original.AddressSpaceMaxBytes)
	return err
}

type nativeBirthStagePhase uint32

const (
	nativeBirthStagePending nativeBirthStagePhase = iota
	nativeBirthStageBase
	nativeBirthStageFinal
	nativeBirthStageReleased
)

type nativeBirthStageEntry struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Scope           string                     `json:"scope"`
	Ref             NativeBirthRef             `json:"ref"`
	Phase           nativeBirthStagePhase      `json:"phase"`
	PreviousSHA256  string                     `json:"previous_sha256"`
	Transcript      nativeBirthTraceTranscript `json:"transcript"`
}

// Snapshot只是原文件内容的非授权观察：不携带可写stage、不自动重新ACK或发证。
type nativeBirthStageSnapshot struct {
	Ref     NativeBirthRef
	Pending nativeBirthPending
	Phase   nativeBirthStagePhase
	Entries []nativeBirthStageEntry
}

func nativeBirthStageRequest(p nativeBirthPending, tr nativeBirthTraceTranscript) error {
	expected, err := p.Source.expected(p.Nonce, p.Original.AddressSpaceMaxBytes)
	if err != nil || tr.Original != p.Original || tr.Request.Original != expected || tr.TracerTID <= 0 ||
		tr.TracerThreadRetired || tr.Cleanup != "" || tr.AuditSHA256 == [32]byte{} {
		return fmt.Errorf("native stage original tracer request differs")
	}
	binarySHA, err := nativeBirthDigest(p.Source.BinarySHA256)
	if err != nil || tr.BinarySHA256 != binarySHA {
		return fmt.Errorf("native stage original binary differs")
	}
	// 复用原wire严格geometry decoder，不能只相信已经反序列化的struct/range。
	var raw [nativeBirthRequestBytes]byte
	copy(raw[:8], nativeBirthWireMagic)
	binary.BigEndian.PutUint32(raw[8:12], 1)
	binary.BigEndian.PutUint32(raw[12:16], 1)
	copy(raw[16:48], expected.Nonce[:])
	copy(raw[48:80], expected.SourceSHA[:])
	copy(raw[80:112], expected.ProfileSHA[:])
	binary.BigEndian.PutUint64(raw[112:120], expected.AddressSpaceBytes)
	binary.BigEndian.PutUint32(raw[120:124], expected.MaxBuffers)
	binary.BigEndian.PutUint32(raw[124:128], expected.PageBytes)
	binary.BigEndian.PutUint64(raw[128:136], tr.Request.TotalReservedBytes)
	for i, r := range tr.Request.Ranges {
		off := 136 + i*24
		binary.BigEndian.PutUint64(raw[off:off+8], r.Base)
		binary.BigEndian.PutUint64(raw[off+8:off+16], r.ReservedBytes)
		binary.BigEndian.PutUint64(raw[off+16:off+24], r.ElementBytes)
	}
	decoded, err := decodeNativeBirthRequest(raw[:], expected)
	if err != nil || decoded != tr.Request {
		return fmt.Errorf("native stage request geometry invalid")
	}
	return nil
}
func (e nativeBirthStageEntry) validate(p nativeBirthPending, ref NativeBirthRef, previous string, prior *nativeBirthStageEntry) error {
	tr := e.Transcript
	if e.ProtocolVersion != 1 || e.Scope != nativeBirthStageScope || e.Ref != ref || ref.validate() != nil ||
		e.PreviousSHA256 != previous || nativeBirthStageRequest(p, tr) != nil {
		return fmt.Errorf("native stage chain differs from original")
	}
	if prior != nil && (tr.Request != prior.Transcript.Request || tr.AuditSHA256 != prior.Transcript.AuditSHA256 ||
		tr.TracerTID != prior.Transcript.TracerTID || tr.BinarySHA256 != prior.Transcript.BinarySHA256) {
		return fmt.Errorf("native stage changed frozen tracer or arrays")
	}
	switch e.Phase {
	case nativeBirthStageBase:
		if prior != nil || !tr.BaseVerified || tr.FinalVerified || tr.BasePersisted || tr.FinalPersisted || tr.DetachedNonAuthorizing {
			return fmt.Errorf("invalid native base pending")
		}
	case nativeBirthStageFinal:
		if prior == nil || prior.Phase != nativeBirthStageBase || !tr.BaseVerified || !tr.FinalVerified || !tr.BasePersisted || tr.FinalPersisted || tr.DetachedNonAuthorizing {
			return fmt.Errorf("invalid native verified pending-release")
		}
	case nativeBirthStageReleased:
		if prior == nil || prior.Phase != nativeBirthStageFinal || !tr.BaseVerified || !tr.FinalVerified || !tr.BasePersisted || !tr.FinalPersisted || !tr.DetachedNonAuthorizing {
			return fmt.Errorf("invalid native detached non-authorizing stage")
		}
	default:
		return fmt.Errorf("unknown native birth stage")
	}
	return nil
}
func nativeBirthStageName(nonce, phase string) string {
	return "native-birth." + nonce + "." + phase + ".json"
}
func nativeBirthStagePhaseName(p nativeBirthStagePhase) string {
	switch p {
	case nativeBirthStagePending:
		return "pending"
	case nativeBirthStageBase:
		return "base"
	case nativeBirthStageFinal:
		return "final"
	case nativeBirthStageReleased:
		return "released"
	}
	return ""
}
func nativeBirthRef(raw []byte, p nativeBirthPending) NativeBirthRef {
	return NativeBirthRef{ProtocolVersion: 1, Nonce: p.Nonce, PendingSHA256: physicalDigest(raw)}
}
