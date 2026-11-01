//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 只是供原FS intent绑定的不可变历史投影，不是可从JSON认领的当前Root Source。
// 仅真实8确认且本Root完成全链fsync的opaque source能生产该投影。
type normalMemoryOriginConfirmedReference struct {
	ProtocolVersion       uint32                          `json:"protocol_version"`
	OriginalRoot          nativeBirthConsumer             `json:"original_root"`
	GuestBootID           string                          `json:"guest_boot_id"`
	RuntimeDocumentSHA256 string                          `json:"runtime_document_sha256"`
	ResourceSourceSHA256  string                          `json:"resource_source_sha256"`
	InitialWALFamily      string                          `json:"original_initial_wal_family,omitempty"`
	InitialWAL            initialPrebootGuestWitnessKey   `json:"original_initial_wal"`
	Files                 [3]arrayPopulateHistoryIdentity `json:"files"`
	SHA256                [3]string                       `json:"sha256"`
}

func normalMemoryConfirmedFiles() [3]string {
	return [3]string{normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile, normalMemoryGuestCommitFile}
}
func (v normalMemoryOriginConfirmedReference) referenceSHA256() (string, error) {
	if v.ProtocolVersion != 2 || v.OriginalRoot.PID <= 1 || v.OriginalRoot.StartTicks == 0 || !physicalDigestValid(v.OriginalRoot.InstanceNonce) || !physicalDigestValid(v.RuntimeDocumentSHA256) || !physicalDigestValid(v.ResourceSourceSHA256) {
		return "", ErrUnknown
	}
	if _, e := normalInitialWALFiles(v.InitialWALFamily); e != nil {
		return "", e
	}
	for _, h := range v.SHA256 {
		if !physicalDigestValid(h) {
			return "", ErrUnknown
		}
	}
	raw, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	return physicalDigest(raw), nil
}
func (o *normalMemoryOriginSource) confirmedReferenceLocked(ctx context.Context, s *lockedState, g *Guard) (normalMemoryOriginConfirmedReference, error) {
	var z normalMemoryOriginConfirmedReference
	if o == nil || o.commit2 == nil {
		return z, errors.Join(ErrUnknown, fmt.Errorf("confirmed memory2 opaque source absent"))
	}
	if e := o.recheckLocked(ctx, s, g); e != nil {
		return z, e
	}
	family, e := normalInitialWALFamilyForHost(o.intent.Host)
	if e != nil {
		return z, e
	}
	ref := normalMemoryOriginConfirmedReference{InitialWALFamily: family, ProtocolVersion: 2, OriginalRoot: o.intent.OriginalRoot, GuestBootID: o.intent.Host.Prepared.GuestBootID, RuntimeDocumentSHA256: o.intent.Host.Prepared.Challenge.RuntimeSHA256, ResourceSourceSHA256: o.intent.Host.Prepared.Challenge.ResourceSHA256, InitialWAL: o.intent.InitialWAL}
	for i, name := range normalMemoryConfirmedFiles() {
		raw, id, e := arrayPopulateHistoryRaw(s, name)
		if e != nil {
			return z, e
		}
		ref.Files[i] = id
		ref.SHA256[i] = physicalDigest(raw)
	}
	if e := normalMemoryObserveConfirmedReferenceLocked(s, ref); e != nil {
		return z, e
	}
	return ref, nil
}

// 运行期/原Broker可持受信FS intent的expected重核历史。零写/零sync，不读取
// 当前PID来重拍OriginalRoot，也绝不返回新的Memory opaque/currentRoot权限。
func normalMemoryObserveConfirmedReferenceLocked(s *lockedState, expected normalMemoryOriginConfirmedReference) error {
	if _, e := expected.referenceSHA256(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	var raw [3][]byte
	for i, name := range normalMemoryConfirmedFiles() {
		b, id, e := arrayPopulateHistoryRaw(s, name)
		if e != nil || id != expected.Files[i] || physicalDigest(b) != expected.SHA256[i] {
			return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 history inode/bytes drifted"), e)
		}
		raw[i] = b
	}
	var in normalMemoryOriginIntent
	var done normalMemoryOriginCompletion
	var final normalMemoryGuestCommitRecord
	for _, item := range []struct {
		raw []byte
		out any
	}{{raw[0], &in}, {raw[1], &done}, {raw[2], &final}} {
		if e := control.UnmarshalObject(item.raw, item.out); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		canonical, e := json.Marshal(item.out)
		if e != nil || !bytes.Equal(item.raw, canonical) {
			return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 history is noncanonical"), e)
		}
	}
	if in.ProtocolVersion != 1 || in.Scope != normalMemoryGuestScope || in.Host.Validate() != nil || in.OriginalRoot != expected.OriginalRoot || in.InitialWAL != expected.InitialWAL || in.Host.Prepared.GuestBootID != expected.GuestBootID || in.Host.Prepared.Challenge.RuntimeSHA256 != expected.RuntimeDocumentSHA256 || in.Host.Prepared.Challenge.ResourceSHA256 != expected.ResourceSourceSHA256 {
		return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 original source differs"))
	}
	if done.ProtocolVersion != 1 || done.Scope != normalMemoryGuestScope || done.IntentSHA256 != expected.SHA256[0] || done.Ack.GuestSourceSHA256 != expected.SHA256[0] || done.Ack.MatchReference(in.Host) != nil || done.Ack.RootInstanceNonce != in.OriginalRoot.InstanceNonce {
		return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 original completion differs"))
	}
	if final.ProtocolVersion != 2 || final.Scope != normalMemoryGuestCommitScope || final.IntentSHA256 != expected.SHA256[0] || final.CompletionSHA256 != expected.SHA256[1] || final.OriginalRoot != expected.OriginalRoot || final.Commit.MatchReferenceAndAck(in.Host, done.Ack) != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 original Host commit differs"))
	}
	family := expected.InitialWALFamily
	if in.Host.ProfileID == NormalSplitMetadataV1PhysicalKernelProfileID {
		selected, e := normalInitialWALFamilyForHost(in.Host)
		if e != nil || selected != family {
			return errors.Join(ErrUnknown, e)
		}
	} else if family != "" {
		return ErrUnknown
	}
	prefix, e := initialPrebootGuestWitnessFamilyLocked(s, in.Host.Prepared.Challenge, family)
	if e != nil || prefix != expected.InitialWAL {
		return errors.Join(ErrUnknown, fmt.Errorf("FS-bound memory2 original8 prefix drifted"), e)
	}
	return nil
}
