//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

const normalMemoryGuestCommitFile = "normal-memory-origin.host-commit.json"
const normalMemoryGuestCommitScope = "normal-memory-origin-guest-host-commit-v2"

type normalMemoryGuestCommitRecord struct {
	ProtocolVersion  uint32                                    `json:"protocol_version"`
	Scope            string                                    `json:"scope"`
	IntentSHA256     string                                    `json:"intent_sha256"`
	CompletionSHA256 string                                    `json:"completion_sha256"`
	OriginalRoot     nativeBirthConsumer                       `json:"original_root"`
	Commit           bootprotocol.NormalMemoryOriginHostCommit `json:"commit"`
}

// 只有真实同Root的单次8已验证、完整11文件/目录已同步后才构造；JSON不产此见证。
type normalMemoryGuestCommitEvidence struct {
	record   normalMemoryGuestCommitRecord
	rawSHA   string
	identity arrayPopulateHistoryIdentity
}

func normalMemoryReadGuestCommit2Locked(s *lockedState, o *normalMemoryOriginSource) (*normalMemoryGuestCommitEvidence, error) {
	if s == nil || o == nil || o.currentRoot != o.intent.OriginalRoot {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("commit2 requires the original live Root; new Root recovery is separate"))
	}
	raw, id, e := arrayPopulateHistoryRaw(s, normalMemoryGuestCommitFile)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	var v normalMemoryGuestCommitRecord
	if e = control.UnmarshalObject(raw, &v); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	canonical, e := json.Marshal(v)
	if e != nil || !bytes.Equal(raw, canonical) || v.ProtocolVersion != 2 || v.Scope != normalMemoryGuestCommitScope || v.IntentSHA256 != o.intentSHA || v.CompletionSHA256 != o.completeSHA || v.OriginalRoot != o.currentRoot || v.Commit.MatchReferenceAndAck(o.intent.Host, o.complete.Ack) != nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original commit2 Guest prefix differs"), e)
	}
	return &normalMemoryGuestCommitEvidence{v, physicalDigest(raw), id}, nil
}
func (o *normalMemoryOriginSource) recheckCommit2Locked(s *lockedState) error {
	if o == nil || o.commit2 == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("memory source2 lacks this Root's durable Host commit8"))
	}
	current, e := normalMemoryReadGuestCommit2Locked(s, o)
	if e != nil {
		return e
	}
	if current.identity != o.commit2.identity || current.rawSHA != o.commit2.rawSHA || current.record != o.commit2.record {
		return errors.Join(ErrUnknown, fmt.Errorf("Host commit8 Guest inode or original bytes changed"))
	}
	return nil
}

// 本方法只有显式恢复入口可调用。A只支持原liveRoot；新Root/旧port恢复未实现。
func normalMemoryRecoverGuestCommit2Locked(ctx context.Context, s *lockedState, g *Guard, o *normalMemoryOriginSource) error {
	in, e := g.initialPrebootInputs(o.intent.Host.Prepared.Challenge)
	if e != nil {
		return e
	}
	if in.requirements.MemoryOriginProtocolVersion() != 2 {
		return nil
	}
	before, e := normalMemoryReadGuestCommit2Locked(s, o)
	if e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = s.syncPhysicalFile(normalMemoryGuestCommitFile); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = s.dir.Sync(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	after, e := normalMemoryReadGuestCommit2Locked(s, o)
	if e != nil {
		return e
	}
	if before.rawSHA != after.rawSHA || before.identity != after.identity {
		return errors.Join(ErrUnknown, fmt.Errorf("Guest commit8 changed across explicit sync"))
	}
	o.commit2 = after
	return nil
}

func normalMemoryAcceptGuestCommit2Locked(ctx context.Context, s *lockedState, g *Guard, o *normalMemoryOriginSource, c bootprotocol.NormalMemoryOriginHostCommit) error {
	if ctx == nil || ctx.Err() != nil || s == nil || g == nil || o == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original Guest memory commit inputs absent"))
	}
	in, e := g.initialPrebootInputs(o.intent.Host.Prepared.Challenge)
	if e != nil {
		return e
	}
	if in.requirements.MemoryOriginProtocolVersion() != 2 {
		return errors.Join(ErrUnknown, fmt.Errorf("original ISO did not select memory handshake2"))
	}
	if e = c.MatchReferenceAndAck(o.intent.Host, o.complete.Ack); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = o.recheckOriginalLocked(ctx, s, g); e != nil {
		return e
	}
	if o.currentRoot != o.intent.OriginalRoot {
		return errors.Join(ErrUnknown, fmt.Errorf("memory commit2 cannot replace original producer"))
	}
	v := normalMemoryGuestCommitRecord{2, normalMemoryGuestCommitScope, o.intentSHA, o.completeSHA, o.currentRoot, c}
	raw, e := json.Marshal(v)
	if e != nil {
		return e
	}
	// 不覆盖已有确认；任何partial/丢同步仍Unknown，普通观察不补签。
	if _, exists, e := s.readPhysicalFile(normalMemoryGuestCommitFile); e != nil || exists {
		return errors.Join(ErrUnknown, e, fmt.Errorf("Guest Host-commit history requires explicit original recovery"))
	}
	if e = s.createPhysicalFile(normalMemoryGuestCommitFile, raw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = g.normalMemorySyncSourceLocked(ctx, s, o.intent, o.complete); e != nil {
		return e
	}
	if e = normalMemoryRecoverGuestCommit2Locked(ctx, s, g, o); e != nil {
		return e
	}
	return o.recheckLocked(ctx, s, g)
}

// A仅走单次6/7/8正链。7完整写也不放行：还要真正收到精确8并持久化。
// 任何丢7/8/半帧/取消都返回Unknown，不重连、不双发、不假定Guest已就绪。
func normalMemoryGuestExchangeV2(ctx context.Context, port io.ReadWriter, g *Guard, prepared bootprotocol.InitialPrebootReceipt, moved bootprotocol.InitialMovableReceipt, done bootprotocol.InitialCompletion) (*normalMemoryOriginSource, error) {
	if ctx == nil || ctx.Err() != nil || g == nil || port == nil || done.MatchReceipt(moved) != nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("fixed original memory Guest channel2 required"))
	}
	in, e := g.initialPrebootInputs(prepared.Challenge)
	if e != nil {
		return nil, e
	}
	if in.requirements.MemoryOriginProtocolVersion() != 2 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("memory2 not selected by original full ISO"))
	}
	host, e := bootprotocol.ReadNormalMemoryOriginHostReference(port)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if host.Prepared != prepared || host.Movable != moved || host.Completion != done {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("memory2 Host changed original1..5"))
	}
	s, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return nil, e
	}
	defer s.close()
	source, e := captureNormalMemoryOriginLocked(ctx, s, g, prepared, moved, done, host)
	if e != nil {
		return nil, e
	}
	if e = source.recheckOriginalLocked(ctx, s, g); e != nil {
		return nil, e
	}
	if e = bootprotocol.WriteNormalMemoryOriginGuestAck(port, source.complete.Ack); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	commit, e := bootprotocol.ReadNormalMemoryOriginHostCommit(port)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if e = normalMemoryAcceptGuestCommit2Locked(ctx, s, g, source, commit); e != nil {
		return nil, e
	}
	if e = g.normalMemoryInstallOriginalConfirmedSourceLocked(ctx, s, source); e != nil {
		return nil, e
	}
	return source, nil
}
