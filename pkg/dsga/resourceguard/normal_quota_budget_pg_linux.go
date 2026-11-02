//go:build linux && amd64

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// caller已持resource flock→原registry.mu。重用原NativeFinal的完整BPF字节审计与
// 现成PG内存控制语义审核：同ELF/有Seccomp2本身不等于mbind/move/migrate被拒。
// 不调用完整PG Source工厂，避免其通用quota pending门拒绝本交易自己的中间态。
func (g *Guard) normalQuotaPGControlsLocked(ctx context.Context, s *lockedState, o *arrayRegistryOriginal) error {
	if g == nil || ctx == nil || ctx.Err() != nil || s == nil || o == nil || o.verifyOriginal() != nil {
		return ErrUnknown
	}
	source, pg, e := readNativeBirthOriginalSource()
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	defer pg.Close()
	if source.binding != o.pending.Source || !reflect.DeepEqual(source.resource.Config, g.config) {
		return errors.Join(ErrUnknown, fmt.Errorf("normal quota original PG ELF/runtime differs"))
	}
	stage, e := observeNativeBirthStageLocked(s, o.birthRef)
	if e != nil || stage.Pending != o.pending || stage.Phase != nativeBirthStageReleased || len(stage.Entries) != 3 {
		return errors.Join(ErrUnknown, e, fmt.Errorf("normal quota original NativeFinal absent"))
	}
	transcript := stage.Entries[2].Transcript
	if transcript.Request != o.reservation || !transcript.FinalVerified || !transcript.FinalPersisted || !transcript.DetachedNonAuthorizing {
		return ErrUnknown
	}
	raw, exists, e := s.readPhysicalFile(nativeBirthStageName(o.birthRef.Nonce, "released"))
	if e != nil || !exists || physicalDigest(raw) != o.releasedSHA {
		return errors.Join(ErrUnknown, e)
	}
	compiler := &nativeBirthTraceKernel{binary: pg}
	bundle, e := compiler.compile(ctx, o.reservation)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if bundle.EncodedSHA256 != transcript.AuditSHA256 {
		return errors.Join(ErrUnknown, fmt.Errorf("normal quota PG program differs from original installed audit"))
	}
	if e = normalPGMemoryControlReviewBundle(bundle); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	return o.verifyOriginal()
}
