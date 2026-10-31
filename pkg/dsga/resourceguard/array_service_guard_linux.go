//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

type arrayGuardBinding struct {
	original *arrayRegistryOriginal
	journal  arrayAuthorityJournal
	rpc      arrayRPCJournal
}

// 仅原Root进程内生产者移交，HTTP/body/JSON/Config都没有注册此binding的入口。
// Root重启后缺原PIDFD/私有见证仍Unknown，不能从WAL数字PID重开补资格。
var arrayGuardBindings sync.Map

func bindArrayRegistryGuardLocked(g *Guard, s *lockedState, r *record, o *arrayRegistryOriginal) error {
	if g == nil || o == nil || r == nil || r.Birth == nil || *r.Birth != o.pending.Original || r.Identity != o.pending.Original.Identity || r.validate(r.Identity) != nil || o.pending.Source.Policy.ArrayHelperProtocolVersion != 1 {
		return errors.Join(ErrUnknown, fmt.Errorf("array registry original Guard/Birth binding differs"))
	}
	var st unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &st) != nil || uint64(o.pending.StateDevice) != st.Dev || uint64(o.pending.StateInode) != st.Ino {
		return errors.Join(ErrUnknown, fmt.Errorf("array registry original state inode differs"))
	}
	o.mu.Lock()
	err := o.verifyOriginal()
	o.mu.Unlock()
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	if _, loaded := arrayGuardBindings.LoadOrStore(g, &arrayGuardBinding{original: o}); loaded {
		return errors.Join(ErrUnknown, fmt.Errorf("array original Guard already retains a birth"))
	}
	return nil
}

// 最终授权来源在原resource flock内重读：原WAL/实际Birth+封存只读source/
// 真正原pidfd+当前四inode注册。所有packet字段只作精确比对，缺任何一源不成功。
func (g *Guard) arrayAuthorityOriginalLocked(ctx context.Context, s *lockedState, r *record, c ArrayAuthorityCommand, b *arrayGuardBinding) (arrayAuthorityOriginal, error) {
	var zero arrayAuthorityOriginal
	if c.Validate() != nil || c.Request.Kind != control.Grow || r.validate(c.Identity) != nil || r.Identity != c.Identity || r.Floor != c.IssuerEpoch || r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth == nil || r.Original != nil && !r.Applied {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority original resource is unconfirmed"))
	}
	if b == nil || b.original == nil {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original array Birth/peer pin missing"))
	}
	o := b.original
	if err := o.verifyOriginal(); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if *r.Birth != o.pending.Original || o.current == nil || o.current.retiring || o.current.request.PGBoot != uint64(c.Request.Key.Instance.PGBootID) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority current original registration differs"))
	}
	if err := validateNativeArrayHelperPolicy(o.pending.Source.Policy, true); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	current, pg, err := readNativeBirthOriginalSource()
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	defer pg.Close()
	if current.binding != o.pending.Source || !reflect.DeepEqual(current.resource.Config, g.config) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority fixed original source changed"))
	}
	snapshot, err := observeNativeBirthStageLocked(s, o.birthRef)
	if err != nil || snapshot.Phase != nativeBirthStageReleased || snapshot.Pending != o.pending {
		return zero, errors.Join(ErrUnknown, err, fmt.Errorf("original array native final gate is not retained"))
	}
	raw, found, err := s.readPhysicalFile(nativeBirthStageName(o.birthRef.Nonce, "released"))
	if err != nil || !found || physicalDigest(raw) != o.releasedSHA {
		return zero, errors.Join(ErrUnknown, err, fmt.Errorf("array original release proof changed"))
	}
	limits := r.Bootstrap.Original.Limits
	budget := r.Bootstrap.Original.BudgetCommitmentID
	if r.Held != nil {
		limits = r.Held.limits()
		budget = r.Held.BudgetCommitmentID
	}
	total, err := limits.totalUpper()
	if err != nil || total > uint64(c.Request.TargetVMMemoryBytes) || c.Request.BudgetCommitmentID != budget {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array command differs from original held budget"))
	}
	actual, err := g.kernel.ReadBounds(r.Identity, limits)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	view := r.view(actual)
	if _, err = view.ConfirmedLimits(); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	reference, err := arrayAuthorityRecordReference(*r)
	if err != nil || reference != c.ResourceReferenceSHA256 {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array command original resource reference changed"))
	}
	if err = g.kernel.VerifyProcess(*r.Birth); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if c.Request.TargetBuffers > o.pending.Source.Policy.MaxBuffers {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array command exceeds original M"))
	}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	return arrayAuthorityOriginal{Command: c, Birth: o.birthRef, ReleasedSHA256: o.releasedSHA, RegistrationNonce: hex.EncodeToString(o.current.nonce[:]), MappingGeneration: control.Uint64(o.current.request.MappingGeneration)}, nil
}

func (g *Guard) arrayAuthority(ctx context.Context, c ArrayAuthorityCommand, mode string) (ArrayAuthorityEvidence, error) {
	var out ArrayAuthorityEvidence
	if ctx == nil || ctx.Err() != nil || c.Validate() != nil {
		return out, errors.Join(ErrUnknown, fmt.Errorf("array authority original context/command invalid"))
	}
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		value, ok := arrayGuardBindings.Load(g)
		if !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("original array registry source is unavailable"))
		}
		binding, ok := value.(*arrayGuardBinding)
		if !ok || binding == nil || binding.original == nil {
			return errors.Join(ErrUnknown, fmt.Errorf("array registry binding invalid"))
		}
		binding.original.mu.Lock()
		defer binding.original.mu.Unlock()
		original, err := g.arrayAuthorityOriginalLocked(ctx, s, r, c, binding)
		if err != nil {
			return err
		}
		out, err = binding.journal.applyLocked(ctx, s, original, mode, arrayAuthorityDiskIO())
		return err
	})
	return out, err
}
func (g *Guard) AuthorizeArrays(ctx context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return g.arrayAuthority(ctx, c, "authorize")
}
func (g *Guard) ObserveArrayAuthority(ctx context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return g.arrayAuthority(ctx, c, "observe")
}
func (g *Guard) ReplayArrayAuthority(ctx context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return g.arrayAuthority(ctx, c, "replay")
}
