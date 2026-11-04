package vmguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const initialStateClaimReadAttempts = 3

type initialAuthorityReadKey struct{}

// 私有能力只由实际Start后的initial drive创建，不可从HTTP、JSON或普通Tick取得。
// 它保存原owner/fence而非上次成功响应；等待不续ctx，不重放任何drive阶段。
type initialAuthorityReadScope struct {
	runner   *Runner
	owner    journal.Owner
	leaseUID string
}

func withInitialAuthorityReadWait(ctx context.Context, r *Runner, owner journal.Owner, leaseUID string) context.Context {
	return context.WithValue(ctx, initialAuthorityReadKey{}, &initialAuthorityReadScope{r, owner, leaseUID})
}

func initialAuthorityReadScopeFor(ctx context.Context, r *Runner) *initialAuthorityReadScope {
	s, _ := ctx.Value(initialAuthorityReadKey{}).(*initialAuthorityReadScope)
	_, bounded := ctx.Deadline()
	if s == nil || s.runner != r || r == nil || r.initial == nil || !bounded || s.owner.Validate() != nil || control.ValidateID(s.leaseUID, "original Lease UID") != nil {
		return nil
	}
	return s
}

// 仅GET失败才构造此类型；成功GET后的UID/owner/JSON/原publication校验不被标为暂态。
// 正常路径返回原错误，不改变普通Tick、发布或初启前的错误处理合同。
type initialStateClaimReadFailure struct {
	ref   budget.ObjectRef
	cause error
}

func (e *initialStateClaimReadFailure) Error() string {
	return "initial_authority_state_claim_get_unconfirmed (" + initialStateReadCauseCode(e.cause) + ")"
}
func (e *initialStateClaimReadFailure) Unwrap() error { return e.cause }

func (r *Runner) readAuthorizationStateClaim(ctx context.Context, scope *initialAuthorityReadScope) (*corev1.ConfigMap, error) {
	if scope == nil {
		return readClaim(ctx, r.core, r.store.Binding(), r.birth.StateClaim, stateKind, false)
	}
	cm, err := r.core.ConfigMaps(r.store.Binding().Namespace).Get(ctx, r.birth.StateClaim.Name, metav1.GetOptions{})
	if err != nil {
		return nil, &initialStateClaimReadFailure{r.birth.StateClaim, err}
	}
	if err := validClaim(cm, r.store.Binding(), r.birth.StateClaim, stateKind, false); err != nil {
		return nil, err
	}
	return cm, nil
}

// 日志只暴露固定阶段和有限原因码；原错误链仍可供errors.Is/As诊断，不输出URL/响应正文。
func initialStateReadCauseCode(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if apierrors.IsForbidden(err) {
		return "forbidden"
	}
	if apierrors.IsUnauthorized(err) {
		return "unauthorized"
	}
	if apierrors.IsNotFound(err) {
		return "not_found"
	}
	if apierrors.IsTooManyRequests(err) {
		return "api_429"
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) {
		return "api_timeout"
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code >= 500 && status.Status().Code <= 599 {
		return "api_5xx"
	}
	var transport net.Error
	if errors.As(err, &transport) && transport.Timeout() {
		return "transport_timeout"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return "transport_io"
	}
	return "other"
}

func retryableInitialStateClaimRead(ctx context.Context, err error, ref budget.ObjectRef) bool {
	if ctx.Err() != nil {
		return false
	}
	var failed *initialStateClaimReadFailure
	if !errors.As(err, &failed) || failed.ref != ref {
		return false
	}
	switch initialStateReadCauseCode(failed.cause) {
	case "api_429", "api_timeout", "api_5xx", "transport_timeout", "transport_io":
		return true
	default:
		return false
	}
}

func (r *Runner) authorize(ctx context.Context) (authorization, error) {
	scope := initialAuthorityReadScopeFor(ctx, r)
	if scope == nil {
		return r.authorizeOnce(ctx, nil)
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return authorization{}, err
		}
		a, err := r.authorizeOnce(ctx, scope)
		if stopped := ctx.Err(); stopped != nil {
			return authorization{}, stopped
		}
		if err == nil || attempt+1 >= initialStateClaimReadAttempts || !retryableInitialStateClaimRead(ctx, err, r.birth.StateClaim) {
			return a, err
		}
		// StateClaim读取耗时后不能沿用旧Lease观测；下一次完整重读原只读授权链。
		// drive仍持原flock，QMP/CAS/Start/Guest阶段都不在此循环内。
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return authorization{}, fmt.Errorf("initial StateClaim read wait ended: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
