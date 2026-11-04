//go:build linux

package vmguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kt "k8s.io/client-go/testing"
)

// 可控父deadline只在首个真实fakeAPI GET中到期，避免race负载下5ms墙钟假失败。
type initialReadDeadlineContext struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	expired  bool
}

func (c *initialReadDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *initialReadDeadlineContext) Done() <-chan struct{}       { return c.done }
func (c *initialReadDeadlineContext) Err() error {
	if c.expired {
		return context.DeadlineExceeded
	}
	return nil
}
func (c *initialReadDeadlineContext) expire() { c.expired = true; close(c.done) }

type initialReadEffects struct{ reads, writes int }

func (e *initialReadEffects) Read(context.Context) (Observation, error) {
	e.reads++
	return Observation{}, nil
}
func (e *initialReadEffects) Set(context.Context, control.Uint64) error { e.writes++; return nil }

func initialReadFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			raw, e := os.ReadFile(p)
			if e != nil {
				return e
			}
			out[p] = SourceDigest(raw)
		}
		return nil
	}))
	return out
}
func initialReadFixture(t *testing.T) (*runnerFixture, *coordination.Lease, context.Context, *initialReadEffects, map[string]string) {
	t.Helper()
	f, _, _ := initialBootFixture(t, false)
	lease, err := f.runner.readLease(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	ctx = withInitialAuthorityReadWait(ctx, f.runner, f.actor, string(lease.UID))
	spy := &initialReadEffects{}
	f.runner.engine.native = spy
	files := initialReadFiles(t, f.config.StateDir)
	f.core.ClearActions()
	f.vms.ClearActions()
	return f, lease, ctx, spy, files
}
func initialReadNoEffects(t *testing.T, f *runnerFixture, spy *initialReadEffects, before map[string]string, producerCAS int) {
	t.Helper()
	require.Zero(t, spy.reads)
	require.Zero(t, spy.writes, "授权读取等待不得重放QMP")
	require.Equal(t, before, initialReadFiles(t, f.config.StateDir), "不得重放WAL/Guest阶段或生成Start/exit")
	for _, a := range f.core.Actions() {
		require.Equal(t, "get", a.GetVerb(), "不能CAS State/Node/Lease或重建来源")
	}
	patches := 0
	for _, a := range f.vms.Actions() {
		if a.GetVerb() == "patch" {
			patches++
		} else {
			require.Equal(t, "get", a.GetVerb())
		}
	}
	require.Equal(t, producerCAS, patches, "只允许测试明确模拟的外部owner生产者CAS")
	var gate error
	require.NoError(t, f.runner.engine.withRecord(context.Background(), func(_ *record, s *lockedState) error { gate = f.runner.initial.gate(s); return nil }))
	require.ErrorIs(t, gate, errInitialBootUnavailable, "读取恢复不等于initial/normal gate开放")
}
func initialStateTimeout() error {
	return &url.Error{Op: "Get", URL: "https://fixture.invalid/original-StateClaim", Err: context.DeadlineExceeded}
}

func TestInitialAuthorityStateReadRetriesOnlyFreshOriginalReads(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{{"timeout", fmt.Errorf("wrapped: %w", initialStateTimeout())}, {"429", apierrors.NewTooManyRequests("unit", 0)}, {"503", apierrors.NewServiceUnavailable("unit")}, {"500", apierrors.NewInternalError(errors.New("unit"))}, {"EOF", io.ErrUnexpectedEOF}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, lease, ctx, spy, files := initialReadFixture(t)
			gets := 0
			f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.GetAction).GetName() == f.runner.birth.StateClaim.Name {
					gets++
					if gets == 1 {
						return true, nil, tc.err
					}
				}
				return false, nil, nil
			})
			a, err := f.runner.authorize(ctx)
			require.NoError(t, err)
			require.Equal(t, f.actor, a.Owner)
			require.Equal(t, string(lease.UID), a.LeaseUID)
			require.Equal(t, 2, gets)
			leaseGets, vmGets := 0, 0
			for _, a := range f.core.Actions() {
				if a.GetResource().Resource == "leases" {
					leaseGets++
				}
			}
			for _, a := range f.vms.Actions() {
				if a.GetVerb() == "get" {
					vmGets++
				}
			}
			require.Equal(t, 4, leaseGets, "超时后的GET必须重新走Lease/journal/Lease")
			require.Equal(t, 2, vmGets)
			initialReadNoEffects(t, f, spy, files, 0)
		})
	}
}

func TestInitialAuthorityStateReadRejectsDefinitiveDrift(t *testing.T) {
	for _, name := range []string{"leaseUID", "holder", "epoch", "expired", "stateUID", "stateJSON", "podUID", "forbidden", "notfound"} {
		t.Run(name, func(t *testing.T) {
			f, lease, ctx, spy, files := initialReadFixture(t)
			gets := 0
			producerCAS := 0
			state, err := f.core.CoreV1().ConfigMaps(f.store.Binding().Namespace).Get(context.Background(), f.runner.birth.StateClaim.Name, metav1.GetOptions{})
			require.NoError(t, err)
			pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(context.Background(), f.config.PodName, metav1.GetOptions{})
			require.NoError(t, err)
			f.core.ClearActions()
			f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.GetAction).GetName() != f.runner.birth.StateClaim.Name {
					return false, nil, nil
				}
				gets++
				if gets != 1 {
					return false, nil, nil
				}
				switch name {
				case "forbidden":
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, f.runner.birth.StateClaim.Name, errors.New("unit"))
				case "notfound":
					return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, f.runner.birth.StateClaim.Name)
				case "leaseUID":
					lease.UID = "replacement-lease"
					require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
				case "holder":
					v := "replacement-holder"
					lease.Spec.HolderIdentity = &v
					require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
				case "expired":
					stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
					lease.Spec.RenewTime = &stamp
					require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
				case "epoch":
					s, e := f.store.Load(context.Background())
					require.NoError(t, e)
					original := s.State().Owner
					// 同holder Takeover是原合同无操作；用两个真实生产者CAS构造同holder更高epoch。
					s, e = f.store.Takeover(context.Background(), s, original, "other-original-owner")
					require.NoError(t, e)
					s, e = f.store.Takeover(context.Background(), s, s.State().Owner, original.HolderID)
					require.NoError(t, e)
					require.Greater(t, uint64(s.State().Owner.Epoch), uint64(original.Epoch))
					require.Equal(t, original.HolderID, s.State().Owner.HolderID)
					producerCAS += 2
				case "stateUID":
					state.UID = "replacement-state"
					require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), state, state.Namespace))
				case "stateJSON":
					state.Data[ClaimKey] = "not JSON"
					require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), state, state.Namespace))
				case "podUID":
					pod.UID = "replacement-pod"
					require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
				}
				return true, nil, initialStateTimeout()
			})
			_, err = f.runner.authorize(ctx)
			require.Error(t, err)
			want := 1
			if name == "stateUID" || name == "stateJSON" || name == "podUID" {
				want = 2
			}
			require.Equal(t, want, gets, "确定性漂移不能继续读取重试")
			initialReadNoEffects(t, f, spy, files, producerCAS)
		})
	}
}

func TestInitialAuthorityStateReadFiniteAttemptsAndOriginalDeadline(t *testing.T) {
	for _, name := range []string{"attempts", "deadline", "cancel"} {
		t.Run(name, func(t *testing.T) {
			f, lease, ctx, spy, files := initialReadFixture(t)
			var cancel context.CancelFunc
			var controlled *initialReadDeadlineContext
			if name == "deadline" {
				controlled = &initialReadDeadlineContext{Context: context.Background(), deadline: time.Now().Add(time.Second), done: make(chan struct{})}
				ctx = withInitialAuthorityReadWait(controlled, f.runner, f.actor, string(lease.UID))
			} else if name == "cancel" {
				ctx, cancel = context.WithCancel(context.Background())
			}
			if cancel != nil {
				defer cancel()
				ctx = withInitialAuthorityReadWait(ctx, f.runner, f.actor, string(lease.UID))
			}
			// cancel子例仍必须继承有界父ctx，不能用无deadline假装真正initial权限。
			if name == "cancel" {
				parent, c := context.WithTimeout(ctx, time.Second)
				defer c()
				ctx = withInitialAuthorityReadWait(parent, f.runner, f.actor, string(lease.UID))
			}
			deadline, bounded := ctx.Deadline()
			require.True(t, bounded)
			gets := 0
			f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.GetAction).GetName() == f.runner.birth.StateClaim.Name {
					gets++
					if name == "cancel" {
						cancel()
					}
					if controlled != nil {
						controlled.expire()
					}
					return true, nil, initialStateTimeout()
				}
				return false, nil, nil
			})
			_, err := f.runner.authorize(ctx)
			require.Error(t, err)
			after, _ := ctx.Deadline()
			require.Equal(t, deadline, after)
			if name == "attempts" {
				require.Equal(t, 3, gets)
				require.NoError(t, ctx.Err())
			} else {
				require.Equal(t, 1, gets)
				require.ErrorIs(t, err, ctx.Err())
			}
			initialReadNoEffects(t, f, spy, files, 0)
		})
	}
}

func TestInitialAuthorityStateReadDoesNotChangeOrdinaryOrUnmarkedCalls(t *testing.T) {
	for _, name := range []string{"unmarked", "other-runner", "unbounded", "normal"} {
		t.Run(name, func(t *testing.T) {
			f, lease, _, spy, files := initialReadFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var call context.Context = ctx
			switch name {
			case "other-runner":
				call = withInitialAuthorityReadWait(ctx, &Runner{}, f.actor, string(lease.UID))
			case "unbounded":
				call = withInitialAuthorityReadWait(context.Background(), f.runner, f.actor, string(lease.UID))
			case "normal":
				f.runner.initial = nil
				call = withInitialAuthorityReadWait(ctx, f.runner, f.actor, string(lease.UID))
			}
			gets := 0
			original := initialStateTimeout()
			f.core.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				if a.(kt.GetAction).GetName() == f.runner.birth.StateClaim.Name {
					gets++
					return true, nil, original
				}
				return false, nil, nil
			})
			_, err := f.runner.authorize(call)
			require.ErrorIs(t, err, original)
			require.Equal(t, 1, gets)
			require.Zero(t, spy.reads)
			require.Zero(t, spy.writes)
			require.Equal(t, files, initialReadFiles(t, f.config.StateDir))
			for _, a := range f.core.Actions() {
				require.Equal(t, "get", a.GetVerb())
			}
		})
	}
}

func TestInitialAuthorityStateReadTypedScopeCannotAuthorizeOtherRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ref := budget.ObjectRef{Name: "original-state", UID: "original-uid"}
	require.False(t, retryableInitialStateClaimRead(ctx, initialStateTimeout(), ref), "无typed GET来源不重试")
	private := &initialStateClaimReadFailure{ref, &url.Error{Op: "Get", URL: "https://fixture.invalid/SECRET_TEST_ONLY", Err: errors.New("PRIVATE_RESPONSE_TEST_ONLY")}}
	require.Equal(t, "initial_authority_state_claim_get_unconfirmed (other)", private.Error())
	require.NotContains(t, private.Error(), "SECRET_TEST_ONLY")
	require.NotContains(t, private.Error(), "PRIVATE_RESPONSE_TEST_ONLY")
	require.ErrorIs(t, private, private.cause)
	require.False(t, retryableInitialStateClaimRead(ctx, &initialStateClaimReadFailure{budget.ObjectRef{Name: "other-state", UID: "other-uid"}, initialStateTimeout()}, ref), "不同原ref不能借暂态重试")
}
