package resourceguard

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实Mac文件锁/原子rename/fsync和正式Guard。内核及出生exec是明确夹具，
// 不能以此认证Linux cgroup、HugeTLB或实际进程/物理内存回收。
func cancelGuardFixture(t *testing.T) (*Guard, *fixtureKernel, Command) {
	t.Helper()
	g, k, bootstrap, command := freshBootstrapFixture(t)
	_, err := g.Bootstrap(context.Background(), bootstrap)
	require.NoError(t, err)
	process := ProcessIdentity{ProtocolVersion: 1, Identity: command.Identity, PostmasterPID: command.PostmasterPID, PostmasterStartTicks: command.PostmasterStartTicks}
	require.NoError(t, g.launchWithBirth(context.Background(), nil, &process, func() error { k.process = &process; return nil }))
	k.e.CgroupPopulated = true
	return g, k, command
}

func TestResourceCancellationDurablyRejectsLateApplyAndAdvancesRevision(t *testing.T) {
	for _, mode := range []string{"bootstrap", "held", "takeover", "consecutive-cancel"} {
		t.Run(mode, func(t *testing.T) {
			g, k, command := cancelGuardFixture(t)
			ctx := context.Background()
			if mode != "bootstrap" {
				_, err := g.Apply(ctx, command)
				require.NoError(t, err)
				command.Revision++
				command.MemoryMaxBytes = 80 << 20
			}
			epoch := command.IssuerEpoch
			if mode == "takeover" {
				epoch++
				_, err := g.Fence(ctx, epoch)
				require.NoError(t, err)
			}
			before, err := g.InspectState(ctx)
			require.NoError(t, err)
			writes := k.writes
			request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: epoch, Command: command}
			receipt, err := g.CancelOriginal(ctx, request)
			require.NoError(t, err)
			require.NoError(t, receipt.ValidateFor(command, epoch))
			require.Equal(t, before.Held, receipt.State.Held)
			require.Equal(t, before.Original, receipt.State.Original)
			require.Equal(t, before.Actual, receipt.State.Actual)
			require.Equal(t, writes, k.writes, "取消不能调用任何资源setter")
			locked, err := lockState(ctx, g.config.StateDir)
			require.NoError(t, err)
			persisted, err := locked.load()
			locked.close()
			require.NoError(t, err)
			require.EqualValues(t, 2, persisted.Version, "旧二进制必须因新格式拒绝回滚执行")
			badVersion := *persisted
			badVersion.Version = 1
			require.Error(t, badVersion.validate(command.Identity))
			badVersion = *persisted
			badVersion.Cancelled = nil
			require.Error(t, badVersion.validate(command.Identity), "不能只删新格式屏障清未知")
			// 同一命令恢复、普通Apply以及另换预算复用被取消revision都不能迟到。
			g = &Guard{config: g.config, kernel: k}
			_, err = g.CancelOriginal(ctx, request)
			require.NoError(t, err)
			_, err = g.Apply(ctx, command)
			require.Error(t, err)
			_, err = g.ReplayOriginal(ctx, command)
			require.Error(t, err)
			other := cloneCommand(command)
			other.BudgetCommitmentID, other.IssuerEpoch = "same-revision-other-command", epoch
			_, err = g.Apply(ctx, other)
			require.Error(t, err)
			view, err := g.InspectState(ctx)
			require.NoError(t, err)
			next, err := view.NextRevision()
			require.NoError(t, err)
			require.Equal(t, command.Revision+1, next)
			if mode == "consecutive-cancel" {
				another := cloneCommand(command)
				another.Revision = next
				another.BudgetCommitmentID = "next-unreceived"
				_, err := g.CancelOriginal(ctx, CancelRequest{ProtocolVersion: 1, IssuerEpoch: epoch, Command: another})
				require.NoError(t, err)
				next++
			}
			other.Revision = next
			_, err = g.Apply(ctx, other)
			require.NoError(t, err, "后继合法请求不能被取消历史永久阻塞")
			require.Equal(t, writes+1, k.writes)
			view, err = g.InspectState(ctx)
			require.NoError(t, err)
			require.True(t, view.Applied)
			require.Equal(t, &other, view.Held)
			_, err = g.CancelOriginal(ctx, request)
			require.Error(t, err, "不能在更高资源命令后补造旧NoEffect")
		})
	}
}

func TestResourceCancellationRejectsIncompleteSourceAndUnknownEffects(t *testing.T) {
	for _, mode := range []string{"received", "partial", "birth-drift", "held-drift", "epoch", "revision-gap", "wrong-guest", "changed-tasks", "no-bootstrap", "cancelled-context"} {
		t.Run(mode, func(t *testing.T) {
			g, k, command := cancelGuardFixture(t)
			ctx := context.Background()
			request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: cloneCommand(command)}
			switch mode {
			case "received", "partial":
				k.fail = mode == "partial"
				_, err := g.Apply(ctx, command)
				if mode == "received" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrUnknown)
				}
			case "birth-drift":
				k.process = nil
			case "held-drift":
				k.e.MemoryMaxBytes++
			case "epoch":
				request.IssuerEpoch++
			case "revision-gap":
				request.Command.Revision++
			case "wrong-guest":
				request.Command.Identity.CgroupInode++
			case "changed-tasks":
				request.Command.PidsMaxTasks = 64
			case "no-bootstrap":
				// 旧独立协议来源没有生产出生，不得借取消入口获得生产权限。
				g, k, command = fixtureGuard(t)
				request.Command = command
			case "cancelled-context":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			path := filepath.Join(g.config.StateDir, "state.json")
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			writes := k.writes
			_, err = g.CancelOriginal(ctx, request)
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, writes, k.writes)
		})
	}
}

type cancelPersistenceKernel struct{ *fixtureKernel }

func (k *cancelPersistenceKernel) ReadBounds(id Identity, limits Limits) (Evidence, error) {
	actual, err := k.fixtureKernel.ReadBounds(id, limits)
	if err != nil {
		return actual, err
	}
	path := filepath.Join(k.stateDir, "state.json")
	if err := os.Remove(path); err != nil {
		return Evidence{}, err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return Evidence{}, err
	}
	return actual, nil
}

func TestResourceCancellationPersistenceFailureDoesNotIssueNoEffect(t *testing.T) {
	g, k, command := cancelGuardFixture(t)
	writes := k.writes
	g.kernel = &cancelPersistenceKernel{fixtureKernel: k}
	receipt, err := g.CancelOriginal(context.Background(), CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: command})
	require.ErrorIs(t, err, ErrUnknown)
	require.Zero(t, receipt.ProtocolVersion)
	require.Equal(t, writes, k.writes)
}

func TestResourceCancellationWaitsForInFlightApplyInsteadOfDeclaringNoEffect(t *testing.T) {
	g, k, command := cancelGuardFixture(t)
	k.entered, k.release = make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := g.Apply(context.Background(), command); done <- err }()
	select {
	case <-k.entered:
	case <-time.After(time.Second):
		t.Fatal("Apply未到原WAL后的阻塞内核")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := g.CancelOriginal(ctx, CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: command})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(k.release)
	require.NoError(t, <-done)
	_, err = g.CancelOriginal(context.Background(), CancelRequest{ProtocolVersion: 1, IssuerEpoch: 1, Command: command})
	require.ErrorIs(t, err, ErrUnknown)
}

func TestResourceCancellationHTTPAuthenticatesCurrentOwnerAndOriginalEpoch(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	g, k, command := cancelGuardFixture(t)
	h.guard = g
	_, err := g.Fence(context.Background(), 2)
	require.NoError(t, err)
	request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: 2, Command: command}
	raw, err := json.Marshal(request)
	require.NoError(t, err)
	path := "/dsg/v1/resource/cancel-original"
	// 签名旧代次不能以正文自报2越过root floor，外部通用scope也不行。
	response := resourceRequest(h, path, resourceToken(t, h, key, nil), raw)
	require.Equal(t, http.StatusForbidden, response.Code)
	response = resourceRequest(h, path, resourceToken(t, h, key, func(c *resourceClaims) { c.Scope = "general-admin" }), raw)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	token := resourceToken(t, h, key, func(c *resourceClaims) { c.IssuerEpoch = 2 })
	response = resourceRequest(h, path, token, raw)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt Cancellation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.NoError(t, receipt.ValidateFor(command, 2))
	require.Equal(t, control.Uint64(1), receipt.Command.IssuerEpoch, "原命令epoch绝不能被接管改写")
	require.Equal(t, 1, k.writes, "只有创建Bootstrap写，没有资源取消setter")
	// 取消回执必须携带完整取消原文，缺字段及未知字段均不可通过严格解码。
	for _, invalid := range [][]byte{[]byte(`{"protocol_version":1}`), append([]byte(`{"extra":true,`), response.Body.Bytes()[1:]...)} {
		require.Error(t, json.Unmarshal(invalid, &receipt))
	}
	view := receipt.State
	view.Cancelled = &command
	view.Cancelled.Revision = control.Uint64(math.MaxUint64)
	// 合法的耗尽水位不能回绕到0/1；这里只核View的NextRevision算术。
	_, err = view.NextRevision()
	require.Error(t, err)
}
