package resourceguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceCancellationClientRetainsLostReplyAndOnlyReadsOriginalBarrier(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		name := "same-owner"
		if takeover {
			name = "new-owner"
		}
		t.Run(name, func(t *testing.T) {
			h, key, _, _, _ := resourceHTTPFixture(t)
			guard, kernel, command := cancelGuardFixture(t)
			h.guard = guard
			epoch := command.IssuerEpoch
			if takeover {
				epoch++
				_, err := guard.Fence(context.Background(), epoch)
				require.NoError(t, err)
			}
			token := resourceToken(t, h, key, func(c *resourceClaims) { c.IssuerEpoch = epoch })
			var posts atomic.Int32
			wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/dsg/v1/resource/cancel-original" {
					posts.Add(1)
					captured := httptest.NewRecorder()
					h.ServeHTTP(captured, r)
					require.Equal(t, http.StatusOK, captured.Code)
					// 屏障已真实落盘，响应在边界丢失，不能靠数字相等清SDK。
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				h.ServeHTTP(w, r)
			})
			binding := cloneCommand(command)
			binding.IssuerEpoch = epoch
			client := resourceClientFixture(t, wrapped, binding, token, &command)
			_, err := client.CancelOriginal(context.Background(), command)
			var lost *ClientError
			require.ErrorAs(t, err, &lost)
			require.True(t, lost.MayHaveExecuted)
			require.Equal(t, &command, client.Pending())
			_, err = client.Observe(context.Background(), command)
			require.Error(t, err, "普通完成查询不把取消屏障当作已应用")
			require.Equal(t, &command, client.Pending())
			_, err = client.Apply(context.Background(), command)
			require.Error(t, err, "取消未知不能重发setter")
			// SDK重建继续保留外层完整原文；仅通过State读回相同取消屏障。
			restored := resourceClientFixture(t, wrapped, binding, token, client.Pending())
			receipt, err := restored.ObserveCancellation(context.Background(), command)
			require.NoError(t, err)
			require.NoError(t, receipt.ValidateFor(command, epoch))
			require.Nil(t, restored.Pending())
			require.EqualValues(t, 1, posts.Load())
			require.Equal(t, 1, kernel.writes, "Root取消/恢复没有任何资源setter")
		})
	}
}

func TestResourceCancellationClientDoesNotAcceptAbsenceOrAnotherOriginal(t *testing.T) {
	h, key, _, _, _ := resourceHTTPFixture(t)
	guard, _, command := cancelGuardFixture(t)
	h.guard = guard
	client := resourceClientFixture(t, h, command, resourceToken(t, h, key, nil), &command)
	_, err := client.ObserveCancellation(context.Background(), command)
	require.ErrorIs(t, err, ErrUnknown, "未找到原命令/取消屏障不是NoEffect")
	require.Equal(t, &command, client.Pending())
	other := cloneCommand(command)
	other.BudgetCommitmentID = "different-original"
	_, err = client.CancelOriginal(context.Background(), other)
	require.Error(t, err)
	require.Equal(t, &command, client.Pending())
}
