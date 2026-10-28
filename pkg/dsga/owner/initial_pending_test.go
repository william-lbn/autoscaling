package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅验实际Session/SDK从独立首次WAL恢复：没有Active Resize也不能丢未知。
// 预算/原生PG是明确领域夹具，没有在这里认证root/资格/Runner/Host或执行
// 首次服务生产者；直接Status只解除SDK本地pending，不能伪造VM完成落账。
func TestInitialAdmissionSDKRestoresUnknownWithoutActiveAndNeverAutoSends(t *testing.T) {
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, DefaultOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
		require.NoError(t, err)
		var fixture struct {
			Initial budget.Initial                 `json:"initial"`
			Command budget.InitialAdmissionCommand `json:"command"`
			Receipt budget.InitialAdmissionReceipt `json:"receipt"`
		}
		require.NoError(t, json.Unmarshal(raw, &fixture))
		i, c := fixture.Initial, fixture.Command
		// Session可能已从初始化owner合法接管到新epoch；预算首次发布采用
		// 真实当前floor，原root bootstrap仍保持创建时epoch，二者不可混用。
		i.IssuerEpoch = session.Actor().Epoch
		i.Shared.Observed.IssuerFloorEpoch = session.Actor().Epoch
		c.InitialSHA256, err = i.Digest()
		require.NoError(t, err)
		c.Request.IssuerEpoch = session.Actor().Epoch
		fixture.Receipt.Response.Request = c.Request
		fixture.Receipt.Response.Observed.IssuerFloorEpoch = session.Actor().Epoch
		fixture.Receipt.Response.ContextSHA256, err = c.Request.ContextSHA256()
		require.NoError(t, err)
		root, resourceConfig := guestRoot(t, store)
		qualifiedRootFixture(root)
		root.mu.Lock()
		root.view.Bootstrap = i.PGAggregate.Bootstrap
		root.view.Birth = &i.PGAggregate.Birth
		root.view.Actual.MemoryMaxBytes = i.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
		root.view.Actual.HugePools = i.PGAggregate.Bootstrap.Limits.HugePools
		root.mu.Unlock()
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		s, err := store.Load(ctx)
		require.NoError(t, err)
		s, err = store.BindResource(ctx, s, session.Actor(), i.PGAggregate.Identity)
		require.NoError(t, err)
		s, err = store.BindPG(ctx, s, session.Actor(), i.Instance)
		require.NoError(t, err)
		s, err = store.BindInitialBudget(ctx, s, session.Actor(), i)
		require.NoError(t, err)
		_, ticket, err := store.StartInitialAdmission(ctx, s, session.Actor(), c)
		require.NoError(t, err)
		require.NotNil(t, ticket)
		request := pgRequest(t, session.Actor())
		request.Key.Instance = i.Instance
		var sends, queries atomic.Int64
		var found atomic.Bool
		config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/dsg/v1/admission/status" {
				sends.Add(1)
				t.Errorf("unexpected first admission setter/route: %s", r.URL.Path)
				w.WriteHeader(500)
				return
			}
			queries.Add(1)
			var original control.AdmissionRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&original))
			require.Equal(t, c.Request, original)
			response := fixture.Receipt.Response
			if found.Load() {
				response.Disposition = control.AdmissionReplayed
			} else {
				response.Disposition, response.Observed = control.AdmissionNotFound, i.Shared.Observed
			}
			pgReply(w, response)
		})
		worker, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		pending, closing := worker.client.PendingAdmission()
		require.Equal(t, &c.Request, pending)
		require.Nil(t, closing)
		fact, err := worker.client.AdmissionStatus(ctx, c.Request)
		require.NoError(t, err)
		require.False(t, fact.Confirms())
		pending, _ = worker.client.PendingAdmission()
		require.Equal(t, &c.Request, pending, "NotFound不能清SDK原意图")
		found.Store(true)
		fact, err = worker.client.AdmissionStatus(ctx, c.Request)
		require.NoError(t, err)
		require.True(t, fact.Confirms())
		pending, _ = worker.client.PendingAdmission()
		require.Nil(t, pending)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().Active)
		require.Equal(t, journal.Unconfirmed, s.State().InitialHeld.Admission.Phase, "原生Status不是完整预算落账")
		rebuilt, err := session.NewPGWorker(ctx, config, resources)
		require.NoError(t, err)
		defer rebuilt.CloseIdleConnections()
		pending, _ = rebuilt.client.PendingAdmission()
		require.Equal(t, &c.Request, pending, "重建再次按仍未知的原WAL恢复，不能继承旧SDK清空状态")
		require.EqualValues(t, 2, queries.Load())
		require.Zero(t, sends.Load())
		return nil
	})
	require.NoError(t, err)
}
