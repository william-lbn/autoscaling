package journal

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 纯模型测试只验同一账本内的身份、序列与历史寿命，不是Kube/PG认证。
func TestPGBindingModelRetainsFirstIdentityThroughHistoryAndTakeover(t *testing.T) {
	m := testModel(t)
	id := testRequest(m, 1, control.Grow).Key.Instance
	bound, err := m.BindPG(m.Owner, id)
	require.NoError(t, err)
	require.EqualValues(t, 2, bound.ProtocolVersion)
	require.Equal(t, m.Sequence+1, bound.Sequence)
	require.Nil(t, bound.HighWater, "只读发现不是伸缩操作，不能伪造HWM")
	require.Nil(t, m.PGInstance, "失败/成功均不能回写输入model")
	cloned := bound.Clone()
	cloned.PGInstance.PGBootID++
	require.Equal(t, id, *bound.PGInstance)
	bound, err = bound.Takeover(bound.Owner, "owner-two")
	require.NoError(t, err)
	require.Equal(t, id, *bound.PGInstance)
	same, err := bound.BindPG(bound.Owner, id)
	require.NoError(t, err)
	require.Equal(t, bound, same, "相同绑定不制造新序列或新PG代次")
	for i := 1; i <= HistoryLimit+1; i++ {
		request := testRequest(bound, uint64(i), control.Grow)
		bound = completeModel(t, beginModel(t, bound, request))
	}
	require.NotZero(t, bound.ExpiredResults)
	require.Nil(t, bound.Active)
	require.Equal(t, id, *bound.PGInstance)
	_, encoded, err := encode(bound, MaxEncodedBytes)
	require.NoError(t, err)
	restored, err := decode(encoded, bound.VMUID, bound.Incarnation)
	require.NoError(t, err)
	require.Equal(t, bound, restored)
	wrong := id
	wrong.PGBootID++
	unchanged, err := restored.BindPG(restored.Owner, wrong)
	require.Error(t, err)
	require.Equal(t, restored, unchanged)
	request := testRequest(restored, HistoryLimit+2, control.Grow)
	request.Key.Instance = wrong
	unchanged, disposition, err := restored.Begin(restored.Owner, request, 0)
	require.Error(t, err)
	require.Equal(t, Stale, disposition)
	require.Equal(t, restored, unchanged)
}

func TestPGBindingRejectsInvalidVersionIdentityAndLegacyAdoption(t *testing.T) {
	for _, mode := range []string{"v1-with-field", "v2-without-field", "zero-boot", "foreign-vm", "foreign-resource", "stale-owner", "active-legacy", "expired-legacy", "wrong-high-water"} {
		t.Run(mode, func(t *testing.T) {
			m := testModel(t)
			id := testRequest(m, 1, control.Grow).Key.Instance
			actor := m.Owner
			switch mode {
			case "v1-with-field":
				m.PGInstance = &id
				require.Error(t, m.Validate())
				require.False(t, PristineBootstrap(m))
				return
			case "v2-without-field":
				m.ProtocolVersion = 2
				require.Error(t, m.Validate())
				return
			case "zero-boot":
				id.PGBootID = 0
			case "foreign-vm":
				id.VMUID = "different-vm"
			case "foreign-resource":
				resource := resourceguard.Identity{VMUID: m.VMUID, ComputeInstanceID: "other-compute", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
				var err error
				m, err = m.BindResource(actor, resource)
				require.NoError(t, err)
			case "stale-owner":
				actor.HolderID = "wrong-owner"
			case "active-legacy":
				m = beginModel(t, m, testRequest(m, 1, control.Grow))
			case "expired-legacy":
				key := testRequest(m, 1, control.Grow).Key
				m.HighWater, m.ExpiredResults = &key, 1
				require.NoError(t, m.Validate())
			case "wrong-high-water":
				var err error
				m, err = m.BindPG(actor, id)
				require.NoError(t, err)
				key := testRequest(m, 1, control.Grow).Key
				key.Instance.PGBootID++
				m.HighWater = &key
				require.Error(t, m.Validate())
				return
			}
			unchanged, err := m.BindPG(actor, id)
			require.Error(t, err)
			require.Equal(t, m, unchanged)
		})
	}
}

// typed Store实际走HTTP/RFC6902，明确只是apiserver协议夹具。核验丢回复
// 没有票据、已提交原身份可恢复；幂等确认绝不重发PATCH或替换PG boot。
func TestStorePGBindingRetainsCommittedIdentityAfterUnknownWrite(t *testing.T) {
	for _, mode := range []string{"lost-reply", "corrupt-reply"} {
		t.Run(mode, func(t *testing.T) {
			f, store := newStoreAPI(t)
			snapshot := initializeStore(t, f, store)
			id := control.Instance{VMUID: string(testBinding().VMUID), ComputeInstanceID: "compute-1", PGBootID: 11}
			if path := os.Getenv("NEON_DSG_PG_BIND_ARTIFACT"); path != "" && mode == "lost-reply" {
				raw, err := json.Marshal(snapshot.VirtualMachine())
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path+".v1.json", raw, 0600))
			}
			f.mu.Lock()
			f.failAfterCommit = mode == "lost-reply"
			f.corruptResponse = mode == "corrupt-reply"
			f.mu.Unlock()
			result, err := store.BindPG(context.Background(), snapshot, snapshot.State().Owner, id)
			var unknown *UnconfirmedWrite
			require.ErrorAs(t, err, &unknown)
			require.Nil(t, result)
			observed, err := store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, id, *observed.State().PGInstance)
			require.EqualValues(t, 2, observed.State().ProtocolVersion)
			require.Nil(t, observed.State().HighWater)
			// 供独立旧源码读者实测v2拒绝；只含测试VM/账本，不含凭据。
			if path := os.Getenv("NEON_DSG_PG_BIND_ARTIFACT"); path != "" && mode == "lost-reply" {
				raw, err := json.Marshal(observed.VirtualMachine())
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, raw, 0600))
			}
			f.mu.Lock()
			patches := f.patches
			f.mu.Unlock()
			confirmed, err := store.BindPG(context.Background(), observed, observed.State().Owner, id)
			require.NoError(t, err)
			require.Equal(t, observed.State(), confirmed.State())
			id.PGBootID++
			_, err = store.BindPG(context.Background(), confirmed, confirmed.State().Owner, id)
			require.Error(t, err)
			f.mu.Lock()
			require.Equal(t, patches, f.patches)
			f.mu.Unlock()
		})
	}
}

func TestStorePGBindingUIDRVRejectConcurrentVMReplacement(t *testing.T) {
	for _, mode := range []string{"uid-with-same-rv", "rv", "bootstrap", "runner"} {
		t.Run(mode, func(t *testing.T) {
			f, store := newStoreAPI(t)
			snapshot := initializeStore(t, f, store)
			f.mu.Lock()
			f.beforePatch = func() {
				f.change(func(vm *vmv1.VirtualMachine) {
					switch mode {
					case "uid-with-same-rv":
						vm.UID = "same-name-recreated-uid"
					case "bootstrap":
						vm.Spec.DSGABootstrap.ComputeInstanceID = "different-compute"
					case "runner":
						vm.Status.PodName = "new-runner"
					case "rv":
						vm.Labels = map[string]string{"concurrent": "change"}
					}
				})
				if mode == "uid-with-same-rv" {
					f.mu.Lock()
					f.vm.ResourceVersion = snapshot.ResourceVersion()
					f.mu.Unlock()
				}
			}
			f.mu.Unlock()
			id := control.Instance{VMUID: string(testBinding().VMUID), ComputeInstanceID: "compute-1", PGBootID: 11}
			_, err := store.BindPG(context.Background(), snapshot, snapshot.State().Owner, id)
			require.Error(t, err)
			f.mu.Lock()
			encoded := f.vm.Annotations[control.JournalAnnotation]
			f.mu.Unlock()
			unchanged, err := decode(encoded, snapshot.State().VMUID, snapshot.State().Incarnation)
			require.NoError(t, err)
			require.Nil(t, unchanged.PGInstance, "拒绝不应写坏同名VM/另一Runner")
		})
	}
}

func TestLegacyJournalNewRequestCannotReplaceBootWhileOriginalHistoryRemainsReadable(t *testing.T) {
	m := testModel(t)
	old := testRequest(m, 1, control.Grow)
	m = completeModel(t, beginModel(t, m, old))
	require.EqualValues(t, 1, m.ProtocolVersion)
	require.Nil(t, m.PGInstance, "旧已执行日志不从当前PG补首次绑定")
	next := testRequest(m, 2, control.Grow)
	next.Key.Instance.PGBootID++
	unchanged, disposition, err := m.Begin(m.Owner, next, 0)
	require.Error(t, err)
	require.Equal(t, Stale, disposition)
	require.Equal(t, m, unchanged)
	readable, disposition, err := m.Begin(m.Owner, old, 0)
	require.NoError(t, err)
	require.Equal(t, Replayed, disposition)
	require.Equal(t, m, readable)
}
