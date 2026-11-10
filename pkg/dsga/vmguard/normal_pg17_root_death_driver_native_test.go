//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard_test

import (
	"context"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"io"
	"net/http"
	"reflect"
	"time"
)

// 真实首Grow的Root已完成但交付丢失。仅恢复原事务，绝不添加新normal授权或活PG接管。
func trustedPG17RootDeathRecovery(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, session *owner.Session, rc resourceguard.ClientConfig, pc compute.Config, qualification budget.QualificationSource, transport *trustedPG17FaultTransport) error {
	beforeSnapshot, e := f.Store.Load(ctx)
	if e != nil {
		return e
	}
	before := beforeSnapshot.State()
	if !transport.dropped.Load() || !transport.holdObserve.Load() || transport.applyCalls.Load() != 1 || before.Active == nil || before.Active.Request.Kind != control.Grow {
		return errors.New("Root death requires original firstGrow lost reply")
	}
	unknown := false
	for _, step := range before.Active.Steps {
		if step.Action == journal.GuestReady && step.Phase == journal.Unconfirmed && step.Observation == nil {
			unknown = true
		}
	}
	if !unknown {
		return errors.New("Root death original GuestReady is not Unknown")
	}
	_, _, publication, e := vmguard.ReadPublished(ctx, f.Core.CoreV1(), f.Store.Binding(), nil)
	if e != nil {
		return e
	}
	if publication.Actual == nil {
		return errors.New("Root death lacks actual QMP before")
	}
	if e = trustedDriverSave("root-death-before", map[string]any{"state": before, "actual_Runner_publication": publication, "old_Run_joined": true, "normal_apply_calls": 1, "E": 0}); e != nil {
		return e
	}
	if e = trustedDriverStage(ctx, "root-death-new-peer"); e != nil {
		return e
	}
	transport.holdObserve.Store(false)
	// 新Root明确不提供旧Held的确认状态。先identity正控，避免把认证或连接故障算安全拒绝。
	formal := func(path string) (int, []byte, error) {
		token, e := rc.Token(ctx)
		if e != nil {
			return 0, nil, e
		}
		request, e := http.NewRequestWithContext(ctx, http.MethodGet, rc.BaseURL+"/dsg/v1/resource/"+path, nil)
		if e != nil {
			return 0, nil, e
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, e := (&http.Client{Timeout: 5 * time.Second, Transport: rc.Transport}).Do(request)
		if e != nil {
			return 0, nil, e
		}
		defer response.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(response.Body, 65537))
		if e != nil || len(raw) > 65536 {
			return 0, nil, errors.New("bounded formal Root response invalid")
		}
		return response.StatusCode, raw, nil
	}
	identityStatus, identityRaw, e := formal("identity")
	if e != nil {
		return e
	}
	if identityStatus != http.StatusOK {
		return errors.New("new Root formal identity positive control failed")
	}
	var identity resourceguard.Identity
	if e = control.UnmarshalObject(identityRaw, &identity); e != nil || identity != rc.Identity {
		return errors.New("new Root formal identity changed")
	}
	stateStatus, stateRaw, e := formal("state")
	if e != nil {
		return e
	}
	var rejection struct {
		Error  string `json:"error"`
		Permit bool   `json:"vm_shrink_permit"`
	}
	if stateStatus != http.StatusConflict || control.UnmarshalObject(stateRaw, &rejection) != nil || rejection.Error != "resource_unconfirmed" || rejection.Permit {
		return errors.New("new Root failed-closed status/code differs")
	}
	afterSnapshot, e := f.Store.Load(ctx)
	if e != nil {
		return e
	}
	after := afterSnapshot.State()
	if after.Active == nil || after.Active.Request != before.Active.Request || after.Active.Origin != before.Active.Origin || len(after.History) != len(before.History) {
		return errors.New("Root death changed or completed original operation")
	}
	if !reflect.DeepEqual(before.Active, after.Active) {
		return errors.New("read-only Root durability confirmation changed original active steps")
	}
	_, _, finalPublication, e := vmguard.ReadPublished(ctx, f.Core.CoreV1(), f.Store.Binding(), nil)
	if e != nil {
		return e
	}
	if finalPublication.Actual == nil || finalPublication.Actual.ActualBytes != publication.Actual.ActualBytes || finalPublication.Actual.RequestedBytes != publication.Actual.RequestedBytes || transport.applyCalls.Load() != 1 {
		return errors.New("Root death sent new normal apply or changed actual QMP")
	}
	// 只读确认原已执行命令可以成功；没有把success观察说成新权限。实际PG/N/P/A另由Guest读回。
	facts := map[string]any{"before": before, "after": after, "new_Root_formal_identity_status": identityStatus, "new_Root_formal_state_status": stateStatus, "new_Root_state_error": rejection.Error, "normal_apply_calls": transport.applyCalls.Load(), "QMP_actual_unchanged": true, "original_Active_completely_unchanged": true, "original_recovery_failure_observed_by_Guest": true, "live_PG_adoption": false, "Host_API_is_model": true, "Source": false, "E": 0}
	if e = trustedDriverSave("root-death-complete", facts); e != nil {
		return e
	}
	return nil
}
