//go:build linux && amd64 && dsga_native_inflight_metadata_origin && dsga_native_stock_preparation && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"testing"
	"time"
)

// 仅该显式DEV图替换broker的测试分派，不修改任何生产constructor或旧P2入口。
// 原checkpoint仅pin已有opaque，所有完整核验/双读在Ready和原R1真退出之后。
func init() {
	prior := normalRoleServiceStockPreparationDEVDispatch
	normalRoleServiceStockPreparationDEVDispatch = func(m *testing.M) bool {
		if len(os.Args) > 1 && os.Args[0] == normalSystemRoleBrokerExecutable && os.Args[1] == "system-role-broker" {
			ctx := normalRoleServiceNativeBrokerContext(context.Background())
			var g *Guard
			var memory *normalMemoryBrokerConsumerSource
			ctx = context.WithValue(ctx, normalOrdinaryStockPreparationCheckpointKey{}, func(call context.Context, state *lockedState, p *normalOrdinaryStockUdevPreparation) error {
				if g != nil || memory != nil || p == nil || p.q == nil || p.q.filesystem == nil || p.q.filesystem.memoryConsumer == nil {
					return ErrUnknown
				}
				g = p.q.g
				memory = p.q.filesystem.memoryConsumer // 仅pin原指针，不触完整proof/reader/额外锁。
				return stockPreparationDEVObserve(call, state, p)
			})
			ctx = context.WithValue(ctx, stockPreparationDEVAfterR1Key{}, func() {
				call, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if g == nil || memory == nil {
					inflightMetadataDEVFinish(ErrUnknown)
					return
				}
				e := g.withRecord(call, func(_ *record, state *lockedState) error { return inflightMetadataDEVExercise(call, state, g, memory) })
				inflightMetadataDEVFinish(e)
			})
			e := RunNormalSystemRoleBroker(ctx)
			inflightMetadataDEVFinish(errors.Join(ErrUnknown, e, fmt.Errorf("broker returned before metadata probe finished")))
			return true
		}
		return prior(m)
	}
}

func inflightMetadataDEVExercise(ctx context.Context, state *lockedState, g *Guard, memory *normalMemoryBrokerConsumerSource) error {
	source, facts, terms, e := g.captureNormalInflightMetadataOriginLocked(ctx, state, memory)
	if e != nil {
		return e
	}
	defer source.close()
	ref, again, more, e := source.referenceLocked(ctx, state, g)
	if e != nil {
		return e
	}
	if ref != source.original || ref.MaximumBytes != 4<<30 || ref.BaseBytes != 512<<20 || ref.BackendBytes != 3584<<20 || ref.ObservedHostMaximumVCPUs != 2 || ref.CompiledCPUCeiling != 64 || facts.Physical.PossibleCPUs != 2 || again.Physical.PossibleCPUs != 2 || terms.CompiledCPUCeiling != 64 || more.CompiledCPUCeiling != 64 || terms.AllSourcesClosed || terms.NormalDecreaseAuthorized || len(terms.MissingSources) != 8 {
		return fmt.Errorf("actual bound metadata origin or C64 terms differs")
	}
	if ref.Build.KernelArtifactSHA256 != "18c9fd3a38bbe5a087b98998c856898e97ee3f0bc68681b13f0287912849e1ad" {
		return ErrUnknown
	}
	plan, e := source.planLocked(ctx, state, g, 1536<<20, 512<<20)
	if e != nil {
		return e
	}
	if plan.CandidateFits || plan.CandidateSpareBytes != 0 || !plan.CandidateOnly || plan.MmaxAuthorized || plan.NormalDecreaseAuthorized {
		return fmt.Errorf("actual4G geometry became sufficient12G-template capacity")
	}
	other := &Guard{config: g.config}
	if _, _, _, e = other.captureNormalInflightMetadataOriginLocked(ctx, state, memory); !errors.Is(e, ErrUnknown) {
		return fmt.Errorf("newGuard borrowed original R2 memory: %v", e)
	}
	if _, _, _, e = source.referenceLocked(ctx, state, other); !errors.Is(e, ErrUnknown) {
		return fmt.Errorf("newGuard borrowed source reference: %v", e)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, e = source.referenceLocked(canceled, state, g); !errors.Is(e, ErrUnknown) {
		return fmt.Errorf("canceled reference returned facts: %v", e)
	}
	var decoded normalInflightMetadataOriginSource
	if e = json.Unmarshal([]byte(`{}`), &decoded); e != nil {
		return e
	}
	if _, _, _, e = decoded.referenceLocked(ctx, state, g); !errors.Is(e, ErrUnknown) {
		return fmt.Errorf("JSON shape produced opaque: %v", e)
	}
	source.close()
	if _, _, _, e = source.referenceLocked(ctx, state, g); !errors.Is(e, ErrUnknown) {
		return fmt.Errorf("closed source remained usable: %v", e)
	}
	raw, e := json.Marshal(map[string]any{"stable_id": "NEON-S-INFLIGHT-METADATA-ORIGIN-001", "scope": normalInflightMetadataOriginScope, "reference": ref, "facts": facts, "terms": terms, "budget": plan, "original_memory2_r2_consumed": true, "original_R1_already_exited": true, "fresh_reference_checked": true, "new_guard_rejected": true, "canceled_rejected": true, "JSON_shape_rejected": true, "closed_source_rejected": true, "Host_capacity_guaranteed": false, "normal_authorized": false, "E": 0})
	if e != nil {
		return e
	}
	fmt.Printf("INFLIGHT_METADATA_ORIGIN_JSON %s\n", raw)
	return nil
}
func inflightMetadataDEVFinish(e error) {
	if e != nil {
		fmt.Printf("INFLIGHT_METADATA_ORIGIN_FAILURE %v\n", e)
		fmt.Println("INFLIGHT_METADATA_GUEST_EXIT=1")
		fmt.Println("STOCK_PREPARATION_GUEST_EXIT=1")
	} else {
		fmt.Println("INFLIGHT_METADATA_GUEST_EXIT=0")
		fmt.Println("STOCK_PREPARATION_GUEST_EXIT=0")
	}
	unix.Sync()
	_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	if e != nil {
		os.Exit(49)
	}
	os.Exit(0)
}
