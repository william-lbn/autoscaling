package hostscheduler

import (
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// PlacementPreview是原同Node读取/CAS试算的副本，只供本地Filter规划，绝非授予或另一个账本。
type PlacementPreview struct {
	Allocation               hostguard.Placement
	Held                     []hostguard.Placement
	SystemCPUMillicores      control.Uint64
	AllocatableCPUMillicores control.Uint64
}
