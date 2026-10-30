package resourceguard

import (
	"reflect"
	"testing"
)

func TestResourceViewCloneOwnsEveryNestedSourceAndEvidence(t *testing.T) {
	_, _, c := fixtureGuard(t)
	// 此项只测对象所有权，允许不完整的错误路径输入；不用Validate把
	// 无效输入丢弃，也不把任意零值Evidence称作资源确认。
	var v View
	v.Bootstrap.Limits = c.limits()
	v.Original, v.Held, v.Cancelled = &c, &c, &c
	var birth ProcessIdentity
	v.Birth = &birth
	v.Birth.PostmasterPID = c.PostmasterPID
	v.Actual.HugePools = append([]HugePool(nil), c.HugePools...)
	var tasks TaskLimitEvidence
	var address AddressSpaceEvidence
	var system SystemEvidence
	v.Actual.TaskLimit, v.Actual.AddressSpace, v.Actual.System = &tasks, &address, &system
	for name, change := range map[string]func(*View){
		"bootstrap":      func(n *View) { n.Bootstrap.Limits.HugePools[0].Pages++ },
		"original":       func(n *View) { n.Original.HugePools[0].Pages++ },
		"held":           func(n *View) { n.Held.HugePools[0].Pages++ },
		"cancelled":      func(n *View) { n.Cancelled.HugePools[0].Pages++ },
		"birth":          func(n *View) { n.Birth.PostmasterPID++ },
		"actual-pools":   func(n *View) { n.Actual.HugePools[0].Pages++ },
		"actual-tasks":   func(n *View) { n.Actual.TaskLimit.MaxTasks++ },
		"actual-address": func(n *View) { n.Actual.AddressSpace.MaxHardBytes++ },
		"actual-system":  func(n *View) { n.Actual.System.MemoryMaxBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			before, next := v.Clone(), v.Clone()
			change(&next)
			if !reflect.DeepEqual(v, before) || reflect.DeepEqual(next, v) {
				t.Fatal("View clone aliases original nested source/evidence")
			}
		})
	}
}
