//go:build linux

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrdinaryUdevV2MaximumGeometryPreservesHolesAndOriginalBase(t *testing.T) {
	c := &normalOrdinaryUdevV2Catalog{memoryBlock: 128 << 20, initialRAM: []normalOrdinaryUdevV2Range{{1 << 20, 2 << 30}}, device: normalOrdinaryUdevV2Range{4 << 30, 5 << 30}, entries: map[string]*normalOrdinaryUdevV2CatalogEntry{"/devices/system/memory/memory0": {subsystem: "memory", name: "memory0"}}}
	require.True(t, c.memoryIndex(0))
	require.False(t, c.memoryIndex(16), "hole after base cannot become future memory")
	require.False(t, c.memoryIndex(31))
	require.True(t, c.memoryIndex(32))
	require.True(t, c.memoryIndex(39))
	require.False(t, c.memoryIndex(40))
	require.False(t, c.memoryIndex(^uint32(0)))
}
func TestOrdinaryUdevV2KindThreeUsesOriginalTicketAndUnknownRulesFail(t *testing.T) {
	c := &normalOrdinaryUdevV2Catalog{entries: map[string]*normalOrdinaryUdevV2CatalogEntry{"/devices/virtual/test0": {path: "/devices/virtual/test0", subsystem: "test", name: "test0", properties: map[string]string{}, projection: normalOrdinaryUdevV2DeviceProjection{Properties: map[string]string{}}}}}
	p := ordinaryUdevV2TestProperties()
	ticket := &normalOrdinaryUdevV2Ticket{sequence: 8, properties: p}
	q, e := decodeNormalOrdinaryUdevV2Request(ordinaryUdevV2TestRequest(t, 3, 0, p))
	require.NoError(t, e)
	require.NoError(t, c.processed(ticket, q))
	q.Properties["DEVPATH"] = "/devices/forged"
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
	q.Properties = ordinaryUdevV2TestProperties()
	q.Properties["ID_UNKNOWN"] = "untrusted"
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
	c.unresolved = []string{"original known rule IMPORT unknown"}
	delete(q.Properties, "ID_UNKNOWN")
	require.ErrorIs(t, c.processed(ticket, q), ErrUnknown)
}
