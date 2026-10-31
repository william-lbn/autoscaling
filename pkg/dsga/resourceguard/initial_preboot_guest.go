package resourceguard

import (
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

// InitialPrebootMovableObservation仅描述原准备之后的实际Guest页几何。
// ExpectedIncrementBytes来自原Host challenge，CurrentPhysical与MovablePresentPages
// 才是固定内核reader的事实；本对象不是QMP观察/Host预算或normal许可。
type InitialPrebootMovableObservation struct {
	ProtocolVersion        uint32                               `json:"protocol_version"`
	Scope                  string                               `json:"scope"`
	Challenge              bootprotocol.InitialPrebootChallenge `json:"challenge"`
	PreparedReceiptSHA256  string                               `json:"prepared_receipt_sha256"`
	GuestBootID            string                               `json:"guest_boot_id"`
	RootInstanceNonce      string                               `json:"root_instance_nonce"`
	PhysicalOriginSHA256   string                               `json:"physical_origin_sha256"`
	PreSystemSHA256        string                               `json:"pre_system_sha256"`
	CurrentPhysical        PhysicalKernelObservation            `json:"current_physical"`
	CurrentPhysicalSHA256  string                               `json:"current_physical_sha256"`
	ExpectedIncrementBytes control.Uint64                       `json:"expected_increment_bytes"`
	MovablePresentPages    control.Uint64                       `json:"movable_present_pages"`
}
