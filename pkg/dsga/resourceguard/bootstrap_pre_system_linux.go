//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// 此loader只在已持原resource flock时调用：从固定ISO取原字节，并在同锁核
// canonical Origin/确认。不会调用公开Observe入口，不重入flock、不捕获新Origin、
// 不重建PreSys或改变内核控制。当前物理/归组复核由原PreSys locked reader完成。
func (k *linuxKernel) bootstrapPreSystemInputs(s *lockedState) (prebootstrapSystemInputs, error) {
	var zero prebootstrapSystemInputs
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return zero, fmt.Errorf("original pre-System bootstrap requires real root")
	}
	g := &Guard{config: k.config, kernel: k}
	physical, err := g.physicalOriginRuntimeInputs()
	if err != nil {
		return zero, err
	}
	if !physical.bootMarker || physical.policy.Validate() != nil || physical.config != k.config || physical.runtimeSHA != physical.policy.RuntimeDocumentSHA256 {
		return zero, fmt.Errorf("original bootstrap physical policy differs")
	}
	raw, exists, err := s.readPhysicalFile(physicalOriginDataName)
	if err != nil || !exists {
		return zero, fmt.Errorf("original bootstrap physical data unavailable: %v", err)
	}
	var origin PhysicalKernelOrigin
	if err = json.Unmarshal(raw, &origin); err != nil {
		return zero, err
	}
	canonical, err := json.Marshal(origin)
	if err != nil || !bytes.Equal(raw, canonical) {
		return zero, fmt.Errorf("original bootstrap physical data is not canonical")
	}
	expected := physical.makeOrigin(origin.First)
	expected.Nonce = origin.Nonce
	expectedRaw, err := json.Marshal(expected)
	if err != nil || !bytes.Equal(expectedRaw, raw) {
		return zero, fmt.Errorf("original bootstrap physical source changed")
	}
	confirm, present, err := s.readPhysicalFile(physicalOriginConfirmName)
	if err != nil || !present {
		return zero, fmt.Errorf("original bootstrap physical confirmation missing: %v", err)
	}
	var confirmation physicalOriginConfirmation
	if err = json.Unmarshal(confirm, &confirmation); err != nil || confirmation.ProtocolVersion != 1 || confirmation.OriginSHA256 != physicalDigest(raw) {
		return zero, fmt.Errorf("original bootstrap physical confirmation differs")
	}
	confirmCanonical, err := json.Marshal(confirmation)
	if err != nil || !bytes.Equal(confirm, confirmCanonical) {
		return zero, fmt.Errorf("original bootstrap physical confirmation is not canonical")
	}
	source, runtimeRaw, err := runtimeSource()
	if err != nil {
		return zero, err
	}
	if physicalDigest(runtimeRaw) != physical.runtimeSHA {
		return zero, fmt.Errorf("original bootstrap runtime bytes changed")
	}
	policyRaw, err := readKernelBootstrapISO(FixedPrebootstrapSystemPolicyPath)
	if err != nil {
		return zero, err
	}
	var policy prebootstrapSystemPolicy
	if err = json.Unmarshal(policyRaw, &policy); err != nil {
		return zero, err
	}
	cmdline, err := kernelText("/proc/cmdline")
	if err != nil {
		return zero, err
	}
	if err = preSystemBootMarker(cmdline, policy); err != nil {
		return zero, err
	}
	var document RuntimeDocument
	if err = json.Unmarshal(runtimeRaw, &document); err != nil {
		return zero, err
	}
	in := prebootstrapSystemInputs{source: source, sourceRaw: append([]byte(nil), document.SourceJSON...), policy: policy, policyRaw: append([]byte(nil), policyRaw...), policySHA: physicalDigest(policyRaw), origin: origin, originSHA: physicalDigest(raw)}
	if err = in.validate(k.config); err != nil {
		return zero, err
	}
	return in, nil
}
