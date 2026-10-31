package bootprotocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func prebootFixture() (InitialPrebootChallenge, InitialPrebootReceipt) {
	h := strings.Repeat("ab", 32)
	c := InitialPrebootChallenge{HostStageSHA256: h, ProtocolVersion: 1, Scope: InitialPrebootScope, SourceSHA256: h, Nonce: h, Identity: RunnerIdentity{VMUID: "model-vm", Incarnation: strings.Repeat("ab", 16), RunnerPodUID: "model-runner-pod", RunnerBootID: strings.Repeat("cd", 16), NodeUID: "model-node", MinimumBytes: 1 << 30, MaximumBytes: 8 << 30, BlockBytes: 2 << 20}, RuntimeSHA256: h, ResourceSHA256: h, ConfigSecretUID: "model-secret", KernelBuildSHA256: h, OriginalBase: 256 << 20, BusinessMinimum: 1 << 30, Maximum: 8 << 30}
	return c, InitialPrebootReceipt{Challenge: c, GuestBootID: "12345678-abcd-1234-abcd-123456789abc", RootInstanceNonce: h, KernelPreparationSHA256: h, PhysicalOriginSHA256: h, PreSystemSHA256: h, CurrentPhysicalSHA256: h}
}

// 模型核跨Host/Guest线合同，不认证任何实际来源或阶段已执行。
func TestInitialPrebootProtocolBinding(t *testing.T) {
	c, r := prebootFixture()
	if err := r.MatchChallenge(c); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var got InitialPrebootReceipt
	if err = json.Unmarshal(raw, &got); err != nil || got != r || got.MatchChallenge(c) != nil {
		t.Fatal("wire round trip", err)
	}
	for name, change := range map[string]func(*InitialPrebootChallenge){
		"host-stage": func(v *InitialPrebootChallenge) { v.HostStageSHA256 = strings.Repeat("cd", 32) },
		"source":     func(v *InitialPrebootChallenge) { v.SourceSHA256 = strings.Repeat("cd", 32) },
		"nonce":      func(v *InitialPrebootChallenge) { v.Nonce = strings.Repeat("cd", 32) },
		"runner":     func(v *InitialPrebootChallenge) { v.Identity.RunnerBootID = strings.Repeat("ef", 16) },
		"runtime":    func(v *InitialPrebootChallenge) { v.RuntimeSHA256 = strings.Repeat("cd", 32) },
		"resource":   func(v *InitialPrebootChallenge) { v.ResourceSHA256 = strings.Repeat("cd", 32) },
		"secret":     func(v *InitialPrebootChallenge) { v.ConfigSecretUID = "other-secret" },
		"kernel":     func(v *InitialPrebootChallenge) { v.KernelBuildSHA256 = strings.Repeat("cd", 32) },
		"base":       func(v *InitialPrebootChallenge) { v.OriginalBase += v.Identity.BlockBytes },
	} {
		t.Run(name, func(t *testing.T) {
			other := c
			change(&other)
			if other.Validate() != nil {
				t.Fatal("mutation not valid fixture")
			}
			if r.MatchChallenge(other) == nil {
				t.Fatal("foreign challenge accepted")
			}
		})
	}
}

func TestInitialPrebootProtocolRejectsInvalid(t *testing.T) {
	for name, change := range map[string]func(*InitialPrebootChallenge){
		"version":        func(v *InitialPrebootChallenge) { v.ProtocolVersion = 2 },
		"scope":          func(v *InitialPrebootChallenge) { v.Scope = "other" },
		"zero-block":     func(v *InitialPrebootChallenge) { v.Identity.BlockBytes = 0 },
		"unaligned-base": func(v *InitialPrebootChallenge) { v.OriginalBase++ },
		"base-over-min":  func(v *InitialPrebootChallenge) { v.OriginalBase = v.Identity.MinimumBytes + v.Identity.BlockBytes },
		"geometry-offset": func(v *InitialPrebootChallenge) {
			v.Identity.MinimumBytes++
			v.Identity.MaximumBytes++
			v.BusinessMinimum++
			v.Maximum++
		},
		"max-overflow":    func(v *InitialPrebootChallenge) { v.Identity.MaximumBytes = 1 << 63; v.Maximum = 1 << 63 },
		"business-min":    func(v *InitialPrebootChallenge) { v.BusinessMinimum += v.Identity.BlockBytes },
		"max":             func(v *InitialPrebootChallenge) { v.Maximum += v.Identity.BlockBytes },
		"uppercase-nonce": func(v *InitialPrebootChallenge) { v.Nonce = strings.ToUpper(v.Nonce) },
		"missing-stage":   func(v *InitialPrebootChallenge) { v.HostStageSHA256 = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c, r := prebootFixture()
			change(&c)
			if c.Validate() == nil || r.MatchChallenge(c) == nil {
				t.Fatal("invalid challenge accepted")
			}
		})
	}
	for name, change := range map[string]func(*InitialPrebootReceipt){
		"boot":        func(v *InitialPrebootReceipt) { v.GuestBootID = "12345678abcd1234abcd123456789abc" },
		"Root":        func(v *InitialPrebootReceipt) { v.RootInstanceNonce = "" },
		"kernel-prep": func(v *InitialPrebootReceipt) { v.KernelPreparationSHA256 = "" },
		"origin":      func(v *InitialPrebootReceipt) { v.PhysicalOriginSHA256 = "" },
		"presys":      func(v *InitialPrebootReceipt) { v.PreSystemSHA256 = "" },
		"current":     func(v *InitialPrebootReceipt) { v.CurrentPhysicalSHA256 = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c, r := prebootFixture()
			change(&r)
			if r.Validate() == nil || r.MatchChallenge(c) == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestInitialPrebootProtocolStrictWire(t *testing.T) {
	_, r := prebootFixture()
	raw, _ := json.Marshal(r)
	text := string(raw)
	cases := map[string]string{
		"duplicate-top":    strings.Replace(text, `"guest_boot_id":`, `"guest_boot_id":"12345678-abcd-1234-abcd-123456789abc","guest_boot_id":`, 1),
		"duplicate-nested": strings.Replace(text, `"vm_uid":`, `"vm_uid":"model-vm","vm_uid":`, 1),
		"unknown-top":      strings.Replace(text, `"guest_boot_id":`, `"verified":true,"guest_boot_id":`, 1),
		"unknown-nested":   strings.Replace(text, `"vm_uid":`, `"pg_version":17,"vm_uid":`, 1),
		"missing":          strings.Replace(text, `"protocol_version":1,`, "", 1),
		"wrong-case":       strings.Replace(text, `"source_sha256":`, `"Source_SHA256":`, 1),
		"number":           strings.Replace(text, `"original_base_bytes":"268435456"`, `"original_base_bytes":268435456`, 1),
		"leading-zero":     strings.Replace(text, `"original_base_bytes":"268435456"`, `"original_base_bytes":"0268435456"`, 1),
		"trailing":         text + `{}`,
		"oversize":         strings.Replace(text, `"model-vm"`, `"`+strings.Repeat("x", MaxFrameBytes)+`"`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			var got InitialPrebootReceipt
			if json.Unmarshal([]byte(body), &got) == nil {
				t.Fatal("ambiguous wire accepted")
			}
		})
	}
}
