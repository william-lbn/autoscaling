//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 只复用原严格策略类型，不复制其ABI/build白名单或放宽既有资格。
// 此处校验静态依赖/额度一致性，不制造需要真实内核与原WAL的Origin对象。
func validateRenderedRuntimePolicyAssets(raw []byte, _ RuntimeDocument, source SourceConfig, assets map[string][]byte) error {
	var physical PhysicalKernelOriginPolicy
	if err := json.Unmarshal(assets["dsga-physical-kernel.json"], &physical); err != nil {
		return err
	}
	if err := physical.Validate(); err != nil {
		return err
	}
	if policy, ok := assets["dsga-kernel-bootstrap.json"]; ok {
		inputs := kernelPrepRawInputs{RuntimeRaw: raw, PhysicalPolicyRaw: assets["dsga-physical-kernel.json"], PolicyRaw: policy}
		if _, err := inputs.decode(source.Config); err != nil {
			return err
		}
	}
	if policy, ok := assets["dsga-prebootstrap-system.json"]; ok {
		var p prebootstrapSystemPolicy
		if err := json.Unmarshal(policy, &p); err != nil {
			return err
		}
		if err := p.validate(); err != nil {
			return err
		}
		if p.PhysicalProfileID != physical.ProfileID || !source.Config.SystemCgroup || source.Limits.SystemMemoryMaxBytes == 0 {
			return fmt.Errorf("pre-system template differs from original physical/System source")
		}
		for _, pool := range source.Limits.HugePools {
			if pool.Pages != 0 {
				return fmt.Errorf("pre-system template requires original zero HugeTLB pools")
			}
		}
	}
	if policy, ok := assets["dsga-normal-split.json"]; ok {
		var p NormalSplitTopologyPolicy
		if err := json.Unmarshal(policy, &p); err != nil {
			return err
		}
		if err := p.validate(); err != nil {
			return err
		}
		if p.PhysicalProfileID != physical.ProfileID || uint64(p.PrivateMemoryMaxBytes)+uint64(p.InitialArraysMemoryMaxBytes) > uint64(source.Limits.MemoryMaxBytes) || source.Limits.PidsMaxTasks <= p.PrivatePidsMaxTasks {
			return fmt.Errorf("split template exceeds original parent memory/tasks")
		}
	}
	if policy, ok := assets["dsga-native-birth.json"]; ok {
		var p nativeBirthPolicy
		if err := control.UnmarshalObject(policy, &p); err != nil {
			return err
		}
		if err := p.validate(); err != nil {
			return err
		}
		// 原ABI/地址空间几何仍由现有reader校验；不在纯模板阶段生成nonce/ProcessIdentity。
		if source.Limits.AddressSpaceMaxBytes == 0 {
			return fmt.Errorf("native template lacks original AS hard limit")
		}
	}
	return nil
}
