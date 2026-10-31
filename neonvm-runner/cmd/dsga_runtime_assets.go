package main

import (
	"bytes"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const runtimeResourceAsset = resourceguard.RuntimeResourceAssetName

type runtimeISOAsset struct {
	name    string
	isoName string
	data    []byte
}

// validatedRuntimeISOAssets在创建ISO输出前重渲染整组固定资产，避免漏写策略后
// 悄悄退成旧模式。此处只核字节/集合；原VM与Secret的授权绑定由入口的
// DecodeRuntimeAssets执行。固定顺序与私有副本也避免后续写盘丢失原始字节。
func validatedRuntimeISOAssets(assets map[string][]byte) ([]runtimeISOAsset, error) {
	if len(assets) == 0 {
		return nil, nil
	}
	if len(assets) > 6 {
		return nil, fmt.Errorf("too many runtime policy assets")
	}
	raw, ok := assets[runtimeResourceAsset]
	if !ok || len(raw) == 0 || len(raw) > 64*1024 {
		return nil, fmt.Errorf("runtime policy assets lack bounded original resource document")
	}
	expected, err := resourceguard.RenderRuntimePolicyAssets(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid runtime policy source: %w", err)
	}
	expected[runtimeResourceAsset] = raw
	if len(assets) != len(expected) {
		return nil, fmt.Errorf("runtime policy asset set differs from original document")
	}
	names := [...]string{
		runtimeResourceAsset,
		"dsga-physical-kernel.json",
		"dsga-kernel-bootstrap.json",
		"dsga-prebootstrap-system.json",
		"dsga-normal-split.json",
		"dsga-native-birth.json",
	}
	result := make([]runtimeISOAsset, 0, len(expected))
	seenISO := map[string]bool{}
	for _, name := range names {
		want, present := expected[name]
		if !present {
			continue
		}
		got, present := assets[name]
		if !present || !bytes.Equal(got, want) {
			return nil, fmt.Errorf("runtime policy asset %q differs from rendered source", name)
		}
		isoName, err := resourceguard.RuntimeISOAssetName(name)
		if err != nil {
			return nil, err
		}
		// 当前写入器含;1最多30字节，超长或碰撞必须在创建输出前拒绝。
		if isoName == "" || len(isoName) > 28 || seenISO[isoName] {
			return nil, fmt.Errorf("invalid or colliding runtime ISO name")
		}
		seenISO[isoName] = true
		result = append(result, runtimeISOAsset{name: name, isoName: isoName, data: append([]byte(nil), got...)})
	}
	if len(result) != len(assets) {
		return nil, fmt.Errorf("runtime policy assets contain an unsupported path")
	}
	return result, nil
}
