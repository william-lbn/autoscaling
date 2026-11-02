package main

import "github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"

// 只在显式native1构建命令中使用；原0库存命令/旧369声明完全不变。
// 此构建声明固定registry metadata18c完整五元组，不替代实际运行内核来源。
func completeNormalSystemBuildManifest(root string, input []byte) ([]byte, error) {
	im, e := newNormalNativeImage(root)
	if e != nil {
		return nil, e
	}
	return resourceguard.BuildNormalSystemClosureManifestJSON(input, im.canonical, im.open, im.lstat)
}
