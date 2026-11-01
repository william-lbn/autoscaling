package main

import (
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"strings"
)

// 仅新controlsVersion2可调用；输出是构建字节，不选择模式/不签Source。
// Root最终写入固定rootdisk、再生成同一System3库存。旧v1/metadata-only3不调用。
type ordinaryV2FixedAsset struct {
	Path  string
	Mode  uint32
	Bytes []byte
}

func normalOrdinaryV2BuildAssets(controlsVersion uint32) ([]ordinaryV2FixedAsset, error) {
	if controlsVersion != 2 {
		return nil, fmt.Errorf("ordinary assets require explicit controls version 2")
	}
	const oldMarker = "/run/neon-dsga/vmstart.allowed"
	start := normalComputeScriptV3()
	if strings.Count(start, oldMarker) != 1 || strings.Count(scriptNormalShutdown, oldMarker) != 1 {
		return nil, fmt.Errorf("ordinary2 fixed marker baseline drift")
	}
	// 启动仍先CheckGuest/allow、原OFD9 flock再空环境exec，关机删同一新marker。
	// 不使用flock命令wrapper fork，也不把普通UID授予/run/neon-dsga父写权限。
	start = strings.Replace(start, oldMarker, resourceguard.NormalSystemOrdinaryV2AllowMarkerPath, 1)
	shutdown := strings.Replace(scriptNormalShutdown, oldMarker, resourceguard.NormalSystemOrdinaryV2AllowMarkerPath, 1)
	const oldData = "data_dir: /tmp\n"
	if strings.Count(configVector, oldData) != 1 {
		return nil, fmt.Errorf("ordinary2 fixed vector baseline drift")
	}
	// API/metrics/source/端口逐字不变，只将持久化/缓冲写移到原/run孔的本角色私有目录。
	vector := strings.Replace(configVector, oldData, "data_dir: "+resourceguard.NormalSystemOrdinaryV2VectorDataPath+"\n", 1)
	return []ordinaryV2FixedAsset{
		{"/neonvm/bin/vmstart", 0755, []byte(start)},
		{"/neonvm/bin/vmshutdown", 0755, []byte(shutdown)},
		{"/neonvm/config/vector.yaml", 0644, []byte(vector)},
		{"/neonvm/config/chrony.conf", 0644, []byte(configChrony)},
	}, nil
}
