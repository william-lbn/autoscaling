package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
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

// normalProductionConfigurationPhase是原controls2构建中的配置物化/核验阶段，
// 不连接Docker、不生成System/Native、不提供任何运行期来源或启动许可。
// prepareOnly必须在新base构建之前显式调用；随后原Collector只验真实文件，
// 绝不把DEV字符串的哈希签成生产配置。旧模式和controls1不隐式改义。
// 写入前一次核完三个目标的父目录/对象，拒绝alias、FIFO及硬链接；失败可留下
// 未发布的部分构建输入，调用者必须停止base/库存发布，不能把失败当成完成票。
func normalProductionConfigurationPhase(root, mode string, controlsVersion uint32, prepareOnly bool, baseSHA string) error {
	selected := mode == normalSplitV3BuildMode && controlsVersion == 2
	if !selected {
		if prepareOnly {
			return fmt.Errorf("configuration preparation requires explicit normal-split-v3 controls2")
		}
		return nil
	}
	// prepare-only属于base尚未构建的输入阶段，不能带着已经选定的base摘要
	// 再补配置。实际base构建后，原Collector继续核其真实摘要和同一配置字节。
	if prepareOnly && baseSHA != "" {
		return fmt.Errorf("configuration preparation must precede the base image digest")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return fmt.Errorf("configuration preparation requires a private canonical image root")
	}
	st, e := os.Lstat(root)
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("configuration image root is not an original directory")
	}
	for _, logical := range []string{"/etc", "/neonvm", "/neonvm/config"} {
		canonical, e := normalImageCanonical(root, logical)
		if e != nil || canonical != logical {
			return fmt.Errorf("configuration parent alias differs: %s", logical)
		}
		st, e := os.Lstat(filepath.Join(root, logical))
		if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("configuration parent is not a directory: %s", logical)
		}
	}

	assets, e := normalOrdinaryV2BuildAssets(2)
	if e != nil {
		return e
	}
	var configurations []ordinaryV2FixedAsset
	for _, a := range assets {
		if a.Path == "/neonvm/config/vector.yaml" || a.Path == "/neonvm/config/chrony.conf" {
			configurations = append(configurations, a)
		}
	}
	if len(configurations) != 2 {
		return fmt.Errorf("canonical ordinary configuration set differs")
	}
	// 只读取固定声明配置和原resolver孔，不扫描/etc或读取Host resolver。
	inputs := map[string][]byte{}
	for _, logical := range []string{"/neonvm/config/vector.yaml", "/neonvm/config/chrony.conf", "/etc/resolv.conf"} {
		path := filepath.Join(root, logical)
		st, e := os.Lstat(path)
		if os.IsNotExist(e) && prepareOnly {
			inputs[logical] = nil
			continue
		}
		if e != nil || !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("configuration input is not a regular original file: %s", logical)
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return fmt.Errorf("configuration input has another inode reference: %s", logical)
		}
		f, e := normalStockAssetReadFile(root, logical, 64<<10, true)
		if e != nil {
			return e
		}
		inputs[logical] = f.raw
	}
	resolver := inputs["/etc/resolv.conf"]
	if bytes.Contains(resolver, []byte("TEST_ONLY")) {
		if !prepareOnly || !bytes.Equal(bytes.TrimSpace(resolver), []byte("# TEST_ONLY network disabled")) {
			return fmt.Errorf("resolver contains an unprepared test-only configuration")
		}
		// 只清除已知离线夹具占位。空普通文件仍由原DHCP hook经单文件RW孔
		// 发布实际DNS；不造nameserver、不改变挂载孔、不复制Host/Pod配置。
		resolver = nil
	}
	if bytes.Contains(resolver, []byte("DEV static configuration")) {
		return fmt.Errorf("resolver contains a development placeholder")
	}
	if !prepareOnly {
		for _, a := range configurations {
			if !bytes.Equal(inputs[a.Path], a.Bytes) {
				return fmt.Errorf("canonical production configuration absent before inventory: %s", a.Path)
			}
		}
		return nil
	}
	// 沿现有原子文件写入和fsync实现，配置对象落盘后才允许调用者构建base。
	// 不碰vmstart/vmshutdown、OTEL环境、hosts、旧System/Native或任何Guest数据。
	for _, a := range configurations {
		if e := normalStockAssetWrite(root, a.Path, a.Bytes, true); e != nil {
			return e
		}
	}
	if e := normalStockAssetWrite(root, "/etc/resolv.conf", resolver, true); e != nil {
		return e
	}
	return normalProductionConfigurationPhase(root, mode, controlsVersion, false, "")
}
