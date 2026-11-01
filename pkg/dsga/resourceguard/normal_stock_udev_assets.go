package resourceguard

import (
	"encoding/json"
	"fmt"
	"sort"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 构建期复用同一固定类别和大小界；类别表不是运行期Source。
func NormalSystemStockUdevAssetClass(name string) (string, uint64, bool) {
	return normalOrdinaryStockUdevFileClass(name)
}

// 元数据来自最终rootdisk实际文件；生成后再过原严格decoder，不能借此重建opaque。
func NormalSystemStockUdevAssetsManifestJSON(packageLockSHA string, files []NormalSystemSourceFile) ([]byte, error) {
	m := normalOrdinaryStockUdevManifest{ProtocolVersion: 2, Scope: normalOrdinaryStockUdevManifestScope, PackageLockSHA256: packageLockSHA}
	for _, f := range files {
		class, _, ok := normalOrdinaryStockUdevFileClass(f.Path)
		kind, kindOK := normalSystemStockUdevFileKind(f.Path)
		if !ok || !kindOK || kind != f.Kind {
			return nil, fmt.Errorf("stock asset class differs: %s", f.Path)
		}
		m.Files = append(m.Files, normalOrdinaryStockUdevFile{Path: f.Path, Kind: class, Code: class == "program" || class == "helper" || class == "dso", Bytes: control.Uint64(f.Bytes), SHA256: f.SHA256})
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	raw, e := json.Marshal(m)
	if e != nil {
		return nil, e
	}
	if _, e = decodeNormalOrdinaryStockUdevManifest(raw); e != nil {
		return nil, e
	}
	return raw, nil
}
