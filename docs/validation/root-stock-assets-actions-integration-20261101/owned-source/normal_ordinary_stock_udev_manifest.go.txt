package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const normalOrdinaryStockUdevManifestScope = "normal-ordinary-stock-udev-manifest-v2"

type normalOrdinaryStockUdevFile struct {
	Path   string         `json:"path"`
	Kind   string         `json:"kind"`
	Code   bool           `json:"code"`
	Bytes  control.Uint64 `json:"bytes"`
	SHA256 string         `json:"sha256"`
}
type normalOrdinaryStockUdevManifest struct {
	ProtocolVersion   uint32                        `json:"protocol_version"`
	Scope             string                        `json:"scope"`
	PackageLockSHA256 string                        `json:"package_lock_sha256"`
	Files             []normalOrdinaryStockUdevFile `json:"files"`
}

// 这只是静态清单，绝不从JSON生成Root/FS/VM/预算能力。Root后继生产者仍须
// 真实只读FD/ELF闭包/原package-lock与prep生命周期；manifest不含循环的runtime/System SHA。
// 所有必填键显式出现且非null，拒Go标准库允许的大小写别名/省略false与重复拼写。
func normalOrdinaryStockUdevExactKeys(raw []byte, want ...string) error {
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil || fields == nil {
		return ErrUnknown
	}
	if len(fields) != len(want) {
		return ErrUnknown
	}
	for _, name := range want {
		b, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
			return ErrUnknown
		}
	}
	return nil
}
func (v *normalOrdinaryStockUdevFile) UnmarshalJSON(raw []byte) error {
	if e := normalOrdinaryStockUdevExactKeys(raw, "path", "kind", "code", "bytes", "sha256"); e != nil {
		return e
	}
	type plain normalOrdinaryStockUdevFile
	var n plain
	if e := control.UnmarshalObject(raw, &n); e != nil {
		return e
	}
	*v = normalOrdinaryStockUdevFile(n)
	return nil
}
func decodeNormalOrdinaryStockUdevManifest(raw []byte) (normalOrdinaryStockUdevManifest, error) {
	var zero normalOrdinaryStockUdevManifest
	fail := func(e error) (normalOrdinaryStockUdevManifest, error) { return zero, errors.Join(ErrUnknown, e) }
	if len(raw) == 0 || len(raw) > 64<<10 {
		return fail(fmt.Errorf("stock udev manifest size differs"))
	}
	if e := normalOrdinaryStockUdevExactKeys(raw, "protocol_version", "scope", "package_lock_sha256", "files"); e != nil {
		return fail(e)
	}
	type plain normalOrdinaryStockUdevManifest
	var n plain
	if e := control.UnmarshalObject(raw, &n); e != nil {
		return fail(e)
	}
	m := normalOrdinaryStockUdevManifest(n)
	if m.ProtocolVersion != 2 || m.Scope != normalOrdinaryStockUdevManifestScope || !normalOrdinaryStockUdevDigest(m.PackageLockSHA256) || len(m.Files) > 100 {
		return fail(fmt.Errorf("stock udev manifest contract differs"))
	}
	required := normalOrdinaryStockUdevRequiredInventory()
	seen := map[string]bool{}
	var total uint64
	previous := ""
	dsos := 0
	for _, f := range m.Files {
		if f.Path <= previous || path.Clean(f.Path) != f.Path || !strings.HasPrefix(f.Path, "/") || strings.ContainsAny(f.Path, "\x00\r\n\t ") || !normalOrdinaryStockUdevDigest(f.SHA256) || f.Bytes == 0 {
			return fail(fmt.Errorf("stock udev inventory path/order/hash differs"))
		}
		previous = f.Path
		kind, limit, ok := normalOrdinaryStockUdevFileClass(f.Path)
		if !ok || f.Kind != kind || f.Code != (kind == "program" || kind == "helper" || kind == "dso") || uint64(f.Bytes) > limit {
			return fail(fmt.Errorf("stock udev fixed file class differs"))
		}
		total += uint64(f.Bytes)
		if total > 256<<20 {
			return fail(fmt.Errorf("stock udev inventory total exceeds bound"))
		}
		if kind == "dso" {
			dsos++
		}
		seen[f.Path] = true
	}
	for name := range required {
		if !seen[name] {
			return fail(fmt.Errorf("stock udev required fixed inventory absent"))
		}
	}
	if dsos == 0 {
		return fail(fmt.Errorf("stock udev dynamic loader closure inventory absent"))
	}
	return m, nil
}
func normalOrdinaryStockUdevDigest(s string) bool {
	return physicalDigestValid(s) && s != strings.Repeat("0", 64)
}

// 构建器复用同一严格schema；返回的元数据仅用于比对最终rootdisk，不产生Source。
func NormalSystemSourceStockUdevManifestFiles(raw []byte) ([]NormalSystemSourceFile, error) {
	m, e := decodeNormalOrdinaryStockUdevManifest(raw)
	if e != nil {
		return nil, e
	}
	out := make([]NormalSystemSourceFile, 0, len(m.Files))
	for _, f := range m.Files {
		kind, ok := normalSystemStockUdevFileKind(f.Path)
		if !ok {
			return nil, ErrUnknown
		}
		out = append(out, NormalSystemSourceFile{Path: f.Path, CanonicalPath: f.Path, Kind: kind, Bytes: uint64(f.Bytes), SHA256: f.SHA256})
	}
	return out, nil
}

// 只导出严格静态MF中的原锁摘要，供构建器比对真实文件。摘要本身不授运行能力。
// 包锁不加入MF.files，不形成自身文件摘要循环；原System3另收固定锁文件。
func NormalSystemSourceStockUdevPackageLockSHA256(raw []byte) (string, error) {
	m, e := decodeNormalOrdinaryStockUdevManifest(raw)
	if e != nil {
		return "", e
	}
	return m.PackageLockSHA256, nil
}
