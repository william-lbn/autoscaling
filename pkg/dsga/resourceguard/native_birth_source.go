package resourceguard

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"path"
	"strings"
)

const nativeBirthPolicyScope = "neon-native-birth-policy-v1"
const nativeBirthBuildScope = "neon-native-pre-main-default-loader-v1"
const nativeBirthStageScope = "neon-native-birth-stage-v1"

// 原ISO策略只选择原构建/几何，不是PG自报profile或租户启动环境。
// PGmajor仅选择已实现的原生ABI；上层出生阶段不按PG版本分叉。
type nativeBirthPolicy struct {
	ProtocolVersion       uint32    `json:"protocol_version"`
	Scope                 string    `json:"scope"`
	RuntimeDocumentSHA256 string    `json:"runtime_document_sha256"`
	ResourceSourceSHA256  string    `json:"resource_source_sha256"`
	BuildManifestSHA256   string    `json:"build_manifest_sha256"`
	PGmajor               uint32    `json:"pg_major"`
	MaxBuffers            uint32    `json:"max_buffers"`
	StartupBuffers        uint32    `json:"startup_buffers"`
	PageBytes             uint32    `json:"page_bytes"`
	ElementBytes          [4]uint64 `json:"element_bytes"`
}
type nativeBirthBuildFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type nativeBirthBuildManifest struct {
	ProtocolVersion uint32                 `json:"protocol_version"`
	Scope           string                 `json:"scope"`
	PGmajor         uint32                 `json:"pg_major"`
	LaunchPath      string                 `json:"launch_path"`
	PostgresPath    string                 `json:"postgres_path"`
	InterpreterPath string                 `json:"interpreter_path"`
	Files           []nativeBirthBuildFile `json:"files"`
}

// Binding保留精确字节摘要与固定ABI；读取闭包不证明post-GUC插件或完整FS/I。
// 不含fixture/production bool；不能由调用方选择“已认证”。
type nativeBirthSourceBinding struct {
	Policy       nativeBirthPolicy `json:"policy"`
	PolicySHA256 string            `json:"policy_sha256"`
	BinarySHA256 string            `json:"binary_sha256"`
}
type nativeBirthSource struct {
	resource SourceConfig
	binding  nativeBirthSourceBinding
	manifest nativeBirthBuildManifest
}

func nativeBirthDigest(raw string) ([32]byte, error) {
	var out [32]byte
	b, err := hex.DecodeString(raw)
	if err != nil || len(b) != 32 || hex.EncodeToString(b) != raw {
		return out, fmt.Errorf("noncanonical native birth digest")
	}
	copy(out[:], b)
	if out == [32]byte{} {
		return out, fmt.Errorf("zero native birth digest")
	}
	return out, nil
}
func newNativeBirthNonce() (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	if nonce == [32]byte{} {
		return "", fmt.Errorf("zero native birth random nonce")
	}
	return hex.EncodeToString(nonce[:]), nil
}
func (p nativeBirthPolicy) validate() error {
	if p.ProtocolVersion != 1 || p.Scope != nativeBirthPolicyScope || p.PGmajor != 17 ||
		p.PageBytes != 4096 || p.ElementBytes != [4]uint64{64, 8192, 16, 20} ||
		p.MaxBuffers == 0 || p.MaxBuffers > 0x7fffffff || p.StartupBuffers == 0 || p.StartupBuffers > p.MaxBuffers {
		return fmt.Errorf("unsupported original native birth ABI or geometry")
	}
	for _, s := range []string{p.RuntimeDocumentSHA256, p.ResourceSourceSHA256, p.BuildManifestSHA256} {
		if _, err := nativeBirthDigest(s); err != nil {
			return err
		}
	}
	return nil
}
func nativeBirthCleanPath(s string) bool {
	return strings.HasPrefix(s, "/") && path.Clean(s) == s && s != "/" && len(s) <= 4096 && !strings.ContainsAny(s, "\x00\r\n")
}
func (m nativeBirthBuildManifest) validate() error {
	if m.ProtocolVersion != 1 || m.Scope != nativeBirthBuildScope || m.PGmajor != 17 ||
		m.LaunchPath != FixedPostgresPath || !nativeBirthCleanPath(m.PostgresPath) ||
		len(m.Files) == 0 || len(m.Files) > 64 || m.InterpreterPath != "" && !nativeBirthCleanPath(m.InterpreterPath) {
		return fmt.Errorf("unsupported native pre-main build manifest")
	}
	seen := map[string]bool{}
	for i, f := range m.Files {
		if !nativeBirthCleanPath(f.Path) || seen[f.Path] || i > 0 && m.Files[i-1].Path >= f.Path {
			return fmt.Errorf("native build files must be unique canonical sorted paths")
		}
		if _, err := nativeBirthDigest(f.SHA256); err != nil {
			return err
		}
		seen[f.Path] = true
	}
	if !seen[m.PostgresPath] || m.InterpreterPath != "" && !seen[m.InterpreterPath] {
		return fmt.Errorf("native build entry or interpreter missing")
	}
	return nil
}
func (b nativeBirthSourceBinding) expected(nonce string, as control.Uint64) (nativeBirthExpected, error) {
	var e nativeBirthExpected
	if b.Policy.validate() != nil {
		return e, fmt.Errorf("invalid native birth source policy")
	}
	var err error
	if e.Nonce, err = nativeBirthDigest(nonce); err != nil {
		return e, err
	}
	if e.SourceSHA, err = nativeBirthDigest(b.Policy.ResourceSourceSHA256); err != nil {
		return e, err
	}
	if e.ProfileSHA, err = nativeBirthDigest(b.PolicySHA256); err != nil {
		return e, err
	}
	if _, err = nativeBirthDigest(b.BinarySHA256); err != nil {
		return e, err
	}
	e.AddressSpaceBytes = uint64(as)
	e.MaxBuffers = b.Policy.MaxBuffers
	e.PageBytes = b.Policy.PageBytes
	e.ElementBytes = b.Policy.ElementBytes
	_, _, err = e.geometry()
	return e, err
}

// 原字节分别绑定：resource原SHA与policy原SHA进入wire，不以重序列化冒充原输入。
func decodeNativeBirthSource(runtimeRaw, policyRaw, manifestRaw []byte) (nativeBirthSource, error) {
	var out nativeBirthSource
	if len(runtimeRaw) == 0 || len(runtimeRaw) > 64<<10 || len(policyRaw) == 0 || len(policyRaw) > 16<<10 || len(manifestRaw) == 0 || len(manifestRaw) > 32<<10 {
		return out, fmt.Errorf("native birth source exceeds fixed bound")
	}
	var document RuntimeDocument
	if err := json.Unmarshal(runtimeRaw, &document); err != nil {
		return out, err
	}
	source, err := document.Source()
	if err != nil {
		return out, err
	}
	var p nativeBirthPolicy
	if err := control.UnmarshalObject(policyRaw, &p); err != nil {
		return out, err
	}
	if p.validate() != nil || p.RuntimeDocumentSHA256 != physicalDigest(runtimeRaw) || p.ResourceSourceSHA256 != document.SourceSHA256 || p.BuildManifestSHA256 != physicalDigest(manifestRaw) {
		return out, fmt.Errorf("native birth exact original source binding differs")
	}
	var m nativeBirthBuildManifest
	if err := control.UnmarshalObject(manifestRaw, &m); err != nil {
		return out, err
	}
	if m.validate() != nil || m.PGmajor != p.PGmajor {
		return out, fmt.Errorf("native birth pre-main build ABI differs")
	}
	binary := ""
	for _, f := range m.Files {
		if f.Path == m.PostgresPath {
			binary = f.SHA256
		}
	}
	out = nativeBirthSource{resource: source, manifest: m, binding: nativeBirthSourceBinding{Policy: p, PolicySHA256: physicalDigest(policyRaw), BinarySHA256: binary}}
	if _, err := out.binding.expected(strings.Repeat("01", 32), source.Limits.AddressSpaceMaxBytes); err != nil {
		return nativeBirthSource{}, err
	}
	return out, nil
}

// NativeBirthRef仅引用原nonce/Pending，后继record v3必须显式配对。它本身不发
// 完整Birth/normal/VM许可；旧record、HTTP和PG不能提交此引用触发自动出生。
type NativeBirthRef struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Nonce           string `json:"nonce"`
	PendingSHA256   string `json:"pending_sha256"`
}

func (r NativeBirthRef) validate() error {
	if r.ProtocolVersion != 1 {
		return fmt.Errorf("invalid native birth reference version")
	}
	if _, err := nativeBirthDigest(r.Nonce); err != nil {
		return err
	}
	_, err := nativeBirthDigest(r.PendingSHA256)
	return err
}
