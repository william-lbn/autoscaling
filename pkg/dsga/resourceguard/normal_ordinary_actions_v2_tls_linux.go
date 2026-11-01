//go:build linux

package resourceguard

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/util"
	k8sutil "k8s.io/kubernetes/pkg/volume/util"
	"os"
	"path/filepath"
)

type normalOrdinaryTLSFile struct {
	Data string `json:"data"`
}

// 与原managed HTTP相同三文件/64KiB上限；密钥正文不进入日志或任何WAL。
func decodeNormalOrdinaryTLSFiles(raw []byte) (map[string]k8sutil.FileProjection, error) {
	if control.ValidateJSON(raw, normalOrdinaryActionMaxBody, 4096) != nil {
		return nil, fmt.Errorf("bounded TLS files required")
	}
	var entries map[string]json.RawMessage
	if json.Unmarshal(raw, &entries) != nil || len(entries) < 1 || len(entries) > 3 {
		return nil, fmt.Errorf("fixed TLS files required")
	}
	out := make(map[string]k8sutil.FileProjection, len(entries))
	for name, rawFile := range entries {
		if name != "tls.crt" && name != "tls.key" && name != "ca.crt" {
			return nil, fmt.Errorf("unknown fixed TLS file")
		}
		var f normalOrdinaryTLSFile
		if control.UnmarshalObject(rawFile, &f) != nil {
			return nil, fmt.Errorf("fixed TLS file object required")
		}
		data, e := base64.StdEncoding.DecodeString(f.Data)
		if e != nil {
			return nil, fmt.Errorf("TLS encoding differs")
		}
		out[name] = k8sutil.FileProjection{Data: data, Mode: 0644, FsUser: nil}
	}
	return out, nil
}

// directory仅由本包固定生产入口或TEST_ONLY夹具给出，永不来自请求字段。
// 复用原AtomicWriter，不另造轮换/文件hash协议。
func normalOrdinaryTLSAtomicWrite(directory string, payload map[string]k8sutil.FileProjection) error {
	writer, e := k8sutil.NewAtomicWriter(directory, "neon-ordinary-root-v2")
	if e != nil {
		return e
	}
	return writer.Write(payload, nil)
}
func normalOrdinaryTLSChecksum(directory string) (string, error) {
	dir := filepath.Join(directory, "..data")
	entries, e := os.ReadDir(dir)
	if e != nil {
		return "", e
	}
	if len(entries) < 1 || len(entries) > 3 {
		return "", fmt.Errorf("fixed TLS current generation required")
	}
	for _, f := range entries {
		if f.Name() != "tls.crt" && f.Name() != "tls.key" && f.Name() != "ca.crt" {
			return "", fmt.Errorf("foreign TLS generation entry")
		}
		info, e := f.Info()
		if e != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("TLS generation is not regular")
		}
	}
	sum, e := util.ChecksumFlatDir(dir)
	if e != nil || len(sum) != 43 {
		return "", fmt.Errorf("TLS checksum unavailable")
	}
	return sum, nil
}
