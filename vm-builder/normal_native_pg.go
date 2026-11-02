package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// major只选择私有PG构建，采集器ABI仍为0/1；不得从seed或ELF存在自动升级。
func validateNormalNativePGSelection(mode, arch string, controls, version, major uint, postgresSHA string) error {
	if err := validateNormalNativeBirthBuildSelection(mode, arch, controls, version); err != nil {
		return err
	}
	if major != 17 && major != 18 {
		return fmt.Errorf("unsupported private native PostgreSQL major")
	}
	if major == 18 && (version != 1 || !normalNativePGDigest(postgresSHA)) {
		return fmt.Errorf("native PostgreSQL18 requires explicit collector1 and original PostgreSQL ELF SHA256")
	}
	if postgresSHA != "" && (version != 1 || !normalNativePGDigest(postgresSHA)) {
		return fmt.Errorf("original PostgreSQL ELF pin requires collector1 and canonical SHA256")
	}
	return nil
}

func normalNativePGDigest(s string) bool {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != s {
		return false
	}
	for _, b := range raw {
		if b != 0 {
			return true
		}
	}
	return false
}

// raw只能是原resourceguard采集器刚完整验证并规范编码的输出，不是外部
// 自报版本或资格。PG18必须另以原资产锁中的ELF摘要选择实际采集到的文件；
// 不执行输入PG，不从版本字符串猜major，也不把这个构建匹配当Source认证。
func validateNormalNativePGManifest(raw []byte, major uint32, postgresSHA string) error {
	if major != 17 && major != 18 || major == 18 && !normalNativePGDigest(postgresSHA) || postgresSHA != "" && !normalNativePGDigest(postgresSHA) {
		return fmt.Errorf("invalid private native PostgreSQL major or ELF pin")
	}
	var manifest struct {
		PGMajor      uint32 `json:"pg_major"`
		PostgresPath string `json:"postgres_path"`
		Files        []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if json.Unmarshal(raw, &manifest) != nil || manifest.PGMajor != major {
		return fmt.Errorf("native image PostgreSQL major differs from explicit build selection")
	}
	if postgresSHA == "" {
		return nil
	} // 原PG17默认字节/声明沿用，不加新参数。
	for _, file := range manifest.Files {
		if file.Path == manifest.PostgresPath {
			if file.SHA256 == postgresSHA {
				return nil
			}
			return fmt.Errorf("native image actual PostgreSQL ELF differs from original selected asset")
		}
	}
	return fmt.Errorf("native image original PostgreSQL ELF entry missing")
}
