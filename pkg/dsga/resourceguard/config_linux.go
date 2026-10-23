//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// LoadConfig读取可信安装者提供的固定配置；不从网络、PG GUC或可写Guest挂载
// 接受StateDir。配置只含身份/状态路径，不含密码，不将请求嵌入shell。
func LoadConfig(path string) (Config, error) {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return Config{}, fmt.Errorf("resource writes require real root")
	}
	return loadConfig(path)
}

func loadConfig(path string) (Config, error) {
	var config Config
	f, err := rootFile(path, false)
	if err != nil {
		return config, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return config, err
	}
	if len(data) > 4096 {
		return config, fmt.Errorf("resource config exceeds bound")
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return config, err
	}
	return config, nil
}
