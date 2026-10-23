//go:build !linux

package resourceguard

import "fmt"

// Mac只执行纯逻辑/文件持久化验证；生产入口明确拒绝，不提供模拟内核成功回执。
func Open(Config) (*Guard, error) {
	return nil, fmt.Errorf("DSGA resource enforcement requires Linux Guest/root")
}

func LoadConfig(string) (Config, error) {
	return Config{}, fmt.Errorf("DSGA resource config requires Linux Guest/root")
}
