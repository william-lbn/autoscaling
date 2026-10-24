//go:build !linux

package resourceguard

import (
	"context"
	"fmt"
)

// Mac只执行纯逻辑/文件持久化验证；生产入口明确拒绝，不提供模拟内核成功回执。
func Open(Config) (*Guard, error) {
	return nil, fmt.Errorf("DSGA resource enforcement requires Linux Guest/root")
}

func LoadConfig(string) (Config, error) {
	return Config{}, fmt.Errorf("DSGA resource config requires Linux Guest/root")
}

func ExecPostgres([]string) error { return fmt.Errorf("managed PG birth requires Linux Guest/root") }

func ProvisionGuest(context.Context) (Evidence, error) {
	return Evidence{}, fmt.Errorf("Guest resource provisioning requires Linux Guest/root")
}
func CheckGuest(context.Context) error {
	return fmt.Errorf("Guest resource check requires Linux Guest/root")
}

func CheckManagementTLSDirectory() error {
	return fmt.Errorf("management TLS directory requires Linux Guest/root")
}

func ServeGuest(context.Context) error {
	return fmt.Errorf("Guest resource service requires Linux Guest/root")
}
