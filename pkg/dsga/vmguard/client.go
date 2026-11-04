package vmguard

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"time"
)

const (
	runnerAdvanceTimeout             = 15 * time.Second
	runnerPublicationTimeout         = 5 * time.Second
	runnerClientQPS          float32 = 50
	runnerClientBurst                = 100
)

// RunnerClientConfig只复制原InCluster配置，不接受外部认证或集群参数。
// 双轮完整authority需要约42次API；默认5QPS/10burst不能满足原Tick窗口。
// 固定有限速率由本Runner的core/coord/VM客户端共享；HTTP单请求仍最多5秒。
func RunnerClientConfig(base *rest.Config) *rest.Config {
	config := rest.CopyConfig(base)
	config.Timeout = 5 * time.Second
	config.UserAgent = "neonvm-runner-dsga-memory"
	config.QPS = runnerClientQPS
	config.Burst = runnerClientBurst
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(runnerClientQPS, runnerClientBurst)
	return config
}
