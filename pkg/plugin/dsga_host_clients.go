package plugin

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
)

const (
	// 三节点owner每2秒最多各尝试16个Pod，普通Reserve至少6次API读取，
	// 即约144请求/秒；200/400给初始41Pod约246次读取及CAS保留有限余量。
	// 不提高framework/watch的原客户端预算，也不取消任何单请求deadline。
	dsgaHostWorkQPS   = float32(200)
	dsgaHostWorkBurst = 400
)

// newDSGAHostClientConfigs保留原连接/TLS/凭据/transport/timeout，仅替换独立
// 有界限流预算。CopyConfig不清已有RateLimiter，因此必须显式清除，防止表面
// 提高QPS却仍复用原桶；绝不能修改handle提供的原config。
func newDSGAHostClientConfigs(base *rest.Config, nodes int) (*rest.Config, *rest.Config, error) {
	if base == nil || nodes < 1 || nodes > 128 {
		return nil, nil, fmt.Errorf("bounded Host clients require config and 1..128 original Node domains")
	}
	work := rest.CopyConfig(base)
	work.RateLimiter, work.QPS, work.Burst = nil, dsgaHostWorkQPS, dsgaHostWorkBurst
	election := rest.CopyConfig(base)
	// 三节点每2秒GET+UPDATE约3请求/秒，10/20足够独立续租；较大配置
	// 按每节点每秒2请求保守伸展，最多256/512，仍是明确有限的桶。
	qps := max(10, 2*nodes)
	election.RateLimiter, election.QPS, election.Burst = nil, float32(qps), 2*qps
	return work, election, nil
}

func newDSGAHostClients(base *rest.Config, nodes int) (*kubernetes.Clientset, *coordclient.CoordinationV1Client, error) {
	workConfig, electionConfig, err := newDSGAHostClientConfigs(base, nodes)
	if err != nil {
		return nil, nil, err
	}
	work, err := kubernetes.NewForConfig(workConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Host work client: %w", err)
	}
	election, err := coordclient.NewForConfig(electionConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("Host election client: %w", err)
	}
	return work, election, nil
}
