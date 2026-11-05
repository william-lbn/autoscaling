package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/tychoish/fun/srv"
	"go.uber.org/zap"

	"k8s.io/client-go/kubernetes"
	scheme "k8s.io/client-go/kubernetes/scheme"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/agent"
	"github.com/neondatabase/autoscaling/pkg/util"
)

func main() {
	logConfig := zap.NewProductionConfig()
	logConfig.Sampling = nil                // Disable sampling, which the production config enables by default.
	logConfig.Level.SetLevel(zap.InfoLevel) // Only "info" level and above (i.e. not debug logs)
	logger := zap.Must(logConfig.Build()).Named("autoscaler-agent")
	defer logger.Sync() //nolint:errcheck // what are we gonna do, log something about it?

	envArgs, err := agent.ArgsFromEnv()
	if err != nil {
		logger.Panic("Failed to get args from environment", zap.Error(err))
	}
	logger.Info("Got environment args", zap.Any("args", envArgs))

	config, err := agent.ReadConfig(envArgs.ConfigPath)
	if err != nil {
		logger.Panic("Failed to read config", zap.Error(err))
	}
	logger.Info("Got config", zap.Any("config", config))

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Panic("Failed to get in-cluster K8s config", zap.Error(err))
	}
	workConfig, electionConfig, err := agentClientConfigs(kubeConfig, config.DSGA != nil)
	if err != nil {
		logger.Panic("Failed to configure bounded DSGA clients", zap.Error(err))
	}
	kubeClient, err := kubernetes.NewForConfig(workConfig)
	if err != nil {
		logger.Panic("Failed to make K8S client", zap.Error(err))
	}
	if err = vmv1.AddToScheme(scheme.Scheme); err != nil {
		logger.Panic("Failed to add NeonVM scheme", zap.Error(err))
	}

	vmClient, err := vmclient.NewForConfig(workConfig)
	if err != nil {
		logger.Panic("Failed to make VM client", zap.Error(err))
	}

	var electionClient coordclient.CoordinationV1Interface
	if electionConfig != nil {
		electionClient, err = coordclient.NewForConfig(electionConfig)
		if err != nil {
			logger.Panic("Failed to make isolated DSGA election client", zap.Error(err))
		}
	}

	runner := agent.MainRunner{
		EnvArgs:            envArgs,
		Config:             config,
		KubeClient:         kubeClient,
		VMClient:           vmClient,
		DSGAElectionClient: electionClient,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	ctx = srv.SetShutdownSignal(ctx)
	ctx = srv.SetBaseContext(ctx)
	ctx = srv.WithOrchestrator(ctx)
	defer func() {
		cancel()
		if err := srv.GetOrchestrator(ctx).Wait(); err != nil {
			logger.Panic("Failed to shut down orchestrator", zap.Error(err))
		}

		logger.Info("Main loop returned without issue. Exiting.")
	}()

	if err := srv.GetOrchestrator(ctx).Add(srv.HTTP("agent-pprof", time.Second, util.MakePPROF("0.0.0.0:7777"))); err != nil {
		logger.Panic("Failed to add pprof service", zap.Error(err))
	}

	if err = runner.Run(logger, ctx); err != nil {
		logger.Panic("Main loop failed", zap.Error(err))
	}
}

// agentClientConfigs只为显式DSGA配置划分两个进程级有限桶。业务Kube和VM
// 使用同一个显式RateLimiter，不能因多个typed client而隐含放大总配额。
// CopyConfig保留既有RateLimiter，因此启用时必须替换；普通Agent保留原默认。
func agentClientConfigs(base *rest.Config, dsga bool) (*rest.Config, *rest.Config, error) {
	if base == nil {
		return nil, nil, fmt.Errorf("Agent clients require original Kubernetes config")
	}
	work := rest.CopyConfig(base)
	if !dsga {
		return work, nil, nil
	}
	work.QPS, work.Burst = 20, 40
	work.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(work.QPS, work.Burst)
	election := rest.CopyConfig(base)
	election.QPS, election.Burst = 10, 20
	election.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(election.QPS, election.Burst)
	return work, election, nil
}
