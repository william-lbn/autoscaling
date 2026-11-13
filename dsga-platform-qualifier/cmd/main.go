// 独立 RBAC issuer 的一次性资格发布入口。没有报告/认证 artifact 就不发布；
// 不创建 Node 预算、不发 Root/PG/QMP 写，也不把发布成功称为平台认证完成。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/qualificationissuer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func run(ctx context.Context, configPath, kubeconfigPath string) error {
	config, bundle, err := qualificationissuer.LoadInputs(configPath)
	if err != nil {
		return &qualificationFailure{"issuer_input", err}
	}
	var connection *rest.Config
	if kubeconfigPath == "" {
		connection, err = rest.InClusterConfig()
	} else {
		var raw []byte
		raw, err = qualificationissuer.ReadTrustedFile(kubeconfigPath, qualificationissuer.MaxArtifactBytes)
		if err == nil {
			connection, err = clientcmd.RESTConfigFromKubeConfig(raw)
		}
	}
	if err != nil {
		return fmt.Errorf("trusted Kubernetes client configuration unavailable")
	}
	connection.Timeout = time.Duration(config.TimeoutSeconds) * time.Second
	connection.UserAgent = "dsga-platform-qualifier"
	core, err := kubernetes.NewForConfig(connection)
	if err != nil {
		return fmt.Errorf("trusted Kubernetes client unavailable")
	}
	vms, err := vmclient.NewForConfig(connection)
	if err != nil {
		return fmt.Errorf("trusted VM client unavailable")
	}
	root, err := qualificationissuer.NewLocalRoot(config.ControlSocketPath, time.Duration(config.TimeoutSeconds)*time.Second)
	if err != nil {
		return &qualificationFailure{"issuer_root", err}
	}
	defer root.Close()
	issuer, err := qualificationissuer.New(core.CoreV1(), core.CoordinationV1(), vms, root, config)
	if err != nil {
		return &qualificationFailure{"issuer_setup", err}
	}
	result, err := issuer.Publish(ctx, bundle)
	if err != nil {
		var unknown *qualificationissuer.Unconfirmed
		if errors.As(err, &unknown) {
			// 仅输出原对象定位信息；不输出报告原文、Root 状态、Kube 错误正文或凭据。
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"issued": false, "unconfirmed": true, "claim": unknown.Claim, "report_sha256": config.ReportSHA256})
			return &qualificationFailure{"publication_unconfirmed", err}
		}
		return &qualificationFailure{"publication", err}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

// runCollect只读API/UDS并归档本地证据；不会调用Issuer.Publish或修改任何Kube对象。
func runCollect(ctx context.Context, configPath, kubeconfigPath string) error {
	return runCollectMode(ctx, configPath, kubeconfigPath, false)
}

// 自动供应允许有界只读重采集。资格发布只执行一次，未知结果保留原对象。
func runCollectMode(ctx context.Context, configPath, kubeconfigPath string, publish bool) error {
	config, err := qualificationissuer.LoadCollectorConfig(configPath)
	if err != nil {
		return &qualificationFailure{"collector_input", err}
	}
	var connection *rest.Config
	if kubeconfigPath == "" {
		connection, err = rest.InClusterConfig()
	} else {
		var raw []byte
		raw, err = qualificationissuer.ReadTrustedFile(kubeconfigPath, qualificationissuer.MaxArtifactBytes)
		if err == nil {
			connection, err = clientcmd.RESTConfigFromKubeConfig(raw)
		}
	}
	if err != nil {
		return fmt.Errorf("trusted Kubernetes collector configuration unavailable")
	}
	connection.Timeout = time.Duration(config.TimeoutSeconds) * time.Second
	connection.UserAgent = "dsga-platform-qualifier-collector-read-only"
	core, err := kubernetes.NewForConfig(connection)
	if err != nil {
		return fmt.Errorf("trusted Kubernetes collector client unavailable")
	}
	vms, err := vmclient.NewForConfig(connection)
	if err != nil {
		return fmt.Errorf("trusted VM collector client unavailable")
	}
	root, err := qualificationissuer.NewLocalRoot(config.ControlSocketPath, time.Duration(config.TimeoutSeconds)*time.Second)
	if err != nil {
		return &qualificationFailure{"collector_root", err}
	}
	defer root.Close()
	collector, err := qualificationissuer.NewCollector(core.CoreV1(), core.CoordinationV1(), vms, root, config)
	if err != nil {
		return &qualificationFailure{"collector_setup", err}
	}
	collection, err := collectQualification(ctx, collector.Collect, publish, time.Duration(config.TimeoutSeconds)*time.Second)
	if err != nil {
		return &qualificationFailure{"collection_" + qualificationissuer.CollectionStage(err), err}
	}
	if publish {
		if err = prepareAutomaticOutput(config.OutputDirectory); err != nil {
			return &qualificationFailure{"output_volume", err}
		}
	}
	if err = qualificationissuer.WriteCollection(collection); err != nil {
		return &qualificationFailure{"evidence_write", err}
	}
	if publish {
		return run(ctx, config.OutputDirectory+"/issuer-config.proposed.json", kubeconfigPath)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"collected": true, "published": false, "report_sha256": collection.ProposedIssuerConfig.ReportSHA256, "snapshot_sha256": collection.SnapshotSHA256, "proposed_issuer_config": config.OutputDirectory + "/issuer-config.proposed.json"})
}

// 只重读尚未签发的采集。统一使用冻结的采集期限，保留独立手动采集的单次语义。
func collectQualification(ctx context.Context, collect func(context.Context) (qualificationissuer.Collection, error), automatic bool, timeout time.Duration) (qualificationissuer.Collection, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		result, err := collect(ctx)
		if err == nil || !automatic || ctx.Err() != nil || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
			return result, err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return qualificationissuer.Collection{}, err
		case <-timer.C:
		}
	}
}

type qualificationFailure struct {
	stage string
	cause error
}

func (e *qualificationFailure) Error() string { return e.cause.Error() }
func (e *qualificationFailure) Unwrap() error { return e.cause }

const automaticJobOutput = "/out/evidence"

func prepareAutomaticOutput(path string) error {
	// Kubernetes 创建 emptyDir 时使用 0777。只收紧 Job 专用的固定卷，
	// 其他部署者路径继续由原私有目录校验负责。
	if path == automaticJobOutput {
		if err := restrictJobOutputVolume(filepath.Dir(path)); err != nil {
			return err
		}
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return fmt.Errorf("fresh private collection directory required")
	}
	return nil
}

func restrictJobOutputVolume(path string) error {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("original qualification output volume unavailable")
	}
	defer syscall.Close(fd)
	var stat syscall.Stat_t
	if syscall.Fstat(fd, &stat) != nil || stat.Uid != 0 || syscall.Fchmod(fd, 0o700) != nil {
		return fmt.Errorf("root must protect the original qualification output volume")
	}
	return nil
}

func main() {
	config := flag.String("config", "", "root protected issuer startup configuration")
	collectConfig := flag.String("collect-config", "", "root protected read-only collector configuration; never publishes")
	collectPublishConfig := flag.String("collect-publish-config", "", "collect original evidence then publish once; never retry an unconfirmed publication")
	kubeconfig := flag.String("kubeconfig", "", "root protected kubeconfig; otherwise fixed in-cluster credentials")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if !oneMode(*config, *collectConfig, *collectPublishConfig) || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "exactly one fixed --config, --collect-config or --collect-publish-config required")
		os.Exit(2)
	}
	var err error
	if *collectPublishConfig != "" {
		err = runCollectMode(ctx, *collectPublishConfig, *kubeconfig, true)
	} else if *collectConfig != "" {
		err = runCollect(ctx, *collectConfig, *kubeconfig)
	} else {
		err = run(ctx, *config, *kubeconfig)
	}
	if err != nil {
		stage := "client_or_output"
		var failed *qualificationFailure
		if errors.As(err, &failed) {
			stage = failed.stage
		}
		reason := "unconfirmed"
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			reason = "deadline"
		case errors.Is(err, context.Canceled):
			reason = "canceled"
		case apierrors.IsForbidden(err):
			reason = "forbidden"
		case apierrors.IsUnauthorized(err):
			reason = "unauthorized"
		case apierrors.IsNotFound(err):
			reason = "not_found"
		}
		fmt.Fprintf(os.Stderr, "qualification not issued stage=%s reason=%s; preserve original input and any unconfirmed protected object\n", stage, reason)
		os.Exit(1)
	}
}

func oneMode(paths ...string) bool {
	count := 0
	for _, p := range paths {
		if p != "" {
			count++
		}
	}
	return count == 1
}
