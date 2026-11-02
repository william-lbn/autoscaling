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
	"syscall"
	"time"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/neondatabase/autoscaling/pkg/dsga/qualificationissuer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func run(ctx context.Context, configPath, kubeconfigPath string) error {
	config, bundle, err := qualificationissuer.LoadInputs(configPath)
	if err != nil {
		return err
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
		return err
	}
	defer root.Close()
	issuer, err := qualificationissuer.New(core.CoreV1(), core.CoordinationV1(), vms, root, config)
	if err != nil {
		return err
	}
	result, err := issuer.Publish(ctx, bundle)
	if err != nil {
		var unknown *qualificationissuer.Unconfirmed
		if errors.As(err, &unknown) {
			// 仅输出原对象定位信息；不输出报告原文、Root 状态、Kube 错误正文或凭据。
			_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"issued": false, "unconfirmed": true, "claim": unknown.Claim, "report_sha256": config.ReportSHA256})
		}
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	config := flag.String("config", "", "root protected issuer startup configuration")
	kubeconfig := flag.String("kubeconfig", "", "root protected kubeconfig; otherwise fixed in-cluster credentials")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *config == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "fixed --config required")
		os.Exit(2)
	}
	if err := run(ctx, *config, *kubeconfig); err != nil {
		fmt.Fprintln(os.Stderr, "qualification not issued; preserve original input and any unconfirmed protected object")
		os.Exit(1)
	}
}
