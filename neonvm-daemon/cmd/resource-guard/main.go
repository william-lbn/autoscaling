// resource-guard是Guest root执行器；不监听网络，不复用旧neonvmd无认证HTTP端口。
// 上层Compute受管通道还须完成授权及固定命令接线；本程序不会因被编译就自动启用。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func run() error {
	configPath := flag.String("config", "/etc/neon-dsga/resource.json", "root-owned immutable VM resource configuration")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("required action: initialize <epoch>, fence <epoch>, apply, observe or replay-original")
	}
	config, err := resourceguard.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	guard, err := resourceguard.Open(config)
	if err != nil {
		return err
	}
	ctx := context.Background()
	var result any
	if args[0] == "fence" || args[0] == "initialize" {
		if len(args) != 2 {
			return fmt.Errorf("fence requires one canonical nonzero epoch")
		}
		epoch, parseErr := strconv.ParseUint(args[1], 10, 64)
		if parseErr != nil || epoch == 0 || args[1] != strconv.FormatUint(epoch, 10) {
			return fmt.Errorf("invalid fence epoch")
		}
		if args[0] == "initialize" {
			result, err = guard.Initialize(ctx, control.Uint64(epoch))
		} else {
			result, err = guard.Fence(ctx, control.Uint64(epoch))
		}
	} else {
		if len(args) != 1 || (args[0] != "apply" && args[0] != "observe" && args[0] != "replay-original") {
			return fmt.Errorf("unsupported resource action")
		}
		data, readErr := io.ReadAll(io.LimitReader(os.Stdin, 64*1024+1))
		if readErr != nil {
			return readErr
		}
		if len(data) > 64*1024 {
			return fmt.Errorf("resource command exceeds bound")
		}
		var command resourceguard.Command
		if err := json.Unmarshal(data, &command); err != nil {
			return err
		}
		switch args[0] {
		case "apply":
			result, err = guard.Apply(ctx, command)
		case "observe":
			result, err = guard.Observe(ctx, command)
		case "replay-original":
			result, err = guard.ReplayOriginal(ctx, command)
		}
	}
	if err != nil {
		return err
	}
	// 输出失败与丢失回复均不能推导NoEffect；原意图仍在持久日志中，必须原查询。
	return json.NewEncoder(os.Stdout).Encode(result)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "DSGA resource guard:", err)
		os.Exit(1)
	}
}
