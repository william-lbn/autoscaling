//go:build linux && amd64 && dsga_native_stock_real_roles && dsga_native_stock_preparation && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"testing"
	"time"
)

// 显式DEV机制探针：所有PID1客户端进入同生产API，既不吞角色也不造完成/租约。
// 初始stock组件仍保旧tag；此后继直到第一个真实拒点，有缺payload/旧Cproducer便FAIL。
func init() {
	prior := normalRoleServiceNativeBrokerContext
	normalRoleServiceNativeBrokerContext = func(ctx context.Context) context.Context {
		ctx = prior(ctx)
		diagnostic := &normalSystemRoleDiagnostic{changed: make(chan struct{}, 1)}
		ctx = context.WithValue(ctx, normalSystemRoleDiagnosticKey{}, diagnostic)
		// changed永不close；生产仅nonblocking通知，不携callback或权限。
		go func() {
			for range diagnostic.changed {
				snapshot := diagnostic.snapshot()
				kill := false
				for _, event := range snapshot.Events {
					if event.Stage >= normalRoleDiagKillAcceptedWrite {
						kill = true
					}
				}
				if kill {
					raw, _ := json.Marshal(snapshot)
					fmt.Printf("REAL_ROLES_DEV_KILL_DIAGNOSTIC_JSON %s\n", raw)
					return
				}
			}
		}()
		ctx = context.WithValue(ctx, stockPreparationDEVAfterR1Key{}, func() {
			fmt.Println("REAL_ROLES_DEV_ORIGINAL_R1_EXIT_CONFIRMED_COMPONENT_ONLY")
			go func() {
				time.Sleep(100 * time.Second)
				stockRealRolesDEVFail("post-R1-role-observation-deadline", ErrUnknown)
			}()
		})
		started := time.Now()
		return context.WithValue(ctx, normalSystemRoleV2CheckpointKey{}, func(stage string, p *normalSystemRoleV2Publication) error {
			role := "absent"
			pid := 0
			if p != nil && p.endpoints != nil {
				role = p.endpoints.role
			}
			if p != nil && p.installed != nil && p.installed.task != nil {
				pid = p.installed.task.source.PID
			}
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CHECKPOINT elapsed_ms=%d stage=%s role=%s pid=%d normal=false\n", time.Since(started).Milliseconds(), stage, role, pid)
			if stage == "wait-confirmed" && p != nil && p.installed != nil {
				i := p.installed
				wait, e := i.child.originalRoleWaitConfirmed(i.creator)
				if e != nil {
					fmt.Printf("REAL_ROLES_DEV_ORIGINAL_WAIT_ERROR role=%s error=%v\n", role, e)
				} else {
					status := syscall.WaitStatus(wait.Status)
					fmt.Printf("REAL_ROLES_DEV_ORIGINAL_WAIT role=%s raw=%d exited=%t exit_status=%d signaled=%t signal=%d\n", role, uint32(status), status.Exited(), status.ExitStatus(), status.Signaled(), int(status.Signal()))
					if !status.Exited() || status.ExitStatus() != 0 {
						raw, _ := json.Marshal(diagnostic.snapshot())
						fmt.Printf("REAL_ROLES_DEV_WAIT_DIAGNOSTIC_JSON %s\n", raw)
						stockRealRolesDEVFailureCounters("original-nonzero-wait")
						if p.runtime != nil && p.runtime.stock != nil {
							q := p.runtime.stock
							q.mu.Lock()
							closed, failure := q.closed, q.ledger.failed
							q.mu.Unlock()
							fmt.Printf("REAL_ROLES_DEV_STOCK_FAILURE_AFTER_WAIT closed=%t error=%v\n", closed, failure)
						}
					}
				}
			}
			return nil // 只观察，不选来源/预算/路径，不构造成功。
		})
	}
	normalRoleServiceStockPreparationDEVDispatch = stockRealRolesDEVDispatch
}

// 仅原失败已决定后读取固定Kernel counters，不读env/argv/凭据，不影响任何判定。
func stockRealRolesDEVFailureCounters(stage string) {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return
	}
	for _, name := range []string{"memory.events", "memory.current", "memory.max", "memory.peak", "pids.events"} {
		raw, e := os.ReadFile(systemCgroup + "/" + name)
		if e == nil && len(raw) <= 1024 {
			fmt.Printf("REAL_ROLES_DEV_FAILURE_CGROUP stage=%s file=%s value=%q\n", stage, name, string(raw))
		} else {
			fmt.Printf("REAL_ROLES_DEV_FAILURE_CGROUP_READ stage=%s file=%s error=%v\n", stage, name, e)
		}
	}
}
func stockRealRolesDEVFail(stage string, e error) {
	fmt.Printf("REAL_ROLES_DEV_FIRST_FAILURE stage=%s error=%v\n", stage, e)
	fmt.Println("STOCK_PREPARATION_GUEST_EXIT=1")
	if os.Getuid() == 0 && os.Geteuid() == 0 {
		stockRealRolesDEVFailureCounters(stage)
		unix.Sync()
		_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	}
	os.Exit(47) // 普通目标不为DEV诊断尝试额外Root syscall，原creator仍真实Wait。
}
func stockRealRolesDEVDispatch(m *testing.M) bool {
	if len(os.Args) > 1 && os.Args[0] == normalSystemRoleBrokerExecutable {
		if os.Args[1] == "exec-system-role" {
			if len(os.Args) != 3 {
				stockRealRolesDEVFail("client-argument-shape", ErrUnknown)
			}
			role := os.Args[2]
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_CLIENT_ENTRY", role)
			code, e := RunNormalSystemRoleClient(context.Background(), role)
			if e != nil {
				stockRealRolesDEVFail("production-client-"+role, e)
			}
			fmt.Printf("REAL_ROLES_DEV_PRODUCTION_CLIENT_RETURN role=%s code=%d\n", role, code)
			if code != 0 {
				stockRealRolesDEVFail("production-client-nonzero-"+role, fmt.Errorf("actual fixed role exit code %d", code))
			}
			os.Exit(code)
		}
		if os.Args[1] == "ordinary-v2-device-check" {
			if len(os.Args) != 2 {
				stockRealRolesDEVFail("device-check-argument-shape", ErrUnknown)
			}
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_DEVICE_CHECK_ENTRY")
			if e := RunNormalSystemOrdinaryV2DeviceCheck(context.Background()); e != nil {
				stockRealRolesDEVFail("production-device-check", e)
			}
			fmt.Println("REAL_ROLES_DEV_PRODUCTION_DEVICE_CHECK_RETURN")
			os.Exit(0)
		}
	}
	return stockPreparationDEVDispatch(m) // initial/broker/Gate保持原真实生产链。
}
