//go:build linux && amd64 && dsga_native_stock_preparation && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// 只新增显式tag的DEV分派，优先于旧fixture但不替换任何生产constructor/provider。
func init() { normalRoleServiceStockPreparationDEVDispatch = stockPreparationDEVDispatch }
func stockPreparationDEVDispatch(m *testing.M) bool {
	if len(os.Args) < 2 || os.Args[0] != normalSystemRoleBrokerExecutable {
		return false
	}
	if os.Args[1] == "initial-boot" || strings.Contains(os.Args[1], "TestNativeNormalSystemRoleTransferV2Initial") || strings.Contains(os.Args[1], "TestNativeNormalSystemRoleOrdinaryV2Initial") {
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalStockUdevPreparationInitial$", "-test.v", "-test.timeout=150s"}
		rc := m.Run()
		os.Exit(rc)
	}
	if os.Args[1] == normalSystemRoleGateAction {
		fmt.Println("STOCK_PREPARATION_GATE_ENTRY")
		stockPreparationDEVDelayedStack("gate")
	}
	if os.Args[1] == "system-role-broker" {
		ctx := normalRoleServiceNativeBrokerContext(context.Background())
		trace := &normalBrokerInspectionDEVTrace{}
		ctx = context.WithValue(ctx, normalBrokerInspectionDEVTraceKey{}, trace)
		started := time.Now()
		fmt.Println("STOCK_PREPARATION_BROKER_ENTRY")
		// 显式DEV下从入口定时观察，覆盖原来源验证尚未进入intent的等待。
		// 有界一次stack，不改生产ctx/路径/协议/超时或来源选择。
		stockPreparationDEVDelayedStack("broker")
		ctx = context.WithValue(ctx, normalOrdinaryStockPreparationStageKey{}, func(stage string) {
			fmt.Printf("STOCK_PREPARATION_STAGE elapsed_ms=%d stage=%s\n", time.Since(started).Milliseconds(), stage)
			if stage == "complete" {
				b, _ := json.Marshal(trace.snapshot())
				fmt.Printf("BROKER_INSPECTION_DEV_PARTIAL_BEFORE_COMPLETION_WAL %s\n", b)
			}
		})
		ctx = context.WithValue(ctx, normalOrdinaryStockPreparationFailureKey{}, func(stage string, e error, p *normalOrdinaryStockUdevPreparation) {
			fmt.Printf("STOCK_PREPARATION_FIRST_ERROR stage=%s error=%v\n", stage, e)
			stockPreparationDEVInspectionReport(ctx)
			// 只该固定公开ACPI modalias，从同catalog原sysfs FD重读；不输出值/raw。
			if stage == "complete processed drain" && p != nil && p.catalog != nil {
				entry := p.catalog.entries["/devices/LNXSYSTM:00/LNXSYBUS:00/PNP0A06:04/PNP0C80:00"]
				if entry != nil && entry.uevent != nil {
					raw, err := normalOrdinaryUdevV2ReadFD(entry.uevent, 8192)
					if err == nil {
						properties, parseErr := normalOrdinaryUdevV2KernelStaticProperties("/sys"+entry.path, entry.subsystem, raw)
						if parseErr == nil {
							value, yes := properties["MODALIAS"]
							fmt.Printf("STOCK_PREPARATION_ACPI_ORIGINAL_FD present=%t bytes=%d sha256=%s\n", yes, len(value), physicalDigest([]byte(value)))
						} else {
							fmt.Println("STOCK_PREPARATION_ACPI_ORIGINAL_FD_PARSE_ERROR", parseErr)
						}
					} else {
						fmt.Println("STOCK_PREPARATION_ACPI_ORIGINAL_FD_READ_ERROR", err)
					}
				}
			}
		})
		ctx = context.WithValue(ctx, normalOrdinaryStockPreparationCheckpointKey{}, stockPreparationDEVObserve)
		if e := RunNormalSystemRoleBroker(ctx); e != nil {
			stockPreparationDEVInspectionReport(ctx)
			fmt.Println("STOCK_PREPARATION_BROKER_ERROR", e)
			normalOrdinaryStockUdevPreparations.Range(func(_, v any) bool {
				if p, ok := v.(*normalOrdinaryStockUdevPreparation); ok {
					p.mu.Lock()
					log := append([]byte(nil), p.stockLog...)
					p.mu.Unlock()
					fmt.Printf("STOCK_PREPARATION_PRIVATE_LOG %q\n", string(log))
				}
				return true
			})
			os.Exit(43)
		}
		os.Exit(0)
	}
	if os.Args[1] == "exec-system-role" {
		// 本根只验初始真实stock/Source，不把其它9角色的DEV client当兼容/exec通过。
		fmt.Println("STOCK_PREPARATION_ORDINARY_NOT_STARTED", os.Args[len(os.Args)-1])
		for {
			time.Sleep(time.Second)
		}
	}
	return false // 原真实Gate动作仍走旧已审RunNormalSystemRoleExecGate。
}

type stockPreparationDEVAfterR1Key struct{}

var stockPreparationDEVInspectionOnce sync.Once

func stockPreparationDEVInspectionReport(ctx context.Context) {
	trace, _ := ctx.Value(normalBrokerInspectionDEVTraceKey{}).(*normalBrokerInspectionDEVTrace)
	if trace == nil {
		return
	}
	stockPreparationDEVInspectionOnce.Do(func() {
		// rchar是整个R2及采样开销，不能解释为准确ELF读取字节。
		b, _ := json.Marshal(trace.snapshot())
		fmt.Printf("BROKER_INSPECTION_DEV_JSON %s\n", b)
	})
}

func stockPreparationDEVDelayedStack(stage string) {
	go func() {
		time.Sleep(10 * time.Second)
		b := make([]byte, 128<<10)
		n := runtime.Stack(b, true)
		fmt.Printf("STOCK_PREPARATION_DEV_STACK stage=%s %s\n", stage, b[:n])
	}()
}

// 仅DEV通知，既不由生产读取也不授Source/COMMIT；R1只在生产Start成功后等待它。
const stockPreparationDEVR1ReleaseName = ".stock-preparation-dev-r1-release"

func stockPreparationDEVObserve(ctx context.Context, _ *lockedState, p *normalOrdinaryStockUdevPreparation) error {
	if p == nil || p.q == nil || p.q.parent == nil || p.q.authority == nil || p.q.authority.admission == nil {
		return ErrUnknown
	}
	// 原Task.mu下dup，goroutine只拥有自己的原pidfd，不读可能close/reuse的字段。
	parent := p.q.parent
	parent.mu.Lock()
	if parent.closed || parent.pidfd < 0 {
		parent.mu.Unlock()
		return ErrUnknown
	}
	pidfd, e := unix.FcntlInt(uintptr(parent.pidfd), unix.F_DUPFD_CLOEXEC, 3)
	parent.mu.Unlock()
	if e != nil {
		return e
	}
	go func() {
		defer unix.Close(pidfd)
		call, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		failure := func(e error) {
			stockPreparationDEVInspectionReport(ctx)
			fmt.Println("STOCK_PREPARATION_AFTER_R1_ERROR", e)
			fmt.Println("STOCK_PREPARATION_GUEST_EXIT=1")
			unix.Sync()
			_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
		}
		// 先等真实COMMIT打开business，再取resource flock；不能在Ready路径抢锁。
		for !p.q.authority.admission.businessAllowed() {
			if p.q.authority.admission.failed.Load() || call.Err() != nil {
				failure(errors.Join(ErrUnknown, call.Err()))
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		var ref normalOrdinaryUdevV2PreparationReference
		e := p.q.g.withRecord(call, func(_ *record, state *lockedState) error {
			if !p.q.authority.admission.businessAllowed() {
				return ErrUnknown
			}
			var e error
			ref, e = p.referenceLocked(call, state)
			if e != nil {
				return e
			}
			if !p.q.authority.admission.businessAllowed() {
				return ErrUnknown
			}
			// 独立完整freshref之后再核原R1仍活，才通知生产Start已返回的R1。
			if e = arrayRegistryPIDFDAlive(pidfd); e != nil {
				return e
			}
			raw, _ := json.Marshal(ref)
			f, e := os.OpenFile(filepath.Join(p.q.g.config.StateDir, stockPreparationDEVR1ReleaseName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if e != nil {
				return e
			}
			_, writeErr := f.WriteString(physicalDigest(raw) + "\n")
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				return errors.Join(writeErr, closeErr)
			}
			fmt.Println("STOCK_PREPARATION_R1_ALIVE_REFERENCE_SHA256", physicalDigest(raw))
			fmt.Printf("STOCK_PREPARATION_ORIGINAL_JSON {\"projection_sha256\":%q,\"wait_sha256\":%q,\"manifest_sha256\":%q,\"package_lock_sha256\":%q,\"targets\":%d,\"runtime_phase\":3,\"typed_source\":true,\"normal\":false}\n", ref.ProjectionSHA256, ref.WaitSHA256, ref.ManifestSHA256, ref.PackageLockSHA256, len(p.catalog.ordered))
			return nil
		})
		if e != nil {
			failure(e)
			return
		}
		for {
			fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
			n, e := unix.Poll(fds, 50)
			if errors.Is(e, unix.EINTR) {
				continue
			}
			if e != nil {
				failure(e)
				return
			}
			if n > 0 && fds[0].Revents&unix.POLLIN != 0 {
				break
			}
			if n > 0 && fds[0].Revents != 0 {
				failure(ErrUnknown)
				return
			}
			if call.Err() != nil {
				failure(call.Err())
				return
			}
		}
		e = p.q.g.withRecord(call, func(_ *record, state *lockedState) error {
			after, e := p.referenceLocked(call, state)
			if e != nil {
				return e
			}
			if after != ref || !p.q.authority.admission.businessAllowed() {
				return ErrUnknown
			}
			return nil
		})
		if e != nil {
			failure(e)
			return
		}
		raw, _ := json.Marshal(ref)
		stockPreparationDEVInspectionReport(ctx)
		fmt.Println("STOCK_PREPARATION_R1_EXITED_REFERENCE_SHA256", physicalDigest(raw))
		// 显式后继DEV只延后观察结束，不改上述独立完整校验或任何生产授权。
		if observe, ok := ctx.Value(stockPreparationDEVAfterR1Key{}).(func()); ok {
			observe()
			return
		}
		fmt.Println("STOCK_PREPARATION_GUEST_EXIT=0")
		unix.Sync()
		_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	}()
	return nil
}
func TestNativeNormalStockUdevPreparationInitial(t *testing.T) {
	defer func() {
		rc := 0
		if t.Failed() {
			rc = 1
		}
		fmt.Printf("STOCK_PREPARATION_INITIAL_EXIT=%d\n", rc)
		if rc != 0 {
			fmt.Println("STOCK_PREPARATION_GUEST_EXIT=1")
			unix.Sync()
			_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
		}
	}()
	require.Equal(t, 1, os.Getppid())
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	source, raw, assets, e := initialGuestChannelInputs()
	require.NoError(t, e)
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, normalInitialWALFamilyKernelPrepV2, plan.InitialWALFamily())
	var port *initialGuestStream
	for {
		port, e = openInitialGuestPort(ctx)
		if e == nil {
			break
		}
		require.True(t, errors.Is(e, errInitialGuestPortPending))
		require.NoError(t, initialGuestWait(ctx))
	}
	defer port.close()
	challenge, e := bootprotocol.ReadInitialPrebootChallenge(port)
	require.NoError(t, e)
	_, e = initialPrebootBindInputs(challenge, raw, assets, source.Config)
	require.NoError(t, e)
	g, e := initialGuestProvisionInputs(source, raw)
	require.NoError(t, e)
	defer g.normalMemoryDiscardOriginalConfirmedSource()
	require.NoError(t, initialGuestExchangeWithRequirements(ctx, port, g, challenge, false, plan))
	port.close()
	state, e := lockState(ctx, g.config.StateDir)
	require.NoError(t, e)
	_, e = g.normalKernelPrepV2OriginalLocked(ctx, state)
	require.NoError(t, e)
	_, e = g.normalMemoryOriginalConfirmedSourceLocked(ctx, state)
	require.NoError(t, e)
	state.close()
	_, e = g.PrepareNormalSplitTopology(ctx)
	require.NoError(t, e)
	_, e = ProvisionGuest(ctx)
	require.NoError(t, e)
	require.NoError(t, CheckGuest(ctx))
	// 不调用旧DEVPrepareFixedDirectories/不注入projection/monitor/Wait；全部真实生产ctor。
	releasePath := filepath.Join(g.config.StateDir, stockPreparationDEVR1ReleaseName)
	_, e = os.Lstat(releasePath)
	require.True(t, errors.Is(e, os.ErrNotExist), "DEV release must not preexist")
	fmt.Println("STOCK_PREPARATION_R1_START_BROKER")
	stockPreparationDEVDelayedStack("initial")
	require.NoError(t, startInitialNormalSystemV3Broker(ctx, g))
	// 生产Ready/COMMIT/ACK已真实返回；现在才增加DEV R1存活观察等待。
	for {
		f, readErr := os.Open(releasePath)
		if readErr == nil {
			raw, readErr := io.ReadAll(io.LimitReader(f, 66))
			closeErr := f.Close()
			require.NoError(t, readErr)
			require.NoError(t, closeErr)
			if len(raw) >= 65 {
				require.Equal(t, 65, len(raw))
				require.Equal(t, byte('\n'), raw[64])
				require.True(t, physicalDigestValid(string(raw[:64])))
				break
			}
			// O_EXCL创建至唯一write之间可能读短；只是DEV未通知，继续受ctx约束。
		} else {
			require.True(t, errors.Is(readErr, os.ErrNotExist))
		}
		require.NoError(t, ctx.Err())
		time.Sleep(10 * time.Millisecond)
	}
	metadata, e := readNormalSystemRoleServiceMetadata()
	require.NoError(t, e)
	require.EqualValues(t, 2, metadata.Origin.ProtocolVersion)
	require.EqualValues(t, 2, metadata.Origin.Filesystem.ProtocolVersion)
	require.NotEmpty(t, metadata.ReadySHA256)
}

// 第一段只导出原Host seed绑定的既有metadata18c/Initial8矩阵。外层实际Builder
// 合入真实包锁/provider后，第二段才重新绑定MF→NativeBuild→原6资产。
func TestExportNormalStockUdevPreparationSeed(t *testing.T) {
	// 与原组件相同的小C只占非stock角色占位；stock Programs随后由原包锁实ELF覆盖。
	t.Setenv("DSGA_SYSTEM_ROLE_ELF", "fixture/system-role")
	ordinary2DEVExportTransfer2(t)
	root := filepath.Join(os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT"), "root")
	for rel, data := range map[string]string{"/etc/passwd": "root:x:0:0:root:/:/neonvm/bin/sh\npostgres:x:1000:1000:PG:/var/db/postgres:/neonvm/bin/sh\n", "/etc/group": "root:x:0:\npostgres:x:1000:\n"} {
		p := filepath.Join(root, rel)
		if _, e := os.Lstat(p); errors.Is(e, os.ErrNotExist) {
			require.NoError(t, os.WriteFile(p, []byte(data), 0644))
		} else {
			require.NoError(t, e)
		}
	}
}
func TestFinalizeNormalStockUdevPreparationFixture(t *testing.T) {
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	require.NotEmpty(t, out)
	root := filepath.Join(out, "root")
	read := func(name string) []byte {
		b, e := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, e)
		return b
	}
	put := func(name string, b []byte) { require.NoError(t, os.WriteFile(filepath.Join(root, name), b, 0444)) }
	manifest, e := DecodeNormalSystemSourceManifest(read(NormalSystemSourceManifestPath))
	require.NoError(t, e)
	require.True(t, normalSystemStockUdevSelected(manifest))
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(read(fixedNativeBirthBuildPath), &build))
	build.RuntimeClosure.SystemManifestSHA256 = physicalDigest(read(NormalSystemSourceManifestPath))
	braw, e := json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, braw)
	var runtime RuntimeDocument
	require.NoError(t, json.Unmarshal(read(FixedSealedRuntimePath), &runtime))
	ts, e := parseRuntimePolicyTemplates(runtime.PolicyTemplatesJSON)
	require.NoError(t, e)
	for n := range ts.Templates {
		if ts.Templates[n].Asset == "dsga-native-birth.json" {
			var value map[string]any
			require.NoError(t, json.Unmarshal(ts.Templates[n].Policy, &value))
			value["build_manifest_sha256"] = physicalDigest(braw)
			ts.Templates[n].Policy, e = json.Marshal(value)
			require.NoError(t, e)
		}
	}
	runtime = runtimeTemplatesSetBlob(runtime, runtimeTemplatesEnvelope(ts.Templates))
	raw, e := json.Marshal(runtime)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, normalInitialWALFamilyKernelPrepV2, plan.InitialWALFamily())
	for logical, data := range assets {
		name, e := RuntimeISOAssetName(logical)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", name), data, 0444))
	}
	put(FixedSealedRuntimePath, raw)
	var source SourceConfig
	require.NoError(t, json.Unmarshal(runtime.SourceJSON, &source))
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	put(FixedConfigPath, cfg)
	require.NoError(t, os.WriteFile(filepath.Join(out, "runtime.json"), raw, 0444))
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(plan.RequiredKernelArgs(), " ")+"\n"), 0444))
}
