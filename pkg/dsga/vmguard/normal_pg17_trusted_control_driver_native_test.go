//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 本组合使用真实production Session/SDK/执行器/原CAS和QEMU；Host apiserver、
// Scheduler分配域及平台资格明确为DEV模型，不能称K8s E。观察日志绝不产生许可。
func TestNativePG17TrustedControlProductionHost(t *testing.T) {
	actualMode, modeError := trustedPG17FaultMode()
	require.NoError(t, modeError)
	require.NoError(t, trustedPG17WriteModeReceipt("/proof/Production-Driver", actualMode))
	require.Equal(t, "qemu18c-trusted-pg17-7g8g", os.Getenv("DSGA_INITIAL_HOST_NATIVE"))
	fixture := vmguard.NewNativePG17TrustedHostFixture(t, "/proof/fixture/runtime.json")
	fixture.Run(t, runTrustedPG17Production)
}

func trustedDriverSave(name string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	dir := "/proof/Production-Driver"
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(append(b, '\n')); e != nil {
		return e
	}
	return f.Sync()
}
func trustedDriverPause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}
func trustedDriverStage(ctx context.Context, stage string) error {
	marker := []byte("TRUSTED_PG17_OBSERVED_STAGE=" + stage)
	for ctx.Err() == nil {
		b, e := os.ReadFile("/proof/console.log")
		if e != nil {
			return e
		}
		if len(b) > 8<<20 {
			return fmt.Errorf("bounded Guest console exceeded")
		}
		if bytes.Contains(b, []byte("TRUSTED_PG17_GUEST_EXIT=1")) || bytes.Contains(b, []byte("--- FAIL:")) {
			return fmt.Errorf("actual Guest failed before %s", stage)
		}
		if bytes.Count(b, marker) == 1 {
			return nil
		}
		if bytes.Count(b, marker) > 1 {
			return fmt.Errorf("Guest stage duplicated: %s", stage)
		}
		if e = trustedDriverPause(ctx); e != nil {
			return e
		}
	}
	return ctx.Err()
}

// 固定合成测试密钥；真实Runtime公钥必须逐字匹配。没有打印Token或改生产鉴权。
func trustedDriverToken(key ed25519.PrivateKey, id resourceguard.Identity, epoch control.Uint64, method, path, bodySHA string) (string, error) {
	pub := key.Public().(ed25519.PublicKey)
	sum := sha256.Sum256(pub)
	header, e := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": hex.EncodeToString(sum[:16])})
	if e != nil {
		return "", e
	}
	now := uint64(time.Now().Unix())
	claims := map[string]any{"aud": "neon-dsga", "scope": "compute_ctl:dsga", "compute_id": "compute-one", "vm_uid": id.VMUID, "compute_instance_id": id.ComputeInstanceID, "issuer_epoch": epoch, "nbf": now - 1, "exp": now + 60}
	if method != "" {
		claims["scope"] = "compute_ctl:dsga:array-authority"
		claims["request"] = map[string]any{"protocol_version": 1, "method": method, "path": path, "body_sha256": bodySHA}
	}
	payload, e := json.Marshal(claims)
	if e != nil {
		return "", e
	}
	enc := base64.RawURLEncoding.EncodeToString
	unsigned := enc(header) + "." + enc(payload)
	return unsigned + "." + enc(ed25519.Sign(key, []byte(unsigned))), nil
}

// 模型上界分项来自固定部署输入；仍由真实Root预算工厂重新检验F/I/P/A/N。
// 该对象只产生Host API模型资格，不把历史planning、RSS或observer marker变成授权。
type trustedDriverBudgetModel struct {
	Scope                         string         `json:"scope"`
	SourceSHA256                  string         `json:"source_sha256"`
	KBytes                        control.Uint64 `json:"K_bytes"`
	KnownConditionalOccupiedBytes control.Uint64 `json:"known_conditional_occupied_bytes"`
}

func trustedDriverQualification(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, v resourceguard.View, lowerNormal bool) (budget.QualificationSource, error) {
	var zero budget.QualificationSource
	raw, e := os.ReadFile("/proof/fixture/host-budget-model.json")
	if e != nil {
		return zero, e
	}
	var model trustedDriverBudgetModel
	if control.UnmarshalObject(raw, &model) != nil || model.Scope != "trusted-controller-host-api-model-v1" || len(model.SourceSHA256) != 64 || model.KBytes == 0 || model.KBytes > 1<<40 || model.KnownConditionalOccupiedBytes == 0 {
		return zero, fmt.Errorf("explicit bounded Host API model budget input missing")
	}
	digest, e := hex.DecodeString(model.SourceSHA256)
	if e != nil || hex.EncodeToString(digest) != model.SourceSHA256 {
		return zero, fmt.Errorf("model source SHA malformed")
	}
	birth, ref, _, e := vmguard.ReadPublished(ctx, f.Core.CoreV1(), f.Store.Binding(), nil)
	if e != nil {
		return zero, e
	}
	before, e := v.ConfirmedLimits()
	if e != nil {
		return zero, e
	}
	bootstrap, e := json.Marshal(v.Bootstrap)
	if e != nil {
		return zero, e
	}
	identity, e := json.Marshal(birth.Identity)
	if e != nil {
		return zero, e
	}
	system := before.SystemMemoryMaxBytes + 2*model.KBytes
	transition := model.KnownConditionalOccupiedBytes
	if system < before.SystemMemoryMaxBytes || transition > before.OutsideUpperBytes || system >= before.OutsideUpperBytes-transition {
		return zero, fmt.Errorf("model Outside partitions overflow or exhaust fixed Outside")
	}
	safety := before.OutsideUpperBytes - system - transition
	targets := make([]budget.QualifiedTarget, 0, 2)
	for _, n := range []control.Uint64{7 << 30, 8 << 30} {
		limits := before
		limits.HugePools = append([]resourceguard.HugePool(nil), before.HugePools...)
		if lowerNormal && n == 7<<30 {
			limits.MemoryMaxBytes = 960 << 20
		}
		targets = append(targets, budget.QualifiedTarget{VMMemoryBytes: n, GuestUsableMemoryBytes: v.Actual.GuestMemTotalBytes, Limits: limits, SystemOutsidePGBytes: system, TransitionBytes: transition, SafetyBytes: safety})
	}
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: v.Identity, Incarnation: f.Store.Binding().Incarnation, ReportID: "explicit-host-api-model-only-" + model.SourceSHA256[:16], RootBootstrapSHA256: vmguard.SourceDigest(bootstrap), RunnerBirth: ref, RunnerIdentitySHA256: vmguard.SourceDigest(identity), RunnerPodName: f.Config.PodName, RunnerPodUID: birth.Identity.RunnerPodUID, RunnerRestartCount: 0, NodeName: f.Config.NodeName, NodeUID: birth.Identity.NodeUID, HoldUntilExplicitSettlement: true, Targets: targets}
	if e = q.Validate(); e != nil {
		return zero, e
	}
	body, e := json.Marshal(q)
	if e != nil {
		return zero, e
	}
	yes := true
	b := f.Store.Binding()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "trusted-pg17-model-" + vmguard.SourceDigest(body)[:24], Namespace: b.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &yes, BlockOwnerDeletion: &yes}}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.QualificationKind}}, Immutable: &yes, Data: map[string]string{budgetclaims.ClaimKey: string(body)}}
	made, e := f.Core.CoreV1().ConfigMaps(b.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if e != nil {
		return zero, e
	}
	got, e := f.Core.CoreV1().ConfigMaps(b.Namespace).Get(ctx, made.Name, metav1.GetOptions{})
	if e != nil {
		return zero, e
	}
	if got.UID == "" || got.UID != made.UID || got.Data[budgetclaims.ClaimKey] != string(body) {
		return zero, fmt.Errorf("model qualification original UID/body not confirmed")
	}
	source := budget.QualificationSource{Claim: budget.ObjectRef{Name: got.Name, UID: string(got.UID)}, SHA256: vmguard.SourceDigest(body)}
	if e = trustedDriverSave("model-qualification-"+source.SHA256[:16], map[string]any{"Host_API_is_model": true, "model_source": model, "qualification": q, "source": source, "E": 0}); e != nil {
		return zero, e
	}
	return source, nil
}

// 只标记原并发失败来源，不替换错误对象或输出正文。
// 固定字面量比对仅作诊断，不产生授权、重试或成功观察。
type trustedDriverFailureOrigin struct {
	source           string
	err              error
	decisionUnixNano int64
}

func trustedDriverRecordFailure(source string, err error, decisionUnixNano ...int64) error {
	facts := map[string]any{"source": source, "error_type": fmt.Sprintf("%T", err)}
	if len(decisionUnixNano) == 1 {
		facts["failed_call_return_unix_nano"] = decisionUnixNano[0]
	}
	facts["recorded_unix_nano"] = time.Now().UnixNano()
	if err != nil {
		text := err.Error()
		facts["runner_not_dispatchable"] = strings.Contains(text, "Runner original command is not currently dispatchable")
		facts["resource_response_budget"] = strings.Contains(text, "resource response cannot confirm original target Guest budget")
		facts["normal_preparation_absent"] = strings.Contains(text, "normal quota original preparation absent")
		facts["normal_pre_grow_cost_fence"] = strings.Contains(text, "normal quota pre-grow requires original prepared cost and fence")
	}
	codes, limited := trustedDriverPrepareFailureCodes(err)
	facts["prepare_reason_codes"] = codes
	facts["prepare_reason_chain_limit"] = limited
	facts["prepare_reason_map_sha256"] = "2219557aba6c113e8975df6f5f2820cedb5e16b327d3c85b31119619de6efde1"
	_ = trustedDriverSave("selected-failure-origin", facts)
	return err
}

func runTrustedPG17Production(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture) (resultErr error) {
	// 只读记录原失败的固定哨兵及日志状态结构，不输出错误正文或凭据。
	// 原返回值不变，不重试业务动作、不清除原持有或Unknown。
	defer func() {
		if resultErr == nil {
			return
		}
		facts := map[string]any{
			"service_safety":  errors.Is(resultErr, owner.ErrServiceSafety),
			"leadership_lost": errors.Is(resultErr, owner.ErrLeadershipLost),
			"canceled":        errors.Is(resultErr, context.Canceled),
			"deadline":        errors.Is(resultErr, context.DeadlineExceeded),
		}
		probe, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		snapshot, readErr := f.Store.Load(probe)
		facts["journal_read_ok"] = readErr == nil
		if readErr == nil {
			// 同一次只读快照私有归档，便于比对原预算/关闭命令；不打印、不进入Git。
			_ = trustedDriverSave("owner-failure-state-private", snapshot.State())
			facts["initial_admission_ready"] = snapshot.State().InitialAdmissionReady()
			facts["has_initial_held"] = snapshot.State().InitialHeld != nil
			facts["has_active"] = snapshot.State().Active != nil
			facts["history_count"] = len(snapshot.State().History)
		}
		_ = trustedDriverSave("owner-failure-sentinels", facts)
	}()

	if e := trustedDriverStage(ctx, "baseline"); e != nil {
		return e
	}
	var doc resourceguard.RuntimeDocument
	if e := json.Unmarshal(f.RuntimeJSON, &doc); e != nil {
		return e
	}
	source, e := doc.Source()
	if e != nil {
		return e
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x39}, 32))
	if source.ControlPublicKey != base64.RawURLEncoding.EncodeToString(key.Public().(ed25519.PublicKey)) {
		return fmt.Errorf("original DEV Runtime signing key differs")
	}
	// 只供最终关机同步；204不参与Prepare/Root/Runner任何授权判断。
	faultCase, faultCaseErr := trustedPG17FaultMode()
	if faultCaseErr != nil {
		return faultCaseErr
	}
	var sqlBusyRelease atomic.Bool
	var completed atomic.Bool
	var rootDeathBegin atomic.Bool
	acknowledged := make(chan struct{}, 1)
	listener, e := net.Listen("tcp4", "127.0.0.1:33381")
	if e != nil {
		return e
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		// 固定 DEV 同步仅安排真实 SQL 任务，不提供任何 PG/Root/VM 许可。
		if r.Method == http.MethodGet && r.URL.RawQuery == "" && r.URL.Path == "/pg17-fault-case" {
			// 该端点仅选择Guest额外SQL任务；完整Host实际mode由独立entry收据核验。
			guestWorkCase := faultCase
			if faultCase == trustedPG17ControllerTakeoverMode {
				guestWorkCase = ""
			}
			_, _ = w.Write([]byte(guestWorkCase))
			return
		}
		if r.Method == http.MethodGet && r.URL.RawQuery == "" && r.URL.Path == "/pg17-root-death-begin" {
			if faultCase == "root-death" && rootDeathBegin.Load() {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusConflict)
			}
			return
		}
		if r.Method == http.MethodGet && r.URL.RawQuery == "" && r.URL.Path == "/pg17-sql-busy-release" {
			if sqlBusyRelease.Load() {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusConflict)
			}
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/pg17-driver-complete" || r.URL.RawQuery != "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if completed.Load() {
			w.WriteHeader(http.StatusNoContent)
			select {
			case acknowledged <- struct{}{}:
			default:
			}
		} else {
			w.WriteHeader(http.StatusConflict)
		}
	})}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = server.Close()
		<-serverDone
	}()
	// 这是显式Host API模型的旧owner退场；不伪造Session/holder或改PG/Root floor。
	lease, e := f.Core.CoordinationV1().Leases(f.Store.Binding().Namespace).Get(ctx, journal.LeaseName(f.Store.Binding()), metav1.GetOptions{})
	if e != nil {
		return e
	}
	one := int32(1)
	lease.Spec.LeaseDurationSeconds = &one
	if _, e = f.Core.CoordinationV1().Leases(lease.Namespace).Update(ctx, lease, metav1.UpdateOptions{}); e != nil {
		return e
	}
	return trustedPG17RunControllers(ctx, f, faultCase, func(ownerCtx context.Context, session *owner.Session, continuation bool, handoff *trustedPG17ControllerHandoff) error {
		if e := session.Check(ownerCtx); e != nil {
			return e
		}
		if session.Actor().Epoch <= f.Actor.Epoch {
			return fmt.Errorf("production Session did not perform original journal takeover")
		}
		transport := &http.Transport{DialTLSContext: func(c context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(c, "tcp", "127.0.0.1:33080")
		}}
		defer transport.CloseIdleConnections()
		faultMode, modeError := trustedPG17FaultMode()
		if modeError != nil {
			return modeError
		}
		faultTransport := &trustedPG17FaultTransport{inner: transport, mode: faultMode}
		sdkTransport, closeFaultRelay, relayError := faultTransport.transportForSDK()
		if relayError != nil {
			return relayError
		}
		defer closeFaultRelay()
		partial := resourceguard.Identity{VMUID: source.Config.VMUID, ComputeInstanceID: source.Config.ComputeInstanceID}
		token := func(context.Context) (string, error) {
			return trustedDriverToken(key, partial, session.Actor().Epoch, "", "", "")
		}
		rc := resourceguard.ClientConfig{Context: ownerCtx, BaseURL: "https://trusted-compute.invalid", Identity: partial, IssuerEpoch: session.Actor().Epoch, Timeout: 75 * time.Second, Transport: sdkTransport, Token: token, ArrayAuthorityToken: func(_ context.Context, m, p, s string) (string, error) {
			return trustedDriverToken(key, partial, session.Actor().Epoch, m, p, s)
		}}
		resources, e := session.NewResourceWorker(ownerCtx, rc)
		if e != nil {
			return e
		}
		defer resources.CloseIdleConnections()
		snap, e := f.Store.Load(ownerCtx)
		if e != nil {
			return e
		}
		if snap.State().ResourceIdentity == nil {
			return fmt.Errorf("production resource binding not committed")
		}
		rc.Identity = *snap.State().ResourceIdentity
		observer, e := resourceguard.NewClient(rc)
		if e != nil {
			return e
		}
		defer observer.CloseIdleConnections()
		view, e := observer.InspectState(ownerCtx)
		if e != nil {
			return e
		}
		var qualification budget.QualificationSource
		if continuation {
			retained, read := f.Store.Load(ownerCtx)
			if read != nil {
				return read
			}
			qualification, e = trustedPG17RetainedQualification(retained.State())
		} else {
			qualification, e = trustedDriverQualification(ownerCtx, f, view, false)
		}
		if e != nil {
			return e
		}
		pc := compute.Config{BaseURL: rc.BaseURL, Instance: control.Instance{VMUID: partial.VMUID, ComputeInstanceID: partial.ComputeInstanceID}, IssuerEpoch: session.Actor().Epoch, Timeout: 75 * time.Second, Transport: sdkTransport, Token: token}
		worker, e := session.NewProductionPGWorker(ownerCtx, pc, resources, f.Core.CoreV1(), qualification)
		if e != nil {
			return e
		}
		defer worker.CloseIdleConnections()
		instance, _ := worker.Binding()
		pc.Instance = instance
		pg, e := compute.New(ownerCtx, pc)
		if e != nil {
			return e
		}
		defer pg.CloseIdleConnections()
		node, e := f.Core.CoreV1().Nodes().Get(ownerCtx, f.Config.NodeName, metav1.GetOptions{})
		if e != nil {
			return e
		}
		var hostModel hostguard.Model
		if e = json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &hostModel); e != nil {
			return e
		}
		hostLease, e := f.Core.CoordinationV1().Leases(hostModel.Binding.Namespace).Get(ownerCtx, hostModel.Binding.LeaseName, metav1.GetOptions{})
		if e != nil {
			return e
		}
		if hostModel.Profile == nil {
			return fmt.Errorf("original Host model profile absent")
		}
		if hostLease.Spec.HolderIdentity == nil {
			return fmt.Errorf("model Host lease holder absent")
		}
		// 原Host模型holder的真实同UID续租，不能等首个tick后才恢复过期租约。
		stamp := metav1.NewMicroTime(time.Now())
		hostLease.Spec.RenewTime = &stamp
		hostLease, e = f.Core.CoordinationV1().Leases(hostLease.Namespace).Update(ownerCtx, hostLease, metav1.UpdateOptions{})
		if e != nil {
			return e
		}
		allocator, e := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), hostModel.Binding, *hostLease.Spec.HolderIdentity)
		if e != nil {
			return e
		}
		publisher, e := hostexec.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.Store, allocator, session.Ownership().LeaseUID, hostModel.Profile.VMOverheadBytes)
		if e != nil {
			return e
		}
		controlCtx, stop := context.WithCancel(ownerCtx)
		defer stop()
		var background sync.WaitGroup
		failures := make(chan trustedDriverFailureOrigin, 4)
		// 实际QMP发布更新模型VM status，不按请求目标填回实际数值。
		background.Add(1)
		go func() {
			defer background.Done()
			for controlCtx.Err() == nil {
				tick := f.Runner.Tick(controlCtx)
				if tick != nil && !errors.Is(tick, vmguard.ErrUnknown) && !errors.Is(tick, vmguard.ErrNotSent) && !errors.Is(tick, vmguard.ErrStopped) {
					select {
					case failures <- trustedDriverFailureOrigin{"runner_tick", tick, time.Now().UnixNano()}:
					default:
					}
					return
				}
				_, _, pub, read := vmguard.ReadPublished(controlCtx, f.Core.CoreV1(), f.Store.Binding(), nil)
				if read == nil && pub.Actual != nil && pub.Actual.RequestedBytes == pub.Actual.ActualBytes && pub.State.Pending == nil {
					vm, get := f.VMs.NeonvmV1().VirtualMachines(f.Store.Binding().Namespace).Get(controlCtx, f.Store.Binding().Name, metav1.GetOptions{})
					if get != nil {
						select {
						case failures <- trustedDriverFailureOrigin{"vm_get", get, time.Now().UnixNano()}:
						default:
						}
						return
					}
					if vm.Status.MemorySize == nil || vm.Status.MemorySize.Value() != int64(pub.Actual.ActualBytes) {
						vm.Status.MemorySize = resource.NewQuantity(int64(pub.Actual.ActualBytes), resource.BinarySI)
						if _, get = f.VMs.NeonvmV1().VirtualMachines(vm.Namespace).UpdateStatus(controlCtx, vm, metav1.UpdateOptions{}); get != nil {
							select {
							case failures <- trustedDriverFailureOrigin{"vm_status_update", get, time.Now().UnixNano()}:
							default:
							}
							return
						}
					}
				}
				if trustedDriverPause(controlCtx) != nil {
					return
				}
			}
		}()
		// 模型Host owner只续同原Lease，容量仍由原Publisher/Node CAS生成与结清。
		background.Add(1)
		go func() {
			defer background.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-controlCtx.Done():
					return
				case <-ticker.C:
					l, get := f.Core.CoordinationV1().Leases(hostLease.Namespace).Get(controlCtx, hostLease.Name, metav1.GetOptions{})
					if get == nil && l.UID == hostLease.UID {
						now := metav1.NewMicroTime(time.Now())
						l.Spec.RenewTime = &now
						_, get = f.Core.CoordinationV1().Leases(l.Namespace).Update(controlCtx, l, metav1.UpdateOptions{})
					}
					if get != nil {
						select {
						case failures <- trustedDriverFailureOrigin{"host_lease_renew", get, time.Now().UnixNano()}:
						default:
						}
						return
					}
				}
			}
		}()
		defer func() { stop(); background.Wait() }()
		// 先用7G/8G均保持出生N的原资格完成真实InitialBudget与InitialAdmission。
		// 该loop退出与join后才选择另一immutable资格，不改旧CM或手填初开事实。
		if !continuation {
			initialCtx, stopInitial := context.WithCancel(controlCtx)
			initialDone := make(chan error, 1)
			go func() {
				initialDone <- worker.Run(initialCtx, owner.PGLoopOptions{PollInterval: 100 * time.Millisecond})
			}()
			initialJoined := false
			defer func() {
				stopInitial()
				if !initialJoined {
					<-initialDone
				}
			}()
			for controlCtx.Err() == nil {
				select {
				case failure := <-failures:
					return trustedDriverRecordFailure(failure.source, failure.err, failure.decisionUnixNano)
				case failure := <-initialDone:
					initialJoined = true
					return trustedDriverRecordFailure("initial_owner_loop", fmt.Errorf("production initial owner loop exited: %w", failure))
				default:
				}
				s, read := f.Store.Load(controlCtx)
				if read != nil {
					return read
				}
				if s.State().InitialAdmissionReady() {
					if e = trustedDriverSave("initial-admission", map[string]any{"state": s.State(), "Host_API_is_model": true, "E": 0}); e != nil {
						return e
					}
					break
				}
				if e = trustedDriverPause(controlCtx); e != nil {
					return e
				}
			}
			if controlCtx.Err() != nil {
				return controlCtx.Err()
			}
			stopInitial()
			initialErr := <-initialDone
			initialJoined = true
			if initialErr != nil && !errors.Is(initialErr, context.Canceled) {
				return initialErr
			}
			worker.CloseIdleConnections()
			view, e = observer.InspectState(controlCtx)
			if e != nil {
				return e
			}
		}
		if !continuation {
			qualification, e = trustedDriverQualification(controlCtx, f, view, true)
			if e != nil {
				return e
			}
			worker, e = session.NewProductionPGWorker(controlCtx, pc, resources, f.Core.CoreV1(), qualification)
			if e != nil {
				return e
			}
			defer worker.CloseIdleConnections()
		}
		pgLoopCtx, stopPGLoop := context.WithCancel(controlCtx)
		defer func() { stopPGLoop() }()
		historyOffset := 0
		if faultMode == "busy-cancel" {
			if e = trustedPG17BusyCancel(controlCtx, f, session, worker, pg, observer); e != nil {
				return e
			}
			historyOffset = 1
		}
		loopDone := make(chan error, 1)
		pgControl := worker.NewControlEndpoint()
		loopStarted := false
		// 正常在线缩容走部署使用的生产审计路径。故障夹具仍按原入口编排。
		loopOptions := func() owner.PGLoopOptions {
			return owner.PGLoopOptions{PollInterval: 100 * time.Millisecond, Control: pgControl, EnableInFlightGrow: faultMode == "" && view.OnlineShrinkProtocolVersion == 1}
		}
		defer func() {
			stop()
			if loopStarted {
				<-loopDone
			}
		}()
		if continuation {
			loopStarted = true
			go func() {
				loopDone <- worker.Run(pgLoopCtx, loopOptions())
			}()
			if e = trustedPG17AwaitControllerAdmission(controlCtx, f, session, pg, pgControl); e != nil {
				return e
			}
		}
		for index, target := range []uint32{1024, 256, 2048} {
			if continuation && index == 0 {
				continue
			}
			current, e := pg.Memory(controlCtx)
			if e != nil {
				return e
			}
			kind := control.Grow
			size := control.Uint64(8 << 30)
			if target < current.Observed.HighBuffers {
				kind = control.Shrink
				size = 7 << 30
			}
			// 这是新端到端意图的总时限，包含 Host 预留和 Guest 启用。
			// PG 单次执行仍取剩余时间且最多 60 秒；旧请求的期限从原账本读取。
			const requestWindow = 2 * time.Minute
			request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: instance, IssuerEpoch: session.Actor().Epoch, Revision: control.Uint64(index + 1 + historyOffset), OperationID: control.OperationID{Low: control.Uint64(index + 1 + historyOffset)}}, Kind: kind, ExpectedCapacityGeneration: current.Observed.CapacityGeneration, TargetBuffers: target, TargetVMMemoryBytes: size, BudgetCommitmentID: fmt.Sprintf("trusted-owner-budget-%d", index+1), DeadlineUnixMillis: control.Uint64(time.Now().Add(requestWindow).UnixMilli())}
			if e = session.Check(controlCtx); e != nil {
				return e
			}
			snapshot, e := f.Store.Load(controlCtx)
			if e != nil {
				return e
			}
			if _, disposition, e := f.Store.Begin(controlCtx, snapshot, session.Actor(), request, uint64(time.Now().UnixMilli())); e != nil || disposition != journal.Accepted {
				return fmt.Errorf("original Begin not confirmed: %v", e)
			}
			if !loopStarted {
				loopStarted = true
				go func() {
					loopDone <- worker.Run(pgLoopCtx, loopOptions())
				}()
			}
			for controlCtx.Err() == nil {
				select {
				case failure := <-failures:
					return trustedDriverRecordFailure(failure.source, failure.err, failure.decisionUnixNano)
				case failure := <-loopDone:
					loopStarted = false
					return trustedDriverRecordFailure("pg_owner_loop", fmt.Errorf("production owner loop exited: %w", failure))
				default:
				}
				s, load := f.Store.Load(controlCtx)
				if load != nil {
					return load
				}
				m := s.State()
				if faultMode == "sql-busy-drain" && index == 1 && !sqlBusyRelease.Load() {
					ready, check := trustedPG17SQLBusyUnchanged(controlCtx, f, pg, observer, m, request)
					if check != nil {
						return check
					}
					if ready {
						sqlBusyRelease.Store(true)
					}
				}
				if faultMode == "root-death" && faultTransport.dropped.Load() {
					stopPGLoop()
					joined := <-loopDone
					loopStarted = false
					if joined != nil && !errors.Is(joined, context.Canceled) {
						return joined
					}
					worker.CloseIdleConnections()
					resources.CloseIdleConnections()
					faultTransport.CloseIdleConnections()
					rootDeathBegin.Store(true)
					if e = trustedPG17RootDeathRecovery(controlCtx, f, session, rc, pc, qualification, faultTransport); e != nil {
						return e
					}
					completed.Store(true)
					select {
					case <-acknowledged:
						return nil
					case <-controlCtx.Done():
						return controlCtx.Err()
					}
				}
				if faultMode == "normal-reply-loss" && faultTransport.dropped.Load() && !faultTransport.recovered.Load() {
					if _, e = trustedPG17LeaseObservation(controlCtx, pg, request.Key, "at-loss"); e != nil {
						return e
					}
					stopPGLoop()
					joined := <-loopDone
					loopStarted = false
					if joined != nil && !errors.Is(joined, context.Canceled) {
						return joined
					}
					worker.CloseIdleConnections()
					resources.CloseIdleConnections()
					faultTransport.CloseIdleConnections()
					resources, worker, e = trustedPG17RecoverReplyLoss(controlCtx, f, session, rc, pc, qualification, pg, faultTransport)
					if e != nil {
						return e
					}
					defer resources.CloseIdleConnections()
					defer worker.CloseIdleConnections()
					pgControl = worker.NewControlEndpoint()
					pgLoopCtx, stopPGLoop = context.WithCancel(controlCtx)
					loopStarted = true
					go func() {
						loopDone <- worker.Run(pgLoopCtx, loopOptions())
					}()
					continue
				}
				// 成功主链收到已结算的拒绝时，保存原始领域回执并及时退出。
				// 故障恢复用例仍沿自己的接管/取消路径继续。
				if faultMode == "" && m.Active != nil {
					for _, step := range m.Active.Steps {
						if step.Observation == nil || step.Observation.Result != journal.Rejected {
							continue
						}
						record := map[string]any{
							"request": m.Active.Request, "action": step.Action, "sequence": m.Sequence,
							"read_unix_ms": time.Now().UnixMilli(), "observation": step.Observation,
						}
						if err := trustedDriverSave(fmt.Sprintf("original-rejection-%d", index+1), record); err != nil {
							return err
						}
						return fmt.Errorf("original %s was rejected at journal sequence %d", step.Action, m.Sequence)
					}
				}
				if m.Active == nil {
					if len(m.History) != index+1+historyOffset || m.History[index+historyOffset].Outcome != journal.Succeeded || m.History[index+historyOffset].Operation.Request != request {
						return fmt.Errorf("production complete original history differs")
					}
					break
				}
				prepared := false
				for _, step := range m.Active.Steps {
					if step.Action == journal.PrepareBudget && step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
						prepared = true
					}
				}
				if prepared {
					if _, publish := publisher.Reconcile(controlCtx); publish != nil && !errors.Is(publish, resourceguard.ErrUnknown) {
						return trustedDriverRecordFailure("host_publisher", publish)
					}
				}
				if e = trustedDriverPause(controlCtx); e != nil {
					return e
				}
			}
			if controlCtx.Err() != nil {
				return controlCtx.Err()
			}
			settled, e := publisher.SettleCurrent(controlCtx)
			if e != nil || !settled {
				return fmt.Errorf("actual Host source settlement not confirmed: %v", e)
			}
			_, _, pub, e := vmguard.ReadPublished(controlCtx, f.Core.CoreV1(), f.Store.Binding(), nil)
			if e != nil {
				return e
			}
			if pub.Actual == nil || pub.Actual.ActualBytes != size || pub.Actual.RequestedBytes != size || pub.State.Pending != nil {
				return fmt.Errorf("actual Runner desired/actual differs after completed original history")
			}
			state, e := f.Store.Load(controlCtx)
			if e != nil {
				return e
			}
			if e = trustedDriverSave(fmt.Sprintf("operation-%d", index+1), map[string]any{"request": request, "state": state.State(), "actual_Runner_publication": pub, "Host_claim_settled": settled, "Host_API_is_model": true, "E": 0}); e != nil {
				return e
			}
			stage := []string{"grow-8", "shrink-7", "regrow-8"}[index]
			if e = trustedDriverStage(controlCtx, stage); e != nil {
				return e
			}
			if faultMode == trustedPG17ControllerTakeoverMode && !continuation && index == 0 {
				if _, e = trustedPG17LeaseObservation(controlCtx, pg, request.Key, "before-controller-yield"); e != nil {
					return e
				}
				handoff.PG, handoff.Root, handoff.Ready = pc, rc, true
				return errTrustedPG17YieldController
			}
		}
		final, e := f.Store.Load(controlCtx)
		if e != nil {
			return e
		}
		if len(final.State().History) != 3+historyOffset || final.State().Active != nil {
			return fmt.Errorf("three original operations not durably completed")
		}
		if faultMode == "normal-reply-loss" && !faultTransport.recovered.Load() {
			return errors.New("DEV normal reply loss case was not executed")
		}
		if e = trustedDriverSave("complete", map[string]any{"Host_API_is_model": true, "production_Session": true, "production_executors": true, "actual_VM_sequence_GiB": []int{7, 8, 7, 8}, "state": final.State(), "E": 0}); e != nil {
			return e
		}
		completed.Store(true)
		select {
		case <-acknowledged:
			return nil
		case <-controlCtx.Done():
			return controlCtx.Err()
		}
	})
}

// 仅在原失败后分类封闭源码常量；不输出错误正文，也不产生执行权限。
type trustedDriverPrepareReasonEntry struct {
	code  uint16
	exact bool
	text  string
}

var trustedDriverPrepareReasons = [...]trustedDriverPrepareReasonEntry{
	{code: 1, exact: false, text: "prepare producer requires trusted original domains and qualification"},
	{code: 2, exact: false, text: "prepare producer domains differ"},
	{code: 3, exact: false, text: "prepare producer lacks original dispatchable VM/PG/Root"},
	{code: 4, exact: false, text: "platform qualification differs from current original Guest/Runner"},
	{code: 5, exact: false, text: "prepare requires original settled Runner actual capacity"},
	{code: 6, exact: false, text: "VM status and native Runner actual capacity differ"},
	{code: 7, exact: false, text: "requested VM size violates original native geometry"},
	{code: 8, exact: false, text: "prepare native cost and current issuer fence differ"},
	{code: 9, exact: false, text: "qualified original root/bootstrap/Postmaster birth changed"},
	{code: 10, exact: false, text: "qualified target changes fixed limits or requires unsupported normal-cap decrease"},
	{code: 11, exact: false, text: "qualified target omits or replaces an original HugeTLB hstate"},
	{code: 12, exact: false, text: "prepare producer requires original active VM snapshot"},
	{code: 13, exact: false, text: "prepare producer blocked by retained service closure"},
	{code: 14, exact: false, text: "prepare producer blocked by retained service closure"},
	{code: 15, exact: false, text: "prepare producer original VM UID/RV or operation changed"},
	{code: 16, exact: false, text: "prepare producer owner changed"},
	{code: 17, exact: false, text: "prepare producer blocked by retained service closure"},
	{code: 18, exact: false, text: "original prepare is not eligible for dispatch/replay"},
	{code: 19, exact: false, text: "original prepare source or current facts changed"},
	{code: 20, exact: false, text: "prepare sources changed before recording"},
	{code: 21, exact: false, text: "original outside creation returned another object"},
	{code: 22, exact: false, text: "original outside complete body differs from saved prepare"},
	{code: 23, exact: false, text: "committed abort changed original complete budget or admission binding"},
	{code: 24, exact: false, text: "committed abort lacks original authenticated final budget/admission outcome"},
	{code: 25, exact: false, text: "committed abort current PG cost exceeds original complete budget"},
	{code: 26, exact: false, text: "committed abort returned native allocation overflows"},
	{code: 27, exact: false, text: "committed abort returned cost differs from original full budget"},
	{code: 28, exact: false, text: "finalization requires original aggregate and persistent sources"},
	{code: 29, exact: false, text: "invalid finalization predecessor digest"},
	{code: 30, exact: false, text: "finalization VM source differs from original predecessor"},
	{code: 31, exact: false, text: "finalized budget differs from original operation or Runner/VM"},
	{code: 32, exact: false, text: "invalid finalized budget source binding"},
	{code: 33, exact: false, text: "independent settlement epoch differs from original issuer or current owner"},
	{code: 34, exact: false, text: "PG budget generation/floor or retained allocation differs"},
	{code: 35, exact: false, text: "finalized budget lacks complete original PG commit"},
	{code: 36, exact: false, text: "invalid finalized reservation sources"},
	{code: 37, exact: false, text: "required system/safety budget is absent"},
	{code: 38, exact: false, text: "aggregate budget duplicates categories or cannot contain PG"},
	{code: 39, exact: false, text: "categorized private/DSM/LFC/HugeTLB budget is incomplete"},
	{code: 40, exact: false, text: "budget bound is not distinct or authoritative"},
	{code: 41, exact: false, text: "complete budget overflows"},
	{code: 42, exact: false, text: "complete retained budget exceeds usable Guest memory"},
	{code: 43, exact: false, text: "invalid original barrier owner"},
	{code: 44, exact: false, text: "host reservation barrier lacks original request, owner or durable closure"},
	{code: 45, exact: false, text: "host reservation barrier lost exact original source identity"},
	{code: 46, exact: false, text: "host reservation barrier changed complete original capacity plan"},
	{code: 47, exact: false, text: "host reservation barrier lost original held source or claim UID"},
	{code: 48, exact: false, text: "host reservation barrier differs from original completed Prepare"},
	{code: 49, exact: false, text: "invalid original held Host source"},
	{code: 50, exact: false, text: "initial budget requires original initial PG capacity and complete root bootstrap"},
	{code: 51, exact: false, text: "initial VM/Guest/Host capacity or original Runner context is incomplete"},
	{code: 52, exact: false, text: "invalid original initial placement identity"},
	{code: 53, exact: false, text: "initial native allocation is unknown"},
	{code: 54, exact: false, text: "initial aggregate cannot contain retained native/system commitments"},
	{code: 55, exact: false, text: "initial outside commitments overlap or overflow"},
	{code: 56, exact: false, text: "initial complete commitments exceed guaranteed usable Guest memory"},
	{code: 57, exact: false, text: "initial complete budget differs from exact original platform qualification"},
	{code: 58, exact: false, text: "current initial budget source, floor or actual capacity changed"},
	{code: 59, exact: false, text: "current native allocation exceeds or replaces original initial capacity"},
	{code: 60, exact: false, text: "current initial native allocation is unknown"},
	{code: 61, exact: false, text: "first admission command differs from complete original initial budget"},
	{code: 62, exact: false, text: "first admission receipt lacks exact original native completion and current fence"},
	{code: 63, exact: false, text: "incomplete original initial VM capacity source"},
	{code: 64, exact: false, text: "initial VM source changed original complete boot budget"},
	{code: 65, exact: false, text: "initial VM recovery requires exclusive original boot source"},
	{code: 66, exact: false, text: "reconciled native VM source differs from actual capacity"},
	{code: 67, exact: false, text: "grow quote requires a stable native prefix"},
	{code: 68, exact: false, text: "grow quote is unknown or overflows"},
	{code: 69, exact: false, text: "prepared budget contains invalid native/resource context"},
	{code: 70, exact: false, text: "invalid prepared outside reservation reference"},
	{code: 71, exact: false, text: "prepared normal quota original fence missing or changed"},
	{code: 72, exact: false, text: "prepared budget differs from original operation/Guest/Runner"},
	{code: 73, exact: false, text: "prepared grow quote or VM direction differs"},
	{code: 74, exact: false, text: "prepared shrink direction differs"},
	{code: 75, exact: false, text: "prepared native allocation is unknown"},
	{code: 76, exact: false, text: "prepared resource cap cannot contain shared/system/safety commitment"},
	{code: 77, exact: false, text: "prepared outside commitments overlap or overflow"},
	{code: 78, exact: false, text: "prepared complete aggregate exceeds target usable Guest budget"},
	{code: 79, exact: false, text: "invalid prepared/current PG cost"},
	{code: 80, exact: false, text: "current PG cost/floor exceeds original commitment"},
	{code: 81, exact: false, text: "current PG allocation remains unknown"},
	{code: 82, exact: false, text: "current grow baseline or future allocation exceeds commitment"},
	{code: 83, exact: false, text: "current shrink lacks complete original native commit"},
	{code: 84, exact: false, text: "current shrink provider is not verified"},
	{code: 85, exact: false, text: "current shrink physical release is not verified"},
	{code: 86, exact: false, text: "invalid original platform qualification reference"},
	{code: 87, exact: false, text: "incomplete original platform qualification"},
	{code: 88, exact: false, text: "invalid qualified platform identity"},
	{code: 89, exact: false, text: "invalid or unordered exact qualified VM target"},
	{code: 90, exact: false, text: "qualified outside commitments overflow"},
	{code: 91, exact: false, text: "qualified complete budget exceeds guaranteed usable memory"},
	{code: 92, exact: false, text: "exact VM size has no retained platform qualification"},
	{code: 93, exact: false, text: "outside source version or original qualification differs"},
	{code: 94, exact: false, text: "outside reservation belongs to another original qualification"},
	{code: 95, exact: false, text: "outside upper bounds differ from exact qualified VM size"},
	{code: 96, exact: false, text: "invalid complete original quiesce command"},
	{code: 97, exact: false, text: "quiesce differs from original shrink/prepared/closing intent"},
	{code: 98, exact: false, text: "quiesce cannot change immutable task limit"},
	{code: 99, exact: false, text: "continuous aggregate quiesce cannot authorize memory.max decrease"},
	{code: 100, exact: false, text: "quiesce lacks original aggregate or native accounting"},
	{code: 101, exact: false, text: "original close is not continuously acknowledged and foreground idle"},
	{code: 102, exact: false, text: "quiesce native backing is unknown"},
	{code: 103, exact: false, text: "initial VM inputs cannot mix native commands or lose boot budget"},
	{code: 104, exact: false, text: "reconciled original native capacity is invalid"},
	{code: 105, exact: false, text: "reconciled outside lost original qualified operation"},
	{code: 106, exact: false, text: "reconciled initial inputs differ from original live boot budget"},
	{code: 107, exact: false, text: "reconciled inputs lost original native, Guest or reservation context"},
	{code: 108, exact: false, text: "reconciled unchanged PG generation changed original buffer prefix"},
	{code: 109, exact: false, text: "reconciled outside commitments overflow"},
	{code: 110, exact: false, text: "reconciled current root limits cannot contain original full commitments"},
	{code: 111, exact: false, text: "service actual outside cannot cover current system hard limit"},
	{code: 112, exact: false, text: "reconciled Host hold does not cover original peak and actual capacity"},
	{code: 113, exact: false, text: "invalid original native capacity source"},
	{code: 114, exact: false, text: "reconciled PG or VM capacity changed from original retained facts"},
	{code: 115, exact: false, text: "reconciled original terminal head changed"},
	{code: 116, exact: false, text: "reconciled PG allocation remains unknown"},
	{code: 117, exact: false, text: "ordinary reconciled capacity cannot contain service recovery PG sources"},
	{code: 118, exact: false, text: "incomplete original reconciled capacity budget"},
	{code: 119, exact: false, text: "reconciled outside source lost exact original operation"},
	{code: 120, exact: false, text: "reconciled original VM direction changed"},
	{code: 121, exact: false, text: "reconciled budget cannot borrow a future or aliased source"},
	{code: 122, exact: false, text: "reconciled budget changed same-key original request or Host source"},
	{code: 123, exact: false, text: "reconciled PG operation is not terminal in the original domain"},
	{code: 124, exact: false, text: "reconciled unchanged PG generation contradicts original operation"},
	{code: 125, exact: false, text: "reconciled PG generation is outside original operation"},
	{code: 126, exact: false, text: "reconciled capacity bounds overlap or overflow"},
	{code: 127, exact: false, text: "reconciled full budget exceeds actual usable Guest memory"},
	{code: 128, exact: false, text: "reconciled abort lacks complete original capacity budget"},
	{code: 129, exact: false, text: "reconciled abort opening differs from actual capacity budget"},
	{code: 130, exact: false, text: "reconciled abort has no current complete actual budget and open admission"},
	{code: 131, exact: false, text: "read-only reconciled abort cannot invent an admission mutation"},
	{code: 132, exact: false, text: "reconciled original opening remains unknown or superseded"},
	{code: 133, exact: false, text: "reconciled returned PG cost overflows"},
	{code: 134, exact: false, text: "reconciled initial outside differs from original complete boot budget"},
	{code: 135, exact: false, text: "ordinary initial actual capacity changed original PG or root budget"},
	{code: 136, exact: false, text: "reconciled initial source changed original root constraints"},
	{code: 137, exact: false, text: "service actual capacity requires one explicit original PG source"},
	{code: 138, exact: false, text: "independent PG recovery cannot borrow an ordinary terminal head"},
	{code: 139, exact: false, text: "service actual capacity changed retained independent PG boundary"},
	{code: 140, exact: false, text: "service unchanged PG source lost original undispatched capacity"},
	{code: 141, exact: false, text: "service unchanged PG source contains unknown allocation"},
	{code: 142, exact: false, text: "undispatched original PG head changed"},
	{code: 143, exact: false, text: "current PG changed independently retained capacity"},
	{code: 144, exact: false, text: "current PG lost retained native head"},
	{code: 145, exact: false, text: "current PG head is in flight or aliases retained intent"},
	{code: 146, exact: false, text: "current PG changed or resurrected original recovered terminal"},
	{code: 147, exact: false, text: "current PG regressed or replaced retained native head"},
	{code: 148, exact: false, text: "current PG successor did not advance original intent"},
	{code: 149, exact: false, text: "PG aggregate lacks original process birth or populated resource domain"},
	{code: 150, exact: false, text: "invalid original PG aggregate source"},
	{code: 151, exact: false, text: "PG aggregate held command differs from original identity/birth"},
	{code: 152, exact: false, text: "PG aggregate bound was changed or double counted"},
	{code: 153, exact: false, text: "PG aggregate original source was replaced"},
	{code: 154, exact: false, text: "invalid original Runner source references"},
	{code: 155, exact: false, text: "invalid original Runner command digest"},
	{code: 156, exact: false, text: "invalid immutable reservation reference"},
	{code: 157, exact: false, text: "outside reservation differs from original prepared commitment"},
	{code: 158, exact: false, text: "host reservation differs from original VM/Guest/Runner/target"},
	{code: 159, exact: false, text: "invalid host reservation identity"},
	{code: 160, exact: false, text: "VM settlement changed its original Runner sources"},
	{code: 161, exact: false, text: "final VM/Host/Runner settlement is incomplete or replaced"},
	{code: 162, exact: false, text: "invalid retained Runner sources"},
	{code: 163, exact: false, text: "invalid finalized source references"},
	{code: 164, exact: false, text: "invalid system charged-domain source"},
	{code: 165, exact: false, text: "system charged-domain bound changed or included residual outside scope"},
	{code: 166, exact: false, text: "system charged-domain original source replaced"},
	{code: 167, exact: false, text: "unchanged abort lost original budget or later cancellation context"},
	{code: 168, exact: false, text: "unchanged abort reopening changed original retained budget"},
	{code: 169, exact: false, text: "unchanged abort lacks current complete original budget/open admission"},
	{code: 170, exact: false, text: "read-only unchanged abort cannot invent admission mutation"},
	{code: 171, exact: false, text: "unchanged abort original reopening remains unconfirmed or superseded"},
	{code: 172, exact: false, text: "unchanged abort returned PG cost overflows"},
	{code: 173, exact: false, text: "active retained budget requires original running VM snapshot"},
	{code: 174, exact: false, text: "active retained source requires original live operation"},
	{code: 175, exact: false, text: "active retained source has ambiguous closed anchors"},
	{code: 176, exact: false, text: "dispatched active retained source lacks captured original anchor"},
	{code: 177, exact: false, text: "active retained source lost original budget closure anchor"},
	{code: 178, exact: false, text: "current budget reader requires original VM snapshot"},
	{code: 179, exact: false, text: "original prior Host boundary changed retained budget"},
	{code: 180, exact: false, text: "current complete budget unavailable"},
	{code: 181, exact: false, text: "current outside source differs from original retained budget"},
	{code: 182, exact: false, text: "current Host source differs from original retained budget"},
	{code: 183, exact: false, text: "current original Runner Pod unavailable"},
	{code: 184, exact: false, text: "current original Runner Pod changed"},
	{code: 185, exact: false, text: "current original Host Node changed"},
	{code: 186, exact: false, text: "current complete Host ledger unavailable"},
	{code: 187, exact: false, text: "current original Host plan is not retained"},
	{code: 188, exact: false, text: "original Host peak declaration changed"},
	{code: 189, exact: false, text: "original current budget observation missing"},
	{code: 190, exact: false, text: "original current budget source changed across verification"},
	{code: 191, exact: false, text: "initial sources require original live bound Guest/PG/VM"},
	{code: 192, exact: false, text: "initial qualification belongs to another original Guest/Runner"},
	{code: 193, exact: false, text: "initial original Runner Pod unavailable"},
	{code: 194, exact: false, text: "initial original live Runner Pod changed"},
	{code: 195, exact: false, text: "initial original Host Node changed"},
	{code: 196, exact: false, text: "initial complete Host profile unavailable"},
	{code: 197, exact: false, text: "initial placement changed qualified Host overhead"},
	{code: 198, exact: false, text: "initial budget differs from original durable VM/PG"},
	{code: 199, exact: false, text: "original initial qualification or Host profile changed"},
	{code: 200, exact: false, text: "initial original source observation missing"},
	{code: 201, exact: false, text: "initial original source changed across domain verification"},
	{code: 202, exact: false, text: "prepared barrier reader requires original coordination client"},
	{code: 203, exact: false, text: "prepared barrier requires original running VM and fixed coordination reader"},
	{code: 204, exact: false, text: "prepared barrier original Runner Pod unavailable"},
	{code: 205, exact: false, text: "prepared barrier changed original live Runner/VM binding"},
	{code: 206, exact: false, text: "prepared barrier original Node unavailable"},
	{code: 207, exact: false, text: "prepared barrier original Node ledger binding unavailable"},
	{code: 208, exact: false, text: "prepared barrier changed original Node or VM closure sources"},
	{code: 209, exact: false, text: "original platform qualification content or VM identity changed"},
	{code: 210, exact: false, text: "qualified original Runner Pod unavailable"},
	{code: 211, exact: false, text: "qualified original live Runner Pod changed"},
	{code: 212, exact: false, text: "qualified original Host Node changed"},
	{code: 213, exact: false, text: "original outside reservation is malformed"},
	{code: 214, exact: false, text: "reservation reader requires fixed trusted Kubernetes client/VM"},
	{code: 215, exact: false, text: "invalid original Host claim binding"},
	{code: 216, exact: false, text: "original Host reservation unavailable"},
	{code: 217, exact: false, text: "invalid original reservation reference"},
	{code: 218, exact: false, text: "original immutable reservation is absent, changed or releasing"},
	{code: 219, exact: false, text: "reservation belongs to another original VM"},
	{code: 220, exact: false, text: "original outside reservation unavailable"},
	{code: 221, exact: false, text: "outside source differs from original prepared budget"},
	{code: 222, exact: false, text: "original outside reservation unavailable"},
	{code: 223, exact: false, text: "outside object differs from original prepared budget"},
	{code: 224, exact: false, text: "host object differs from original operation"},
	{code: 225, exact: false, text: "Runner pod observation missing"},
	{code: 226, exact: false, text: "original live Runner pod changed"},
	{code: 227, exact: false, text: "original Host node changed"},
	{code: 228, exact: false, text: "original Host source has no matching published Node capacity hold"},
	{code: 229, exact: false, text: "original Host source has no matching published Node capacity hold: "},
	{code: 230, exact: false, text: "reservation observation missing"},
	{code: 231, exact: false, text: "original reservation changed during reconciliation"},
	{code: 232, exact: false, text: "actual budget declarations require original active cancellation"},
	{code: 233, exact: false, text: "actual budget outside declaration changed original domain"},
	{code: 234, exact: false, text: "actual budget Host declaration lost original peak"},
	{code: 235, exact: false, text: "reconciled sources require original live aborted VM and actual budget"},
	{code: 236, exact: false, text: "reconciled active source reader requires original cancellation"},
	{code: 237, exact: false, text: "reconciled current source reader requires original closed permanent budget"},
	{code: 238, exact: false, text: "reconciled Host source changed original peak hold"},
	{code: 239, exact: false, text: "reconciled initial and current Host domains differ"},
	{code: 240, exact: false, text: "reconciled original Runner Pod unavailable"},
	{code: 241, exact: false, text: "reconciled original Runner Pod changed"},
	{code: 242, exact: false, text: "reconciled original Host Node changed"},
	{code: 243, exact: false, text: "reconciled original Host peak no longer covers actual VM"},
	{code: 244, exact: false, text: "original reconciled sources missing"},
	{code: 245, exact: false, text: "reconciled original source changed during verification"},
	{code: 246, exact: false, text: "initial recovery requires original retained boot budget and active cancellation"},
	{code: 247, exact: false, text: "initial recovery changed original retained complete budget"},
	{code: 248, exact: false, text: "initial recovery lost original live Host peak"},
	{code: 249, exact: false, text: "reconciled initial source changed original Guest or incarnation"},
	{code: 250, exact: false, text: "reconciled outside source does not cover original actual VM"},
	{code: 251, exact: false, text: "service actual sources require original live VM"},
	{code: 252, exact: false, text: "service actual sources require original confirmed resource closure"},
	{code: 253, exact: false, text: "service actual outside is not an exact original prepared or retained VM source"},
	{code: 254, exact: false, text: "service actual declarations lost original complete Prepared or exclusive outside source"},
	{code: 255, exact: false, text: "service actual initial outside replaced original retained qualification"},
	{code: 256, exact: false, text: "service actual outside lost exact original VM specification"},
	{code: 257, exact: false, text: "service actual usable memory differs from exact original platform guarantee"},
	{code: 258, exact: false, text: "service actual Host lost original peak or placement"},
	{code: 259, exact: false, text: "service actual reader lost exact original capacity and closed operation"},
	{code: 260, exact: false, text: "service actual original declarations changed retained budget"},
	{code: 261, exact: false, text: "service actual source observation missing"},
	{code: 262, exact: false, text: "service actual original source changed during verification"},
}

func trustedDriverPrepareFailureCodes(original error) ([]uint16, bool) {
	if original == nil {
		return nil, false
	}
	var queue [16]error
	var matched [512]bool
	queue[0] = original
	count, limited := 1, false
	add := func(next error) {
		if next == nil {
			return
		}
		if count == len(queue) {
			limited = true
			return
		}
		queue[count] = next
		count++
	}
	for i := 0; i < count; i++ {
		current := queue[i]
		text := current.Error()
		complete := len(text) <= 4096
		if !complete {
			text = text[:4096]
		}
		for j, entry := range trustedDriverPrepareReasons {
			if entry.exact && complete && text == entry.text || !entry.exact && strings.HasPrefix(text, entry.text) {
				matched[j] = true
			}
		}
		switch nested := current.(type) {
		case interface{ Unwrap() error }:
			add(nested.Unwrap())
		case interface{ Unwrap() []error }:
			children := nested.Unwrap()
			for j, child := range children {
				if j == len(queue) {
					limited = true
					break
				}
				add(child)
			}
		}
	}
	var codes []uint16
	for i, entry := range trustedDriverPrepareReasons {
		if matched[i] {
			codes = append(codes, entry.code)
		}
	}
	return codes, limited
}

func TestTrustedPG17PrepareFailureReasonsAreBoundedScalars(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		codes, limited := trustedDriverPrepareFailureCodes(nil)
		require.Empty(t, codes)
		require.False(t, limited)
	})
	t.Run("unmatched", func(t *testing.T) {
		codes, limited := trustedDriverPrepareFailureCodes(errors.New("DEV diagnostic unmatched opaque input"))
		require.Empty(t, codes)
		require.False(t, limited)
	})
	t.Run("wrapped_join", func(t *testing.T) {
		first := trustedDriverPrepareReasons[0]
		text := first.text
		if !first.exact {
			text += "PRIVATE_RUNTIME_PARAMETER"
		}
		original := errors.Join(errors.New("DEV diagnostic unmatched opaque input"), fmt.Errorf("DEV opaque wrapper: %w", errors.New(text)))
		codes, limited := trustedDriverPrepareFailureCodes(original)
		require.Contains(t, codes, first.code)
		require.False(t, limited)
		raw, e := json.Marshal(map[string]any{"codes": codes, "limited": limited})
		require.NoError(t, e)
		require.NotContains(t, string(raw), "PRIVATE_RUNTIME_PARAMETER")
		require.NotContains(t, string(raw), first.text)
	})
	t.Run("chain_limit", func(t *testing.T) {
		var original error = errors.New("DEV diagnostic unmatched opaque input")
		for i := 0; i < 20; i++ {
			original = fmt.Errorf("DEV opaque wrapper: %w", original)
		}
		codes, limited := trustedDriverPrepareFailureCodes(original)
		require.Empty(t, codes)
		require.True(t, limited)
	})
}
