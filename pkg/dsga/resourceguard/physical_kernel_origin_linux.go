//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"golang.org/x/sys/unix"
)

const FixedPhysicalKernelPolicyPath = "/neonvm/runtime/dsga-physical-kernel.json"

// 三入口均不接收请求 policy/path。当前无 provision/HTTP/CLI/预算消费者，缺省关闭；
// 未来原顺序必须是建空组→首次组件捕获→Bootstrap→完整出生/约束封存，不能事后补签。
// 返回对象仍 vm_shrink_permit=false；它不证明 Guest 全部历史或未来特权写者受约束。
func (g *Guard) CapturePhysicalKernelOrigin(ctx context.Context) (PhysicalKernelOrigin, error) {
	return g.physicalOriginFromRuntime(ctx, physicalOriginCapture)
}
func (g *Guard) ObservePhysicalKernelOrigin(ctx context.Context) (PhysicalKernelOrigin, error) {
	return g.physicalOriginFromRuntime(ctx, physicalOriginObserve)
}
func (g *Guard) ReplayPhysicalKernelOrigin(ctx context.Context) (PhysicalKernelOrigin, error) {
	return g.physicalOriginFromRuntime(ctx, physicalOriginReplay)
}

func (g *Guard) physicalOriginFromRuntime(ctx context.Context, op physicalOriginOperation) (PhysicalKernelOrigin, error) {
	in, err := g.physicalOriginRuntimeInputs()
	if err != nil {
		return PhysicalKernelOrigin{}, err
	}
	return g.physicalOrigin(ctx, in, op)
}
func (g *Guard) physicalOriginRuntimeInputs() (physicalOriginInputs, error) {
	var in physicalOriginInputs
	// runtimeSource 已校验根所有权、O_NOFOLLOW、ISO9660/只读挂载、完整原文档及 Source SHA。
	source, raw, err := runtimeSource()
	if err != nil {
		return in, err
	}
	if source.Config != g.config {
		return in, fmt.Errorf("physical policy resource configuration differs")
	}
	var d RuntimeDocument
	if err = json.Unmarshal(raw, &d); err != nil {
		return in, err
	}
	f, err := rootFile(FixedPhysicalKernelPolicyPath, false)
	if err != nil {
		return in, err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err = unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return in, err
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return in, fmt.Errorf("physical policy must be root-controlled read-only runtime ISO")
	}
	policyRaw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return in, err
	}
	if len(policyRaw) > 64*1024 {
		return in, fmt.Errorf("physical policy exceeds bound")
	}
	var policy PhysicalKernelOriginPolicy
	if err = json.Unmarshal(policyRaw, &policy); err != nil {
		return in, err
	}
	id, err := g.InspectIdentity()
	if err != nil {
		return in, err
	}
	cmdline, err := kernelText("/proc/cmdline")
	if err != nil {
		return in, err
	}
	marker := false
	for _, token := range strings.Fields(cmdline) {
		if strings.HasPrefix(token, "neonvm.dsga.physical_profile=") {
			if token != PhysicalKernelOriginArg || marker {
				return in, fmt.Errorf("ambiguous physical origin boot marker")
			}
			marker = true
		}
	}
	in = physicalOriginInputs{policy: policy, policySHA: physicalDigest(policyRaw), sourceSHA: d.SourceSHA256, runtimeSHA: physicalDigest(raw), incarnation: d.Incarnation, secretUID: d.ConfigSecretUID, config: source.Config, bootstrap: BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: source.IssuerEpoch, BudgetCommitmentID: source.BudgetCommitmentID, Limits: source.Limits}, bootMarker: marker}
	return in, nil
}
func (k *linuxKernel) physicalOriginRead() (PhysicalKernelObservation, error) {
	id, err := k.Identity()
	if err != nil {
		return PhysicalKernelObservation{}, err
	}
	return readPhysicalKernelComponent(id)
}
func (k *linuxKernel) physicalOriginPGEmpty() error {
	// 同资源锁内重复核真实 procs/threads。它只证明此受管组当前空，不冒充全 Guest 历史。
	for _, name := range []string{"cgroup.procs", "cgroup.threads"} {
		s, err := kernelText(pgCgroup + "/" + name)
		if err != nil {
			return err
		}
		if s != "" {
			return fmt.Errorf("physical first capture requires empty managed PG group")
		}
	}
	return nil
}
