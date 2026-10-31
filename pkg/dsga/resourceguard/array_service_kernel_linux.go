//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// 仅新显式原创建profile可改变旧flat reader。固定ISO/封存源/完成拓扑及
// 原inode都只读核对；无marker收养、无请求路径，也不重入外层resource flock。
func arraySplitKernelLayout(config Config) (NormalSplitTopologySnapshot, error) {
	var zero NormalSplitTopologySnapshot
	source, raw, e := runtimeSource()
	if e != nil {
		return zero, e
	}
	version, e := deriveArrayAuthorityRuntimeProtocol(raw, source)
	if e != nil || version != 1 || source.Config != config {
		return zero, fmt.Errorf("explicit original array split source missing: %v", e)
	}
	if e = matchSealedFile(FixedSealedRuntimePath, raw); e != nil {
		return zero, e
	}
	native, pg, e := readNativeBirthOriginalSource()
	if e != nil {
		return zero, e
	}
	pg.Close()
	if !reflect.DeepEqual(native.resource, source) {
		return zero, fmt.Errorf("array split native/resource original source differs")
	}
	assets, e := RenderRuntimePolicyAssets(raw)
	if e != nil {
		return zero, e
	}
	policyRaw := assets["dsga-normal-split.json"]
	var policy NormalSplitTopologyPolicy
	if json.Unmarshal(policyRaw, &policy) != nil {
		return zero, fmt.Errorf("array split fixed policy invalid")
	}
	f, e := rootFile(FixedNormalSplitTopologyPolicyPath, false)
	if e != nil {
		return zero, e
	}
	var fs unix.Statfs_t
	if unix.Fstatfs(int(f.Fd()), &fs) != nil || fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		f.Close()
		return zero, fmt.Errorf("array split policy is not original read-only ISO")
	}
	actual, e := io.ReadAll(io.LimitReader(f, 64*1024+1))
	f.Close()
	if e != nil || string(actual) != string(policyRaw) {
		return zero, fmt.Errorf("array split actual policy differs from original renderer")
	}
	cmdline, e := kernelText("/proc/cmdline")
	if e != nil || normalSplitBootMarker(cmdline) != nil {
		return zero, fmt.Errorf("array split original boot selection missing")
	}
	read := func(name string) ([]byte, error) {
		f, e := rootFile(filepath.Join(config.StateDir, name), false)
		if e != nil {
			return nil, e
		}
		defer f.Close()
		raw, e := io.ReadAll(io.LimitReader(f, 64*1024+1))
		if e != nil || len(raw) > 64*1024 {
			return nil, fmt.Errorf("array split original history too large/unreadable")
		}
		return raw, nil
	}
	intentRaw, e := read(normalSplitIntentName)
	if e != nil {
		return zero, e
	}
	completeRaw, e := read(normalSplitCompleteName)
	if e != nil {
		return zero, e
	}
	var intent normalSplitIntent
	var complete normalSplitCompletion
	if control.UnmarshalObject(intentRaw, &intent) != nil || control.UnmarshalObject(completeRaw, &complete) != nil || intent.ProtocolVersion != 1 || intent.Scope != normalSplitScope || intent.Policy != policy || intent.PolicySHA256 != physicalDigest(policyRaw) || complete.ProtocolVersion != 1 || complete.Scope != normalSplitScope || complete.IntentSHA256 != physicalDigest(intentRaw) || splitBasic(intent.Original) != nil || splitEmpty(intent.Original) != nil || intent.Original.Private != nil || intent.Original.Arrays != nil || splitStable(intent.Original, complete.Observed) != nil || splitEmpty(complete.Observed) != nil || splitPlaced(policy, complete.Observed) != nil {
		return zero, fmt.Errorf("array split complete original prefix invalid")
	}
	for _, v := range []struct {
		value any
		raw   []byte
	}{{intent, intentRaw}, {complete, completeRaw}} {
		encoded, e := json.Marshal(v.value)
		if e != nil || string(encoded) != string(v.raw) {
			return zero, fmt.Errorf("array split original history noncanonical")
		}
	}
	// 不用complete可见性发durability/normal许可；这里只拒绝错误布局及身份。
	current, e := (&linuxKernel{config: config}).normalSplitRead()
	if e != nil {
		return zero, e
	}
	if splitStable(intent.Original, current) != nil || splitPlaced(policy, current) != nil || !splitSameNode(*complete.Observed.Private, *current.Private) || !splitSameNode(*complete.Observed.Arrays, *current.Arrays) || complete.Observed.Arrays.MemoryMaxBytes != policy.InitialArraysMemoryMaxBytes {
		return zero, fmt.Errorf("array split current topology/caps differ")
	}
	for _, name := range []string{"private", "arrays"} {
		raw, e := read(splitIdentityName(name))
		if e != nil {
			return zero, e
		}
		var id normalSplitLeafIdentity
		n := current.Private
		if name == "arrays" {
			n = current.Arrays
		}
		if control.UnmarshalObject(raw, &id) != nil || id.ProtocolVersion != 1 || id.Name != name || id.IntentSHA256 != physicalDigest(intentRaw) || id.Device != n.Device || id.Inode != n.Inode {
			return zero, fmt.Errorf("array split original leaf inode changed")
		}
	}
	if current.Identity.VMUID != config.VMUID || current.Identity.ComputeInstanceID != config.ComputeInstanceID {
		return zero, fmt.Errorf("array split current VM differs")
	}
	return current, nil
}
func readArraySplitAddressSpaces(l Limits) (*AddressSpaceEvidence, error) {
	source, _, e := runtimeSource()
	if e != nil {
		return nil, e
	}
	layout, e := arraySplitKernelLayout(source.Config)
	if e != nil {
		return nil, e
	}
	// helper仅在同原flock内暂运行，通用资源观察不收养未知array task。
	// A的已退出收费可以继续存在；不把数组FD/pin计入PG私有VA预算。
	if !layout.Arrays.Empty {
		return nil, fmt.Errorf("array producer still active during PG address inspection")
	}
	uid, e := postgresUID()
	if e != nil {
		return nil, e
	}
	out, e := collectAddressSpaces(l, func() (string, error) { return kernelText(pgCgroup + "/private/cgroup.procs") }, func(pid int) (processAddressSample, error) {
		return inspectProcessAddressSpaceGroup(pid, l.AddressSpaceMaxBytes, uid, "0::/neon-postgres/private\n")
	})
	if e != nil {
		return nil, e
	}
	after, e := arraySplitKernelLayout(source.Config)
	if e != nil || !splitSameNode(*layout.Private, *after.Private) || !splitSameNode(*layout.Arrays, *after.Arrays) || !after.Arrays.Empty {
		return nil, fmt.Errorf("array split changed during address inspection")
	}
	return out, nil
}
func arrayKernelPostmasterGroup(k *linuxKernel, actual string) error {
	if actual == "0::/neon-postgres" {
		return nil
	} // 原旧域路径保持；新split若有父task由下读者拒绝。
	if actual != "0::/neon-postgres/private" {
		return fmt.Errorf("Postmaster is not in the guarded domain")
	}
	if _, e := arraySplitKernelLayout(k.config); e != nil {
		return e
	}
	return nil
}

// 明确只接受固定两种内部组名，不能从用户参数扩展可读进程范围。
func arrayAllowedAddressGroup(group string) bool {
	return group == "0::/neon-postgres\n" || group == "0::/neon-postgres/private\n"
}
