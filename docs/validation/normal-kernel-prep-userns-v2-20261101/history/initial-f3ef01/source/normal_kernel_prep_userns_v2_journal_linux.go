//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"os"
)

// 新family还未接原initial8图；不自动改旧selector或生产入口。
func (g *Guard) normalKernelPreparationV2Locked(ctx context.Context, in kernelPrepRawInputs, op kernelPrepOperation, store *lockedState, backend normalKernelPrepV2Backend) (result NormalV2KernelBootstrapEvidence, err error) {
	var zero NormalV2KernelBootstrapEvidence
	if g == nil || ctx == nil || ctx.Err() != nil || store == nil || store.dir == nil || store.lock == nil {
		return zero, ErrUnknown
	}
	d, err := decodeNormalKernelPrepV2(in, g.config)
	if err != nil {
		return zero, err
	}
	if op > kernelPrepReplay {
		return zero, fmt.Errorf("unknown kernel preparation operation")
	}
	if backend == nil {
		return zero, fmt.Errorf("kernel preparation backend unavailable")
	}
	raw, exists, err := store.readPhysicalFile(normalKernelPrepV2IntentName)
	if err != nil {
		return zero, err
	}
	completeRaw, complete, err := store.readPhysicalFile(normalKernelPrepV2CompleteName)
	if err != nil {
		return zero, err
	}
	if complete && !exists {
		return zero, fmt.Errorf("%w: completion without original intent", ErrUnknown)
	}
	pending := exists && !complete
	defer func() {
		if err != nil && (pending || exists || complete) && !errors.Is(err, ErrUnknown) {
			err = fmt.Errorf("%w: pending kernel preparation: %v", ErrUnknown, err)
		}
	}()
	current, err := backend.kernelPrepV2Read()
	if err != nil {
		return zero, err
	}
	var locked unix.Stat_t
	if err = unix.Fstat(int(store.dir.Fd()), &locked); err != nil {
		return zero, err
	}
	if current.Kernel.StateDevice != control.Uint64(locked.Dev) || current.Kernel.StateInode != control.Uint64(locked.Ino) {
		return zero, fmt.Errorf("kernel preparation state path differs from locked inode")
	}
	if err = current.validate(d); err != nil {
		return zero, err
	}
	boundary := func() error {
		record, err := store.load()
		if err != nil {
			return err
		}
		if record != nil {
			return fmt.Errorf("kernel preparation cannot acquire historical resource journal")
		}
		fd, err := unix.Openat(int(store.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		directory := os.NewFile(uintptr(fd), "original-kernel-state")
		entries, err := directory.ReadDir(-1)
		directory.Close()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "lock" && entry.Name() != normalKernelPrepV2IntentName && entry.Name() != normalKernelPrepV2CompleteName {
				return fmt.Errorf("kernel preparation found other original-stage state")
			}
		}
		if !current.zeroMovable() {
			return fmt.Errorf("kernel preparation requires original zero MOVABLE phase")
		}
		if err := backend.kernelPrepCensus(d.policy.Controls.PIDMax); err != nil {
			return err
		}
		return backend.kernelPrepPGEmpty()
	}
	var intent normalKernelPrepV2Intent
	if !exists {
		if op != kernelPrepPrepare {
			return zero, fmt.Errorf("%w: original kernel preparation missing", ErrUnknown)
		}
		if err = boundary(); err != nil {
			return zero, err
		}
		// 已禁模块却无原intent，无法排除旧模块生命周期或先前未绑定准备，拒绝补签。
		if current.Kernel.ModulesDisabled != 0 || current.MaxUserNamespaces == 0 {
			return zero, fmt.Errorf("modules already disabled without original intent")
		}
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		var nonce [16]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			return zero, err
		}
		if hex.EncodeToString(nonce[:]) == "00000000000000000000000000000000" {
			return zero, fmt.Errorf("reserved zero original nonce")
		}
		intent = normalKernelPrepV2Intent{ProtocolVersion: 2, Nonce: hex.EncodeToString(nonce[:]), Inputs: in, Before: current}
		raw, err = json.Marshal(intent)
		if err != nil {
			return zero, err
		}
		// O_EXCL+file/dir fsync在第一项sysctl写前。失败残留文件不覆盖、不新建nonce。
		pending = true
		err = normalKernelPrepV2PersistOriginal(store, current, normalKernelPrepV2IntentName, raw)
		normalKernelPrepV2RetainOriginalBackend(current, backend)
		if err != nil {
			return zero, fmt.Errorf("%w: save kernel intent: %v", ErrUnknown, err)
		}
	} else {
		if err = control.UnmarshalObject(raw, &intent); err != nil {
			return zero, fmt.Errorf("%w: invalid original kernel intent: %v", ErrUnknown, err)
		}
		canonical, marshalErr := json.Marshal(intent)
		if marshalErr != nil || !bytes.Equal(canonical, raw) {
			return zero, fmt.Errorf("%w: noncanonical original kernel intent", ErrUnknown)
		}
		if intent.ProtocolVersion != 2 || intent.Nonce == "00000000000000000000000000000000" || control.ValidateIncarnation(intent.Nonce) != nil || !bytes.Equal(intent.Inputs.RuntimeRaw, in.RuntimeRaw) || !bytes.Equal(intent.Inputs.PhysicalPolicyRaw, in.PhysicalPolicyRaw) || !bytes.Equal(intent.Inputs.PolicyRaw, in.PolicyRaw) {
			return zero, fmt.Errorf("kernel preparation original raw source differs")
		}
		if err = intent.Before.validate(d); err != nil {
			return zero, err
		}
		if intent.Before.Kernel.ModulesDisabled != 0 || intent.Before.MaxUserNamespaces == 0 || !intent.Before.zeroMovable() {
			return zero, fmt.Errorf("original kernel intent birth phase differs")
		}
		if err = normalKernelPrepV2VerifyPrivateHistory(store, intent.Before, normalKernelPrepV2IntentName); err != nil {
			return zero, err
		}
		if err = intent.Before.fixedMatch(current); err != nil {
			return zero, err
		}
		if !complete && op != kernelPrepReplay {
			return zero, fmt.Errorf("%w: explicit original kernel replay required", ErrUnknown)
		}
	}
	if complete {
		var e NormalV2KernelBootstrapEvidence
		if err = control.UnmarshalObject(completeRaw, &e); err != nil {
			return zero, fmt.Errorf("%w: invalid kernel completion: %v", ErrUnknown, err)
		}
		if _, err = e.ReferenceSHA256(); err != nil {
			return zero, err
		}
		if e.Nonce != intent.Nonce || e.IntentSHA256 != physicalDigest(raw) || e.PolicySHA256 != physicalDigest(in.PolicyRaw) || e.RuntimeDocumentSHA256 != physicalDigest(in.RuntimeRaw) || e.PhysicalOriginPolicySHA256 != physicalDigest(in.PhysicalPolicyRaw) || !e.After.target(d.policy) {
			return zero, fmt.Errorf("original kernel completion differs")
		}
		if err = e.After.validate(d); err != nil {
			return zero, err
		}
		if err = e.After.fixedMatch(current); err != nil {
			return zero, err
		}
		if !current.target(d.policy) {
			return zero, fmt.Errorf("kernel bootstrap controls drifted; no automatic repair")
		}
		canonical, marshalErr := json.Marshal(e)
		if marshalErr != nil || !bytes.Equal(canonical, completeRaw) {
			return zero, fmt.Errorf("%w: noncanonical kernel completion", ErrUnknown)
		}
		if err = normalKernelPrepV2VerifyPrivateHistory(store, intent.Before, normalKernelPrepV2CompleteName); err != nil {
			return zero, err
		}
		if op == kernelPrepReplay {
			if err = store.syncPhysicalFile(normalKernelPrepV2IntentName); err != nil {
				return zero, err
			}
			if err = store.syncPhysicalFile(normalKernelPrepV2CompleteName); err != nil {
				return zero, err
			}
			e.durabilityConfirmed = true
			e.source, err = normalKernelPrepV2MakeSource(store, backend, d, e)
			if err != nil {
				return zero, errors.Join(ErrUnknown, err)
			}
		}
		return e, nil
	}
	if err = boundary(); err != nil {
		return zero, err
	}
	if op == kernelPrepReplay {
		if current.MaxUserNamespaces != intent.Before.MaxUserNamespaces && current.MaxUserNamespaces != 0 {
			return zero, fmt.Errorf("unknown third-party userns drift")
		}
		if (current.Kernel.ModulesDisabled != intent.Before.Kernel.ModulesDisabled && current.Kernel.ModulesDisabled != 1) || (current.Kernel.NUMABalancing != intent.Before.Kernel.NUMABalancing && current.Kernel.NUMABalancing != 0) || (current.Kernel.DemotionEnabled != intent.Before.Kernel.DemotionEnabled && current.Kernel.DemotionEnabled) {
			return zero, fmt.Errorf("unknown force-control drift")
		}
		before, target, now := intent.Before.controls(), d.policy.Controls, current.controls()
		old := []uint64{uint64(before.PIDMax), uint64(before.MinFreeKBytes), uint64(before.WatermarkScaleFactor), uint64(before.WatermarkBoostFactor), uint64(before.PerCPUPagelistHighFraction), uint64(before.CompactionProactiveness)}
		want := []uint64{uint64(target.PIDMax), uint64(target.MinFreeKBytes), uint64(target.WatermarkScaleFactor), uint64(target.WatermarkBoostFactor), uint64(target.PerCPUPagelistHighFraction), uint64(target.CompactionProactiveness)}
		actual := []uint64{uint64(now.PIDMax), uint64(now.MinFreeKBytes), uint64(now.WatermarkScaleFactor), uint64(now.WatermarkBoostFactor), uint64(now.PerCPUPagelistHighFraction), uint64(now.CompactionProactiveness)}
		for i := range old {
			if actual[i] != old[i] && actual[i] != want[i] {
				return zero, fmt.Errorf("unknown third-party kernel control drift")
			}
		}
		if err = store.syncPhysicalFile(normalKernelPrepV2IntentName); err != nil {
			return zero, err
		}
	}
	if err = backend.kernelPrepV2ZeroUserNamespaces(intent.Before.Control); err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	target := d.policy.Controls
	// 先PID与禁boost/proactive，再显式PCP/scale/minfree；不回滚放宽原硬源。
	writes := []struct {
		name  string
		value uint64
	}{
		{"modules_disabled", 1}, {"numa_balancing", 0}, {"demotion_enabled", 0},
		{"watermark_boost_factor", 0}, {"compaction_proactiveness", 0},
		{"percpu_pagelist_high_fraction", uint64(target.PerCPUPagelistHighFraction)}, {"watermark_scale_factor", uint64(target.WatermarkScaleFactor)}, {"min_free_kbytes", uint64(target.MinFreeKBytes)},
	}
	for _, write := range writes {
		if err = ctx.Err(); err != nil {
			return zero, fmt.Errorf("%w: interrupted original kernel writes: %v", ErrUnknown, err)
		}
		if err = backend.kernelPrepWrite(write.name, write.value); err != nil {
			return zero, fmt.Errorf("%w: kernel control %s: %v", ErrUnknown, write.name, err)
		}
	}
	if err = backend.kernelPrepCensus(target.PIDMax); err != nil {
		return zero, fmt.Errorf("%w: kernel PID observation: %v", ErrUnknown, err)
	}
	current, err = backend.kernelPrepV2Read()
	if err != nil {
		return zero, fmt.Errorf("%w: kernel readback: %v", ErrUnknown, err)
	}
	if err = current.validate(d); err != nil {
		return zero, err
	}
	if err = normalKernelPrepV2VerifyPrivateHistory(store, intent.Before, normalKernelPrepV2IntentName); err != nil {
		return zero, err
	}
	if err = intent.Before.fixedMatch(current); err != nil {
		return zero, err
	}
	if !current.zeroMovable() || !current.target(d.policy) {
		return zero, fmt.Errorf("%w: kernel preparation target differs", ErrUnknown)
	}
	if err = backend.kernelPrepPGEmpty(); err != nil {
		return zero, err
	}
	evidence := NormalV2KernelBootstrapEvidence{ProtocolVersion: 2, Scope: normalKernelPrepV2Scope, Nonce: intent.Nonce, IntentSHA256: physicalDigest(raw), PolicySHA256: physicalDigest(in.PolicyRaw), RuntimeDocumentSHA256: physicalDigest(in.RuntimeRaw), PhysicalOriginPolicySHA256: physicalDigest(in.PhysicalPolicyRaw), After: current}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	done, err := json.Marshal(evidence)
	if err != nil {
		return zero, err
	}
	if err = normalKernelPrepV2PersistOriginal(store, intent.Before, normalKernelPrepV2CompleteName, done); err != nil {
		return zero, fmt.Errorf("%w: kernel completion persistence: %v", ErrUnknown, err)
	}
	evidence.durabilityConfirmed = true
	evidence.source, err = normalKernelPrepV2MakeSource(store, backend, d, evidence)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	return evidence, nil
}
