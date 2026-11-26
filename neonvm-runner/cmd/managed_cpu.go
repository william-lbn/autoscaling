package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"go.uber.org/zap"
)

var errCPUUnconfirmed = errors.New("original CPU action remains unconfirmed")

// cpuIntent 是原执行日志，不是调度预算。先持久意图再写内核/Guest；未知不由新目标覆盖。
type cpuIntent struct {
	MinimumMillicores   uint32             `json:"minimum_millicores"`
	CommittedMillicores uint32             `json:"committed_millicores"`
	ProtocolVersion     uint32             `json:"protocol_version"`
	Identity            runnercpu.Identity `json:"identity"`
	MaximumMillicores   uint32             `json:"maximum_millicores"`
	MaximumVCPUs        uint32             `json:"maximum_vcpus"`
	CgroupDevice        uint64             `json:"cgroup_device"`
	CgroupInode         uint64             `json:"cgroup_inode"`
	Generation          int64              `json:"vm_generation"`
	TargetMillicores    uint32             `json:"target_millicores"`
	Pending             bool               `json:"pending"`
}

type cpuKernel interface {
	identity() (uint64, uint64, error)
	read(string) ([]byte, error)
	writeMax([]byte) error
	close() error
}

type managedCPU struct {
	verifyCommitment func(context.Context) (runnercpu.NodeCommitment, error)
	process          func() (int, uint64, error)
	mu               sync.Mutex
	kernel           cpuKernel
	intent           cpuIntent
	// durable=false 表示意图或完成落盘结果未知；本进程只能按原字节补持久，不能换目标。
	durable   bool
	persist   func(cpuIntent) error
	authorize func(context.Context, runnercpu.Command, vmv1.MilliCPU) error
	getGuest  func() (vmv1.MilliCPU, int, error)
	setGuest  func(vmv1.MilliCPU) (int, error)
}

func (s *managedCPU) validateTarget(cpu vmv1.MilliCPU) error {
	if s.intent.MinimumMillicores < 10 || uint32(cpu) < s.intent.MinimumMillicores || uint32(cpu) > s.intent.MaximumMillicores ||
		cpu.RoundedUp() > s.intent.MaximumVCPUs {
		return fmt.Errorf("CPU entitlement exceeds original fixed topology or Pod budget")
	}
	return nil
}

func (s *managedCPU) saveLocked() error {
	s.durable = false
	if err := s.persist(s.intent); err != nil {
		return errors.Join(errCPUUnconfirmed, err)
	}
	s.durable = true
	return nil
}

func (s *managedCPU) applyKernelLocked() error {
	dev, ino, err := s.kernel.identity()
	if err != nil || dev != s.intent.CgroupDevice || ino != s.intent.CgroupInode {
		return errors.Join(errCPUUnconfirmed, err, fmt.Errorf("original CPU cgroup changed before write"))
	}
	// 最大 64 vCPU 的固定包络已校验；乘法先提升到 uint64，没有 float 或 uint32 绕回。
	quota := uint64(s.intent.TargetMillicores) * runnercpu.PeriodUS / 1000
	if err := s.kernel.writeMax([]byte(fmt.Sprintf("%d %d", quota, runnercpu.PeriodUS))); err != nil {
		return errors.Join(errCPUUnconfirmed, err)
	}
	raw, err := s.kernel.read("cpu.max")
	if err != nil {
		return errors.Join(errCPUUnconfirmed, err)
	}
	q, p, err := parseCPUQuota(raw)
	if err != nil || q != quota || p != runnercpu.PeriodUS {
		return errors.Join(errCPUUnconfirmed, err, fmt.Errorf("kernel CPU quota readback differs"))
	}
	return nil
}

func (s *managedCPU) change(ctx context.Context, command api.VCPUChange) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if command.Managed == nil || command.Managed.Validate() != nil ||
		command.Managed.Identity != s.intent.Identity || s.validateTarget(command.VCPUs) != nil {
		return http.StatusConflict, fmt.Errorf("original CPU command identity or envelope differs")
	}
	exactPending := s.durable && s.intent.Pending && command.Managed.Generation == s.intent.Generation &&
		uint32(command.VCPUs) == s.intent.TargetMillicores
	if exactPending {
		// 只恢复已fsync的逐字原intent。API当前目标即使已前进，也不能覆盖未知旧动作。
		// 原VM/冻结min/max、Pod/Node完整持有、原Runner/QEMU/cgroup仍必须成立。
		if s.verifyCommitment == nil {
			return http.StatusServiceUnavailable, errCPUUnconfirmed
		}
		if _, err := s.verifyCommitment(ctx); err != nil {
			return http.StatusServiceUnavailable, errors.Join(errCPUUnconfirmed, err)
		}
	} else if err := s.authorize(ctx, *command.Managed, command.VCPUs); err != nil {
		return http.StatusConflict, err
	}
	if !s.durable {
		if err := s.saveLocked(); err != nil {
			return http.StatusServiceUnavailable, err
		}
	}
	if command.Managed.Generation < s.intent.Generation ||
		(command.Managed.Generation == s.intent.Generation && uint32(command.VCPUs) != s.intent.TargetMillicores) {
		return http.StatusConflict, fmt.Errorf("CPU command is stale or changes an accepted original generation")
	}
	if s.intent.Pending && (command.Managed.Generation != s.intent.Generation ||
		uint32(command.VCPUs) != s.intent.TargetMillicores) {
		return http.StatusConflict, fmt.Errorf("pending original CPU action cannot be superseded")
	}
	if err := ctx.Err(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	s.intent.Generation = command.Managed.Generation
	s.intent.TargetMillicores = uint32(command.VCPUs)
	s.intent.Pending = true
	if err := s.saveLocked(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	if err := ctx.Err(); err != nil {
		return http.StatusServiceUnavailable, errors.Join(errCPUUnconfirmed, err)
	}
	if err := s.applyKernelLocked(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	if _, err := s.setGuest(command.VCPUs); err != nil {
		return http.StatusServiceUnavailable, errors.Join(errCPUUnconfirmed, err)
	}
	if _, err := s.observeLocked(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	if s.intent.Pending {
		return http.StatusServiceUnavailable, errCPUUnconfirmed
	}
	return http.StatusOK, nil
}

func (s *managedCPU) observeLocked() (*api.VCPUCgroup, error) {
	if s.verifyCommitment == nil {
		return nil, fmt.Errorf("original CPU commitment source unavailable")
	}
	verifyCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	commitment, verifyErr := s.verifyCommitment(verifyCtx)
	cancel()
	if verifyErr != nil {
		return nil, errors.Join(errCPUUnconfirmed, verifyErr)
	}
	if !s.durable {
		if err := s.saveLocked(); err != nil {
			return nil, err
		}
	}
	if s.process == nil {
		return nil, fmt.Errorf("original CPU QEMU process source unavailable")
	}
	pid, start, processErr := s.process()
	if processErr != nil || pid <= 0 || start == 0 {
		return nil, errors.Join(errCPUUnconfirmed, processErr)
	}
	dev, ino, err := s.kernel.identity()
	if err != nil || dev != s.intent.CgroupDevice || ino != s.intent.CgroupInode {
		return nil, errors.Join(errCPUUnconfirmed, err, fmt.Errorf("original CPU cgroup identity differs"))
	}
	raw, err := s.kernel.read("cpu.max")
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	quota, period, err := parseCPUQuota(raw)
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	weightRaw, err := s.kernel.read("cpu.weight")
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	weight, err := parseCPUUnsigned(weightRaw)
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	statRaw, err := s.kernel.read("cpu.stat")
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	pressureRaw, err := s.kernel.read("cpu.pressure")
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	statistics, err := parseCPUStatistics(statRaw, pressureRaw)
	if err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	guest, _, err := s.getGuest()
	if err != nil || guest == 0 || guest%1000 != 0 {
		return nil, errors.Join(errCPUUnconfirmed, err, fmt.Errorf("Guest online CPU topology is not an integer fact"))
	}
	bootNS, clockErr := cpuObservationBootTime()
	if clockErr != nil {
		return nil, errors.Join(errCPUUnconfirmed, clockErr)
	}
	observed := &runnercpu.Observation{ObservedBootTimeNS: bootNS, NodeCommitment: commitment, ProtocolVersion: runnercpu.ProtocolVersion, Scope: runnercpu.Scope,
		Identity: s.intent.Identity, Generation: s.intent.Generation, CgroupDevice: dev, CgroupInode: ino, QEMUPID: uint32(pid), QEMUStartTicks: start,
		QuotaUS: quota, PeriodUS: period, Weight: weight, OnlineVCPUs: uint32(guest) / 1000,
		MaximumVCPUs: s.intent.MaximumVCPUs, TargetMillicores: s.intent.TargetMillicores, MaximumMillicores: s.intent.MaximumMillicores, CommittedMillicores: s.intent.CommittedMillicores,
		Pending: true, Statistics: statistics}
	current, err := observed.Millicores()
	if err != nil || current > s.intent.MaximumMillicores {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	aligned := current == s.intent.TargetMillicores && observed.OnlineVCPUs == (current+999)/1000
	if aligned == s.intent.Pending {
		s.intent.Pending = !aligned
		if err := s.saveLocked(); err != nil {
			return nil, err
		}
	}
	observed.Pending = s.intent.Pending || !aligned
	if err := observed.Validate(); err != nil {
		return nil, errors.Join(errCPUUnconfirmed, err)
	}
	return &api.VCPUCgroup{VCPUs: vmv1.MilliCPU(current), Managed: observed}, nil
}

func (s *managedCPU) observe() (*api.VCPUCgroup, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.observeLocked()
	if err != nil {
		return nil, http.StatusServiceUnavailable, err
	}
	return result, http.StatusOK, nil
}

func managedCPUCallbacks(s *managedCPU) cpuServerCallbacks {
	return cpuServerCallbacks{
		get: func(_ *zap.Logger) (*api.VCPUCgroup, int, error) { return s.observe() },
		set: func(ctx context.Context, _ *zap.Logger, c api.VCPUChange) (int, error) { return s.change(ctx, c) },
		ready: func(_ *zap.Logger) bool {
			current, _, err := s.observe()
			return err == nil && current.Managed != nil && !current.Managed.Pending
		},
	}
}
