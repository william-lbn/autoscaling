//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// 原pin只能从真实Birth driver原SO_PEERPIDFD复制。日志数字PID/starttime或
// 注册报文均没有构造此对象的入口；Root重启丢pin后必须保持Unknown。
type arrayRegistryOriginal struct {
	pidfd, procfd int
	pending       nativeBirthPending
	birthRef      NativeBirthRef
	reservation   nativeBirthRequest
	releasedSHA   string
	mu            sync.Mutex
	current       *arrayRegistryGeneration
	retained      []*arrayRegistryGeneration
	failed        bool
}
type arrayRegistryGeneration struct {
	request    arrayRegistryWireRequest
	nonce      [32]byte
	files      [4]*os.File
	identities [4]arrayPopulateFile
	retiring   bool
	startup    [4]bool
}

func (g *arrayRegistryGeneration) close() {
	if g == nil {
		return
	}
	for _, f := range g.files {
		if f != nil {
			_ = f.Close()
		}
	}
}
func (o *arrayRegistryOriginal) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failed = true
	for _, generation := range o.retained {
		generation.close()
	}
	o.retained = nil
	if o.current != nil {
		o.current.close()
		o.current = nil
	}
	if o.pidfd >= 0 {
		_ = unix.Close(o.pidfd)
		o.pidfd = -1
	}
	if o.procfd >= 0 {
		_ = unix.Close(o.procfd)
		o.procfd = -1
	}
}
func arrayRegistryPIDFDAlive(fd int) error {
	if fd < 0 {
		return fmt.Errorf("original pidfd missing")
	}
	p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(p, 0)
	if err != nil {
		return err
	}
	if n != 0 || p[0].Revents != 0 {
		return fmt.Errorf("original pidfd exited or unknown")
	}
	return nil
}
func arrayRegistryDup(fd int) (int, error) {
	if fd < 0 {
		return -1, fmt.Errorf("original descriptor missing")
	}
	return unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
}

// 与旧Birth同一个worker/driver执行；原released四链fsync成功后才移交pin。
// 它仍不产生normal许可，也不让Root自身或Go launcher先进入PG任务域。
// caller必须持原resource flock，stage/state不允许在callback中重入withRecord。
func runArrayRegistryBirthTraceSession(ctx context.Context, conn *net.UnixConn, cfg nativeBirthTraceConfig, stage *nativeBirthStage, guard *Guard, splitInputs normalSplitInputs, launchPin *arrayLaunchPin) (nativeBirthTraceTranscript, *arrayRegistryOriginal, error) {
	var original *arrayRegistryOriginal
	if ctx == nil {
		return nativeBirthTraceTranscript{}, nil, fmt.Errorf("array Birth requires bounded original context")
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || !deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Second)) || ctx.Err() != nil {
		return nativeBirthTraceTranscript{}, nil, fmt.Errorf("array Birth original deadline missing")
	}
	if guard == nil || stage == nil || conn == nil || stage.snapshot.Pending.Original != cfg.Original || stage.snapshot.Pending.Source.BinarySHA256 != fmt.Sprintf("%x", cfg.BinarySHA256) {
		return nativeBirthTraceTranscript{}, nil, fmt.Errorf("array registry requires original Birth writer/source")
	}
	expected, expectedErr := stage.snapshot.Pending.Source.expected(stage.snapshot.Pending.Nonce, stage.snapshot.Pending.Original.AddressSpaceMaxBytes)
	if expectedErr != nil || cfg.Expected != expected || cfg.PeerUID != stage.snapshot.Pending.PeerUID || cfg.PeerGID != stage.snapshot.Pending.PeerGID || cfg.PeerUID == 0 || cfg.PeerGID == 0 || cfg.AuditBinary == nil || cfg.BinarySHA256 == [32]byte{} {
		return nativeBirthTraceTranscript{}, nil, fmt.Errorf("array Birth exact original driver fields changed")
	}
	defer conn.Close()
	tr, err := nativeTraceRunLockedWorker(func(tid int) (nativeBirthTraceTranscript, error, bool) {
		driver, e := newNativeBirthTraceKernel(conn, cfg, tid)
		if e != nil {
			return nativeBirthTraceTranscript{}, e, false
		}
		defer driver.close()
		if launchPin == nil || launchPin.pending != stage.snapshot.Pending {
			return nativeBirthTraceTranscript{}, errors.Join(ErrUnknown, fmt.Errorf("native original pre-exec pin missing")), false
		}
		if e := arrayMatchPIDFD(launchPin.pidfd, driver.pidfd); e != nil {
			return nativeBirthTraceTranscript{}, errors.Join(ErrUnknown, e), false
		}

		// 固定生产hook：真正原生已被本线程ptrace停住且PMbase完整审计后，
		// 在ACK前先持久迁组意图→同原PID迁private→读回→完成前缀，再存Base。
		// 不是请求/配置可注入的callback，不让Go多线程helper提前进入private。
		cfg.Persist = func(call context.Context, phase nativeFilterPhase, tr nativeBirthTraceTranscript) error {
			if phase == nativeFilterPMBase {
				if err := moveArrayNativeBirthPrivateLocked(call, guard, stage, driver, splitInputs, tr); err != nil {
					return err
				}
			}
			return stage.persist(call, phase, tr)
		}
		transcript, e, retire := nativeBirthTraceFlow(ctx, cfg, driver, tid)
		if e != nil {
			return transcript, e, retire
		}
		if e = driver.alive(); e != nil {
			return transcript, e, retire
		}
		pidfd, e := arrayRegistryDup(driver.pidfd)
		if e != nil {
			return transcript, e, retire
		}
		procfd, e := arrayRegistryDup(driver.procfd)
		if e != nil {
			_ = unix.Close(pidfd)
			return transcript, e, retire
		}
		candidate := &arrayRegistryOriginal{pidfd: pidfd, procfd: procfd, pending: stage.snapshot.Pending, birthRef: stage.snapshot.Ref, reservation: transcript.Request}
		// PM可能已connect等待数组服务，但释放本原flock/发布对象前不得让它预触。
		if e = stage.released(ctx, transcript); e != nil {
			candidate.close()
			return transcript, e, retire
		}
		candidate.releasedSHA = stage.previous
		original = candidate
		return transcript, nil, retire
	})
	if err != nil {
		if original != nil {
			original.close()
		}
		return tr, nil, errors.Join(ErrUnknown, err)
	}
	if original == nil {
		return tr, nil, errors.Join(ErrUnknown, fmt.Errorf("array original pin was not transferred"))
	}
	return tr, original, nil
}

func (o *arrayRegistryOriginal) verifyOriginal() error {
	if o == nil || o.failed || o.pending.validate() != nil || o.birthRef.validate() != nil || !physicalDigestValid(o.releasedSHA) {
		return fmt.Errorf("array original Birth pin unavailable")
	}
	if err := arrayRegistryPIDFDAlive(o.pidfd); err != nil {
		return err
	}
	raw, err := nativeTraceProcText(o.procfd, "stat")
	if err != nil {
		return err
	}
	ticks, err := postmasterTicks(raw, o.pending.Original.PostmasterPID)
	if err != nil || ticks != uint64(o.pending.Original.PostmasterStartTicks) {
		return fmt.Errorf("array original PM start changed")
	}
	status, err := nativeTraceProcText(o.procfd, "status")
	if err != nil {
		return err
	}
	if err = processUID(status, uint64(o.pending.PeerUID)); err != nil {
		return err
	}
	if err = nativeBirthCurrentConsumer(o.pending); err != nil {
		return err
	}
	return arrayRegistryPIDFDAlive(o.pidfd)
}

// readlink文本只供诊断，不用它重新open；精确range magic link才是原shmem对象。
// dirfd钉住已核proc，打开前后确认同原pidfd存活；不接受SCM_RIGHTS/租户路径。
func arrayRegistryOpenMappings(procfd int, pidfd int, request arrayRegistryWireRequest) ([4]*os.File, [4]arrayPopulateFile, error) {
	var files [4]*os.File
	var identities [4]arrayPopulateFile
	cleanup := func(err error) ([4]*os.File, [4]arrayPopulateFile, error) {
		for _, f := range files {
			if f != nil {
				_ = f.Close()
			}
		}
		return [4]*os.File{}, [4]arrayPopulateFile{}, err
	}
	if err := arrayRegistryPIDFDAlive(pidfd); err != nil {
		return cleanup(err)
	}
	maps, err := nativeTraceProcText(procfd, "maps")
	if err != nil {
		return cleanup(err)
	}
	mapfd, err := unix.Openat(procfd, "map_files", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return cleanup(err)
	}
	defer unix.Close(mapfd)
	seen := map[[2]uint64]bool{}
	for i, r := range request.Ranges {
		name := strconv.FormatUint(r.Base, 16) + "-" + strconv.FormatUint(r.Base+r.Mapped, 16)
		var fields []string
		for _, line := range strings.Split(maps, "\n") {
			parts := strings.Fields(line)
			if len(parts) > 0 && parts[0] == name {
				if fields != nil {
					return cleanup(fmt.Errorf("array maps range duplicated"))
				}
				fields = parts
			}
		}
		if len(fields) < 5 || fields[1] != "rw-s" || fields[2] != "00000000" {
			return cleanup(fmt.Errorf("array exact RW shared VMA missing"))
		}
		inode, e := strconv.ParseUint(fields[4], 10, 64)
		if e != nil || inode == 0 {
			return cleanup(fmt.Errorf("array backing inode missing"))
		}
		dev := strings.Split(fields[3], ":")
		if len(dev) != 2 {
			return cleanup(fmt.Errorf("array mapping dev malformed"))
		}
		major, e1 := strconv.ParseUint(dev[0], 16, 32)
		minor, e2 := strconv.ParseUint(dev[1], 16, 32)
		if e1 != nil || e2 != nil {
			return cleanup(fmt.Errorf("array mapping dev malformed"))
		}
		// 此处有意不加O_NOFOLLOW：我们要打开内核map_files magic link本身的对象。
		fd, e := unix.Openat(mapfd, name, unix.O_RDWR|unix.O_CLOEXEC, 0)
		if e != nil {
			return cleanup(e)
		}
		files[i] = os.NewFile(uintptr(fd), "original-array-shmem")
		var st unix.Stat_t
		var fs unix.Statfs_t
		if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.TMPFS_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Ino != inode || uint64(unix.Major(st.Dev)) != major || uint64(unix.Minor(st.Dev)) != minor || st.Size != int64(r.Mapped) || seen[[2]uint64{st.Dev, st.Ino}] {
			return cleanup(fmt.Errorf("array original backing identity/span differs"))
		}
		seen[[2]uint64{st.Dev, st.Ino}] = true
		identities[i] = arrayPopulateFile{Device: control.Uint64(st.Dev), Inode: control.Uint64(st.Ino), FileBytes: control.Uint64(st.Size)}
	}
	if err := arrayRegistryPIDFDAlive(pidfd); err != nil {
		return cleanup(err)
	}
	return files, identities, nil
}
