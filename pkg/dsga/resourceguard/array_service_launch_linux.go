//go:build linux

package resourceguard

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const fixedArrayBirthConsumerPath = "/run/neon-dsga/birth-consumer.json"
const fixedArrayBirthSocketPath = "/run/neon-dsga/birth.sock"
const fixedArrayPopulateSocketPath = "/run/neon-dsga/array-populate.sock"
const arrayBirthConsumerScope = "original-native-array-birth-consumer-v1"

type arrayBirthConsumerMetadata struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	Scope           string                   `json:"scope"`
	GuestBootID     string                   `json:"guest_boot_id"`
	Consumer        nativeBirthConsumer      `json:"consumer"`
	Source          nativeBirthSourceBinding `json:"source"`
}

func (m arrayBirthConsumerMetadata) validate() error {
	if m.ProtocolVersion != 1 || m.Scope != arrayBirthConsumerScope || !validGuestBoot(m.GuestBootID) || m.Consumer.PID <= 1 || m.Consumer.StartTicks == 0 {
		return fmt.Errorf("native original Root consumer metadata invalid")
	}
	if _, err := nativeBirthDigest(m.Consumer.InstanceNonce); err != nil {
		return err
	}
	if err := m.Source.Policy.validate(); err != nil {
		return err
	}
	for _, digest := range []string{m.Source.PolicySHA256, m.Source.BinarySHA256} {
		if _, err := nativeBirthDigest(digest); err != nil {
			return err
		}
	}
	return nil
}
func readArrayBirthConsumer(source nativeBirthSource) (arrayBirthConsumerMetadata, error) {
	var zero arrayBirthConsumerMetadata
	f, err := rootFile(fixedArrayBirthConsumerPath, false)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 16*1024+1))
	if err != nil || len(raw) > 16*1024 {
		return zero, fmt.Errorf("native Root consumer metadata exceeds bound")
	}
	type plain arrayBirthConsumerMetadata
	var next plain
	if err = control.UnmarshalObject(raw, &next); err != nil {
		return zero, err
	}
	m := arrayBirthConsumerMetadata(next)
	if err = m.validate(); err != nil {
		return zero, err
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil || boot != m.GuestBootID || m.Source != source.binding {
		return zero, fmt.Errorf("native Root consumer original source/Guest differs")
	}
	// 这份nonce不是独立授权能力。真正消费者还会核原SO_PEERPIDFD与自己的
	// 当前进程nonce；旧metadata/PID复用只能产生Unknown，不能接管Pending。
	return m, nil
}
func arrayRootEnvironment(clean []string, p nativeBirthPending) []string {
	result := append([]string(nil), clean...)
	result = append(result,
		"NEON_DSGA_EARLY_ARRAYS_PROFILE=root-v1:"+strconv.FormatUint(uint64(p.Source.Policy.MaxBuffers), 10),
		"NEON_DSGA_EARLY_ARRAYS_FILTER=root-v1",
		"NEON_DSGA_NATIVE_BIRTH_NONCE="+p.Nonce,
		"NEON_DSGA_NATIVE_BIRTH_SOURCE_SHA256="+p.Source.Policy.ResourceSourceSHA256,
		"NEON_DSGA_NATIVE_BIRTH_PROFILE_SHA256="+p.Source.PolicySHA256,
		"NEON_DSGA_NATIVE_BIRTH_STARTUP_BUFFERS="+strconv.FormatUint(uint64(p.Source.Policy.StartupBuffers), 10),
		"NEON_DSGA_ARRAY_ROOT_PROFILE=root-v1")
	return result
}

// 新profile的Go helper始终留System；只有原生同PID A/R/filter全核验后的
// Root tracer才迁private。普通旧v1不进入此函数，旧launch分支保持原样。
func (g *Guard) launchArrayRoot(ctx context.Context, source SourceConfig, server bool, args, environment []string, uid, gid int) error {
	// 仍是原SUID真实postgres/euid0，不改真实UID，不调用real-root-only Source
	// reader。Source/预算/原WAL由已有real-root daemon生产；本侧不持resource锁。
	if os.Geteuid() != 0 || os.Getuid() != 0 && os.Getuid() != uid || uid <= 0 || gid <= 0 {
		return fmt.Errorf("array launcher original caller differs")
	}
	membership, err := bootProcGroup(os.Getpid())
	if err != nil || membership != strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fmt.Errorf("array launcher must remain in original System domain")
	}
	consumer, err := readArrayBirthConsumerForLaunch(source)
	if err != nil {
		return err
	}
	ref, err := arrayPreexecBegin(ctx, consumer, server, uid, gid)
	if err != nil {
		return err
	}
	if server {
		// 只把原Root回包nonce与同固定元数据交给原生；这些值不是normal许可。
		environment = arrayRootEnvironment(environment, nativeBirthPending{Nonce: ref.Nonce, Source: consumer.Source})
	}
	return arrayRootDropAndExec(args, environment, uid, gid)
}

func arrayRootDropAndExec(args, environment []string, uid, gid int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// 当前M独立fd表只标记>=3为CLOEXEC，成功exec原子丢弃caller遗留对象；
	// 不关闭Go其它线程fd，198由exec后C握手新建，不能在这里例外保留。
	// stdio只完成危险对象拒绝，固定日志/pipe的原typed来源尚须System链。
	if _, err := normalPGMemoryControlPrepareExecDescriptors(); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_KEEPCAPS, 0, 0, 0, 0); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); err != nil {
		return err
	}
	if err := syscall.Setgroups([]int{gid}); err != nil {
		return err
	}
	if err := syscall.Setresgid(gid, gid, gid); err != nil {
		return err
	}
	if err := syscall.Setresuid(uid, uid, uid); err != nil {
		return err
	}
	if os.Getuid() != uid || os.Geteuid() != uid || os.Getgid() != gid || os.Getegid() != gid {
		return fmt.Errorf("native array PG privilege drop incomplete")
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	return syscall.Exec(FixedPostgresPath, append([]string{FixedPostgresPath}, args...), environment)
}
