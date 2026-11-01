//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

func normalSystemRoleDeclarationDigest(h string) bool { _, e := nativeBirthDigest(h); return e == nil }
func decodeNormalSystemRoleBrokerDeclaration(raw []byte) (normalSystemRoleBrokerDeclaration, error) {
	var d normalSystemRoleBrokerDeclaration
	if len(raw) == 0 || len(raw) > 16<<10 {
		return d, fmt.Errorf("fixed broker declaration exceeds bound")
	}
	if e := control.UnmarshalObject(raw, &d); e != nil {
		return d, fmt.Errorf("fixed broker declaration malformed")
	}
	argv, _ := normalSystemVectorSHA(normalSystemRoleBrokerArgv())
	env, _ := normalSystemVectorSHA(normalSystemRoleBrokerEnvironment())
	if d.ProtocolVersion != 1 || d.Scope != normalSystemRoleBrokerDeclarationScope || d.ExecutablePath != normalSystemRoleBrokerExecutable || d.ArgvSHA256 != argv || d.EnvironmentSHA256 != env || d.NamespaceProtocolVersion != 1 || d.GateProtocolVersion != 1 || d.Lifetime != normalSystemRoleBrokerLifetime || !normalSystemRoleDeclarationDigest(d.ExecutableSHA256) || !normalSystemRoleDeclarationDigest(d.ComputeBuildPlanSHA256) {
		return d, fmt.Errorf("fixed broker declaration differs from supported original role")
	}
	return d, nil
}

type normalSystemRoleDeclarationSource struct {
	mu                                sync.Mutex
	closed                            bool
	source                            *normalSystemLifecycleSource
	sourceSHA                         string
	rawSHA                            string
	declaration                       normalSystemRoleBrokerDeclaration
	file, executable                  *os.File
	fileDevice, fileInode             uint64
	executableDevice, executableInode uint64
	compute                           normalSystemRoleBuildPlan
}

func (d *normalSystemRoleDeclarationSource) close() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		if d.file != nil {
			d.file.Close()
		}
		if d.executable != nil {
			d.executable.Close()
		}
		d.closed = true
	}
}

// 完整声明和compute向量均来自原System清单覆盖的RO文件。它们由同一原
// RuntimeClosure SHA绑定，而不是把请求body或调用者给的hash当trusted输入。
func readNormalSystemRoleDeclarationOriginal(ctx context.Context, s *normalSystemLifecycleSource) (*normalSystemRoleDeclarationSource, error) {
	if ctx == nil || ctx.Err() != nil || s == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original broker declaration requires real root source"))
	}
	sourceSHA, e := s.referenceSHA256()
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrUnknown
	}
	d := &normalSystemRoleDeclarationSource{source: s, sourceSHA: sourceSHA}
	success := false
	defer func() {
		if !success {
			d.close()
		}
	}()
	read := func(path string, limit int64) (*os.File, []byte, unix.Stat_t, error) {
		var zero unix.Stat_t
		var wanted *NormalSystemSourceFile
		for _, entry := range s.manifest.Files {
			if entry.Path == path {
				copy := entry
				wanted = &copy
				break
			}
		}
		if wanted == nil || wanted.Kind != "config" || wanted.CanonicalPath != path {
			return nil, nil, zero, fmt.Errorf("fixed broker data absent from original System inventory")
		}
		f, e := nativeBirthOpenRO(path)
		if e != nil {
			return nil, nil, zero, e
		}
		raw, e := nativeBirthReadBounded(f, limit)
		if e != nil || physicalDigest(raw) != wanted.SHA256 || uint64(len(raw)) != wanted.Bytes {
			f.Close()
			return nil, nil, zero, fmt.Errorf("original broker data bytes differ")
		}
		if e = unix.Fstat(int(f.Fd()), &zero); e != nil {
			f.Close()
			return nil, nil, zero, e
		}
		return f, raw, zero, nil
	}
	file, raw, st, e := read(normalSystemRoleBrokerDeclarationPath, 16<<10)
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	d.file = file
	d.fileDevice = st.Dev
	d.fileInode = st.Ino
	d.rawSHA = physicalDigest(raw)
	d.declaration, e = decodeNormalSystemRoleBrokerDeclaration(raw)
	if e != nil {
		return nil, e
	}
	planFile, planRaw, _, e := read(normalSystemRoleBuildPlanPath, 64<<10)
	if e != nil {
		return nil, e
	}
	planFile.Close()
	if physicalDigest(planRaw) != d.declaration.ComputeBuildPlanSHA256 {
		return nil, fmt.Errorf("original broker compute plan digest differs")
	}
	d.compute, e = decodeNormalSystemRoleBuildPlan(planRaw)
	if e != nil {
		return nil, e
	}
	a, _ := normalSystemVectorSHA(d.compute.ComputeArgv)
	b, _ := normalSystemVectorSHA(d.compute.ComputeEnv)
	if a != s.manifest.ComputeArgvSHA256 || b != s.manifest.ComputeEnvironmentSHA256 {
		return nil, fmt.Errorf("original broker compute vectors differ")
	}
	executable := s.files[s.logical[normalSystemRoleBrokerExecutable]]
	meta := s.metadata[s.logical[normalSystemRoleBrokerExecutable]]
	if executable == nil || s.logical[normalSystemRoleBrokerExecutable] != normalSystemRoleBrokerExecutable || meta.Interpreter != "" || len(meta.Needed) != 0 {
		return nil, fmt.Errorf("original broker requires fixed static guard ELF")
	}
	sha, e := nativeBirthFileHash(executable)
	if e != nil || sha != d.declaration.ExecutableSHA256 {
		return nil, fmt.Errorf("original broker executable digest differs")
	}
	fd, e := unix.FcntlInt(executable.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	d.executable = os.NewFile(uintptr(fd), "original-role-broker-ELF")
	if e = unix.Fstat(fd, &st); e != nil {
		return nil, e
	}
	d.executableDevice = st.Dev
	d.executableInode = st.Ino
	success = true
	return d, nil
}
func (d *normalSystemRoleDeclarationSource) verify(ctx context.Context) error {
	if d == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrUnknown
	}
	observed, e := readNormalSystemRoleDeclarationOriginal(ctx, d.source)
	if e != nil {
		return e
	}
	defer observed.close()
	if d.sourceSHA != observed.sourceSHA || d.rawSHA != observed.rawSHA || d.declaration != observed.declaration || d.fileDevice != observed.fileDevice || d.fileInode != observed.fileInode || d.executableDevice != observed.executableDevice || d.executableInode != observed.executableInode || !reflect.DeepEqual(d.compute, observed.compute) {
		return errors.Join(ErrUnknown, fmt.Errorf("original broker declaration source changed"))
	}
	return nil
}
