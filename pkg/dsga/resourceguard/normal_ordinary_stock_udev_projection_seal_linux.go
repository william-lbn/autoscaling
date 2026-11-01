//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
)

const normalOrdinaryStockUdevProjectionSeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// 此对象只保证独立快照的内核不可写封存，不产生处理完成或Source资格。
// Root factory仍必须先证明原stock处理/全Wait/同g来源，不能从调用者map补签。
type normalOrdinaryStockUdevMemfdFact struct {
	Device, Inode     uint64
	Filesystem, Bytes int64
}

func normalOrdinaryStockUdevMemfdObject(fd int) (normalOrdinaryStockUdevMemfdFact, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if e := unix.Fstat(fd, &st); e != nil {
		return normalOrdinaryStockUdevMemfdFact{}, e
	}
	if e := unix.Fstatfs(fd, &fs); e != nil {
		return normalOrdinaryStockUdevMemfdFact{}, e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Dev == 0 || st.Ino == 0 || st.Size <= 0 || st.Size > 16<<20 || fs.Type != unix.TMPFS_MAGIC {
		return normalOrdinaryStockUdevMemfdFact{}, ErrUnknown
	}
	return normalOrdinaryStockUdevMemfdFact{uint64(st.Dev), st.Ino, int64(fs.Type), st.Size}, nil
}

type normalOrdinaryStockUdevProjectionSeal struct {
	mu      sync.Mutex
	file    *os.File
	object  normalOrdinaryStockUdevMemfdFact
	sha     string
	records map[string]normalOrdinaryUdevV2DeviceProjection
}

func normalOrdinaryStockUdevCopyProjection(v normalOrdinaryUdevV2DeviceProjection) normalOrdinaryUdevV2DeviceProjection {
	p := make(map[string]string, len(v.Properties))
	for k, s := range v.Properties {
		p[k] = s
	}
	v.Properties = p
	v.DynamicProperties = append([]normalOrdinaryUdevV2DynamicProperty(nil), v.DynamicProperties...)
	return v
}
func sealNormalOrdinaryStockUdevProjection(ctx context.Context, records []normalOrdinaryUdevV2DeviceProjection) (_ *normalOrdinaryStockUdevProjectionSeal, err error) {
	if ctx == nil || ctx.Err() != nil || len(records) == 0 || len(records) > 4096 {
		return nil, ErrUnknown
	}
	p := &normalOrdinaryStockUdevProjectionSeal{records: map[string]normalOrdinaryUdevV2DeviceProjection{}}
	ordered := make([]normalOrdinaryUdevV2DeviceProjection, 0, len(records))
	for _, record := range records {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		if !normalOrdinaryStockUdevAbsolute(record.Devpath, "") || !normalOrdinaryStockUdevWord(record.Subsystem) || record.UeventObject.Device == 0 || record.UeventObject.Inode == 0 || record.UeventObject.MountID == 0 || record.UeventObject.Filesystem != unix.SYSFS_MAGIC {
			return nil, ErrUnknown
		}
		if record.Properties["DEVPATH"] != record.Devpath || record.Properties["SUBSYSTEM"] != record.Subsystem {
			return nil, ErrUnknown
		}
		if _, e := normalOrdinaryUdevV2CanonicalProperties(record.Properties); e != nil {
			return nil, e
		}
		if _, yes := p.records[record.Devpath]; yes {
			return nil, ErrUnknown
		}
		seen := map[normalOrdinaryUdevV2DynamicProperty]bool{}
		for _, kind := range record.DynamicProperties {
			if kind != normalOrdinaryUdevV2DynamicInitializedMonotonicUsec || seen[kind] {
				return nil, ErrUnknown
			}
			// 本批只接已真实观测到的初始化时间；不从缺省常数生成动态许可。
			text := record.Properties["USEC_INITIALIZED"]
			n, e := strconv.ParseUint(text, 10, 64)
			if e != nil || n == 0 || strconv.FormatUint(n, 10) != text {
				return nil, ErrUnknown
			}
			seen[kind] = true
		}
		copy := normalOrdinaryStockUdevCopyProjection(record)
		p.records[record.Devpath] = copy
		ordered = append(ordered, copy)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Devpath < ordered[j].Devpath })
	raw, e := json.Marshal(struct {
		ProtocolVersion uint32
		Scope           string
		Records         []normalOrdinaryUdevV2DeviceProjection
	}{2, "original-stock-udev-projection-seal-v2", ordered})
	if e != nil || len(raw) > 16<<20 {
		return nil, errors.Join(ErrUnknown, e)
	}
	fd, e := unix.MemfdCreate("neon-stock-projection-v2", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if e != nil {
		return nil, e
	}
	writer := os.NewFile(uintptr(fd), "stock-projection-writer")
	defer writer.Close()
	if n, e := writer.Write(raw); e != nil || n != len(raw) {
		return nil, errors.Join(ErrUnknown, e, io.ErrShortWrite)
	}
	if e = unix.Fchmod(fd, 0444); e != nil {
		return nil, e
	}
	if _, e = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, normalOrdinaryStockUdevProjectionSeals); e != nil {
		return nil, e
	}
	ro, e := unix.Open("/proc/self/fd/"+strconv.Itoa(fd), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	p.file = os.NewFile(uintptr(ro), "original-stock-projection-sealed")
	good := false
	defer func() {
		if !good {
			p.close()
		}
	}()
	p.object, e = normalOrdinaryStockUdevMemfdObject(ro)
	if e != nil {
		return nil, e
	}
	// 封完后通过独立只读FD读回同一原字节，不信只写内存的预期摘要。
	actual, e := normalOrdinaryUdevV2ReadFD(p.file, (16 << 20))
	if e != nil || physicalDigest(actual) != physicalDigest(raw) {
		return nil, errors.Join(ErrUnknown, e)
	}
	p.sha = physicalDigest(actual)
	if e = p.verifyLocked(); e != nil {
		return nil, e
	}
	good = true
	return p, nil
}
func (p *normalOrdinaryStockUdevProjectionSeal) verifyLocked() error {
	if p == nil || p.file == nil || !physicalDigestValid(p.sha) {
		return ErrUnknown
	}
	fd := int(p.file.Fd())
	object, e := normalOrdinaryStockUdevMemfdObject(fd)
	if e != nil || object != p.object || object.Filesystem != unix.TMPFS_MAGIC {
		return errors.Join(ErrUnknown, e)
	}
	closeFlags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if e != nil || closeFlags&unix.FD_CLOEXEC == 0 {
		return errors.Join(ErrUnknown, e)
	}
	flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if e != nil || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return errors.Join(ErrUnknown, e)
	}
	seals, e := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if e != nil || seals&normalOrdinaryStockUdevProjectionSeals != normalOrdinaryStockUdevProjectionSeals {
		return errors.Join(ErrUnknown, e)
	}
	// 原同inode的四seal已不可撤销，因此复核时无需反复hash整个可写DB或大快照。
	return nil
}
func (p *normalOrdinaryStockUdevProjectionSeal) digest() (string, error) {
	if p == nil {
		return "", ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.verifyLocked(); e != nil {
		return "", e
	}
	return p.sha, nil
}
func (p *normalOrdinaryStockUdevProjectionSeal) projection(path string) (normalOrdinaryUdevV2DeviceProjection, error) {
	var zero normalOrdinaryUdevV2DeviceProjection
	if p == nil {
		return zero, ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.verifyLocked(); e != nil {
		return zero, e
	}
	record, ok := p.records[path]
	if !ok {
		return zero, fmt.Errorf("%w: original sealed device projection absent", ErrUnknown)
	}
	return normalOrdinaryStockUdevCopyProjection(record), nil
}
func (p *normalOrdinaryStockUdevProjectionSeal) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.file != nil {
		_ = p.file.Close()
		p.file = nil
	}
	p.records = nil
}
