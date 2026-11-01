//go:build linux

package resourceguard

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Root fixed stock-preparation的原opaque，不允许从JSON/current DB补签。
type normalOrdinaryUdevV2PreparationReader interface {
	normalOrdinaryUdevV2PreparationLocked(context.Context, *lockedState, *record, *normalFilesystemOriginSource, *normalSystemRoleBrokerExecProof, *normalSystemResourceAuthority) (normalOrdinaryUdevV2Preparation, error)
}
type normalOrdinaryUdevV2Preparation interface {
	referenceLocked(context.Context, *lockedState) (normalOrdinaryUdevV2PreparationReference, error)
	projectionForOriginalDeviceLocked(context.Context, *lockedState, *normalOrdinaryUdevV2CatalogEntry) (normalOrdinaryUdevV2DeviceProjection, error)
}

// 全值可比历史引用；没有ready/verified bool，引用本身不是能力。
type normalOrdinaryUdevV2PreparationReference struct {
	ProtocolVersion                                                                                uint32
	InstanceNonce                                                                                  string
	OriginalR2                                                                                     nativeBirthConsumer
	Identity                                                                                       Identity
	OriginalInitialWAL                                                                             initialPrebootGuestWitnessKey
	AuthorityPrepared                                                                              normalSystemRoleFileWitness
	FilesystemSHA256, ManifestSHA256, PackageLockSHA256, RulesSHA256, ProjectionSHA256, WaitSHA256 string
}
type normalOrdinaryUdevV2DeviceProjection struct {
	Devpath, Subsystem string
	UeventObject       normalFilesystemObject
	Properties         map[string]string
	// 动态规则字段是固定枚举，由原prep语义生成，不能传任意ENV白名单。
	DynamicProperties []normalOrdinaryUdevV2DynamicProperty
}
type normalOrdinaryUdevV2DynamicProperty uint32

const normalOrdinaryUdevV2DynamicInitializedMonotonicUsec normalOrdinaryUdevV2DynamicProperty = 1

const normalOrdinaryUdevV2CatalogMaximum = 4096
const normalOrdinaryUdevV2CatalogDepth = 32

type normalOrdinaryUdevV2Range struct{ Start, End uint64 }
type normalOrdinaryUdevV2CatalogEntry struct {
	path, subsystem, name string
	properties            map[string]string
	uevent                *os.File
	object                normalFilesystemObject
	node                  *os.File
	nodeObject            normalFilesystemObject
	virtioPortName        string
	projection            normalOrdinaryUdevV2DeviceProjection
}

// Reader仅由同Root preparation提供原Inputs RO dup；返回值移交给catalog，不能从路径重开补签。
type normalOrdinaryUdevV2OriginalRuleReader interface {
	duplicateOriginalRules(context.Context) ([]normalOrdinaryUdevV2RuleSource, error)
}

type normalOrdinaryUdevV2RuleSource struct {
	path, sha string
	raw       []byte
	original  *os.File
}
type normalOrdinaryUdevV2Catalog struct {
	entries     map[string]*normalOrdinaryUdevV2CatalogEntry
	ordered     []*normalOrdinaryUdevV2CatalogEntry
	rules       []normalOrdinaryUdevV2RuleSource
	memoryBlock uint64
	initialRAM  []normalOrdinaryUdevV2Range
	device      normalOrdinaryUdevV2Range
	unresolved  []string
}

func normalOrdinaryUdevV2ReadFD(f *os.File, max int) ([]byte, error) {
	if f == nil {
		return nil, ErrUnknown
	}
	b := make([]byte, max+1)
	n, e := f.ReadAt(b, 0)
	if e != nil && !errors.Is(e, io.EOF) {
		return nil, e
	}
	if n > max {
		return nil, fmt.Errorf("udev original object exceeds bound")
	}
	return b[:n], nil
}
func normalOrdinaryUdevV2StaticProperties(raw []byte) (map[string]string, error) {
	raw = bytes.TrimSuffix(raw, []byte("\n"))
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	raw = bytes.ReplaceAll(raw, []byte("\n"), []byte{0})
	raw = append(raw, 0)
	return normalOrdinaryUdevV2Properties(raw, false)
}
func normalOrdinaryUdevV2RAMRanges(text string) ([]normalOrdinaryUdevV2Range, error) {
	if len(text) == 0 || len(text) > 256<<10 {
		return nil, ErrUnknown
	}
	var out []normalOrdinaryUdevV2Range
	scan := bufio.NewScanner(strings.NewReader(text))
	for scan.Scan() {
		line := scan.Text()
		if len(line) > 0 && line[0] == ' ' {
			continue
		}
		span, label, ok := strings.Cut(line, " : ")
		if !ok || label != "System RAM" {
			continue
		}
		lo, hi, ok := strings.Cut(span, "-")
		if !ok {
			return nil, ErrUnknown
		}
		start, a := strconv.ParseUint(lo, 16, 64)
		end, b := strconv.ParseUint(hi, 16, 64)
		if a != nil || b != nil || end < start || end == math.MaxUint64 {
			return nil, ErrUnknown
		}
		out = append(out, normalOrdinaryUdevV2Range{start, end + 1})
		if len(out) > 128 {
			return nil, ErrUnknown
		}
	}
	if scan.Err() != nil || len(out) == 0 {
		return nil, ErrUnknown
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	for i := 1; i < len(out); i++ {
		if out[i].Start < out[i-1].End {
			return nil, ErrUnknown
		}
	}
	return out, nil
}
func (c *normalOrdinaryUdevV2Catalog) memoryIndex(index uint32) bool {
	if c == nil || c.memoryBlock == 0 || uint64(index) > math.MaxUint64/c.memoryBlock {
		return false
	}
	lo := uint64(index) * c.memoryBlock
	if lo > math.MaxUint64-c.memoryBlock {
		return false
	}
	hi := lo + c.memoryBlock
	inside := func(r normalOrdinaryUdevV2Range) bool { return lo >= r.Start && hi <= r.End }
	if inside(c.device) {
		return true
	}
	if c.entries["/devices/system/memory/memory"+strconv.FormatUint(uint64(index), 10)] != nil {
		for _, r := range c.initialRAM {
			if lo < r.End && hi > r.Start {
				return true
			}
		}
	}
	return false
}

// 原prepared阶段的一次性全/sys/devices捕获，路径只来自内核目录枚举，非请求正文。
func captureNormalOrdinaryUdevV2Catalog(ctx context.Context, system *normalSystemLifecycleSource, memory *normalMemoryBrokerConsumerSource, rules normalOrdinaryUdevV2OriginalRuleReader) (out *normalOrdinaryUdevV2Catalog, err error) {
	if ctx == nil || ctx.Err() != nil || system == nil || memory == nil || memory.kernel == nil || rules == nil {
		return nil, ErrUnknown
	}
	c := &normalOrdinaryUdevV2Catalog{entries: map[string]*normalOrdinaryUdevV2CatalogEntry{}}
	good := false
	defer func() {
		if !good {
			c.close()
		}
	}()
	var filesystem unix.Statfs_t
	if e := unix.Statfs("/sys/devices", &filesystem); e != nil || filesystem.Type != unix.SYSFS_MAGIC {
		return nil, errors.Join(ErrUnknown, e)
	}
	block, e := kernelText("/sys/devices/system/memory/block_size_bytes")
	if e != nil {
		return nil, e
	}
	c.memoryBlock, e = strconv.ParseUint(block, 16, 64)
	if e != nil || c.memoryBlock == 0 || c.memoryBlock&(c.memoryBlock-1) != 0 {
		return nil, ErrUnknown
	}
	region := memory.intent.Region
	if region.validate() != nil || region.ProtocolVersion != 2 || region.Address == 0 || uint64(region.Address) > math.MaxUint64-uint64(region.RegionBytes) {
		return nil, ErrUnknown
	}
	c.device = normalOrdinaryUdevV2Range{uint64(region.Address), uint64(region.Address) + uint64(region.RegionBytes)}
	iomem, e := kernelText("/proc/iomem")
	if e != nil {
		return nil, e
	}
	c.initialRAM, e = normalOrdinaryUdevV2RAMRanges(iomem)
	if e != nil {
		return nil, e
	}
	e = filepath.WalkDir("/sys/devices", func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if strings.Count(strings.TrimPrefix(path, "/sys/devices"), "/") > normalOrdinaryUdevV2CatalogDepth {
			return fmt.Errorf("udev static catalog depth exceeded")
		}
		if !d.IsDir() || d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		name := filepath.Join(path, "uevent")
		fd, e := unix.Open(name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(e, unix.ENOENT) {
			return nil
		}
		if e != nil {
			return e
		}
		held := os.NewFile(uintptr(fd), "original catalog uevent")
		o, e := normalFilesystemFDObject(fd)
		if e != nil || o.Filesystem != unix.SYSFS_MAGIC {
			held.Close()
			return errors.Join(ErrUnknown, e)
		}
		subsystem, e := os.Readlink(filepath.Join(path, "subsystem"))
		if e != nil {
			held.Close()
			return e
		}
		raw, e := normalOrdinaryUdevV2ReadFD(held, 8192)
		if e != nil {
			held.Close()
			return e
		}
		props, e := normalOrdinaryUdevV2StaticProperties(raw)
		if e != nil {
			held.Close()
			return e
		}
		devpath := strings.TrimPrefix(path, "/sys")
		if len(c.entries) >= normalOrdinaryUdevV2CatalogMaximum {
			held.Close()
			return fmt.Errorf("udev complete static catalog exceeds bound")
		}
		entry := &normalOrdinaryUdevV2CatalogEntry{path: devpath, subsystem: filepath.Base(subsystem), name: filepath.Base(path), properties: props, uevent: held, object: o}
		c.entries[devpath] = entry
		c.ordered = append(c.ordered, entry)
		if entry.subsystem == "virtio-ports" {
			entry.virtioPortName, e = kernelText(filepath.Join(path, "name"))
			if e != nil {
				return e
			}
			if strings.ContainsAny(entry.virtioPortName, "/\x00\r\n") || entry.virtioPortName == "" {
				return ErrUnknown
			}
		}
		if dev := props["DEVNAME"]; dev != "" {
			if strings.HasPrefix(dev, "/") || filepath.Clean(dev) != dev || dev == ".." || strings.HasPrefix(dev, "../") {
				return ErrUnknown
			}
			nodePath := filepath.Join("/dev", dev)
			node, e := unix.Open(nodePath, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if e != nil {
				return errors.Join(ErrUnknown, fmt.Errorf("original static device node absent: %s", dev), e)
			}
			entry.node = os.NewFile(uintptr(node), "original static device node")
			entry.nodeObject, e = normalFilesystemFDObject(node)
			if e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if len(c.ordered) == 0 {
		return nil, ErrUnknown
	}
	sort.Slice(c.ordered, func(i, j int) bool { return c.ordered[i].path < c.ordered[j].path })
	// 原规则的config从Inputs原RO pin交接，不放进只含exec/loader的system.files。
	c.rules, e = takeNormalOrdinaryUdevV2OriginalRules(ctx, system, rules)
	if e != nil {
		return nil, e
	}
	c.auditRules()
	good = true
	return c, nil
}

// 调用方持resource锁；这里仅短持System.mu复制不变库存，释放后才进入Inputs读者。
// 原RO字节/FD的完整检查不是规则语义证明；实际stock Wait与sealed projection仍由prep另核。
func takeNormalOrdinaryUdevV2OriginalRules(ctx context.Context, system *normalSystemLifecycleSource, reader normalOrdinaryUdevV2OriginalRuleReader) (out []normalOrdinaryUdevV2RuleSource, err error) {
	if ctx == nil || ctx.Err() != nil || system == nil || reader == nil {
		return nil, ErrUnknown
	}
	system.mu.Lock()
	if system.closed {
		system.mu.Unlock()
		return nil, ErrUnknown
	}
	expected := map[string]NormalSystemSourceFile{}
	for _, entry := range system.manifest.Files {
		if !strings.HasSuffix(entry.Path, ".rules") {
			continue
		}
		if entry.Kind != "config" || entry.Path == "" || entry.CanonicalPath == "" || !physicalDigestValid(entry.SHA256) || entry.Bytes > 256<<10 {
			system.mu.Unlock()
			return nil, ErrUnknown
		}
		if _, exists := expected[entry.Path]; exists {
			system.mu.Unlock()
			return nil, ErrUnknown
		}
		expected[entry.Path] = entry
	}
	system.mu.Unlock()
	if len(expected) == 0 || len(expected) > 512 {
		return nil, ErrUnknown
	}
	rules, e := reader.duplicateOriginalRules(ctx)
	good := false
	defer func() {
		if !good {
			for _, rule := range rules {
				if rule.original != nil {
					rule.original.Close()
				}
			}
		}
	}()
	if e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if len(rules) != len(expected) {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original complete rule inventory count differs"))
	}
	seen := map[string]bool{}
	for _, rule := range rules {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		entry, exists := expected[rule.path]
		if !exists || seen[rule.path] || rule.sha != entry.SHA256 || rule.original == nil || uint64(len(rule.raw)) != entry.Bytes {
			return nil, errors.Join(ErrUnknown, fmt.Errorf("original rule inventory path/hash/kind differs"))
		}
		seen[rule.path] = true
		fd := int(rule.original.Fd())
		var st unix.Stat_t
		if e = unix.Fstat(fd, &st); e != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Size < 0 || uint64(st.Size) != entry.Bytes {
			return nil, errors.Join(ErrUnknown, e)
		}
		flags, x := unix.FcntlInt(rule.original.Fd(), unix.F_GETFL, 0)
		if x != nil || flags&unix.O_ACCMODE != unix.O_RDONLY || flags&unix.O_PATH != 0 {
			return nil, errors.Join(ErrUnknown, x)
		}
		flags, x = unix.FcntlInt(rule.original.Fd(), unix.F_GETFD, 0)
		if x != nil || flags&unix.FD_CLOEXEC == 0 {
			return nil, errors.Join(ErrUnknown, x)
		}
		object, x := normalFilesystemFDObject(fd)
		if x != nil || !object.Readonly {
			return nil, errors.Join(ErrUnknown, x)
		}
		raw, x := normalOrdinaryUdevV2ReadFD(rule.original, 256<<10)
		if x != nil || !bytes.Equal(raw, rule.raw) || physicalDigest(raw) != entry.SHA256 {
			return nil, errors.Join(ErrUnknown, x)
		}
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].path < rules[j].path })
	// 不保留调用者可变slice/raw，原dup本身由catalog唯一接管。
	out = make([]normalOrdinaryUdevV2RuleSource, len(rules))
	for i, rule := range rules {
		rule.raw = bytes.Clone(rule.raw)
		out[i] = rule
	}
	good = true
	return out, nil
}

// 只有同g原stock prep opaque可安装全表，不能从runtime可写DB或调用者map补签。
func (c *normalOrdinaryUdevV2Catalog) installPreparationLocked(ctx context.Context, state *lockedState, prepared normalOrdinaryUdevV2Preparation) error {
	if c == nil || prepared == nil {
		return ErrUnknown
	}
	for _, entry := range c.ordered {
		projection, e := prepared.projectionForOriginalDeviceLocked(ctx, state, entry)
		if e != nil {
			return e
		}
		if projection.Devpath != entry.path || projection.Subsystem != entry.subsystem || projection.UeventObject != entry.object {
			return errors.Join(ErrUnknown, fmt.Errorf("original stock projection device differs"))
		}
		if _, e = normalOrdinaryUdevV2CanonicalProperties(projection.Properties); e != nil {
			return e
		}
		entry.projection = normalOrdinaryUdevV2DeviceProjection{Devpath: projection.Devpath, Subsystem: projection.Subsystem, UeventObject: projection.UeventObject, Properties: map[string]string{}, DynamicProperties: append([]normalOrdinaryUdevV2DynamicProperty(nil), projection.DynamicProperties...)}
		for key, value := range projection.Properties {
			entry.projection.Properties[key] = value
		}
		seen := map[normalOrdinaryUdevV2DynamicProperty]bool{}
		for _, dynamic := range projection.DynamicProperties {
			if dynamic != normalOrdinaryUdevV2DynamicInitializedMonotonicUsec || seen[dynamic] {
				return ErrUnknown
			}
			seen[dynamic] = true
		}
	}
	// 原规则实际执行/原Wait与完整投影由opaque另核；这里不把SHA自行当语义证明。
	c.unresolved = nil
	return nil
}

func (c *normalOrdinaryUdevV2Catalog) auditRules() {
	// 具体未证语义逐行留痕，绝不以“只读”泛许IMPORT/ATTR/RUN或未知ENV。
	for _, rule := range c.rules {
		for line, text := range strings.Split(string(rule.raw), "\n") {
			text = strings.TrimSpace(text)
			if text == "" || strings.HasPrefix(text, "#") {
				continue
			}
			if strings.Contains(text, "IMPORT{") || strings.Contains(text, "RUN") || strings.Contains(text, "ATTR{") && strings.Contains(text, "}=\"") || strings.Contains(text, "NAME=") {
				c.unresolved = append(c.unresolved, fmt.Sprintf("%s:%d unresolved rule behavior", rule.path, line+1))
			}
		}
	}
}
func (c *normalOrdinaryUdevV2Catalog) verify() error {
	if c == nil {
		return ErrUnknown
	}
	for _, e := range c.ordered {
		o, x := normalFilesystemFDObject(int(e.uevent.Fd()))
		if x != nil || o != e.object {
			return errors.Join(ErrUnknown, x)
		}
		if e.node != nil {
			o, x = normalFilesystemFDObject(int(e.node.Fd()))
			if x != nil || o != e.nodeObject {
				return errors.Join(ErrUnknown, x)
			}
		}
	}
	for _, r := range c.rules {
		raw, e := normalOrdinaryUdevV2ReadFD(r.original, 256<<10)
		if e != nil || physicalDigest(raw) != r.sha {
			return errors.Join(ErrUnknown, e)
		}
	}
	return nil
}
func (c *normalOrdinaryUdevV2Catalog) originalTicket(ticket *normalOrdinaryUdevV2Ticket) (*normalOrdinaryUdevV2CatalogEntry, error) {
	if c == nil || ticket == nil {
		return nil, ErrUnknown
	}
	path := ticket.properties["DEVPATH"]
	if e := c.entries[path]; e != nil {
		if ticket.properties["SUBSYSTEM"] != e.subsystem {
			return nil, ErrUnknown
		}
		for _, key := range []string{"DEVNAME", "MAJOR", "MINOR", "DEVTYPE"} {
			if ticket.properties[key] != e.properties[key] {
				return nil, ErrUnknown
			}
		}
		return e, nil
	}
	if ticket.properties["SUBSYSTEM"] == "memory" && strings.HasPrefix(path, "/devices/system/memory/memory") {
		n, e := strconv.ParseUint(strings.TrimPrefix(path, "/devices/system/memory/memory"), 10, 32)
		if e == nil && strconv.FormatUint(n, 10) == strings.TrimPrefix(path, "/devices/system/memory/memory") && c.memoryIndex(uint32(n)) {
			return &normalOrdinaryUdevV2CatalogEntry{path: path, subsystem: "memory", name: "memory" + strconv.FormatUint(n, 10)}, nil
		}
	}
	return nil, errors.Join(ErrUnknown, fmt.Errorf("kernel event is outside original complete catalog and maximum memory geometry"))
}
func (c *normalOrdinaryUdevV2Catalog) processed(ticket *normalOrdinaryUdevV2Ticket, request normalOrdinaryUdevV2Request) error {
	entry, e := c.originalTicket(ticket)
	if e != nil {
		return e
	}
	if request.Sequence != ticket.sequence {
		return ErrUnknown
	}
	for key, value := range ticket.properties {
		expectedKernel := value
		if key == "DEVNAME" && value != "" && !strings.HasPrefix(value, "/") {
			expectedKernel = "/dev/" + value
		}
		if request.Properties[key] != expectedKernel {
			return errors.Join(ErrUnknown, fmt.Errorf("processed original kernel property differs: %s", key))
		}
	}
	switch request.Kind {
	case 1:
		if entry.subsystem != "virtio-ports" || entry.virtioPortName != "tech.neon.log.0" || request.Index != 0 {
			return ErrUnknown
		}
	case 2:
		if entry.subsystem != "memory" || entry.name != "memory"+strconv.FormatUint(uint64(request.Index), 10) || !c.memoryIndex(request.Index) {
			return ErrUnknown
		}
	case 3:
		if request.Index != 0 || entry.subsystem == "memory" || entry.virtioPortName == "tech.neon.log.0" {
			return ErrUnknown
		}
	default:
		return ErrUnknown
	}
	if len(c.unresolved) > 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("original complete rule semantics unresolved: %s", c.unresolved[0]))
	}
	projection := entry.projection
	if projection.Properties == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original stock device projection absent"))
	}
	dynamicInitialized := false
	for _, kind := range projection.DynamicProperties {
		if kind == normalOrdinaryUdevV2DynamicInitializedMonotonicUsec {
			dynamicInitialized = true
		}
	}
	for key, value := range request.Properties {
		if _, kernel := ticket.properties[key]; kernel {
			continue
		}
		if key == "USEC_INITIALIZED" && dynamicInitialized {
			v, e := strconv.ParseUint(value, 10, 64)
			var clock unix.Timespec
			if e != nil || v == 0 || strconv.FormatUint(v, 10) != value || unix.ClockGettime(unix.CLOCK_MONOTONIC, &clock) != nil {
				return ErrUnknown
			}
			now := uint64(clock.Sec)*1000000 + uint64(clock.Nsec)/1000
			if v > now {
				return ErrUnknown
			}
			continue
		}
		expected, ok := projection.Properties[key]
		if !ok || expected != value {
			return errors.Join(ErrUnknown, fmt.Errorf("processed property differs from original sealed stock projection: %s", key))
		}
	}
	for key, value := range projection.Properties {
		if key == "ACTION" || key == "SEQNUM" || key == "SYNTH_UUID" {
			continue
		}
		if _, kernel := ticket.properties[key]; kernel {
			continue
		}
		if key == "USEC_INITIALIZED" && dynamicInitialized {
			if request.Properties[key] == "" {
				return ErrUnknown
			}
			continue
		}
		if request.Properties[key] != value {
			return errors.Join(ErrUnknown, fmt.Errorf("required original stock property absent: %s", key))
		}
	}

	return nil
}
func (c *normalOrdinaryUdevV2Catalog) close() {
	if c == nil {
		return
	}
	for _, e := range c.ordered {
		if e.uevent != nil {
			e.uevent.Close()
		}
		if e.node != nil {
			e.node.Close()
		}
	}
	for _, r := range c.rules {
		if r.original != nil {
			r.original.Close()
		}
	}
}
