//go:build linux

package resourceguard

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type arrayPopulateImageSpan struct{ Start, End uint64 }

// 原helper必须是受信只读manifest中的静态ET_EXEC。复用已审raw ELF检查后，
// 这里只收窄到C自身mlock使用的最多16个PHDR；不接受PIE重定位或动态加载器。
func arrayPopulateLockedImage(f *os.File) ([]arrayPopulateImageSpan, uint64, error) {
	meta, err := nativeBirthParseELF(f)
	if err != nil || meta.Interpreter != "" || len(meta.Needed) != 0 {
		return nil, 0, fmt.Errorf("original helper must have bounded static ELF metadata")
	}
	var h [64]byte
	if _, err = f.ReadAt(h[:], 0); err != nil {
		return nil, 0, err
	}
	order := binary.LittleEndian
	count := uint64(order.Uint16(h[56:58]))
	off := order.Uint64(h[32:40])
	if order.Uint16(h[16:18]) != 2 || count == 0 || count > 16 || order.Uint16(h[54:56]) != 56 {
		return nil, 0, fmt.Errorf("helper exact ET_EXEC/PHDR geometry required")
	}
	var spans []arrayPopulateImageSpan
	var total uint64
	for i := uint64(0); i < count; i++ {
		var raw [56]byte
		if off > math.MaxInt64-i*56 {
			return nil, 0, fmt.Errorf("helper PHDR offset overflow")
		}
		if _, err = f.ReadAt(raw[:], int64(off+i*56)); err != nil {
			return nil, 0, err
		}
		kind := order.Uint32(raw[:4])
		if kind == 2 || kind == 3 {
			return nil, 0, fmt.Errorf("helper cannot contain DYNAMIC or INTERP")
		}
		if kind != 1 {
			continue
		}
		base, size := order.Uint64(raw[16:24]), order.Uint64(raw[40:48])
		if size == 0 || base > math.MaxUint64-size || base+size > math.MaxUint64-4095 {
			return nil, 0, fmt.Errorf("helper LOAD span overflow")
		}
		span := arrayPopulateImageSpan{base &^ uint64(4095), (base + size + 4095) &^ uint64(4095)}
		if span.Start == 0 || total > math.MaxUint64-(span.End-span.Start) {
			return nil, 0, fmt.Errorf("helper LOAD total overflow")
		}
		total += span.End - span.Start
		spans = append(spans, span)
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].Start < spans[j].Start })
	if total == 0 || total > 1<<20 {
		return nil, 0, fmt.Errorf("helper locked image outside supported bound")
	}
	for i := 1; i < len(spans); i++ {
		if spans[i].Start < spans[i-1].End {
			return nil, 0, fmt.Errorf("helper LOAD pages overlap")
		}
	}
	return spans, total, nil
}

type arrayPopulateSmapsSpan struct {
	Start, End, RSS, Locked        uint64
	Flags                          map[string]bool
	haveRSS, haveLocked, haveFlags bool
}

// smaps仅核实固定helper映射的mlock执行结果，不把RSS当预算。Locked是PSS口径，
// 共享文件页可小于映射大小；必须逐VMA有lo且无lf，并确实resident，不能仅核总VmLck。
func arrayPopulateVerifyLockedSmaps(raw string, image []arrayPopulateImageSpan, expected uint64, targets [4][2]uint64) error {
	if len(raw) > 1<<20 || len(image) == 0 || expected == 0 {
		return fmt.Errorf("helper bounded smaps/image required")
	}
	var spans []arrayPopulateSmapsSpan
	for _, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return fmt.Errorf("helper smaps empty line")
		}
		if ends := strings.Split(fields[0], "-"); len(ends) == 2 && len(fields) >= 5 {
			a, e1 := strconv.ParseUint(ends[0], 16, 64)
			b, e2 := strconv.ParseUint(ends[1], 16, 64)
			if e1 != nil || e2 != nil || a >= b || a%4096 != 0 || b%4096 != 0 || len(fields[1]) != 4 || len(spans) > 0 && a < spans[len(spans)-1].End {
				return fmt.Errorf("helper smaps range invalid")
			}
			spans = append(spans, arrayPopulateSmapsSpan{Start: a, End: b, Flags: map[string]bool{}})
			continue
		}
		if len(spans) == 0 {
			return fmt.Errorf("helper smaps missing mapping header")
		}
		s := &spans[len(spans)-1]
		switch fields[0] {
		case "Rss:", "Locked:":
			if len(fields) != 3 || fields[2] != "kB" {
				return fmt.Errorf("helper smaps size malformed")
			}
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || n > math.MaxUint64/1024 {
				return fmt.Errorf("helper smaps size overflow")
			}
			n *= 1024
			if fields[0] == "Rss:" {
				if s.haveRSS {
					return fmt.Errorf("duplicate helper smaps RSS")
				}
				s.haveRSS = true
				s.RSS = n
			} else {
				if s.haveLocked {
					return fmt.Errorf("duplicate helper smaps Locked")
				}
				s.haveLocked = true
				s.Locked = n
			}
		case "VmFlags:":
			if s.haveFlags {
				return fmt.Errorf("duplicate helper smaps flags")
			}
			s.haveFlags = true
			for _, flag := range fields[1:] {
				if s.Flags[flag] {
					return fmt.Errorf("duplicate helper VMA flag")
				}
				s.Flags[flag] = true
			}
		}
	}
	var locked uint64
	for _, s := range spans {
		if !s.haveRSS || !s.haveLocked || !s.haveFlags || s.RSS > s.End-s.Start || s.Locked > s.RSS {
			return fmt.Errorf("helper smaps mapping facts missing or excessive")
		}
		for _, t := range targets {
			if t[1] != 0 && s.Start < t[0]+t[1] && t[0] < s.End && s.Flags["lo"] {
				return fmt.Errorf("helper must not mlock array target")
			}
		}
		if !s.Flags["lo"] {
			continue
		}
		if s.Flags["lf"] || s.RSS != s.End-s.Start {
			return fmt.Errorf("helper image must be fully resident ordinary mlock")
		}
		covered := false
		for _, p := range image {
			if p.Start <= s.Start && s.End <= p.End {
				covered = true
				break
			}
		}
		if !covered {
			return fmt.Errorf("helper locked VMA escapes original LOAD pages")
		}
		locked += s.End - s.Start
	}
	if locked != expected {
		return fmt.Errorf("helper exact locked LOAD total differs")
	}
	for _, p := range image {
		cursor := p.Start
		for _, s := range spans {
			if s.Start <= cursor && cursor < s.End {
				if !s.Flags["lo"] || s.Flags["lf"] || s.RSS != s.End-s.Start {
					return fmt.Errorf("helper LOAD page not resident and locked")
				}
				cursor = min(p.End, s.End)
				if cursor == p.End {
					break
				}
			}
		}
		if cursor != p.End {
			return fmt.Errorf("helper LOAD coverage missing")
		}
	}
	return nil
}
