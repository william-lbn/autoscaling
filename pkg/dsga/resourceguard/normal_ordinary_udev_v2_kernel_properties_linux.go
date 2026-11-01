//go:build linux

package resourceguard

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// 仅供原kernel group1或已核Root sender的stock monitor解释Linux CPU原属性。
// 这是字节语义适配，不授来源：原credentials、原catalog及逐sequence投影仍必须全核。
// Linux6.12.26 cpu_uevent的MODALIAS自带单LF，libudev NUL parser原样保留。
// 公共SCM/wire/DB parser不调用此函数；原raw保留，不原地改事件证据。
func normalOrdinaryUdevV2CPUPropertyBytes(raw []byte, maximum int) ([]byte, error) {
	if maximum <= 0 || maximum > normalOrdinaryUdevV2MaximumBody || len(raw) == 0 || len(raw) > maximum || raw[len(raw)-1] != 0 {
		return nil, ErrUnknown
	}
	if !bytes.Contains(raw, []byte{'\n'}) {
		return raw, nil
	}
	var path, subsystem string
	var paths, subsystems, aliases int
	offset, remove := -1, -1
	for _, field := range bytes.Split(raw[:len(raw)-1], []byte{0}) {
		offset++
		key, value, ok := strings.Cut(string(field), "=")
		if !ok {
			return nil, ErrUnknown
		}
		switch key {
		case "DEVPATH":
			paths++
			path = value
		case "SUBSYSTEM":
			subsystems++
			subsystem = value
		case "MODALIAS":
			aliases++
			if !strings.HasPrefix(value, "cpu:type:") || !strings.HasSuffix(value, "\n") || strings.Count(value, "\n") != 1 {
				return nil, ErrUnknown
			}
			remove = offset + len(field) - 1
		}
		if key != "MODALIAS" && strings.ContainsRune(value, '\n') {
			return nil, ErrUnknown
		}
		offset += len(field)
	}
	index := strings.TrimPrefix(path, "/devices/system/cpu/cpu")
	n, e := strconv.ParseUint(index, 10, 32)
	if paths != 1 || subsystems != 1 || aliases != 1 || subsystem != "cpu" || index == path || e != nil || strconv.FormatUint(n, 10) != index || remove < 0 || remove >= len(raw) || raw[remove] != '\n' {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original single-LF property is not exact CPU modalias"))
	}
	normalized := make([]byte, 0, len(raw)-1)
	normalized = append(normalized, raw[:remove]...)
	normalized = append(normalized, raw[remove+1:]...)
	return normalized, nil
}
func normalOrdinaryUdevV2KernelProperties(raw []byte) (map[string]string, error) {
	p, e := normalOrdinaryUdevV2CPUPropertyBytes(raw, normalOrdinaryUdevV2MaximumBody)
	if e != nil {
		return nil, e
	}
	return normalOrdinaryUdevV2Properties(p, false)
}
