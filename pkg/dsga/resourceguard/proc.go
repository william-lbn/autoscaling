package resourceguard

import (
	"fmt"
	"strconv"
	"strings"
)

func processStatFields(text string, pid int) ([]string, error) {
	// comm可包含空格和括号，从最后一个')'分割；starttime为第22字段，即后缀索引19。
	end := strings.LastIndex(text, ")")
	start := strings.Index(text, " (")
	if end < 0 || start < 0 || text[:start] != strconv.Itoa(pid) {
		return nil, fmt.Errorf("invalid /proc stat")
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		return nil, fmt.Errorf("short /proc stat")
	}
	return fields, nil
}

func postmasterTicks(text string, pid int) (uint64, error) {
	fields, err := processStatFields(text, pid)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid process starttime")
	}
	return n, nil
}
