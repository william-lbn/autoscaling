//go:build linux

package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

type initialBootHardware struct {
	SMPArgument               string `json:"smp_argument"`
	MaximumVCPUs              uint32 `json:"maximum_vcpus"`
	CPUInventorySHA256        string `json:"cpu_inventory_sha256"`
	PeripheralInventorySHA256 string `json:"peripheral_inventory_sha256"`
	UnattachedInventorySHA256 string `json:"unattached_inventory_sha256"`
}

func initialBootSMPFromCommand(cmd *exec.Cmd, required bool) (uint32, string, error) {
	if cmd == nil {
		return 0, "", fmt.Errorf("original command absent")
	}
	seen := false
	var value string
	for i := 1; i < len(cmd.Args); i++ {
		if cmd.Args[i] != "-smp" {
			continue
		}
		if seen || i+1 >= len(cmd.Args) {
			return 0, "", fmt.Errorf("duplicate/incomplete original SMP")
		}
		seen = true
		i++
		value = cmd.Args[i]
	}
	if !seen {
		if required {
			return 0, "", fmt.Errorf("explicit original SMP maximum required")
		}
		return 0, "", nil
	}
	fields, e := initialBootFields(value)
	if e != nil {
		return 0, "", e
	}
	if len(fields) != 5 || fields["sockets"] != "1" || fields["threads"] != "1" {
		return 0, "", fmt.Errorf("unsupported original SMP topology")
	}
	maximum, e := strconv.ParseUint(fields["maxcpus"], 10, 32)
	if e != nil || maximum == 0 || maximum > 64 || strconv.FormatUint(maximum, 10) != fields["maxcpus"] || fields["cpus"] != fields["maxcpus"] || fields["cores"] != fields["maxcpus"] {
		return 0, "", fmt.Errorf("original SMP is not fixed sysfs hardware maximum")
	}
	return uint32(maximum), value, nil
}
func validateInitialBootLaunch(cmd *exec.Cmd, r *Runner, bootArgs []string) error {
	if cmd == nil || r == nil || r.birth.InitialMode == nil {
		return fmt.Errorf("explicit original initial command required")
	}
	if _, e := initialBootArgv(cmd, r.birth.Identity, r.birth.InitialMode.OriginalBaseBytes); e != nil {
		return e
	}
	if _, _, e := initialBootSMPFromCommand(cmd, true); e != nil {
		return e
	}
	counts := map[string]int{}
	for i := 0; i < len(bootArgs); i += 2 {
		counts[bootArgs[i]+"\x00"+bootArgs[i+1]] = 0
	}
	monitors := 0
	for i := 1; i < len(cmd.Args); i++ {
		flag := cmd.Args[i]
		if flag == "-mon" || flag == "-balloon" || flag == "-mem-path" || flag == "-mem-prealloc" {
			return fmt.Errorf("extra monitor/balloon/memory path forbidden")
		}
		if flag == "-monitor" {
			if i+1 >= len(cmd.Args) || cmd.Args[i+1] != "none" {
				return fmt.Errorf("HMP monitor forbidden in initial mode")
			}
			monitors++
			i++
			continue
		}
		if flag == "-serial" && i+1 < len(cmd.Args) && strings.Contains(cmd.Args[i+1], "mon:") {
			return fmt.Errorf("serial HMP mux forbidden")
		}
		if flag != "-device" && flag != "-chardev" {
			continue
		}
		if i+1 >= len(cmd.Args) {
			return fmt.Errorf("incomplete original device")
		}
		i++
		value := cmd.Args[i]
		key := flag + "\x00" + value
		if _, ok := counts[key]; ok {
			counts[key]++
		}
		low := strings.ToLower(value)
		if flag == "-chardev" {
			if strings.Contains(low, "mux=on") || strings.Contains(low, "dsga_boot") && counts[key] == 0 {
				return fmt.Errorf("boot chardev replacement or monitor mux forbidden")
			}
			continue
		}
		driver, _, _ := strings.Cut(low, ",")
		if strings.HasPrefix(driver, "driver=") || strings.Contains(driver, "balloon") || strings.HasPrefix(driver, "xen") || driver == "pc-dimm" || driver == "nvdimm" || strings.Contains(driver, "pmem") {
			return fmt.Errorf("extra balloon/Xen/plugged memory driver forbidden")
		}
		if strings.Contains(low, "dsga_boot") && counts[key] == 0 {
			return fmt.Errorf("original boot bus/port differs")
		}
	}
	if monitors != 1 {
		return fmt.Errorf("exactly one explicit -monitor none required")
	}
	for _, n := range counts {
		if n != 1 {
			return fmt.Errorf("fixed original boot channel arguments missing/duplicated")
		}
	}
	return nil
}
func readInitialBootHardware(ctx context.Context, q *nativeQMP, p *initialBootProcess, source initialBootSource) (initialBootHardware, error) {
	var out initialBootHardware
	if source.SMPMaximum == 0 || source.SMPMaximum > 64 || source.SMPArgument == "" {
		return out, fmt.Errorf("original SMP source missing")
	}
	c, e := q.connect(ctx)
	if e != nil {
		return out, e
	}
	defer c.close()
	if e = p.checkPeer(c, source); e != nil {
		return out, e
	}
	cpu, e := c.execute("query-hotpluggable-cpus", nil)
	if e != nil {
		return out, e
	}
	if control.ValidateJSON(cpu, qmpFrameLimit, 4096) != nil {
		return out, fmt.Errorf("invalid hardware CPU inventory")
	}
	var cpus []struct {
		Type  string          `json:"type"`
		Count *uint64         `json:"vcpus-count"`
		Props json.RawMessage `json:"props"`
	}
	if json.Unmarshal(cpu, &cpus) != nil || cpus == nil || len(cpus) > 64 {
		return out, fmt.Errorf("hardware CPU slots missing/unbounded")
	}
	total := uint64(0)
	unique := map[string]bool{}
	for _, slot := range cpus {
		var props map[string]json.RawMessage
		if slot.Type == "" || slot.Count == nil || *slot.Count == 0 || *slot.Count > 64 || json.Unmarshal(slot.Props, &props) != nil || props == nil {
			return out, fmt.Errorf("hardware CPU slot incomplete")
		}
		for _, field := range []string{"socket-id", "core-id", "thread-id"} {
			var n uint64
			if json.Unmarshal(props[field], &n) != nil || n > 64 {
				return out, fmt.Errorf("hardware CPU topology incomplete")
			}
		}
		key := initialBootSHA(initialBootCanonical(props))
		if unique[key] {
			return out, fmt.Errorf("hardware CPU topology duplicated")
		}
		unique[key] = true
		total += *slot.Count
		if total > 64 {
			return out, fmt.Errorf("hardware CPU maximum exceeds fixed C64 baseline")
		}
	}
	if total != uint64(source.SMPMaximum) {
		return out, fmt.Errorf("QMP hardware maximum differs from original -smp")
	}
	inventory := func(path string) (string, error) {
		raw, e := c.execute("qom-list", struct {
			Path string `json:"path"`
		}{path})
		if e != nil {
			return "", e
		}
		if control.ValidateJSON(raw, qmpFrameLimit, 4096) != nil {
			return "", fmt.Errorf("invalid actual device inventory")
		}
		var objects []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &objects) != nil || objects == nil || len(objects) > 256 {
			return "", fmt.Errorf("actual device inventory missing/unbounded")
		}
		for _, o := range objects {
			low := strings.ToLower(o.Name + " " + o.Type)
			if strings.Contains(low, "balloon") || strings.Contains(low, "xen") {
				return "", fmt.Errorf("balloon/Xen device source is forbidden")
			}
		}
		return initialBootSHA(raw), nil
	}
	peripheral, e := inventory("/machine/peripheral")
	if e != nil {
		return out, e
	}
	unattached, e := inventory("/machine/unattached")
	if e != nil {
		return out, e
	}
	if e = p.checkPeer(c, source); e != nil {
		return out, e
	}
	return initialBootHardware{source.SMPArgument, source.SMPMaximum, initialBootSHA(cpu), peripheral, unattached}, nil
}
