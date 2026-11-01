package resourceguard

import (
	"encoding/binary"
	"fmt"
)

const normalPGMemoryControlScope = "original-pg17-memory-syscall-controls-v1-non-authorizing"

// 这里只承载来源引用；无JSON→opaque构造器、路径/预算参数或verified开关。
// 完整外域控制仍由Root组合真正opaque FS/System/Kernel来源，不能拿本引用发normal票据。
type normalPGMemoryControlReference struct {
	ProtocolVersion         uint32               `json:"protocol_version"`
	Scope                   string               `json:"scope"`
	Birth                   NativeBirthRef       `json:"birth"`
	ReleasedSHA256          string               `json:"released_sha256"`
	AuditSHA256             string               `json:"audit_sha256"`
	PGELFSHA256             string               `json:"pg_elf_sha256"`
	ResourceReferenceSHA256 string               `json:"resource_reference_sha256"`
	RegistrationNonce       string               `json:"registration_nonce"`
	MappingGeneration       uint64               `json:"mapping_generation"`
	PGBootID                uint64               `json:"pg_boot_id"`
	ProgramInstructions     [3]int               `json:"program_instructions"`
	Files                   [4]arrayPopulateFile `json:"files"`
}

// 在原完整字节已由NativeFinal内核读回匹配的基础上逐指令审核支持集。
// 不认识的opcode、内存槽、跳转/return一律Unknown，不能只核len/自报hash。
func normalPGMemoryControlValidateProgram(p []nativeFilterInstruction) error {
	if len(p) == 0 || len(p) > nativeAuditMaxInstructions {
		return fmt.Errorf("PG control BPF length invalid")
	}
	for i, x := range p {
		if x.Code != 0x15 && x.Code != 0x25 && x.Code != 0x35 && x.Code != 0x3d && x.Code != 0x45 && (x.JT != 0 || x.JF != 0) {
			return fmt.Errorf("PG BPF nonbranch jump fields at %d", i)
		}
		switch x.Code {
		case 0x20:
			// 原三个程序只读取arch/nr/args，不使用IP。拒绝IP条件程序，不能以
			// 解释器固定IP=0且未读args推导任意执行地址上的无条件拒绝。
			if x.K > 60 || x.K%4 != 0 || x.K == 8 || x.K == 12 {
				return fmt.Errorf("PG BPF absolute load at %d", i)
			}
		case 0x60, 0x61, 0x02:
			if x.K >= 16 {
				return fmt.Errorf("PG BPF scratch slot at %d", i)
			}
		case 0x00, 0x54, 0x4c, 0x04, 0x0c, 0x07:
		case 0x05:
			if uint64(i)+1+uint64(x.K) >= uint64(len(p)) {
				return fmt.Errorf("PG BPF JA range at %d", i)
			}
		case 0x15, 0x25, 0x35, 0x3d, 0x45:
			if i+1+int(x.JT) >= len(p) || i+1+int(x.JF) >= len(p) {
				return fmt.Errorf("PG BPF branch range at %d", i)
			}
		case 0x06:
			if x.K != 0x7fff0000 && x.K != 0x50001 && x.K != 0x50026 && x.K != 0x80000000 {
				return fmt.Errorf("PG BPF return at %d", i)
			}
		default:
			return fmt.Errorf("unsupported PG BPF instruction %d opcode %#x", i, x.Code)
		}
	}
	return nil
}

// 小解释器只检验已锁源完整cBPF，不替代内核安装事实。readArgs为实际路径读过的
// 参数位图；无参数读取的EPERM路径严格与任意64bit参数无关，并非采样args=0猜结论。
func normalPGMemoryControlEvaluate(p []nativeFilterInstruction, nr uint32, args [6]uint64) (uint32, uint8, error) {
	if err := normalPGMemoryControlValidateProgram(p); err != nil {
		return 0, 0, err
	}
	var data [64]byte
	binary.LittleEndian.PutUint32(data[:4], nr)
	binary.LittleEndian.PutUint32(data[4:8], 0xc000003e)
	for i, a := range args {
		binary.LittleEndian.PutUint64(data[16+8*i:24+8*i], a)
	}
	var a, x uint32
	var mem [16]uint32
	var reads uint8
	for pc, steps := 0, 0; pc < len(p) && steps < len(p); steps++ {
		in := p[pc]
		pc++
		switch in.Code {
		case 0x20:
			a = binary.LittleEndian.Uint32(data[in.K : in.K+4])
			if in.K >= 16 {
				reads |= 1 << ((in.K - 16) / 8)
			}
		case 0x60:
			a = mem[in.K]
		case 0x61:
			x = mem[in.K]
		case 0x02:
			mem[in.K] = a
		case 0x00:
			a = in.K
		case 0x54:
			a &= in.K
		case 0x4c:
			a |= x
		case 0x04:
			a += in.K
		case 0x0c:
			a += x
		case 0x07:
			x = a
		case 0x05:
			pc += int(in.K)
		case 0x15, 0x25, 0x35, 0x3d, 0x45:
			yes := false
			switch in.Code {
			case 0x15:
				yes = a == in.K
			case 0x25:
				yes = a > in.K
			case 0x35:
				yes = a >= in.K
			case 0x3d:
				yes = a >= x
			case 0x45:
				yes = a&in.K != 0
			}
			if yes {
				pc += int(in.JT)
			} else {
				pc += int(in.JF)
			}
		case 0x06:
			return in.K, reads, nil
		}
	}
	return 0, reads, fmt.Errorf("PG control BPF did not terminate")
}

// amd64固定内核ABI；该集合逐完整程序路径验证，不能从PG代码通常不调用推断。
var normalPGMemoryControlDeniedSyscalls = []struct {
	Name   string
	Number uint32
}{
	{"execve", 59}, {"execveat", 322}, {"vfork", 58}, {"mremap", 25}, {"remap_file_pages", 216},
	{"unshare", 272}, {"setns", 308}, {"memfd_create", 319}, {"userfaultfd", 323}, {"ptrace", 101},
	{"process_vm_readv", 310}, {"process_vm_writev", 311}, {"pidfd_getfd", 438},
	{"io_uring_setup", 425}, {"io_uring_enter", 426}, {"io_uring_register", 427},
	{"vmsplice", 278}, {"splice", 275}, {"tee", 276}, {"sendfile", 40}, {"mlock", 149}, {"mlock2", 325}, {"mlockall", 151}, {"recvmmsg", 299},
	{"mbind", 237}, {"set_mempolicy", 238}, {"move_pages", 279}, {"migrate_pages", 256}, {"set_mempolicy_home_node", 450}, {"process_madvise", 440}, {"process_mrelease", 448},
}

func normalPGMemoryControlReviewBundle(bundle nativeFilterAuditBundle) error {
	if err := validateNativeBirthRequestValue(bundle.Request); err != nil {
		return err
	}
	for kind, p := range bundle.Programs {
		if err := normalPGMemoryControlValidateProgram(p); err != nil {
			return err
		}
		for _, call := range normalPGMemoryControlDeniedSyscalls {
			result, reads, err := normalPGMemoryControlEvaluate(p, call.Number, [6]uint64{})
			if err != nil || result != 0x50001 || reads != 0 {
				return fmt.Errorf("PG program%d does not unconditionally deny %s: action=%#x args=%#x: %v", kind+1, call.Name, result, reads, err)
			}
		}
		for _, advice := range []uint64{14, 25, 20, 21} {
			result, reads, err := normalPGMemoryControlEvaluate(p, 28, [6]uint64{0x10000, 4096, advice})
			if err != nil || result != 0x50001 || reads & ^uint8(4) != 0 {
				return fmt.Errorf("PG program%d advice%d not denied before address paths", kind+1, advice)
			}
		}
		if kind >= 1 {
			result, reads, err := normalPGMemoryControlEvaluate(p, 47, [6]uint64{})
			if err != nil || result != 0x50001 || reads != 0 {
				return fmt.Errorf("PG final/child must deny recvmsg")
			}
		}
	}
	return nil
}
