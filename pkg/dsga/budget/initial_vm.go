package budget

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// InitialVMSources只描述原Runner从未执行内存命令的启动容量，不是Resize
// 回执。没有Request、命令SHA或Receipt UID；真实读者必须重新核原Birth/
// State、当前owner floor、无Pending/Last/HighWater及requested==actual。
// 字段均可比较，供Node持久关闭水位按值比较，不能按解码后的指针地址认同。
type InitialVMSources struct {
	Instance       control.Instance `json:"instance"`
	BirthClaim     ObjectRef        `json:"birth_claim"`
	StateClaim     ObjectRef        `json:"state_claim"`
	IdentitySHA256 string           `json:"identity_sha256"`
	MemoryBytes    control.Uint64   `json:"memory_bytes"`
}

func (v InitialVMSources) Validate() error {
	if v.Instance.Validate() != nil || v.BirthClaim.Validate() != nil || v.StateClaim.Validate() != nil || !unchangedDigest(v.IdentitySHA256) || v.MemoryBytes == 0 {
		return fmt.Errorf("incomplete original initial VM capacity source")
	}
	return nil
}

// MatchInitial排除同数值的另一boot/PG/Runner。State UID仍由原Birth给出并由
// Reader认证；本函数不把纯结构校验提升为网络来源或Host释放许可。
func (v InitialVMSources) MatchInitial(i Initial) error {
	if v.Validate() != nil || i.Validate() != nil || v.Instance != i.Instance || v.BirthClaim != i.RunnerBirth || v.IdentitySHA256 != i.RunnerIdentitySHA256 || v.MemoryBytes != i.VMMemoryBytes {
		return fmt.Errorf("initial VM source changed original complete boot budget")
	}
	return nil
}

func (v *InitialVMSources) UnmarshalJSON(raw []byte) error {
	type plain InitialVMSources
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*v = InitialVMSources(p)
	return v.Validate()
}

func (c ReconciledCapacity) validateVMSource() error {
	if c.InitialVM != nil {
		var noNative NativeCapacity
		if c.ProtocolVersion != 3 || c.VM != noNative || c.InitialOutside == nil || c.InitialVM.MatchInitial(*c.InitialOutside) != nil || c.InitialVM.Instance != c.Request.Key.Instance || c.InitialVM.MemoryBytes != c.VMMemoryBytes {
			return fmt.Errorf("initial VM recovery requires exclusive original boot source")
		}
	} else if c.ProtocolVersion == 3 || c.VM.Validate() != nil || c.VM.Request.Key.Instance != c.Request.Key.Instance || c.VM.MemoryBytes() != c.VMMemoryBytes {
		return fmt.Errorf("reconciled native VM source differs from actual capacity")
	}
	return nil
}
