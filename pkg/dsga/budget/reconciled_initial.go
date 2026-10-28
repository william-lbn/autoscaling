package budget

import (
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 初始来源只覆盖原启动容量/PG代次及原root，不能拿原启动证书覆盖一次
// 已改变的Guest限额、较大VM或另一boot。v2保留真实原VM终态，v3独立保留
// 从未执行命令的原Runner启动来源，不伪造“初始化VM完成”。网络认证由Reader执行。
func (c ReconciledCapacity) validateInitialOutside() error {
	i := c.InitialOutside
	var noRef ObjectRef
	var noRequest control.Request
	birth := c.VM.Sources.BirthClaim
	if c.InitialVM != nil {
		birth = c.InitialVM.BirthClaim
	}
	if i == nil || i.Validate() != nil || (c.ProtocolVersion != 2 && c.ProtocolVersion != 3) || c.OutsideClaim != noRef || c.OutsideRequest != noRequest ||
		i.Instance != c.Request.Key.Instance || i.IssuerEpoch > c.IssuerEpoch || i.VMMemoryBytes != c.VMMemoryBytes || i.VMMemoryBytes != c.VMBeforeMemoryBytes ||
		i.RunnerPodName != c.RunnerPodName || i.RunnerRestartCount != c.RunnerRestartCount || i.RunnerBirth != birth ||
		i.Shared.Observed.CapacityGeneration != c.CapacityGeneration || i.Shared.Observed.HighBuffers != c.Buffers || i.Shared.Observed.MaximumBuffers != c.MaximumBuffers ||
		i.Shared.StartupMainUpperBytes != c.StartupMainUpperBytes || i.Shared.StartupArraysUpperBytes != c.StartupArraysUpperBytes ||
		i.PGAggregate.Bound != c.PGAggregate || c.RetainedSharedUpperBytes > i.PGAggregate.Bound.UpperBytes || c.GuestUsableMemoryBytes != i.GuestUsableMemoryBytes ||
		c.ResidualProfileID != "qualified-"+i.Qualification.SHA256 || i.SystemOutsidePG != c.SystemOutsidePG || i.Transition != c.Transition || i.Safety != c.Safety {
		return fmt.Errorf("reconciled initial outside differs from original complete boot budget")
	}
	digest, err := RootDigest(i.PGAggregate)
	if err != nil || digest != c.RootSHA256 {
		return fmt.Errorf("reconciled initial source changed original root constraints")
	}
	return nil
}

// ReconcileInitial复用同一完整预算算术，但明确选择原Initial来源；输入不得
// 同时夹带Outside CM或改写的Resize请求。返回值深拷贝原Initial及其slice/
// 指针；后续源寿命由永久实际预算持有，不依赖被原子替换的InitialHeld头。
func ReconcileInitial(inputs ReconcileInputs, initial Initial) (ReconciledCapacity, error) {
	raw, err := json.Marshal(initial)
	if err != nil {
		return ReconciledCapacity{}, err
	}
	var owned Initial
	if err := json.Unmarshal(raw, &owned); err != nil {
		return ReconciledCapacity{}, err
	}
	return reconcile(inputs, &owned)
}

// Request是严格解码的领域类型，空struct即使带omitempty也会写成畸形
// “请求”，读回时被拒绝。v2不存在OutsideRequest，必须真正省略两个旧
// 字段；v1逐字保留原字段形状。若调用者混入非空旧来源则不隐藏它，交
// 严格Validate拒绝，不能让序列化替调用者擦掉冲突后变成合法预算。
func (c ReconciledCapacity) MarshalJSON() ([]byte, error) {
	type plain ReconciledCapacity
	var noRef ObjectRef
	var noRequest control.Request
	var noNative NativeCapacity
	if c.ProtocolVersion == 3 && c.InitialVM != nil && c.VM == noNative && c.InitialOutside != nil && c.OutsideClaim == noRef && c.OutsideRequest == noRequest {
		// 缺失native_vm字段才表示没有Resize来源；空NativeCapacity内的
		// Request也是严格类型，不能输出一条零值“原命令”再期待读者忽略。
		return json.Marshal(struct {
			plain
			OutsideClaim   *ObjectRef       `json:"outside_claim,omitempty"`
			OutsideRequest *control.Request `json:"outside_request,omitempty"`
			VM             *NativeCapacity  `json:"native_vm,omitempty"`
		}{plain: plain(c), OutsideClaim: nil, OutsideRequest: nil, VM: nil})
	}
	if c.ProtocolVersion == 2 && c.InitialOutside != nil && c.OutsideClaim == noRef && c.OutsideRequest == noRequest {
		return json.Marshal(struct {
			plain
			OutsideClaim   *ObjectRef       `json:"outside_claim,omitempty"`
			OutsideRequest *control.Request `json:"outside_request,omitempty"`
		}{plain: plain(c), OutsideClaim: nil, OutsideRequest: nil})
	}
	return json.Marshal(plain(c))
}
