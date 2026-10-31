package resourceguard

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// 内部record v3持久标记最近一次quota原命令。旧Guard只支持v1/v2，必须拒绝回滚；
// 对外View仍投影同一版本中性Original/Held/预算合同，不按PG主版本分叉。
// 该引用只指原WAL，结构合法不等于durable、Source资格或normal许可。
type normalSplitQuotaStateReference struct {
	ProtocolVersion uint32  `json:"protocol_version"`
	Command         Command `json:"command"`
	IntentSHA256    string  `json:"intent_sha256"`
}

func (r *record) validateNormalSplitQuotaState() error {
	if r.NormalSplitQuota == nil {
		if r.Version == 3 {
			return fmt.Errorf("quota v3 lacks original reference")
		}
		return nil
	}
	q := r.NormalSplitQuota
	if r.Version != 3 || q.ProtocolVersion != 1 || q.Command.Validate() != nil || !physicalDigestValid(q.IntentSHA256) || r.Original == nil || r.Bootstrap == nil || !r.Bootstrap.Applied || q.Command.Identity != r.Identity || q.Command.IssuerEpoch > r.Floor || q.Command.Revision > r.Original.Revision {
		return fmt.Errorf("quota record original reference differs")
	}
	if q.Command.PidsMaxTasks != r.taskLimit() || q.Command.SystemMemoryMaxBytes != r.systemLimit() || q.Command.AddressSpaceMaxBytes != r.addressSpaceLimit() {
		return fmt.Errorf("quota reference changed immutable bootstrap bounds")
	}
	if q.Command.Revision == r.Original.Revision && !reflect.DeepEqual(q.Command, *r.Original) {
		return fmt.Errorf("same quota revision changed original command")
	}
	if r.Held == nil || r.Held.Revision < q.Command.Revision {
		if r.Applied || !reflect.DeepEqual(q.Command, *r.Original) {
			return fmt.Errorf("pending quota reference lost original resource intent")
		}
	} else if r.Held.MemoryMaxBytes < q.Command.MemoryMaxBytes {
		return fmt.Errorf("later held bound fell below retained quota without new reference")
	}
	if r.Original.Revision > q.Command.Revision && (r.Original.MemoryMaxBytes != q.Command.MemoryMaxBytes || r.Original.OutsideUpperBytes != q.Command.OutsideUpperBytes || !reflect.DeepEqual(r.Original.HugePools, q.Command.HugePools)) {
		return fmt.Errorf("quota profile requires native quota transitions for actual cap changes")
	}
	return nil
}

func normalSplitQuotaCommandKey(c Command) string { b, _ := json.Marshal(c); return physicalDigest(b) }

// v3普通资源接口只续接同额度预算/元数据；实际N/A变化统一走quota事务，
// 不能用generic Grow仅增N后跳过A或构造无历史恢复入口的新路径。
func normalQuotaGenericCommand(r *record, c Command) error {
	if r.NormalSplitQuota == nil {
		return nil
	}
	q := r.NormalSplitQuota.Command
	if c.MemoryMaxBytes != q.MemoryMaxBytes || c.OutsideUpperBytes != q.OutsideUpperBytes || !reflect.DeepEqual(c.HugePools, q.HugePools) {
		return fmt.Errorf("quota profile needs original native quota transition")
	}
	return nil
}
