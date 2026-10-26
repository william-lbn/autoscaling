// Package vmguard承载Runner唯一内存执行者；不把PG完成、VM spec、普通QMP回复
// 或owner租约单独当作VM完成。外部入口必须由本包固定Kube来源构造，不能接受
// HTTP调用者自报“已授权”。本包的原生读回也不替代独立Host/PG/Guest预算证明。
package vmguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Identity在本Runner启动、QEMU出生之前固定。Pod UID/Runner随机boot/Node UID
// 不随同名对象或Agent接管改变。几何是原QEMU启动配置，不接受运行期重写Min/Max
// 以便把任意目标解释成合法粒度；这里只支持原生virtio-mem，不降级balloon。
type Identity struct {
	VMUID        string         `json:"vm_uid"`
	Incarnation  string         `json:"incarnation"`
	RunnerPodUID string         `json:"runner_pod_uid"`
	RunnerBootID string         `json:"runner_boot_id"`
	NodeUID      string         `json:"node_uid"`
	MinimumBytes control.Uint64 `json:"minimum_bytes"`
	MaximumBytes control.Uint64 `json:"maximum_bytes"`
	BlockBytes   control.Uint64 `json:"block_bytes"`
}

func (id Identity) Validate() error {
	if control.ValidateID(id.VMUID, "VM") != nil || control.ValidateIncarnation(id.Incarnation) != nil ||
		control.ValidateID(id.RunnerPodUID, "Runner Pod UID") != nil || control.ValidateIncarnation(id.RunnerBootID) != nil || control.ValidateID(id.NodeUID, "Node UID") != nil ||
		id.MinimumBytes == 0 || id.MaximumBytes < id.MinimumBytes || id.MaximumBytes > math.MaxInt64 || id.BlockBytes == 0 || id.BlockBytes&(id.BlockBytes-1) != 0 || (id.MaximumBytes-id.MinimumBytes)%id.BlockBytes != 0 {
		return fmt.Errorf("invalid original Runner identity or virtio-mem geometry")
	}
	return nil
}

func (id Identity) memoryValid(n control.Uint64) bool {
	return id.Validate() == nil && n >= id.MinimumBytes && n <= id.MaximumBytes && (n-id.MinimumBytes)%id.BlockBytes == 0
}

// Command引用同一VM完整WAL中的原Prepare/Guest及真实Host承诺，避免把多个完整
// PG成本/Root审计重复嵌入8KiB命令。身份、原请求和来源摘要均不可变；来源真实性
// 由固定Kube读者复核，摘要本身不是授权。Grow没有未来GuestReady摘要，Shrink
// 则必须绑定VM动作之前已完成的原GuestReady，不能临时换同数值来源。
type Command struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Identity        Identity         `json:"identity"`
	BindingClaim    budget.ObjectRef `json:"binding_claim"`
	HostClaim       budget.ObjectRef `json:"host_claim"`
	Request         control.Request  `json:"request"`
	BeforeBytes     control.Uint64   `json:"before_bytes"`
	PreparedSHA256  string           `json:"prepared_sha256"`
	HostSHA256      string           `json:"host_sha256"`
	GuestSHA256     string           `json:"guest_sha256"`
}

func digestValid(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func (c Command) Validate() error {
	if c.ProtocolVersion != 1 || c.Identity.Validate() != nil || c.BindingClaim.Validate() != nil || c.HostClaim.Validate() != nil ||
		c.Request.Validate() != nil || c.Request.Key.Instance.VMUID != c.Identity.VMUID || !c.Identity.memoryValid(c.BeforeBytes) || !c.Identity.memoryValid(c.Request.TargetVMMemoryBytes) || !digestValid(c.PreparedSHA256) || !digestValid(c.HostSHA256) {
		return fmt.Errorf("incomplete original VM memory command")
	}
	if c.Request.Kind == control.Grow {
		if c.Request.TargetVMMemoryBytes < c.BeforeBytes || c.GuestSHA256 != "" {
			return fmt.Errorf("grow VM command changes direction or invents future Guest source")
		}
	} else if c.Request.TargetVMMemoryBytes > c.BeforeBytes || !digestValid(c.GuestSHA256) {
		return fmt.Errorf("shrink VM command lacks original pre-VM Guest source")
	}
	return nil
}

func (c Command) digest() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
	return hex.EncodeToString(h[:]), nil
}

// Observation均为total guest physical bytes，设备适配器必须在同一QEMU上分别
// 读取virtio-mem requested-size和实际plugged/base总量，不能把requested-size
// 当实际值。两者相等仍只证明本域，是否有足够可用Guest预算由协调器独立复核。
type Observation struct {
	RequestedBytes control.Uint64 `json:"requested_bytes"`
	ActualBytes    control.Uint64 `json:"actual_bytes"`
}

func (o Observation) validate(id Identity) error {
	if !id.memoryValid(o.RequestedBytes) || !id.memoryValid(o.ActualBytes) {
		return fmt.Errorf("native memory observation violates original geometry")
	}
	return nil
}

type stage string

const (
	prepared    stage = "prepared"
	mayHaveSent stage = "may_have_sent"
)

type resultKind string

const (
	completed resultKind = "completed"
	notSent   resultKind = "not_sent"
)

type intent struct {
	Command       Command       `json:"command"`
	CommandSHA256 string        `json:"command_sha256"`
	JournalSHA256 string        `json:"journal_sha256"`
	Owner         journal.Owner `json:"owner"`
	Stage         stage         `json:"stage"`
	ReplayNonce   string        `json:"replay_nonce"`
	Before        Observation   `json:"before"`
}

type receipt struct {
	Intent intent      `json:"intent"`
	Kind   resultKind  `json:"kind"`
	Actual Observation `json:"actual"`
}

type record struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	Identity        Identity          `json:"identity"`
	BindingClaim    budget.ObjectRef  `json:"binding_claim"`
	Sequence        control.Uint64    `json:"sequence"`
	Floor           journal.Owner     `json:"floor"`
	LeaseUID        string            `json:"lease_uid"`
	HighWater       *control.Key      `json:"high_water"`
	Pending         *intent           `json:"pending"`
	Last            *receipt          `json:"last"`
	LastPublished   *budget.ObjectRef `json:"last_published"`
}

func compareKey(a, b control.Key) int {
	if a.IssuerEpoch < b.IssuerEpoch || (a.IssuerEpoch == b.IssuerEpoch && a.Revision < b.Revision) {
		return -1
	}
	if a.IssuerEpoch > b.IssuerEpoch || a.Revision > b.Revision {
		return 1
	}
	return 0
}

func (i intent) validate(r record) error {
	h, err := i.Command.digest()
	if err != nil || h != i.CommandSHA256 || !digestValid(i.JournalSHA256) || i.Command.Identity != r.Identity || i.Command.BindingClaim != r.BindingClaim || i.Owner.Validate() != nil ||
		i.Owner.Epoch != i.Command.Request.Key.IssuerEpoch || i.Owner.Epoch > r.Floor.Epoch || (i.Owner.Epoch == r.Floor.Epoch && i.Owner != r.Floor) ||
		(i.Stage != prepared && i.Stage != mayHaveSent) || i.Before.validate(r.Identity) != nil || i.Before.ActualBytes != i.Command.BeforeBytes || i.Before.RequestedBytes != i.Command.BeforeBytes || (i.ReplayNonce != "" && control.ValidateIncarnation(i.ReplayNonce) != nil) {
		return fmt.Errorf("retained Runner memory intent is invalid or changed")
	}
	if i.Stage == mayHaveSent && i.Command.BeforeBytes == i.Command.Request.TargetVMMemoryBytes {
		return fmt.Errorf("no-op memory action must never send a QMP mutation")
	}
	return nil
}

func (r record) validate() error {
	if r.ProtocolVersion != 1 || r.Sequence == 0 || r.Identity.Validate() != nil || r.BindingClaim.Validate() != nil || r.Floor.Validate() != nil {
		return fmt.Errorf("invalid durable Runner record")
	}
	if r.HighWater != nil && (r.HighWater.Validate() != nil || r.HighWater.Instance.VMUID != r.Identity.VMUID || r.HighWater.IssuerEpoch > r.Floor.Epoch) {
		return fmt.Errorf("invalid Runner high water")
	}
	if r.HighWater == nil && (r.Pending != nil || r.Last != nil) {
		return fmt.Errorf("Runner effects have no original high water")
	}
	if r.LeaseUID != "" && control.ValidateID(r.LeaseUID, "original owner Lease UID") != nil || r.HighWater != nil && r.LeaseUID == "" {
		return fmt.Errorf("Runner effects lack the original owner Lease UID")
	}
	if r.LastPublished != nil && (r.Last == nil || r.LastPublished.Validate() != nil) {
		return fmt.Errorf("published Runner result lost its original immutable receipt reference")
	}
	if r.Pending != nil {
		if r.Pending.validate(r) != nil || r.HighWater == nil || r.Pending.Command.Request.Key != *r.HighWater {
			return fmt.Errorf("Runner pending differs from immutable high water")
		}
	}
	if r.Last != nil {
		last := r.Last
		if last.Intent.validate(r) != nil || last.Actual.validate(r.Identity) != nil || (last.Kind != completed && last.Kind != notSent) || r.HighWater == nil || compareKey(last.Intent.Command.Request.Key, *r.HighWater) > 0 {
			return fmt.Errorf("invalid retained Runner result")
		}
		if last.Kind == completed && (last.Actual.RequestedBytes != last.Intent.Command.Request.TargetVMMemoryBytes || last.Actual.ActualBytes != last.Actual.RequestedBytes) {
			return fmt.Errorf("Runner completed without exact physical readback")
		}
		if last.Kind == completed && last.Intent.Stage == prepared && last.Intent.Command.BeforeBytes != last.Intent.Command.Request.TargetVMMemoryBytes {
			return fmt.Errorf("unsent non-noop cannot be completed")
		}
		if last.Kind == notSent && last.Intent.Stage != prepared {
			return fmt.Errorf("possible QMP send cannot become not-sent")
		}
		if last.Intent.Command.Request.Key.Instance != r.HighWater.Instance {
			return fmt.Errorf("Runner result changed the original PG instance")
		}
		if r.Pending != nil && compareKey(last.Intent.Command.Request.Key, r.Pending.Command.Request.Key) == 0 &&
			(last.Kind != notSent || last.Intent.Command != r.Pending.Command || last.Intent.JournalSHA256 != r.Pending.JournalSHA256 || r.Pending.ReplayNonce == "" || r.Pending.ReplayNonce == last.Intent.ReplayNonce) {
			return fmt.Errorf("same-key Runner pending is not an explicit original not-sent replay")
		}
	}
	if r.Pending == nil && r.HighWater != nil && (r.Last == nil || r.Last.Intent.Command.Request.Key != *r.HighWater) {
		return fmt.Errorf("Runner lost original pending/result")
	}
	return nil
}

// 深拷贝仅含值和四个显式指针；完整请求/身份/来源本身均为不可变值，不含切片。
func (r record) clone() record {
	if r.HighWater != nil {
		v := *r.HighWater
		r.HighWater = &v
	}
	if r.Pending != nil {
		v := *r.Pending
		r.Pending = &v
	}
	if r.Last != nil {
		v := *r.Last
		r.Last = &v
	}
	if r.LastPublished != nil {
		v := *r.LastPublished
		r.LastPublished = &v
	}
	return r
}

func strictDecode[T any](data []byte, into *T) error {
	return control.UnmarshalObjectBounded(data, into, 64*1024, 4096)
}

func (c *Command) UnmarshalJSON(data []byte) error {
	type plain Command
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	*c = Command(p)
	return nil
}

func (id *Identity) UnmarshalJSON(data []byte) error {
	type plain Identity
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	*id = Identity(p)
	return nil
}

func (o *Observation) UnmarshalJSON(data []byte) error {
	type plain Observation
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	*o = Observation(p)
	return nil
}

func (i *intent) UnmarshalJSON(data []byte) error {
	type plain intent
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	*i = intent(p)
	return nil
}

func (v *receipt) UnmarshalJSON(data []byte) error {
	type plain receipt
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	*v = receipt(p)
	return nil
}

func (r *record) UnmarshalJSON(data []byte) error {
	type plain record
	var p plain
	if err := strictDecode(data, &p); err != nil {
		return err
	}
	next := record(p)
	if err := next.validate(); err != nil {
		return err
	}
	*r = next
	return nil
}
