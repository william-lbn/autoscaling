package budget

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 冻结提取前的五字段布局，独立核对 wire 顺序及无 domain 前缀的旧名称摘要。
// 这些领域夹具只证明合同兼容和拒绝边界，不认证 Linux 资格或真实资源效果。
type originalPrepareContractWire struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	Qualification   QualificationSource `json:"qualification"`
	Prepared        Prepared            `json:"prepared"`
	RootSHA256      string              `json:"root_sha256"`
	OutsideName     string              `json:"outside_name"`
}

func originalPrepareContract(c PrepareCommand) originalPrepareContractWire {
	return originalPrepareContractWire{c.ProtocolVersion, c.Qualification, c.Prepared, c.RootSHA256, c.OutsideName}
}

func originalPrepareContractName(t *testing.T, c PrepareCommand) string {
	t.Helper()
	old := originalPrepareContract(c)
	old.OutsideName = ""
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return "dsga-o-" + hex.EncodeToString(sum[:])[:56]
}

func prepareCommandContractFixture(t *testing.T, normal bool) PrepareCommand {
	t.Helper()
	initial, qualification := initialFixture(t)
	require.NoError(t, initial.PGAggregate.Validate())
	require.NoError(t, initial.MatchQualification(qualification))
	p, _ := settlementPrepared(t)
	p.Request.Key.Instance, p.Request.Key.IssuerEpoch = initial.Instance, initial.IssuerEpoch
	p.Request.ExpectedCapacityGeneration = initial.Shared.Observed.CapacityGeneration
	p.Request.TargetVMMemoryBytes = initial.VMMemoryBytes
	p.Identity = initial.PGAggregate.Identity
	p.RunnerPodName, p.RunnerRestartCount = initial.RunnerPodName, initial.RunnerRestartCount
	p.VMBeforeMemoryBytes, p.GuestUsableMemoryBytes = initial.VMMemoryBytes, initial.GuestUsableMemoryBytes
	p.Shared = initial.Shared
	p.Limits = initial.PGAggregate.Bootstrap.Limits
	var err error
	p.TargetSharedUpperBytes, err = GrowSharedUpper(p.Shared, p.Request.TargetBuffers)
	require.NoError(t, err)
	target, err := qualification.Target(p.Request.TargetVMMemoryBytes)
	require.NoError(t, err)
	p.SystemOutsidePG, p.Transition, p.Safety = target.Bounds(initial.Qualification)
	p.OutsideClaim = nil
	if normal {
		p.NormalQuotaProtocolVersion = 1
		p.NormalQuotaFence = &control.FenceResponse{ProtocolVersion: 1, RequestedInstance: p.Request.Key.Instance, RequestedEpoch: p.Request.Key.IssuerEpoch, Disposition: control.Fenced, Observed: p.Shared.Observed}
	}
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	rootHash, err := RootDigest(initial.PGAggregate)
	require.NoError(t, err)
	c := PrepareCommand{ProtocolVersion: 1, Qualification: initial.Qualification, Prepared: p, RootSHA256: rootHash}
	c.OutsideName = originalPrepareContractName(t, c)
	require.NoError(t, c.Validate())
	return c
}

func TestPrepareCommandContractPreservesOriginalWireAndName(t *testing.T) {
	for _, normal := range []bool{false, true} {
		name := "legacy-normal0"
		if normal {
			name = "complete-normal1"
		}
		t.Run(name, func(t *testing.T) {
			c := prepareCommandContractFixture(t, normal)
			oldRaw, err := json.Marshal(originalPrepareContract(c))
			require.NoError(t, err)
			raw, err := json.Marshal(c)
			require.NoError(t, err)
			require.Equal(t, oldRaw, raw, "提取 DTO 不改变字段顺序或原命令字节")
			name, err := c.CanonicalOutsideName()
			require.NoError(t, err)
			require.Equal(t, originalPrepareContractName(t, c), name)
			var decoded PrepareCommand
			require.NoError(t, json.Unmarshal(oldRaw, &decoded))
			require.Equal(t, c, decoded)
			restored, err := json.Marshal(decoded)
			require.NoError(t, err)
			require.Equal(t, oldRaw, restored)
			if !normal {
				require.Zero(t, decoded.Prepared.NormalQuotaProtocolVersion)
				require.Nil(t, decoded.Prepared.NormalQuotaFence, "旧合法生产路径继续可读")
				require.NotContains(t, string(raw), `"normal_quota_protocol_version"`)
				require.NotContains(t, string(raw), `"normal_quota_fence"`)
			}
		})
	}
}

func TestPrepareCommandContractRejectsIncompleteOrReboundFacts(t *testing.T) {
	original, err := json.Marshal(prepareCommandContractFixture(t, true))
	require.NoError(t, err)
	for name, change := range map[string]func(*PrepareCommand){
		"unknown-command-version":               func(c *PrepareCommand) { c.ProtocolVersion = 2 },
		"missing-qualification-name":            func(c *PrepareCommand) { c.Qualification.Claim.Name = "" },
		"missing-qualification-uid":             func(c *PrepareCommand) { c.Qualification.Claim.UID = "" },
		"short-qualification-digest":            func(c *PrepareCommand) { c.Qualification.SHA256 = strings.Repeat("a", 63) },
		"noncanonical-qualification-digest":     func(c *PrepareCommand) { c.Qualification.SHA256 = strings.Repeat("A", 64) },
		"qualification-rebound-with-old-bounds": func(c *PrepareCommand) { c.Qualification.SHA256 = strings.Repeat("f", 64) },
		"missing-root-digest":                   func(c *PrepareCommand) { c.RootSHA256 = "" },
		"short-root-digest":                     func(c *PrepareCommand) { c.RootSHA256 = strings.Repeat("a", 63) },
		"noncanonical-root-digest":              func(c *PrepareCommand) { c.RootSHA256 = strings.Repeat("A", 64) },
		"vm-permit":                             func(c *PrepareCommand) { c.Prepared.VMShrinkPermit = true },
		"already-published-outside":             func(c *PrepareCommand) { c.Prepared.OutsideClaim = &ObjectRef{Name: "outside", UID: "outside-uid"} },
		"unknown-normal-version":                func(c *PrepareCommand) { c.Prepared.NormalQuotaProtocolVersion = 2 },
		"missing-normal-fence":                  func(c *PrepareCommand) { c.Prepared.NormalQuotaFence = nil },
		"normal0-retains-fence":                 func(c *PrepareCommand) { c.Prepared.NormalQuotaProtocolVersion = 0 },
		"unfenced-normal":                       func(c *PrepareCommand) { c.Prepared.NormalQuotaFence.Disposition = "" },
		"fence-owner-epoch":                     func(c *PrepareCommand) { c.Prepared.NormalQuotaFence.RequestedEpoch++ },
		"fence-current-snapshot-drift":          func(c *PrepareCommand) { c.Prepared.NormalQuotaFence.Observed.AdmissionRevision++ },
		"wrong-resource-identity":               func(c *PrepareCommand) { c.Prepared.Identity.ComputeInstanceID = "different-compute" },
		"unknown-native-allocation":             func(c *PrepareCommand) { c.Prepared.Shared.Observed.Arrays[0].AllocationUnknown = true },
		"system-source-name":                    func(c *PrepareCommand) { c.Prepared.SystemOutsidePG.SourceID += "-different" },
		"transition-source-name":                func(c *PrepareCommand) { c.Prepared.Transition.SourceID += "-different" },
		"safety-source-name":                    func(c *PrepareCommand) { c.Prepared.Safety.SourceID += "-different" },
		"qualified-bound-kind":                  func(c *PrepareCommand) { c.Prepared.Safety.Kind = HardLimit },
		"unfunded-guest-budget":                 func(c *PrepareCommand) { c.Prepared.GuestUsableMemoryBytes = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			var changed PrepareCommand
			require.NoError(t, json.Unmarshal(original, &changed))
			change(&changed)
			// 重算名称，确保拒绝来自完整 typed 合同，而非仅因外层名称没有跟随变化。
			changed.OutsideName = originalPrepareContractName(t, changed)
			require.Error(t, changed.Validate())
			raw, err := json.Marshal(changed)
			require.NoError(t, err)
			var decoded PrepareCommand
			require.Error(t, json.Unmarshal(raw, &decoded))
		})
	}
	c := prepareCommandContractFixture(t, true)
	c.OutsideName += "-different"
	require.Error(t, c.Validate(), "名称必须精确对应完整原命令")
	raw, err := json.Marshal(c)
	require.NoError(t, err)
	var decoded PrepareCommand
	require.Error(t, json.Unmarshal(raw, &decoded))
}

func TestPrepareCommandContractRejectsAmbiguousWire(t *testing.T) {
	raw, err := json.Marshal(prepareCommandContractFixture(t, true))
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for field := range fields {
		t.Run("missing-"+field, func(t *testing.T) {
			var missing map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &missing))
			delete(missing, field)
			body, err := json.Marshal(missing)
			require.NoError(t, err)
			var decoded PrepareCommand
			require.Error(t, json.Unmarshal(body, &decoded))
		})
	}
	for name, body := range map[string][]byte{
		"unknown-field":          append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"ignored":true}`)...),
		"duplicate-top-field":    append([]byte(`{"protocol_version":1,`), raw[1:]...),
		"duplicate-nested-field": bytes.Replace(raw, []byte(`"qualification":{`), []byte(`"qualification":{"sha256":"duplicate",`), 1),
		"case-folded-field":      bytes.Replace(raw, []byte(`"outside_name":`), []byte(`"Outside_Name":`), 1),
		"null-version":           bytes.Replace(raw, []byte(`"protocol_version":1`), []byte(`"protocol_version":null`), 1),
		"trailing-object":        append(append([]byte(nil), raw...), []byte(`{}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			var decoded PrepareCommand
			require.Error(t, json.Unmarshal(body, &decoded))
		})
	}
}

// 可选的原存储字节回归只从私有验收目录读取，不把原命令或应用日志带入 Git。
// 失败信息仅报告步骤/长度，不打印真实命令内容或其完整对象。
func TestPrepareCommandContractOriginalStoredWire(t *testing.T) {
	artifactDir := os.Getenv("DSGA_PREPARE_COMMAND_ARTIFACT_DIR")
	if artifactDir == "" {
		t.Skip("private original Prepare artifacts not selected")
	}
	for index, size := range []int{6451, 7338} {
		name := []string{"operation-1.json", "operation-2.json"}[index]
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(artifactDir, name))
			if err != nil {
				t.Fatal("cannot read selected private Prepare artifact")
			}
			var artifact struct {
				State struct {
					History []struct {
						Operation struct {
							Steps []struct {
								Action  string `json:"action"`
								Command []byte `json:"command"`
							} `json:"steps"`
						} `json:"operation"`
					} `json:"history"`
				} `json:"state"`
			}
			if json.Unmarshal(raw, &artifact) != nil || len(artifact.State.History) != index+1 {
				t.Fatal("invalid selected private Prepare artifact shape")
			}
			var commandRaw []byte
			for _, step := range artifact.State.History[index].Operation.Steps {
				if step.Action == "prepare_budget" {
					commandRaw = step.Command
					break
				}
			}
			if len(commandRaw) != size {
				t.Fatalf("original Prepare size: got %d, want %d", len(commandRaw), size)
			}
			var c PrepareCommand
			if json.Unmarshal(commandRaw, &c) != nil {
				t.Fatal("original stored Prepare DTO rejected")
			}
			restored, err := json.Marshal(c)
			if err != nil || !bytes.Equal(commandRaw, restored) {
				t.Fatal("original stored Prepare wire bytes changed")
			}
			original, err := json.Marshal(originalPrepareContract(c))
			if err != nil || !bytes.Equal(commandRaw, original) {
				t.Fatal("original five-field wire bytes changed")
			}
			if c.OutsideName != originalPrepareContractName(t, c) {
				t.Fatal("original stored Outside name changed")
			}
			name, err := c.CanonicalOutsideName()
			if err != nil || name != c.OutsideName {
				t.Fatal("canonical name differs from original stored Outside name")
			}
		})
	}
}
