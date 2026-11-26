package compute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNativeGoRustSQLLFCAllocation(t *testing.T) {
	ready := os.Getenv("NEON_DSG_LFC_PIPELINE_READY")
	if ready == "" {
		t.Skip("explicit real PG/Rust component required")
	}
	content, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		URL   string `json:"url"`
		Token string `json:"token"`
		Epoch string `json:"epoch"`
	}
	if json.Unmarshal(content, &config) != nil {
		t.Fatal("invalid private fixture readiness")
	}
	target, err := url.Parse(config.URL)
	if err != nil || target.Scheme != "http" || !strings.HasPrefix(target.Host, "127.0.0.1:") {
		t.Fatal("invalid local Rust fixture address")
	}
	// TLS 桥只提供测试证书和逐字 HTTP 传输，不伪造任何 DSGA 响应/授权/原生事实。
	bridge := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(target))
	defer bridge.Close()
	transport := bridge.Client().Transport.(*http.Transport)
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("test transport must verify fixture TLS certificate")
	}
	epoch, err := strconv.ParseUint(config.Epoch, 10, 64)
	if err != nil {
		t.Fatal("bad fixture epoch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, err := DiscoverInitial(ctx, Config{BaseURL: bridge.URL, Instance: control.Instance{
		VMUID: "lfc-allocation-vm", ComputeInstanceID: "lfc-allocation-compute"}, IssuerEpoch: control.Uint64(epoch),
		Timeout: 20 * time.Second, Transport: transport, Token: func(context.Context) (string, error) { return config.Token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	instance, _ := client.Binding()
	fence, err := client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: control.Uint64(epoch)})
	if err != nil {
		t.Fatal(err)
	}
	_ = fence
	initial, err := client.LFCAllocationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := control.Key{Instance: instance, IssuerEpoch: control.Uint64(epoch), Revision: control.Uint64(epoch + 2),
		OperationID: control.OperationID{High: control.Uint64(epoch + 3), Low: 17}}
	request := control.LFCAllocationRequest{ProtocolVersion: 1, Key: key, StepOrdinal: control.Uint64(epoch + 4),
		ExpectedCapacityGeneration: initial.Observed.CapacityGeneration, ExpectedLFCGeneration: initial.Observed.LFCGeneration,
		ExpectedLFCSequence: initial.Observed.LFCSequence, FromLimitBytes: initial.Observed.SharedLimitBytes, ToLimitBytes: 0,
		AllocationPlanSHA256: strings.Repeat("b", 64), BudgetCommitmentID: `budget"\>&:pipeline`,
		DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	fact, err := client.AllocateLFC(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !fact.AppliedFor(request) || !fact.SynchronousExitFor(request) {
		t.Fatal("actual native close/exit not proved")
	}
	if fact.Requested == nil || *fact.Requested != request || fact.Original == nil || *fact.Original != request {
		t.Fatal("full original/requested fields changed across Go/Rust/SQL")
	}
	digest, err := request.ContextSHA256()
	if err != nil || fact.OriginalContextSHA256 == nil || *fact.OriginalContextSHA256 != digest {
		t.Fatal("native canonical context hash changed")
	}
	raw, _ := json.Marshal(request)
	rawDigest := sha256.Sum256(raw)
	if hex.EncodeToString(rawDigest[:]) == digest {
		t.Fatal("fixture failed to distinguish raw Go JSON from PG jsonb_out")
	}
	replay, err := client.AllocateLFC(ctx, request)
	if err != nil || replay.Disposition != "replayed" || !replay.AppliedFor(request) {
		t.Fatal("original native replay failed", err)
	}
	different := request
	different.ToLimitBytes = 16 << 20
	conflict, err := client.LFCAllocationStatus(ctx, different)
	if err != nil || conflict.Disposition != "conflict" || conflict.Requested == nil || *conflict.Requested != different || conflict.Original == nil || *conflict.Original != request {
		t.Fatal("conflict did not preserve complete original and requested contexts", err)
	}
	next := request
	next.StepOrdinal++
	next.FromLimitBytes = 0
	next.ToLimitBytes = 16 << 20
	next.ExpectedLFCGeneration = fact.Observed.LFCGeneration
	next.ExpectedLFCSequence = fact.Observed.LFCSequence
	opened, err := client.AllocateLFC(ctx, next)
	if err != nil || !opened.AppliedFor(next) || opened.Observed.SharedLimitBytes != 16<<20 {
		t.Fatal("same-parent next native ordinal failed", err)
	}
	if opened.Observed.ReleaseAuthorized || opened.Observed.KernelReleaseProven || client.PendingLFCAllocation() != nil {
		t.Fatal("logical execution created a physical permit or left a false pending")
	}
	if err := os.WriteFile(os.Getenv("NEON_DSG_LFC_PIPELINE_DONE"), []byte("PASS\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("PASS production Go→TLS transport→Rust router/auth/SDK→SQL C; complete high-uint64 original, canonical hash, ordinal and replay retained; no physical release permit")
}
