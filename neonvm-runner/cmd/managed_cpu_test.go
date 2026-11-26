package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type fakeCPUGroup struct {
	dev, ino           uint64
	files              map[string]string
	writes             []string
	writeFail          bool
	writeLost          bool
	readFailAfterWrite bool
}

func cpuFixtureIdentity() runnercpu.Identity {
	return runnercpu.Identity{VMUID: "vm-one", ComputeInstanceID: "compute-one",
		Incarnation: strings.Repeat("1", 32), RunnerPodUID: "pod-one", RunnerBootID: strings.Repeat("2", 32), NodeUID: "node-one"}
}

func (g *fakeCPUGroup) identity() (uint64, uint64, error) { return g.dev, g.ino, nil }
func (g *fakeCPUGroup) close() error                      { return nil }
func (g *fakeCPUGroup) read(name string) ([]byte, error) {
	if g.readFailAfterWrite && len(g.writes) > 0 && name == "cpu.max" {
		return nil, errors.New("readback unavailable")
	}
	value, ok := g.files[name]
	if !ok {
		return nil, errors.New("source absent")
	}
	return []byte(value), nil
}
func (g *fakeCPUGroup) writeMax(raw []byte) error {
	g.writes = append(g.writes, string(raw))
	if g.writeFail {
		return errors.New("kernel rejected quota")
	}
	g.files["cpu.max"] = string(raw)
	if g.writeLost {
		return errors.New("quota outcome unknown")
	}
	return nil
}

func managedCPUFixture() (*managedCPU, *fakeCPUGroup, *vmv1.MilliCPU) {
	group := &fakeCPUGroup{dev: 1, ino: 2, files: map[string]string{"cpu.max": "50000 100000",
		"cpu.weight": "100", "cpu.stat": "usage_usec 100\nnr_periods 2\nnr_throttled 1\nthrottled_usec 40\n",
		"cpu.pressure": "some avg10=0.00 avg60=0.00 avg300=0.00 total=12\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"}}
	online := vmv1.MilliCPU(1000)
	state := &managedCPU{verifyCommitment: func(context.Context) (runnercpu.NodeCommitment, error) { return cpuNodeFactFixture(), nil }, process: func() (int, uint64, error) { return 42, 7, nil }, kernel: group, durable: true, intent: cpuIntent{MinimumMillicores: 500, ProtocolVersion: 1, Identity: cpuFixtureIdentity(),
		MaximumMillicores: 2000, CommittedMillicores: 2000, MaximumVCPUs: 2, CgroupDevice: 1, CgroupInode: 2, Generation: 1, TargetMillicores: 500},
		persist: func(cpuIntent) error { return nil }, authorize: func(context.Context, runnercpu.Command, vmv1.MilliCPU) error { return nil },
		getGuest: func() (vmv1.MilliCPU, int, error) { return online, http.StatusOK, nil },
		setGuest: func(cpu vmv1.MilliCPU) (int, error) {
			online = vmv1.MilliCPU(cpu.RoundedUp() * 1000)
			return http.StatusOK, nil
		}}
	return state, group, &online
}

func cpuCommand(generation int64, cpu vmv1.MilliCPU) api.VCPUChange {
	return api.VCPUChange{VCPUs: cpu, Managed: &runnercpu.Command{ProtocolVersion: 1,
		Identity: cpuFixtureIdentity(), Generation: generation}}
}

func TestManagedCPUReadsKernelAndGuestWithoutBorrowingLogicalTarget(t *testing.T) {
	state, group, online := managedCPUFixture()
	value, _, err := state.observe()
	if err != nil || value.VCPUs != 500 || value.Managed.OnlineVCPUs != 1 || value.Managed.Pending {
		t.Fatalf("fractional quota did not use actual readback: %#v %v", value, err)
	}
	group.files["cpu.max"] = "75000 100000"
	value, _, err = state.observe()
	if err != nil || value.VCPUs != 750 || !value.Managed.Pending || !state.intent.Pending {
		t.Fatalf("external quota drift was hidden by logical500: %#v %v", value, err)
	}
	group.files["cpu.max"] = "max 100000"
	if value, _, err = state.observe(); err == nil || value != nil {
		t.Fatal("unbounded quota borrowed logical target")
	}
	group.files["cpu.max"] = "50000 100000"
	*online = 1500
	if _, _, err = state.observe(); err == nil {
		t.Fatal("Guest topology was rounded from noninteger")
	}
}

func TestManagedCPUUnknownPrefixRetainsOriginalAndCanResume(t *testing.T) {
	for _, mode := range []string{"write-rejected", "write-outcome-lost", "readback-lost", "guest-partial"} {
		t.Run(mode, func(t *testing.T) {
			state, group, online := managedCPUFixture()
			switch mode {
			case "write-rejected":
				group.writeFail = true
			case "write-outcome-lost":
				group.writeLost = true
			case "readback-lost":
				group.readFailAfterWrite = true
			case "guest-partial":
				state.setGuest = func(vmv1.MilliCPU) (int, error) { return 503, errors.New("Guest change may be partial") }
			}
			_, err := state.change(context.Background(), cpuCommand(2, 1500))
			if !errors.Is(err, errCPUUnconfirmed) || !state.intent.Pending || state.intent.Generation != 2 {
				t.Fatalf("unknown prefix discarded: %v %#v", err, state.intent)
			}
			before := len(group.writes)
			if _, err = state.change(context.Background(), cpuCommand(3, 500)); err == nil || len(group.writes) != before {
				t.Fatal("new goal overwrote original pending quota")
			}
			group.writeFail, group.writeLost, group.readFailAfterWrite = false, false, false
			value, _, err := state.observe()
			if err != nil || !value.Managed.Pending {
				t.Fatal("partial topology or old quota became complete", err)
			}
			state.setGuest = func(cpu vmv1.MilliCPU) (int, error) { *online = vmv1.MilliCPU(cpu.RoundedUp() * 1000); return 200, nil }
			if _, err = state.change(context.Background(), cpuCommand(2, 1500)); err != nil {
				t.Fatal("same original retry failed:", err)
			}
			value, _, err = state.observe()
			if err != nil || value.Managed.Pending || value.VCPUs != 1500 || value.Managed.OnlineVCPUs != 2 {
				t.Fatal("retry not backed by actual quota and integer topology", err)
			}
			if _, err = state.change(context.Background(), cpuCommand(1, 500)); err == nil {
				t.Fatal("late old generation changed CPU")
			}
			if _, err = state.change(context.Background(), cpuCommand(2, 500)); err == nil {
				t.Fatal("accepted generation changed its target")
			}
		})
	}
}

func TestManagedCPUIntentDurabilityAndOriginalAuthorityPrecedeEffects(t *testing.T) {
	state, group, _ := managedCPUFixture()
	state.persist = func(cpuIntent) error { return errors.New("state fsync outcome unknown") }
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); !errors.Is(err, errCPUUnconfirmed) {
		t.Fatal(err)
	}
	if len(group.writes) != 0 || state.durable || !state.intent.Pending {
		t.Fatal("quota write bypassed durable original intent")
	}
	state.persist = func(cpuIntent) error { return nil }
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); err != nil {
		t.Fatal("exact prefix did not recover:", err)
	}
	state.authorize = func(context.Context, runnercpu.Command, vmv1.MilliCPU) error {
		return errors.New("current original VM generation differs")
	}
	before := len(group.writes)
	if _, err := state.change(context.Background(), cpuCommand(3, 500)); err == nil || len(group.writes) != before {
		t.Fatal("caller invented a generation")
	}
	state.authorize = func(context.Context, runnercpu.Command, vmv1.MilliCPU) error { return nil }
	group.ino++
	if _, err := state.change(context.Background(), cpuCommand(3, 500)); err == nil || len(group.writes) != before {
		t.Fatal("replacement cgroup received original quota")
	}
}

func TestManagedCPUJournalReconstructsOriginalFloorAndPendingPrefix(t *testing.T) {
	journal, err := openCPUJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(journal.fd)
	state, group, online := managedCPUFixture()
	state.persist = journal.save
	group.writeLost = true
	if _, err = state.change(context.Background(), cpuCommand(2, 1500)); err == nil {
		t.Fatal("lost quota write accepted")
	}
	original, err := journal.load()
	if err != nil || original == nil || original.Generation != 2 || !original.Pending {
		t.Fatal("pending intent not durable:", err)
	}
	restored := &managedCPU{verifyCommitment: state.verifyCommitment, process: state.process, kernel: group, intent: *original, durable: true, persist: journal.save,
		authorize: state.authorize, getGuest: state.getGuest, setGuest: state.setGuest}
	group.writeLost = false
	before := len(group.writes)
	if _, err = restored.change(context.Background(), cpuCommand(1, 500)); err == nil || len(group.writes) != before {
		t.Fatal("restored floor forgot old write")
	}
	if _, err = restored.change(context.Background(), cpuCommand(3, 500)); err == nil || len(group.writes) != before {
		t.Fatal("restored original pending was replaced")
	}
	if _, err = restored.change(context.Background(), cpuCommand(2, 1500)); err != nil || *online != 2000 {
		t.Fatal("exact persisted original did not finish", err)
	}
	terminal, err := journal.load()
	if err != nil || terminal.Pending || terminal.Generation != 2 {
		t.Fatal("completion was not durable", err)
	}
}

func TestManagedCPUHTTPPropagatesVersionedFactsAndBoundsInput(t *testing.T) {
	state, group, _ := managedCPUFixture()
	cb := managedCPUCallbacks(state)
	body, err := json.Marshal(cpuCommand(2, 750))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/cpu_change", strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	handleCPUChange(zap.NewNop(), response, request, cb.set)
	if response.Code != 200 || group.files["cpu.max"] != "75000 100000" {
		t.Fatalf("actual fractional write absent %d", response.Code)
	}
	response = httptest.NewRecorder()
	handleCPUCurrent(zap.NewNop(), response, httptest.NewRequest(http.MethodGet, "/cpu_current", nil), cb.get)
	var current api.VCPUCgroup
	if json.Unmarshal(response.Body.Bytes(), &current) != nil || current.Managed == nil || current.Managed.Generation != 2 || current.VCPUs != 750 {
		t.Fatal("HTTP lost actual kernel/source contract")
	}
	before := len(group.writes)
	response = httptest.NewRecorder()
	handleCPUChange(zap.NewNop(), response, httptest.NewRequest(http.MethodPost, "/cpu_change", strings.NewReader(strings.Repeat(" ", 4097))), cb.set)
	if response.Code != 400 || len(group.writes) != before {
		t.Fatal("oversized CPU request reached kernel")
	}
	response = httptest.NewRecorder()
	handleCPUChange(zap.NewNop(), response, httptest.NewRequest(http.MethodPost, "/cpu_change", strings.NewReader("{\"VCPUs\":\"500m\"}")), cb.set)
	if response.Code != 409 || len(group.writes) != before {
		t.Fatal("legacy logical write entered managed path")
	}
}

func TestCPUQuotaParserRejectsMaxOverflowAndAmbiguousSources(t *testing.T) {
	for _, raw := range []string{"max 100000", "0 100000", "50000 0", "50000 100000 extra", "-1 100000",
		"+50000 100000", "18446744073709551615 100000", "50000 18446744073709551615", "50001 100000"} {
		if _, _, err := parseCPUQuota([]byte(raw)); err == nil {
			t.Fatalf("invalid quota accepted %q", raw)
		}
	}
	if _, err := parseCPUStatistics([]byte("nr_periods 1\nnr_periods 2\nnr_throttled 0\nthrottled_usec 0"), []byte("some total=0")); err == nil {
		t.Fatal("duplicate statistic accepted")
	}
	if _, err := parseCPUStatistics([]byte("nr_periods 1\nnr_throttled 0\nthrottled_usec 0"), []byte("some avg10=0")); err == nil {
		t.Fatal("missing actual pressure source accepted")
	}
}

// 只有主任务明确启动本测试专属 systemd 组时才写真实 cpu.max；不改宿主其他组。
func TestManagedCPUOwnedKernel(t *testing.T) {
	expected := os.Getenv("DSGA_CPU_OWNED_UNIT")
	if expected == "" {
		t.Skip("requires an explicitly owned isolated systemd unit")
	}
	if strings.ContainsAny(expected, "/\\\n") || !strings.HasPrefix(expected, "dsga403-host-cpu-native-") {
		t.Fatal("invalid owned test unit")
	}
	current, err := unifiedCPUPath()
	if err != nil || !strings.HasSuffix(current, "/"+expected+".service") {
		t.Fatalf("not the original isolated unit: %q %v", current, err)
	}
	group, err := openOriginalCPUGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.close()
	memoryBefore, err := os.ReadFile(cgroupMountPoint + current + "/memory.max")
	if err != nil {
		t.Fatal(err)
	}
	beforeGroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	if err = group.writeMax([]byte("50000 100000")); err != nil {
		t.Fatal(err)
	}
	raw, err := group.read("cpu.max")
	if err != nil || strings.TrimSpace(string(raw)) != "50000 100000" {
		t.Fatalf("finite 0.5c kernel readback absent %q %v", raw, err)
	}
	readStatistics := func() runnercpu.Statistics {
		stat, e := group.read("cpu.stat")
		if e != nil {
			t.Fatal(e)
		}
		pressure, e := group.read("cpu.pressure")
		if e != nil {
			t.Fatal(e)
		}
		data, e := parseCPUStatistics(stat, pressure)
		if e != nil {
			t.Fatal(e)
		}
		return data
	}
	before := readStatistics()
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	deadline := time.Now().Add(1800 * time.Millisecond)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var n uint64
			for time.Now().Before(deadline) {
				n++
				runtime.KeepAlive(n)
			}
		}()
	}
	workers.Wait()
	after := readStatistics()
	if after.ThrottledUS <= before.ThrottledUS {
		t.Fatal("real owned cgroup did not throttle under load")
	}
	memoryAfter, err := os.ReadFile(cgroupMountPoint + current + "/memory.max")
	if err != nil || string(memoryBefore) != string(memoryAfter) {
		t.Fatal("CPU quota changed memory hard ownership")
	}
	afterGroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil || string(beforeGroup) != string(afterGroup) {
		t.Fatal("CPU quota moved original process out of memory group")
	}
	if err = group.writeMax([]byte("100000 100000")); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("owned CPU kernel component PASS: quota=50000/100000 throttle_delta_us=%d memory.max=%s; real QEMU/Guest NOT_RUN\n", after.ThrottledUS-before.ThrottledUS, string(memoryAfter))
}

func TestManagedCPUReadRequiresCurrentOriginalPodCommitment(t *testing.T) {
	state, group, _ := managedCPUFixture()
	state.verifyCommitment = func(context.Context) (runnercpu.NodeCommitment, error) {
		return runnercpu.NodeCommitment{}, fmt.Errorf("original Pod resources changed")
	}
	if value, _, err := state.observe(); err == nil || value != nil {
		t.Fatal("lost Node-Pod commitment was replaced by cached quota")
	}
	if len(group.writes) != 0 {
		t.Fatal("read-only failed commitment issued a quota write")
	}
}

func cpuNodeFactFixture() runnercpu.NodeCommitment {
	return runnercpu.NodeCommitment{ProtocolVersion: 1, Scope: runnercpu.NodeCommitmentScope,
		LedgerOrigin: strings.Repeat("a", 32), LedgerSequence: 1, NodeResourceVersion: "1", CommittedMillicores: 2000}
}
