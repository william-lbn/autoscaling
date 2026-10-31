//go:build linux && dsga_native_initial_host_channel

package resourceguard

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

// DEV-only：实际固定ISO/sysfs字符端口/生产producer+exchange。
// 止于初始完成；不调用正式Provision/CheckGuest，不吞其失败或声明资格。
func TestNativeFixedInitialGuestExchange(t *testing.T) {
	if os.Getenv("DSGA_FIXED_INITIAL_GUEST_NATIVE") != "isolated369-fixed-v1" || os.Getuid() != 0 || os.Geteuid() != 0 {
		t.Fatal("explicit isolated Root Guest required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	source, raw, assets, e := initialGuestChannelInputs()
	if e != nil {
		t.Fatal(e)
	}
	var port *initialGuestStream
	for {
		port, e = openInitialGuestPort(ctx)
		if e == nil {
			break
		}
		if !errors.Is(e, errInitialGuestPortPending) {
			t.Fatal(e)
		}
		if e = initialGuestWait(ctx); e != nil {
			t.Fatal(e)
		}
	}
	defer port.close()
	challenge, e := bootprotocol.ReadInitialPrebootChallenge(port)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = initialPrebootBindInputs(challenge, raw, assets, source.Config); e != nil {
		t.Fatal(e)
	}
	g, e := initialGuestProvisionInputs(source, raw)
	if e != nil {
		t.Fatal(e)
	}
	if e = initialGuestExchange(ctx, port, g, challenge, false); e != nil {
		t.Fatal(e)
	}
	names, e := os.ReadDir(source.Config.StateDir)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	var intent initialPrebootGuestIntent
	var complete initialPrebootGuestComplete
	for _, n := range names {
		if !n.Type().IsRegular() || filepath.Ext(n.Name()) != ".json" {
			continue
		}
		b, e := os.ReadFile(filepath.Join(source.Config.StateDir, n.Name()))
		if e != nil {
			t.Fatal(e)
		}
		if n.Name() == "state.json" {
			t.Fatal("formal resource journal was created")
		}
		if n.Name() == "initial-preboot-guest.intent.json" {
			if e = json.Unmarshal(b, &intent); e != nil {
				t.Fatal(e)
			}
		}
		if n.Name() == "initial-preboot-guest.complete.json" {
			if e = json.Unmarshal(b, &complete); e != nil {
				t.Fatal(e)
			}
		}
		fmt.Printf("INITIAL_FIXED_GUEST_WAL=%s:%s:%s\n", n.Name(), physicalDigest(b), base64.StdEncoding.EncodeToString(b))
		count++
	}
	if count != 8 {
		t.Fatal("actual initial WAL file count", count)
	}
	prepared, e := initialPrebootOriginalReceipt(intent, complete)
	if e != nil {
		t.Fatal(e)
	}
	observation, e := g.InspectInitialPrebootMovable(ctx, prepared)
	if e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(observation)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Printf("INITIAL_FIXED_GUEST_PHYSICAL=%s:%s\n", physicalDigest(b), base64.StdEncoding.EncodeToString(b))
	fmt.Println("INITIAL_FIXED_GUEST_POST_BOOTSTRAP_NOT_RUN=1")
	time.Sleep(3 * time.Second)
}
