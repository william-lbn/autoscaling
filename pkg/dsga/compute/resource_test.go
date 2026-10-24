package compute

import (
	"context"
	"net/http"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func TestResourceIdentityDiscoveryDoesNotRewritePGBootBinding(t *testing.T) {
	request, _ := fixtures(t)
	id := resourceguard.Identity{VMUID: request.Key.Instance.VMUID, ComputeInstanceID: request.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 100}
	client, _ := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dsg/v1/resource/identity" {
			t.Fatal("unexpected resource discovery path")
		}
		reply(w, id)
	})
	got, err := client.ResourceIdentity(context.Background())
	if err != nil || got != id {
		t.Fatal("resource identity not preserved", err)
	}
	bound, _ := client.Binding()
	if bound != request.Key.Instance {
		t.Fatal("resource discovery changed PG boot")
	}
	id.GuestBootID = "not-a-real-boot"
	if _, err := client.ResourceIdentity(context.Background()); err == nil {
		t.Fatal("malformed Guest identity accepted")
	}
}
