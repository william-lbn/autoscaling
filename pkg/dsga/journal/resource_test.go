package journal

import (
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func TestResourceBindingSurvivesOwnerAndCannotAdoptHistory(t *testing.T) {
	m, err := New("vm-one", "1234567890abcdef1234567890abcdef", "owner-one")
	if err != nil {
		t.Fatal(err)
	}
	id := resourceguard.Identity{VMUID: "vm-one", ComputeInstanceID: "compute-one", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	forged := m.Clone()
	forged.ResourceIdentity = &id
	if forged.Validate() == nil || PristineBootstrap(forged) {
		t.Fatal("resource binding cannot appear in untouched original bootstrap")
	}
	m, err = m.BindResource(m.Owner, id)
	if err != nil {
		t.Fatal(err)
	}
	copied := m.Clone()
	copied.ResourceIdentity.CgroupInode++
	if *m.ResourceIdentity != id {
		t.Fatal("Clone exposed original resource binding")
	}
	m, err = m.Takeover(m.Owner, "owner-two")
	if err != nil || *m.ResourceIdentity != id {
		t.Fatal("takeover lost original Guest", err)
	}
	wrong := id
	wrong.GuestBootID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	if _, err := m.BindResource(m.Owner, wrong); err == nil {
		t.Fatal("new Guest replaced original")
	}
	legacy, err := New("vm-one", "1234567890abcdef1234567890abcdef", "owner-one")
	if err != nil {
		t.Fatal(err)
	}
	legacy.ExpiredResults = 1
	key := testRequest(legacy, 1, control.Grow).Key
	legacy.HighWater = &key
	if err := legacy.Validate(); err != nil {
		t.Fatal("history fixture invalid", err)
	}
	if _, err := legacy.BindResource(legacy.Owner, id); err == nil {
		t.Fatal("expired history adopted current Guest")
	}
}
