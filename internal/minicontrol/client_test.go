package minicontrol_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/minicontrol/fake"
)

const testKey = "mck_test"

func newClient(t *testing.T) (*minicontrol.Client, *fake.Server) {
	t.Helper()
	srv := fake.New(testKey)
	t.Cleanup(srv.Close)
	c := minicontrol.New(srv.BaseURL(), testKey, 5*time.Second, slog.Default())
	return c, srv
}

func TestAuthAndSKUs(t *testing.T) {
	c, srv := newClient(t)
	skus, err := c.ListSKUs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(skus) != 2 || skus[0].ID != "m4-big-v1" {
		t.Fatalf("unexpected skus: %+v", skus)
	}

	bad := minicontrol.New(srv.BaseURL(), "mck_wrong", 5*time.Second, slog.Default())
	if _, err := bad.ListSKUs(context.Background()); err == nil {
		t.Fatal("expected 401 with wrong key")
	} else if ae, ok := err.(*minicontrol.APIError); !ok || ae.Status != 401 {
		t.Fatalf("want APIError 401, got %v", err)
	}
}

func TestCreateIdempotencyReplay(t *testing.T) {
	c, srv := newClient(t)
	ctx := context.Background()
	req := minicontrol.CreateVMRequest{SKU: "m4-big-v1"}
	vm1, err := c.CreateVM(ctx, req, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	vm2, err := c.CreateVM(ctx, req, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if vm1.ID != vm2.ID {
		t.Fatalf("idempotent replay returned different vm: %s vs %s", vm1.ID, vm2.ID)
	}
	if srv.CreateCount != 1 {
		t.Fatalf("server created %d vms, want 1", srv.CreateCount)
	}
	// Different body under the same key must conflict.
	if _, err := c.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: "m4-tiny-v1"}, "key-1"); err == nil {
		t.Fatal("expected 409 for idempotency mismatch")
	}
}

// A deployment that does not accept the worker tag must fail the create, not
// quietly place the VM on an arbitrary worker.
func TestCreateTagRejectionIsFatal(t *testing.T) {
	c, srv := newClient(t)
	srv.RejectTag = true
	vm, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1", Tag: "cn-east-01-test"}, "key-tag")
	if err == nil {
		t.Fatalf("tagged create succeeded against a server that rejects tags: %+v", vm)
	}
	if created, _ := srv.Counts(); created != 0 {
		t.Fatalf("server created %d vms after a rejected tag, want 0", created)
	}
}

// The tag is a placement constraint, so the response's worker must carry it,
// and a tag no worker carries must surface as the server's structured 409 —
// never as a VM somewhere else.
func TestCreateTagPlacement(t *testing.T) {
	c, srv := newClient(t)
	srv.FleetTag = "cn-east-01"
	srv.WorkerName = "zz-mini-worker-05"
	ctx := context.Background()

	vm, err := c.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: "m4-big-v1", Tag: "cn-east-01"}, "key-place")
	if err != nil {
		t.Fatalf("tagged create failed: %v", err)
	}
	if vm.WorkerName != "zz-mini-worker-05" {
		t.Fatalf("placement not reported: worker_name=%q", vm.WorkerName)
	}
	if len(vm.WorkerTags) != 1 || vm.WorkerTags[0] != "cn-east-01" {
		t.Fatalf("worker_tags = %v, want [cn-east-01]", vm.WorkerTags)
	}

	_, err = c.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: "m4-big-v1", Tag: "jp-east-01"}, "key-nomatch")
	if !minicontrol.IsCapacity(err) {
		t.Fatalf("unmatched tag error = %v, want a structured capacity 409", err)
	}
}

func TestCreate429RetryDelay(t *testing.T) {
	c, srv := newClient(t)
	srv.Inject429 = 1
	_, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-429")
	if err == nil {
		t.Fatal("expected 429")
	}
	d, ok := minicontrol.RetryDelay(err)
	if !ok || d != time.Second {
		t.Fatalf("want Retry-After 1s, got %v %v", d, ok)
	}
}

func TestDeleteGoneIsSuccess(t *testing.T) {
	c, _ := newClient(t)
	if err := c.DeleteVM(context.Background(), "never-existed"); err != nil {
		t.Fatalf("delete of missing vm should be nil, got %v", err)
	}
}

func TestVMLifecycleAndExec(t *testing.T) {
	c, srv := newClient(t)
	srv.GetsUntilReady = 2
	srv.Exec = func(vmID, cmd string) (int, string, string) { return 0, "Darwin fake 25.0\n", "" }
	ctx := context.Background()
	vm, err := c.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-lc")
	if err != nil {
		t.Fatal(err)
	}
	if vm.Status != "provisioning" {
		t.Fatalf("want provisioning, got %s", vm.Status)
	}
	var cur *minicontrol.VM
	for range 3 {
		cur, err = c.GetVM(ctx, vm.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if cur.Status != "ready" || cur.Username == "" || cur.Password == "" {
		t.Fatalf("want ready with creds, got %+v", cur)
	}
	res, err := c.Exec(ctx, vm.ID, minicontrol.ExecRequest{Command: "uname -a"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 0 || res.Stdout == "" {
		t.Fatalf("unexpected exec result: %+v", res)
	}
	if err := c.DeleteVM(ctx, vm.ID); err != nil {
		t.Fatal(err)
	}
	if len(srv.VMIDs()) != 0 {
		t.Fatal("vm still present after delete")
	}
}

func TestCapacityError(t *testing.T) {
	c, srv := newClient(t)
	srv.CapacityReason = "no_slots_available"
	_, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-cap")
	if !minicontrol.IsCapacity(err) {
		t.Fatalf("want capacity error, got %v", err)
	}
}

func TestCreateValidatesIdempotencyKeyLength(t *testing.T) {
	c, srv := newClient(t)
	if _, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, ""); err == nil {
		t.Fatal("empty idempotency key accepted")
	}
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	if _, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, string(long)); err == nil {
		t.Fatal("129-byte idempotency key accepted")
	}
	if creates, _ := srv.Counts(); creates != 0 {
		t.Fatal("invalid keys reached the server")
	}
}

// The server commits capacity rejections under the idempotency key and
// replays them even after capacity frees up — the reason the scaler must
// mint a fresh key per capacity retry.
func TestCapacity409ReplayIsSticky(t *testing.T) {
	c, srv := newClient(t)
	srv.SetCapacityReason("no_slots_available")
	_, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-sticky")
	if !minicontrol.IsCapacity(err) {
		t.Fatalf("want capacity 409, got %v", err)
	}
	srv.SetCapacityReason("")
	_, err = c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-sticky")
	if !minicontrol.IsCapacity(err) {
		t.Fatalf("replayed key should still return the stored 409, got %v", err)
	}
	if _, err := c.CreateVM(context.Background(), minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, "key-fresh"); err != nil {
		t.Fatalf("fresh key should succeed once capacity is back: %v", err)
	}
}
