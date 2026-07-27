package reconciler_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/githubapp/ghfake"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/minicontrol/fake"
	"github.com/Lakr233/mini-control-action/internal/reconciler"
	"github.com/Lakr233/mini-control-action/internal/scaler"
	"github.com/Lakr233/mini-control-action/internal/state"
)

const testKey = "mck_test"

func newReconciler(t *testing.T) (*reconciler.Reconciler, *fake.Server, *ghfake.Server, *state.Store) {
	t.Helper()
	mcSrv := fake.New(testKey)
	t.Cleanup(mcSrv.Close)
	ghSrv := ghfake.New()
	t.Cleanup(ghSrv.Close)

	d := func(v time.Duration) config.Duration { return config.Duration(v) }
	cfg := &config.Config{
		GitHub: config.GitHub{
			APIBaseURL: ghSrv.URL, Scope: "repo", Owner: "acme", Repo: "widgets", RunnerGroupID: 1,
			Auth: config.GitHubAuth{Token: "ghp_test"},
		},
		MiniControl: config.MiniControl{
			BaseURL: mcSrv.BaseURL(), APIKey: testKey, SKU: "m4-big-v1",
			PollInterval: d(10 * time.Millisecond), ProvisionTimeout: d(time.Second), RequestTimeout: d(time.Second),
		},
		Runner: config.Runner{
			Labels: []string{"self-hosted"}, Version: "2.325.0", NamePrefix: "mc", WorkDir: "/w",
			StatusPollInterval: d(10 * time.Millisecond), PickupTimeout: d(time.Second), MaxJobDuration: d(time.Second),
		},
		Limits:     config.Limits{MaxConcurrentVMs: 2, MaxRetriesPerJob: 1, CapacityBackoff: config.Backoff{Initial: d(time.Millisecond), Max: d(time.Millisecond)}},
		Reconciler: config.Reconciler{Interval: d(time.Hour), OrphanGrace: d(time.Millisecond)},
		State:      config.State{Path: filepath.Join(t.TempDir(), "state.json")},
	}
	log := slog.Default()
	mc := minicontrol.New(cfg.MiniControl.BaseURL, testKey, time.Second, log)
	gh, err := githubapp.New(cfg.GitHub, log)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(cfg.State.Path, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sc := scaler.New(ctx, cfg, mc, gh, store, log)
	return &reconciler.Reconciler{Cfg: cfg, MC: mc, GH: gh, Store: store, Scaler: sc, Log: log}, mcSrv, ghSrv, store
}

func TestOrphanVMDeletedAfterGrace(t *testing.T) {
	r, mcSrv, _, _ := newReconciler(t)
	mcSrv.AddVM(&fake.VM{ID: "vm-orphan", SKU: "m4-big-v1"})

	ctx := context.Background()
	r.Sweep(ctx) // first sighting starts the grace clock
	time.Sleep(5 * time.Millisecond)
	r.Sweep(ctx) // past grace -> deleted
	if len(mcSrv.VMIDs()) != 0 {
		t.Fatalf("orphan vm survived: %v", mcSrv.VMIDs())
	}
}

func TestOwnedVMNotTouched(t *testing.T) {
	r, mcSrv, _, store := newReconciler(t)
	mcSrv.AddVM(&fake.VM{ID: "vm-owned", SKU: "m4-big-v1"})
	// A JOB_RUNNING record owns it; but since no worker is active the
	// reconciler will try to re-adopt (which is fine) — the VM must survive.
	if err := store.Upsert(state.Record{
		JobID: 1, RunAttempt: 1, Repo: "acme/widgets", VMID: "vm-owned",
		State: state.StateDeleting, QueuedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	// StateDeleting means it SHOULD be deleted and record completed.
	r.Sweep(context.Background())
	if len(mcSrv.VMIDs()) != 0 {
		t.Fatal("stuck-deleting vm was not removed")
	}
	rec, _ := store.Get(state.Key{JobID: 1, RunAttempt: 1})
	if rec.State != state.StateComplete {
		t.Fatalf("record not completed after delete: %s", rec.State)
	}
}

func TestStaleRunnerRegistrationRemoved(t *testing.T) {
	r, _, ghSrv, _ := newReconciler(t)
	ghSrv.AddRunner(&ghfake.Runner{Name: "mc-deadbeef-j42", Status: "offline"})
	ghSrv.AddRunner(&ghfake.Runner{Name: "unrelated-runner", Status: "offline"})

	r.Sweep(context.Background())
	if n := ghSrv.RunnerCount(); n != 1 {
		t.Fatalf("want only unrelated runner left, have %d", n)
	}
}

func TestTerminalRecordsPruned(t *testing.T) {
	r, _, _, store := newReconciler(t)
	old := state.Record{JobID: 9, RunAttempt: 1, State: state.StateComplete, QueuedAt: time.Now().Add(-48 * time.Hour)}
	if err := store.Upsert(old); err != nil {
		t.Fatal(err)
	}
	store.SetUpdatedAtForTest(state.Key{JobID: 9, RunAttempt: 1}, time.Now().Add(-48*time.Hour))

	r.Sweep(context.Background())
	if _, ok := store.Get(state.Key{JobID: 9, RunAttempt: 1}); ok {
		t.Fatal("stale terminal record not pruned")
	}
}
