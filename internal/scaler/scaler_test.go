package scaler_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/githubapp/ghfake"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/minicontrol/fake"
	"github.com/Lakr233/mini-control-action/internal/scaler"
	"github.com/Lakr233/mini-control-action/internal/state"
)

const testKey = "mck_test"

func testConfig(t *testing.T, mcURL, ghURL string) *config.Config {
	t.Helper()
	d := func(v time.Duration) config.Duration { return config.Duration(v) }
	return &config.Config{
		Log:    config.Log{Level: "debug", Format: "text"},
		Server: config.Server{Listen: ":0"},
		GitHub: config.GitHub{
			APIBaseURL: ghURL, Scope: "repo", Owner: "acme", Repo: "widgets", RunnerGroupID: 1,
			Auth: config.GitHubAuth{Token: "ghp_test"},
			Poll: config.Poll{Interval: d(time.Minute)},
		},
		MiniControl: config.MiniControl{
			BaseURL: mcURL, APIKey: testKey, SKU: "m4-big-v1",
			PollInterval: d(10 * time.Millisecond), ProvisionTimeout: d(5 * time.Second), RequestTimeout: d(5 * time.Second),
		},
		Runner: config.Runner{
			Labels: []string{"self-hosted", "macos", "arm64", "mini-control"}, Version: "2.325.0",
			NamePrefix: "mc", WorkDir: "/Users/admin/_work",
			StatusPollInterval: d(15 * time.Millisecond), PickupTimeout: d(10 * time.Second), MaxJobDuration: d(10 * time.Second),
		},
		Limits: config.Limits{
			MaxConcurrentVMs: 2, MaxRetriesPerJob: 1,
			CapacityBackoff: config.Backoff{Initial: d(10 * time.Millisecond), Max: d(50 * time.Millisecond)},
		},
		Reconciler: config.Reconciler{Interval: d(50 * time.Millisecond), OrphanGrace: d(time.Millisecond)},
		State:      config.State{Path: filepath.Join(t.TempDir(), "state.json")},
	}
}

// vmExec simulates the in-VM bootstrap protocol: empty status until launched,
// then a configurable status payload.
type vmExec struct {
	mu       sync.Mutex
	launched map[string]bool
	status   string
}

func (v *vmExec) fn(vmID, cmd string) (int, string, string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case strings.Contains(cmd, "base64 -d"):
		return 0, "staged", ""
	case strings.Contains(cmd, "nohup"):
		v.launched[vmID] = true
		return 0, "launched", ""
	case strings.Contains(cmd, "mcra-status"):
		if !v.launched[vmID] {
			return 0, "\n---MCRA-LOG---\n", ""
		}
		return 0, v.status, ""
	}
	return 0, "", ""
}

func (v *vmExec) setStatus(s string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.status = s
}

type harness struct {
	mc    *fake.Server
	gh    *ghfake.Server
	store *state.Store
	sc    *scaler.Scaler
	exec  *vmExec
	cfg   *config.Config
}

// newHarness wires the scaler to both fakes. opts run after the servers exist
// but before the scaler starts, so knobs and config can be adjusted without
// racing the FSM's goroutines.
func newHarness(t *testing.T, opts ...func(*config.Config, *fake.Server)) *harness {
	t.Helper()
	mcSrv := fake.New(testKey)
	t.Cleanup(mcSrv.Close)
	ghSrv := ghfake.New()
	t.Cleanup(ghSrv.Close)

	exec := &vmExec{launched: map[string]bool{}, status: "PHASE=run\n---MCRA-LOG---\nListening for Jobs"}
	mcSrv.Exec = exec.fn
	mcSrv.GetsUntilReady = 1

	cfg := testConfig(t, mcSrv.BaseURL(), ghSrv.URL)
	for _, opt := range opts {
		opt(cfg, mcSrv)
	}
	log := slog.Default()
	mc := minicontrol.New(cfg.MiniControl.BaseURL, cfg.MiniControl.APIKey, cfg.MiniControl.RequestTimeout.D(), log)
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
	return &harness{mc: mcSrv, gh: ghSrv, store: store, sc: sc, exec: exec, cfg: cfg}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func queuedEvent(jobID int64) githubapp.JobEvent {
	return githubapp.JobEvent{
		Action: "queued",
		Job: githubapp.JobRef{
			Repo: "acme/widgets", JobID: jobID, RunID: jobID * 10, RunAttempt: 1,
			Labels: []string{"self-hosted", "macos", "arm64", "mini-control"},
		},
	}
}

func TestHappyPathWebhookCompletion(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 10, RunID: 100, RunAttempt: 1, Status: "queued"})

	h.sc.Submit(queuedEvent(10))
	h.sc.Submit(queuedEvent(10)) // redelivery must not create a second VM

	key := state.Key{JobID: 10, RunAttempt: 1}
	waitFor(t, "runner up", 5*time.Second, func() bool {
		rec, ok := h.store.Get(key)
		return ok && (rec.State == state.StateRunnerUp || rec.State == state.StateJobRunning)
	})
	if creates, _ := h.mc.Counts(); creates != 1 {
		t.Fatalf("created %d vms, want 1 (dedup failed)", creates)
	}
	if n := h.gh.JITCount(); n != 1 {
		t.Fatalf("JIT config generated %d times, want 1", n)
	}

	// Job completes via webhook signal.
	ev := queuedEvent(10)
	ev.Action = "completed"
	ev.Conclusion = "success"
	h.sc.Submit(ev)

	waitFor(t, "record complete", 5*time.Second, func() bool {
		rec, _ := h.store.Get(key)
		return rec.State == state.StateComplete
	})
	if n := len(h.mc.VMIDs()); n != 0 {
		t.Fatalf("%d vms still alive after completion", n)
	}

	// Late redelivery of queued must not resurrect the job.
	h.sc.Submit(queuedEvent(10))
	time.Sleep(50 * time.Millisecond)
	if creates, _ := h.mc.Counts(); creates != 1 {
		t.Fatalf("stale queued redelivery created a vm (count=%d)", creates)
	}
}

func TestMarkerCompletionPath(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 20, RunID: 200, RunAttempt: 1, Status: "queued"})
	h.sc.Submit(queuedEvent(20))

	key := state.Key{JobID: 20, RunAttempt: 1}
	waitFor(t, "runner up", 5*time.Second, func() bool {
		rec, ok := h.store.Get(key)
		return ok && rec.State == state.StateRunnerUp
	})
	// The job completes on GitHub and the ephemeral runner exits cleanly.
	h.gh.SetJob(&ghfake.Job{ID: 20, RunID: 200, RunAttempt: 1, Status: "completed", Conclusion: "success"})
	h.exec.setStatus("PHASE=done\nEXIT=0\n---MCRA-LOG---\nJob done")

	waitFor(t, "record complete", 5*time.Second, func() bool {
		rec, _ := h.store.Get(key)
		return rec.State == state.StateComplete
	})
	if n := len(h.mc.VMIDs()); n != 0 {
		t.Fatalf("%d vms alive after marker completion", n)
	}
}

func TestBootstrapFailureRetriesThenFails(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 30, RunID: 300, RunAttempt: 1, Status: "queued"})
	h.exec.setStatus("PHASE=download_failed\nEXIT=1\n---MCRA-LOG---\ncurl: (6) Could not resolve host")

	h.sc.Submit(queuedEvent(30))
	key := state.Key{JobID: 30, RunAttempt: 1}
	waitFor(t, "permanent failure", 10*time.Second, func() bool {
		rec, _ := h.store.Get(key)
		return rec.State == state.StateFailed
	})
	rec, _ := h.store.Get(key)
	if rec.Retry != h.cfg.Limits.MaxRetriesPerJob {
		t.Fatalf("retries used = %d, want %d", rec.Retry, h.cfg.Limits.MaxRetriesPerJob)
	}
	if !strings.Contains(rec.LastError, "download_failed") {
		t.Fatalf("failure reason not captured: %q", rec.LastError)
	}
	// Every VM from every attempt must be gone.
	if n := len(h.mc.VMIDs()); n != 0 {
		t.Fatalf("%d vms leaked after failure", n)
	}
	if creates, _ := h.mc.Counts(); creates != h.cfg.Limits.MaxRetriesPerJob+1 {
		t.Fatalf("create count = %d, want %d", creates, h.cfg.Limits.MaxRetriesPerJob+1)
	}
}

// The configured worker tag must reach the create request — an untagged
// create is a different placement, not a harmless simplification.
func TestConfiguredWorkerTagIsSent(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config, mc *fake.Server) {
		cfg.MiniControl.WorkerTag = "cn-east-01"
		mc.FleetTag = "cn-east-01"
		mc.WorkerName = "zz-mini-worker-05"
	})
	h.gh.SetJob(&ghfake.Job{ID: 36, RunID: 360, RunAttempt: 1, Status: "queued"})

	h.sc.Submit(queuedEvent(36))
	key := state.Key{JobID: 36, RunAttempt: 1}
	waitFor(t, "vm created", 5*time.Second, func() bool {
		rec, ok := h.store.Get(key)
		return ok && rec.VMID != ""
	})
	rec, _ := h.store.Get(key)
	vm, ok := h.mc.VM(rec.VMID)
	if !ok {
		t.Fatalf("vm %s missing from server", rec.VMID)
	}
	if vm.Tag != "cn-east-01" {
		t.Fatalf("create requested tag %q, want cn-east-01", vm.Tag)
	}
}

// A configured worker tag the deployment refuses must fail the job. Placing
// the VM on an arbitrary worker instead would silently break the placement
// the operator configured.
func TestRejectedWorkerTagFailsJob(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config, mc *fake.Server) {
		cfg.MiniControl.WorkerTag = "cn-east-01-test"
		mc.RejectTag = true
	})
	h.gh.SetJob(&ghfake.Job{ID: 35, RunID: 350, RunAttempt: 1, Status: "queued"})

	h.sc.Submit(queuedEvent(35))
	key := state.Key{JobID: 35, RunAttempt: 1}
	waitFor(t, "permanent failure", 10*time.Second, func() bool {
		rec, _ := h.store.Get(key)
		return rec.State == state.StateFailed
	})
	rec, _ := h.store.Get(key)
	if !strings.Contains(rec.LastError, "create vm") {
		t.Fatalf("failure reason not captured: %q", rec.LastError)
	}
	if creates, _ := h.mc.Counts(); creates != 0 {
		t.Fatalf("created %d vms despite a rejected tag, want 0", creates)
	}
	if n := len(h.mc.VMIDs()); n != 0 {
		t.Fatalf("%d vms exist after a rejected tag", n)
	}
}

func TestCrashResumeReusesIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 40, RunID: 400, RunAttempt: 1, Status: "queued"})

	// Simulate a pre-crash state file: record persisted in CREATING with the
	// idempotency key already derived, and the create already accepted
	// server-side (the response never made it into the state file).
	rec := state.Record{
		JobID: 40, RunAttempt: 1, Repo: "acme/widgets",
		Labels: []string{"self-hosted", "macos", "arm64", "mini-control"},
		State:  state.StateCreating, QueuedAt: time.Now().UTC(),
	}
	rec.IdempotencyKey = state.NewIdemKey(rec.JobID, rec.RunAttempt, rec.Retry)
	ctx := context.Background()
	mcClient := minicontrol.New(h.cfg.MiniControl.BaseURL, testKey, time.Second, slog.Default())
	pre, err := mcClient.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: "m4-big-v1"}, rec.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.Upsert(rec); err != nil {
		t.Fatal(err)
	}

	h.sc.Resume()
	key := state.Key{JobID: 40, RunAttempt: 1}
	waitFor(t, "resume reaches runner", 5*time.Second, func() bool {
		r, _ := h.store.Get(key)
		return r.State == state.StateRunnerUp || r.State == state.StateJobRunning
	})
	got, _ := h.store.Get(key)
	if got.VMID != pre.ID {
		t.Fatalf("resume created a different vm: %s != %s", got.VMID, pre.ID)
	}
	if creates, _ := h.mc.Counts(); creates != 1 {
		t.Fatalf("idempotent resume created %d vms", creates)
	}
}

func TestCapacityBackoffThenSuccess(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 50, RunID: 500, RunAttempt: 1, Status: "queued"})
	h.mc.SetCapacityReason("no_slots_available")

	h.sc.Submit(queuedEvent(50))
	time.Sleep(60 * time.Millisecond) // let it hit capacity a few times
	h.mc.SetCapacityReason("")

	key := state.Key{JobID: 50, RunAttempt: 1}
	waitFor(t, "vm created after capacity clears", 5*time.Second, func() bool {
		rec, _ := h.store.Get(key)
		return rec.VMID != ""
	})
}

func TestSubmitClampsAttemptAndDropsWaiting(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 60, RunID: 600, RunAttempt: 1, Status: "queued"})

	waiting := queuedEvent(61)
	waiting.Action = "waiting"
	h.sc.Submit(waiting)
	time.Sleep(30 * time.Millisecond)
	if _, ok := h.store.Get(state.Key{JobID: 61, RunAttempt: 1}); ok {
		t.Fatal("waiting event must not create a record")
	}

	ev := queuedEvent(60)
	ev.Job.RunAttempt = 0 // missing run_attempt in the payload
	h.sc.Submit(ev)
	waitFor(t, "clamped record", 5*time.Second, func() bool {
		_, ok := h.store.Get(state.Key{JobID: 60, RunAttempt: 1})
		return ok
	})
}

// Capacity retries must use fresh idempotency keys: the server replays the
// stored 409 for a reused key even after capacity frees up.
func TestCapacityRetryMintsFreshKeys(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 70, RunID: 700, RunAttempt: 1, Status: "queued"})
	h.mc.SetCapacityReason("no_slots_available")

	h.sc.Submit(queuedEvent(70))
	waitFor(t, "several distinct keys under sustained capacity pressure", 5*time.Second, func() bool {
		return h.mc.IdemKeyCount() >= 3
	})
	h.mc.SetCapacityReason("")
	waitFor(t, "vm created after capacity clears", 5*time.Second, func() bool {
		rec, _ := h.store.Get(state.Key{JobID: 70, RunAttempt: 1})
		return rec.VMID != ""
	})
}

// A runner that exits after serving a SIBLING's job must not mark this
// record complete while its own job never ran.
func TestMarkerExitWhileJobStillPending(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 80, RunID: 800, RunAttempt: 1, Status: "waiting"})
	h.exec.setStatus("PHASE=done\nEXIT=0\n---MCRA-LOG---\nserved another job")

	h.sc.Submit(queuedEvent(80))
	key := state.Key{JobID: 80, RunAttempt: 1}
	waitFor(t, "terminal state", 10*time.Second, func() bool {
		rec, ok := h.store.Get(key)
		return ok && rec.State.Terminal()
	})
	rec, _ := h.store.Get(key)
	if rec.State == state.StateComplete {
		t.Fatal("record marked complete although its job never ran")
	}
	if creates, _ := h.mc.Counts(); creates < 2 {
		t.Fatalf("expected retries with fresh VMs, got %d creates", creates)
	}
	if n := len(h.mc.VMIDs()); n != 0 {
		t.Fatalf("%d vms leaked", n)
	}
}

// When this record's job finishes elsewhere but our runner is busy running
// another job, the VM must be held until the runner is idle.
func TestVMHeldWhileRunnerBusy(t *testing.T) {
	h := newHarness(t)
	h.gh.SetJob(&ghfake.Job{ID: 90, RunID: 900, RunAttempt: 1, Status: "queued"})
	h.sc.Submit(queuedEvent(90))
	key := state.Key{JobID: 90, RunAttempt: 1}
	waitFor(t, "runner up", 5*time.Second, func() bool {
		rec, ok := h.store.Get(key)
		return ok && rec.State == state.StateRunnerUp
	})
	rec, _ := h.store.Get(key)
	h.gh.SetRunnerBusy(rec.RunnerName, true)

	done := queuedEvent(90)
	done.Action = "completed"
	h.sc.Submit(done)
	time.Sleep(150 * time.Millisecond) // several ticks
	if n := len(h.mc.VMIDs()); n != 1 {
		t.Fatalf("vm released while our runner was busy (vms=%d)", n)
	}

	h.gh.SetRunnerBusy(rec.RunnerName, false)
	waitFor(t, "vm released once idle", 5*time.Second, func() bool {
		return len(h.mc.VMIDs()) == 0
	})
}

// "ready" without published SSH credentials (recovery-mode shape) must keep
// polling instead of racing into exec.
func TestReadyWithoutCredentialsWaits(t *testing.T) {
	h := newHarness(t)
	h.mc.CredsDelay = 3
	h.gh.SetJob(&ghfake.Job{ID: 95, RunID: 950, RunAttempt: 1, Status: "queued"})
	h.sc.Submit(queuedEvent(95))
	waitFor(t, "runner up despite delayed credentials", 5*time.Second, func() bool {
		rec, ok := h.store.Get(state.Key{JobID: 95, RunAttempt: 1})
		return ok && (rec.State == state.StateRunnerUp || rec.State == state.StateJobRunning)
	})
}
