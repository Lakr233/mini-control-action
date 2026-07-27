// Package scaler turns workflow-job events into VM lifecycles: it dedupes
// queued signals from the webhook and poller, bounds concurrency, and runs a
// per-job state machine that is guaranteed to end in VM deletion.
package scaler

import (
	"context"
	"log/slog"
	"sync"

	"github.com/Lakr233/mini-control-action/internal/bootstrap"
	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/state"
)

type Scaler struct {
	cfg   *config.Config
	mc    *minicontrol.Client
	gh    *githubapp.Client
	store *state.Store
	log   *slog.Logger

	ctx context.Context
	sem chan struct{}
	wg  sync.WaitGroup

	mu     sync.Mutex
	active map[state.Key]chan githubapp.JobEvent

	urlMu  sync.Mutex
	urlVal string
}

func New(ctx context.Context, cfg *config.Config, mc *minicontrol.Client, gh *githubapp.Client, store *state.Store, log *slog.Logger) *Scaler {
	return &Scaler{
		cfg:    cfg,
		mc:     mc,
		gh:     gh,
		store:  store,
		log:    log,
		ctx:    ctx,
		sem:    make(chan struct{}, cfg.Limits.MaxConcurrentVMs),
		active: map[state.Key]chan githubapp.JobEvent{},
	}
}

// Submit routes an event from the webhook or poller. Safe for concurrent
// use. This is the single chokepoint where dedup keys are formed and where
// dispatch policy lives.
func (s *Scaler) Submit(ev githubapp.JobEvent) {
	if ev.Job.RunAttempt < 1 {
		ev.Job.RunAttempt = 1 // run_attempt is not schema-required; keep dedup keys stable
	}
	if ev.Action == "waiting" {
		// Environment-protection gate: not runnable yet; it re-enters
		// "queued" when approved. Provisioning now would idle a billed VM.
		s.log.Info("job waiting on approval; not provisioning", "job_id", ev.Job.JobID)
		return
	}
	key := state.Key{JobID: ev.Job.JobID, RunAttempt: ev.Job.RunAttempt}

	s.mu.Lock()
	ch, isActive := s.active[key]
	s.mu.Unlock()
	if isActive {
		select { // non-blocking: workers also poll, a dropped event is not fatal
		case ch <- ev:
		default:
		}
		return
	}

	switch ev.Action {
	case "queued":
		if rec, ok := s.store.Get(key); ok {
			if rec.State.Terminal() {
				return // stale redelivery of an already-handled job
			}
			s.resume(rec) // crash leftover without an active worker
			return
		}
		rec := state.Record{
			JobID:      ev.Job.JobID,
			RunAttempt: ev.Job.RunAttempt,
			Repo:       ev.Job.Repo,
			Labels:     ev.Job.Labels,
			State:      state.StateQueued,
			QueuedAt:   nowUTC(),
		}
		if err := s.store.Upsert(rec); err != nil {
			s.log.Error("persist new job record", "job_id", rec.JobID, "error", err)
			return
		}
		s.log.Info("job queued", "repo", rec.Repo, "job_id", rec.JobID, "attempt", rec.RunAttempt)
		s.spawn(rec)
	case "completed":
		// No active worker: if we still track a VM for this job, spin up a
		// worker purely to clean it up.
		if rec, ok := s.store.Get(key); ok && !rec.State.Terminal() {
			s.resume(rec)
		}
	}
}

// Resume re-attaches workers to every non-terminal record (startup recovery).
func (s *Scaler) Resume() {
	for _, rec := range s.store.List() {
		if !rec.State.Terminal() {
			s.log.Info("resuming job from state file", "job_id", rec.JobID, "state", rec.State, "vm_id", rec.VMID)
			s.resume(rec)
		}
	}
}

// Active reports whether a worker currently owns key.
func (s *Scaler) Active(key state.Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.active[key]
	return ok
}

// ResumeRecord is the reconciler's hook to re-adopt an unowned record.
func (s *Scaler) ResumeRecord(rec state.Record) { s.resume(rec) }

func (s *Scaler) resume(rec state.Record) {
	s.spawn(rec)
}

func (s *Scaler) spawn(rec state.Record) {
	key := rec.Key()
	s.mu.Lock()
	if _, exists := s.active[key]; exists {
		s.mu.Unlock()
		return
	}
	ch := make(chan githubapp.JobEvent, 8)
	s.active[key] = ch
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.active, key)
			s.mu.Unlock()
		}()
		s.runJob(s.ctx, rec, ch)
	}()
}

// Wait blocks until all job workers have returned (graceful shutdown; workers
// stop at the next safe point when the root context is cancelled — running
// VMs are intentionally NOT deleted, startup resume picks them back up).
func (s *Scaler) Wait() { s.wg.Wait() }

// downloadURL resolves the runner tarball URL, caching success but retrying
// after transient failures. For "latest" the URL comes from the release's
// own asset list, so a release whose assets are still uploading fails at
// resolve time instead of inside a billed VM.
func (s *Scaler) downloadURL(ctx context.Context) (string, error) {
	if s.cfg.Runner.PreinstalledPath != "" {
		return "", nil
	}
	s.urlMu.Lock()
	defer s.urlMu.Unlock()
	if s.urlVal != "" {
		return s.urlVal, nil
	}
	version := s.cfg.Runner.Version
	assetURL := ""
	if version == "latest" {
		v, u, err := s.gh.ResolveRunnerDownload(ctx)
		if err != nil {
			return "", err
		}
		version, assetURL = v, u
	}
	switch {
	case s.cfg.Runner.DownloadURL != "":
		s.urlVal = bootstrap.MirrorURL(s.cfg.Runner.DownloadURL, version)
	case assetURL != "":
		s.urlVal = assetURL
	default:
		s.urlVal = bootstrap.DefaultDownloadURL(version)
	}
	s.log.Info("resolved runner download", "version", version, "url", s.urlVal)
	return s.urlVal, nil
}
